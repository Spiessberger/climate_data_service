use crate::storage::{History, StoredReading, Stream, row_to_reading};
use chrono::{
    DateTime, Datelike, Days, LocalResult, Months, NaiveDate, NaiveTime, Offset, TimeDelta,
    TimeZone, Timelike, Utc,
};
use chrono_tz::Europe::Vienna;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use serde::Serialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) const STATION_TIMEZONE: &str = "Europe/Vienna";
pub(crate) const STALE_AFTER_MS: i64 = 300_000;
pub(crate) const MAX_SUMMARY_DAYS: i64 = 366;
pub(crate) const MAX_SUMMARY_POINTS: usize = 600;
pub(crate) const MAX_INSTANT_DURATION_MS: i64 = (366 * 24 + 1) * 60 * 60 * 1_000;
const MAX_AGGREGATE_ROWS: usize = 2_000_000;
const AGGREGATE_TIME_BUDGET: Duration = Duration::from_secs(4);
const RAIN_COMPLETE_GAP_MS: i64 = 300_000;

#[derive(Debug)]
pub(crate) enum AggregateError {
    RowBudget,
    Timeout,
    Database(rusqlite::Error),
}

impl From<rusqlite::Error> for AggregateError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

#[derive(Serialize)]
pub(crate) struct Dashboard {
    generated_at_unix_ms: i64,
    station_timezone: &'static str,
    stale_after_ms: i64,
    latest_stored: LatestStored,
    history: HistoryExtents,
    rain_last_24_hours: Rain,
    night_temperature: NightTemperature,
}

#[derive(Serialize)]
struct LatestStored {
    weather: Option<StoredReading>,
    indoor: Option<StoredReading>,
}

#[derive(Serialize)]
struct HistoryExtents {
    weather: Option<Extent>,
    indoor: Option<Extent>,
}

#[derive(Serialize)]
struct Extent {
    first_received_at_unix_ms: i64,
    last_received_at_unix_ms: i64,
}

#[derive(Serialize)]
pub(crate) struct Rain {
    total_mm: Option<f64>,
    coverage: RainCoverage,
    excluded_transitions: usize,
    largest_gap_ms: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum RainCoverage {
    Complete,
    Partial,
    Unavailable,
}

#[derive(Serialize)]
struct NightTemperature {
    from_unix_ms: i64,
    to_unix_ms: i64,
    state: NightState,
    min_celsius: Option<f64>,
    observations: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum NightState {
    Ongoing,
    Completed,
}

#[derive(Serialize)]
pub(crate) struct WeatherSummary {
    range: SummaryRange,
    sample_count: usize,
    statistics: SummaryStatistics,
    buckets: Vec<SummaryBucket>,
    rain_grouping: RainGrouping,
    available_rain_groupings: Vec<RainGrouping>,
    rain_buckets: Vec<RainBucket>,
}

#[derive(Serialize)]
struct SummaryRange {
    mode: SummaryMode,
    from_date: String,
    through_date: String,
    from_unix_ms: i64,
    to_unix_ms: i64,
    timezone: &'static str,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum SummaryMode {
    Dates,
    Instants,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RainGrouping {
    Auto,
    Hour,
    Day,
    Week,
    Month,
}

impl RainGrouping {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "auto" => Some(Self::Auto),
            "hour" => Some(Self::Hour),
            "day" => Some(Self::Day),
            "week" => Some(Self::Week),
            "month" => Some(Self::Month),
            _ => None,
        }
    }
}

#[derive(Serialize)]
struct RainBucket {
    from_unix_ms: i64,
    to_unix_ms: i64,
    observed_through_unix_ms: i64,
    partial_period: bool,
    ongoing: bool,
    future: bool,
    rain: Rain,
}

pub(crate) struct SummaryRequest {
    mode: SummaryMode,
    pub(crate) rain_grouping: RainGrouping,
    from_date: NaiveDate,
    through_date: NaiveDate,
    from_unix_ms: i64,
    to_unix_ms: i64,
}

impl SummaryRequest {
    pub(crate) fn dates(from_date: NaiveDate, through_date: NaiveDate) -> Option<Self> {
        let days = through_date.signed_duration_since(from_date).num_days() + 1;
        if !(1..=MAX_SUMMARY_DAYS).contains(&days) {
            return None;
        }
        let after_through = through_date.checked_add_days(Days::new(1))?;
        Some(Self {
            mode: SummaryMode::Dates,
            rain_grouping: RainGrouping::Auto,
            from_date,
            through_date,
            from_unix_ms: local_midnight(from_date),
            to_unix_ms: local_midnight(after_through),
        })
    }

    pub(crate) fn instants(from_unix_ms: i64, to_unix_ms: i64) -> Option<Self> {
        let duration = to_unix_ms.checked_sub(from_unix_ms)?;
        if !(1..=MAX_INSTANT_DURATION_MS).contains(&duration) {
            return None;
        }
        let from_date = station_date(from_unix_ms)?;
        let through_date = station_date(to_unix_ms.checked_sub(1)?)?;
        let covered_dates = through_date.signed_duration_since(from_date).num_days() + 1;
        if !(1..=MAX_SUMMARY_DAYS).contains(&covered_dates) {
            return None;
        }
        Some(Self {
            mode: SummaryMode::Instants,
            rain_grouping: RainGrouping::Auto,
            from_date,
            through_date,
            from_unix_ms,
            to_unix_ms,
        })
    }
}

#[derive(Default, Serialize)]
struct MinMaxAverage {
    min: Option<f64>,
    max: Option<f64>,
    average: Option<f64>,
}

#[derive(Default, Serialize)]
struct Average {
    average: Option<f64>,
}

#[derive(Default, Serialize)]
struct AverageMax {
    average: Option<f64>,
    max: Option<f64>,
}

#[derive(Default, Serialize)]
struct Maximum {
    max: Option<f64>,
}

#[derive(Serialize)]
struct SummaryStatistics {
    temperature_celsius: MinMaxAverage,
    rain: Rain,
    wind_speed_mps: AverageMax,
    gust_speed_mps: Maximum,
}

#[derive(Serialize)]
struct SummaryBucket {
    from_unix_ms: i64,
    to_unix_ms: i64,
    sample_count: usize,
    temperature_celsius: MinMaxAverage,
    relative_humidity_percent: Average,
    wind_speed_mps: AverageMax,
    gust_speed_mps: Maximum,
    rain: Rain,
}

// Boundaries are calendar periods in station time, clipped to the exact selected range.
// Hourly stepping uses elapsed hours so both occurrences of the autumn 02:00 are kept.
fn calendar_rain_intervals(
    request: &SummaryRequest,
    grouping: RainGrouping,
) -> Vec<(i64, i64, bool)> {
    let from = request.from_unix_ms;
    let to = request.to_unix_ms;
    let local = DateTime::from_timestamp_millis(from)
        .unwrap()
        .with_timezone(&Vienna);
    let date = local.date_naive();
    let mut start = match grouping {
        RainGrouping::Hour => {
            from - i64::from(local.minute()) * 60_000
                - i64::from(local.second()) * 1_000
                - i64::from(local.timestamp_subsec_millis())
        }
        RainGrouping::Day => local_midnight(date),
        RainGrouping::Week => local_midnight(
            date.checked_sub_days(Days::new(u64::from(date.weekday().num_days_from_monday())))
                .unwrap_or(date),
        ),
        RainGrouping::Month => local_midnight(date.with_day(1).unwrap()),
        RainGrouping::Auto => unreachable!("resolve automatic grouping first"),
    };
    let mut intervals = Vec::new();
    while start < to && intervals.len() <= MAX_SUMMARY_POINTS {
        let date = DateTime::from_timestamp_millis(start)
            .unwrap()
            .with_timezone(&Vienna)
            .date_naive();
        let end = match grouping {
            RainGrouping::Hour => start + 3_600_000,
            RainGrouping::Day => date
                .checked_add_days(Days::new(1))
                .map(local_midnight)
                .unwrap_or(to),
            RainGrouping::Week => date
                .checked_add_days(Days::new(7))
                .map(local_midnight)
                .unwrap_or(to),
            RainGrouping::Month => date
                .checked_add_months(Months::new(1))
                .map(local_midnight)
                .unwrap_or(to),
            RainGrouping::Auto => unreachable!(),
        };
        intervals.push((start.max(from), end.min(to), start < from || end > to));
        start = end;
    }
    intervals
}

type RainIntervals = (RainGrouping, Vec<RainGrouping>, Vec<(i64, i64, bool)>);

fn rain_intervals(request: &SummaryRequest) -> RainIntervals {
    let days = request
        .through_date
        .signed_duration_since(request.from_date)
        .num_days()
        + 1;
    let auto = if days <= 2 {
        RainGrouping::Hour
    } else if days <= 42 {
        RainGrouping::Day
    } else if request
        .from_date
        .checked_add_months(Months::new(6))
        .is_some_and(|end| request.through_date < end)
    {
        RainGrouping::Week
    } else {
        RainGrouping::Month
    };
    let available = [
        RainGrouping::Hour,
        RainGrouping::Day,
        RainGrouping::Week,
        RainGrouping::Month,
    ]
    .into_iter()
    .filter(|grouping| calendar_rain_intervals(request, *grouping).len() <= MAX_SUMMARY_POINTS)
    .collect::<Vec<_>>();
    let grouping = if available.contains(&request.rain_grouping) {
        request.rain_grouping
    } else {
        auto
    };
    (
        grouping,
        available,
        calendar_rain_intervals(request, grouping),
    )
}

struct Budget {
    started: Instant,
    rows: usize,
}

impl Budget {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            rows: 0,
        }
    }

    fn check(&self) -> Result<(), AggregateError> {
        if self.started.elapsed() >= AGGREGATE_TIME_BUDGET {
            Err(AggregateError::Timeout)
        } else {
            Ok(())
        }
    }

    fn row(&mut self) -> Result<(), AggregateError> {
        self.rows += 1;
        if self.rows > MAX_AGGREGATE_ROWS {
            return Err(AggregateError::RowBudget);
        }
        self.check()
    }
}

#[derive(Clone)]
struct WeatherSample {
    received_at_unix_ms: i64,
    station_id: u8,
    temperature_celsius: Option<f64>,
    relative_humidity_percent: Option<u8>,
    wind_speed_mps: Option<f64>,
    gust_speed_mps: Option<f64>,
    rain_mm: f64,
}

#[derive(Default)]
struct RainAccumulator {
    total_mm: f64,
    comparable_pairs: usize,
    excluded_transitions: usize,
    largest_gap_ms: Option<i64>,
    incomplete: bool,
    last_observation_ms: Option<i64>,
}

impl RainAccumulator {
    fn observe(&mut self, previous: Option<&WeatherSample>, current: &WeatherSample) {
        self.last_observation_ms = Some(current.received_at_unix_ms);
        let Some(previous) = previous else {
            self.incomplete = true;
            return;
        };
        let gap = current.received_at_unix_ms - previous.received_at_unix_ms;
        self.largest_gap_ms = Some(self.largest_gap_ms.map_or(gap, |largest| largest.max(gap)));
        if gap > RAIN_COMPLETE_GAP_MS {
            self.incomplete = true;
        }
        let delta = current.rain_mm - previous.rain_mm;
        if current.station_id != previous.station_id || delta < 0.0 {
            self.excluded_transitions += 1;
            self.incomplete = true;
        } else {
            self.total_mm += delta;
            self.comparable_pairs += 1;
        }
    }

    fn finish(mut self, to_unix_ms: i64) -> Rain {
        if let Some(last) = self.last_observation_ms {
            let trailing_gap = to_unix_ms - last;
            if trailing_gap > RAIN_COMPLETE_GAP_MS {
                self.incomplete = true;
                self.largest_gap_ms = Some(
                    self.largest_gap_ms
                        .map_or(trailing_gap, |largest| largest.max(trailing_gap)),
                );
            }
        }
        if self.comparable_pairs == 0 {
            Rain {
                total_mm: None,
                coverage: RainCoverage::Unavailable,
                excluded_transitions: self.excluded_transitions,
                largest_gap_ms: self.largest_gap_ms,
            }
        } else {
            Rain {
                total_mm: Some(self.total_mm),
                coverage: if self.incomplete {
                    RainCoverage::Partial
                } else {
                    RainCoverage::Complete
                },
                excluded_transitions: self.excluded_transitions,
                largest_gap_ms: self.largest_gap_ms,
            }
        }
    }
}

#[derive(Default)]
struct Values {
    count: usize,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}

impl Values {
    fn add(&mut self, value: Option<f64>) {
        let Some(value) = value else { return };
        self.count += 1;
        self.sum += value;
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
    }

    fn average(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }

    fn min_max_average(self) -> MinMaxAverage {
        let average = self.average();
        MinMaxAverage {
            min: self.min,
            max: self.max,
            average,
        }
    }

    fn average_max(self) -> AverageMax {
        let average = self.average();
        AverageMax {
            average,
            max: self.max,
        }
    }
}

#[derive(Default)]
struct BucketAccumulator {
    sample_count: usize,
    temperature: Values,
    humidity: Values,
    wind: Values,
    gust: Values,
    rain: RainAccumulator,
}

impl History {
    pub(crate) fn dashboard(&self, now: i64) -> Result<Dashboard, AggregateError> {
        let mut connection = read_connection(&self.path)?;
        let transaction = connection.transaction()?;
        let mut budget = Budget::new();
        // The first read establishes the snapshot used by every dashboard query.
        let latest_weather = latest(&transaction, Stream::Weather)?;
        budget.check()?;
        let latest_indoor = latest(&transaction, Stream::Indoor)?;
        let weather_extent = extent(&transaction, "weather_readings")?;
        let indoor_extent = extent(&transaction, "indoor_readings")?;
        let rain_from = now.saturating_sub(24 * 60 * 60 * 1_000);
        let rain_predecessor = weather_predecessor(&transaction, rain_from)?;
        let mut rain = RainAccumulator::default();
        let mut previous = rain_predecessor;
        scan_weather(&transaction, rain_from, now, &mut budget, |sample| {
            rain.observe(previous.as_ref(), sample);
            previous = Some(sample.clone());
        })?;
        let (night_from, night_to, night_state) = night_range(now);
        let mut minimum: Option<f64> = None;
        let mut observations = 0;
        scan_weather(&transaction, night_from, night_to, &mut budget, |sample| {
            if let Some(value) = sample.temperature_celsius {
                observations += 1;
                minimum = Some(minimum.map_or(value, |current| current.min(value)));
            }
        })?;
        transaction.commit()?;
        Ok(Dashboard {
            generated_at_unix_ms: now,
            station_timezone: STATION_TIMEZONE,
            stale_after_ms: STALE_AFTER_MS,
            latest_stored: LatestStored {
                weather: latest_weather,
                indoor: latest_indoor,
            },
            history: HistoryExtents {
                weather: weather_extent,
                indoor: indoor_extent,
            },
            rain_last_24_hours: rain.finish(now),
            night_temperature: NightTemperature {
                from_unix_ms: night_from,
                to_unix_ms: night_to,
                state: night_state,
                min_celsius: minimum,
                observations,
            },
        })
    }

    pub(crate) fn weather_summary(
        &self,
        request: SummaryRequest,
        max_points: usize,
    ) -> Result<WeatherSummary, AggregateError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let (rain_grouping, available_rain_groupings, rain_intervals) = rain_intervals(&request);
        let mut rainfall = rain_intervals
            .iter()
            .map(|_| RainAccumulator::default())
            .collect::<Vec<_>>();
        let mut rain_index = 0;
        let SummaryRequest {
            mode,
            rain_grouping: _,
            from_date,
            through_date,
            from_unix_ms,
            to_unix_ms,
        } = request;
        let mut connection = read_connection(&self.path)?;
        let transaction = connection.transaction()?;
        let mut budget = Budget::new();
        // Establish one read snapshot before loading the predecessor and range.
        let predecessor = weather_predecessor(&transaction, from_unix_ms)?;
        let mut overall = BucketAccumulator::default();
        let duration = to_unix_ms - from_unix_ms;
        let bucket_count = if duration < max_points as i64 {
            duration as usize
        } else {
            max_points
        };
        let mut buckets = (0..bucket_count)
            .map(|_| BucketAccumulator::default())
            .collect::<Vec<_>>();
        let mut previous = predecessor;
        scan_weather(
            &transaction,
            from_unix_ms,
            to_unix_ms,
            &mut budget,
            |sample| {
                overall.sample_count += 1;
                add_values(&mut overall, sample);
                overall.rain.observe(previous.as_ref(), sample);
                let offset = sample.received_at_unix_ms - from_unix_ms;
                let index = ((((offset + 1) as i128 * bucket_count as i128) - 1) / duration as i128)
                    as usize;
                let bucket = &mut buckets[index.min(bucket_count - 1)];
                bucket.sample_count += 1;
                add_values(bucket, sample);
                bucket.rain.observe(previous.as_ref(), sample);
                while rain_index + 1 < rain_intervals.len()
                    && sample.received_at_unix_ms >= rain_intervals[rain_index].1
                {
                    rain_index += 1;
                }
                rainfall[rain_index].observe(previous.as_ref(), sample);
                previous = Some(sample.clone());
            },
        )?;
        transaction.commit()?;

        let sample_count = overall.sample_count;
        let statistics = SummaryStatistics {
            temperature_celsius: overall.temperature.min_max_average(),
            rain: overall.rain.finish(to_unix_ms),
            wind_speed_mps: overall.wind.average_max(),
            gust_speed_mps: Maximum {
                max: overall.gust.max,
            },
        };
        let buckets = buckets
            .into_iter()
            .enumerate()
            .map(|(index, bucket)| SummaryBucket {
                from_unix_ms: bucket_boundary(from_unix_ms, duration, index, bucket_count),
                to_unix_ms: bucket_boundary(from_unix_ms, duration, index + 1, bucket_count),
                sample_count: bucket.sample_count,
                temperature_celsius: bucket.temperature.min_max_average(),
                relative_humidity_percent: Average {
                    average: bucket.humidity.average(),
                },
                wind_speed_mps: bucket.wind.average_max(),
                gust_speed_mps: Maximum {
                    max: bucket.gust.max,
                },
                rain: bucket.rain.finish(bucket_boundary(
                    from_unix_ms,
                    duration,
                    index + 1,
                    bucket_count,
                )),
            })
            .collect();
        Ok(WeatherSummary {
            range: SummaryRange {
                mode,
                from_date: from_date.to_string(),
                through_date: through_date.to_string(),
                from_unix_ms,
                to_unix_ms,
                timezone: STATION_TIMEZONE,
            },
            sample_count,
            statistics,
            buckets,
            rain_grouping,
            available_rain_groupings,
            rain_buckets: rain_intervals
                .into_iter()
                .zip(rainfall)
                .map(|((from, to, partial), rain)| {
                    let observed_through = now.clamp(from, to);
                    RainBucket {
                        from_unix_ms: from,
                        to_unix_ms: to,
                        observed_through_unix_ms: observed_through,
                        partial_period: partial,
                        ongoing: from <= now && now < to,
                        future: now < from,
                        rain: rain.finish(observed_through),
                    }
                })
                .collect(),
        })
    }
}

fn read_connection(path: &std::path::Path) -> rusqlite::Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(Duration::from_millis(100))?;
    Ok(connection)
}

fn latest(
    transaction: &Transaction<'_>,
    stream: Stream,
) -> rusqlite::Result<Option<StoredReading>> {
    let sql = format!(
        "SELECT {} FROM {} ORDER BY received_at_unix_ms DESC, id DESC LIMIT 1",
        stream.columns(),
        stream.table()
    );
    transaction
        .query_row(&sql, [], |row| row_to_reading(row, stream))
        .optional()
}

fn extent(transaction: &Transaction<'_>, table: &str) -> rusqlite::Result<Option<Extent>> {
    let first_sql =
        format!("SELECT received_at_unix_ms FROM {table} ORDER BY received_at_unix_ms, id LIMIT 1");
    let last_sql = format!(
        "SELECT received_at_unix_ms FROM {table} ORDER BY received_at_unix_ms DESC, id DESC LIMIT 1"
    );
    let first = transaction
        .query_row(&first_sql, [], |row| row.get::<_, i64>(0))
        .optional()?;
    let last = transaction
        .query_row(&last_sql, [], |row| row.get::<_, i64>(0))
        .optional()?;
    Ok(first.zip(last).map(|(first, last)| Extent {
        first_received_at_unix_ms: first,
        last_received_at_unix_ms: last,
    }))
}

fn weather_predecessor(
    transaction: &Transaction<'_>,
    from_unix_ms: i64,
) -> rusqlite::Result<Option<WeatherSample>> {
    transaction
        .query_row(
            "SELECT received_at_unix_ms, station_id, temperature_celsius,
                    relative_humidity_percent, wind_speed_mps, gust_speed_mps, rain_mm
             FROM weather_readings
             WHERE received_at_unix_ms < ?1
             ORDER BY received_at_unix_ms DESC, id DESC
             LIMIT 1",
            [from_unix_ms],
            weather_sample,
        )
        .optional()
}

fn scan_weather(
    transaction: &Transaction<'_>,
    from_unix_ms: i64,
    to_unix_ms: i64,
    budget: &mut Budget,
    mut observe: impl FnMut(&WeatherSample),
) -> Result<(), AggregateError> {
    let mut statement = transaction.prepare(
        "SELECT received_at_unix_ms, station_id, temperature_celsius,
                relative_humidity_percent, wind_speed_mps, gust_speed_mps, rain_mm
         FROM weather_readings
         WHERE received_at_unix_ms >= ?1 AND received_at_unix_ms < ?2
         ORDER BY received_at_unix_ms, id",
    )?;
    let mut rows = statement.query(params![from_unix_ms, to_unix_ms])?;
    while let Some(row) = rows.next()? {
        budget.row()?;
        let sample = weather_sample(row)?;
        observe(&sample);
    }
    Ok(())
}

fn weather_sample(row: &rusqlite::Row<'_>) -> rusqlite::Result<WeatherSample> {
    Ok(WeatherSample {
        received_at_unix_ms: row.get(0)?,
        station_id: row.get(1)?,
        temperature_celsius: row.get(2)?,
        relative_humidity_percent: row.get(3)?,
        wind_speed_mps: row.get(4)?,
        gust_speed_mps: row.get(5)?,
        rain_mm: row.get(6)?,
    })
}

fn add_values(accumulator: &mut BucketAccumulator, sample: &WeatherSample) {
    accumulator.temperature.add(sample.temperature_celsius);
    accumulator
        .humidity
        .add(sample.relative_humidity_percent.map(f64::from));
    accumulator.wind.add(sample.wind_speed_mps);
    accumulator.gust.add(sample.gust_speed_mps);
}

fn night_range(now_unix_ms: i64) -> (i64, i64, NightState) {
    let now = DateTime::<Utc>::from_timestamp_millis(now_unix_ms)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .with_timezone(&Vienna);
    let date = now.date_naive();
    let time = now.time();
    let six = NaiveTime::from_hms_opt(6, 0, 0).unwrap();
    let eighteen = NaiveTime::from_hms_opt(18, 0, 0).unwrap();
    if time >= eighteen {
        (local_time(date, eighteen), now_unix_ms, NightState::Ongoing)
    } else if time < six {
        (
            local_time(date.checked_sub_days(Days::new(1)).unwrap(), eighteen),
            now_unix_ms,
            NightState::Ongoing,
        )
    } else {
        (
            local_time(date.checked_sub_days(Days::new(1)).unwrap(), eighteen),
            local_time(date, six),
            NightState::Completed,
        )
    }
}

pub(crate) fn local_midnight(date: NaiveDate) -> i64 {
    local_time(date, NaiveTime::MIN)
}

fn station_date(unix_ms: i64) -> Option<NaiveDate> {
    let utc = DateTime::<Utc>::from_timestamp_millis(unix_ms)?;
    let offset = Vienna
        .offset_from_utc_datetime(&utc.naive_utc())
        .fix()
        .local_minus_utc();
    utc.naive_utc()
        .checked_add_signed(TimeDelta::seconds(i64::from(offset)))
        .map(|local| local.date())
}

fn local_time(date: NaiveDate, time: NaiveTime) -> i64 {
    match Vienna.from_local_datetime(&date.and_time(time)) {
        LocalResult::Single(value) => value.timestamp_millis(),
        LocalResult::Ambiguous(earlier, _) => earlier.timestamp_millis(),
        LocalResult::None => unreachable!("Vienna 00:00, 06:00 and 18:00 always exist"),
    }
}

fn bucket_boundary(from: i64, duration: i64, index: usize, count: usize) -> i64 {
    from + ((duration as i128 * index as i128) / count as i128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(time: i64, station: u8, rain: f64) -> WeatherSample {
        WeatherSample {
            received_at_unix_ms: time,
            station_id: station,
            temperature_celsius: None,
            relative_humidity_percent: None,
            wind_speed_mps: None,
            gust_speed_mps: None,
            rain_mm: rain,
        }
    }

    #[test]
    fn rain_distinguishes_flat_pairs_resets_and_trailing_gaps() {
        let previous = sample(0, 1, 5.0);
        let current = sample(60_000, 1, 5.0);
        let mut flat = RainAccumulator::default();
        flat.observe(Some(&previous), &current);
        let flat = flat.finish(120_000);
        assert_eq!(flat.total_mm, Some(0.0));
        assert!(matches!(flat.coverage, RainCoverage::Complete));

        let reset = sample(60_000, 1, 1.0);
        let after_reset = sample(120_000, 1, 1.5);
        let mut partial = RainAccumulator::default();
        partial.observe(Some(&previous), &reset);
        partial.observe(Some(&reset), &after_reset);
        let partial = partial.finish(180_000);
        assert_eq!(partial.total_mm, Some(0.5));
        assert_eq!(partial.excluded_transitions, 1);
        assert!(matches!(partial.coverage, RainCoverage::Partial));

        let mut trailing = RainAccumulator::default();
        trailing.observe(Some(&previous), &current);
        let trailing = trailing.finish(400_001);
        assert!(matches!(trailing.coverage, RainCoverage::Partial));
        assert_eq!(trailing.largest_gap_ms, Some(340_001));
    }

    #[test]
    fn completed_autumn_dst_night_spans_thirteen_real_hours() {
        let date = NaiveDate::from_ymd_opt(2026, 10, 25).unwrap();
        let now = local_time(date, NaiveTime::from_hms_opt(7, 0, 0).unwrap());
        let (from, to, state) = night_range(now);
        assert!(matches!(state, NightState::Completed));
        assert_eq!(to - from, 13 * 60 * 60 * 1_000);
    }

    #[test]
    fn aggregate_budget_reports_row_and_elapsed_limits() {
        let mut rows = Budget {
            started: Instant::now(),
            rows: MAX_AGGREGATE_ROWS,
        };
        assert!(matches!(rows.row(), Err(AggregateError::RowBudget)));

        let elapsed = Budget {
            started: Instant::now() - AGGREGATE_TIME_BUDGET,
            rows: 0,
        };
        assert!(matches!(elapsed.check(), Err(AggregateError::Timeout)));
    }

    #[test]
    fn instant_ranges_reject_more_than_366_containing_station_dates() {
        let from = local_time(
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(23, 59, 0).unwrap(),
        );
        let to = local_time(
            NaiveDate::from_ymd_opt(2027, 1, 2).unwrap(),
            NaiveTime::from_hms_opt(0, 1, 0).unwrap(),
        );
        assert!(to - from < MAX_INSTANT_DURATION_MS);
        assert!(SummaryRequest::instants(from, to).is_none());
        assert!(SummaryRequest::instants(i64::MAX - 1, i64::MAX).is_none());
        let maximum_utc = DateTime::<Utc>::MAX_UTC.timestamp_millis();
        assert!(station_date(maximum_utc).is_none());
        assert!(SummaryRequest::instants(maximum_utc - 1, maximum_utc).is_none());
    }
    fn date_request(from: &str, through: &str, grouping: RainGrouping) -> SummaryRequest {
        let mut request =
            SummaryRequest::dates(from.parse().unwrap(), through.parse().unwrap()).unwrap();
        request.rain_grouping = grouping;
        request
    }

    #[test]
    fn rain_hours_and_days_follow_both_dst_transitions() {
        for (date, hours) in [("2026-03-29", 23), ("2026-10-25", 25)] {
            let request = date_request(date, date, RainGrouping::Hour);
            let (_, _, intervals) = rain_intervals(&request);
            assert_eq!(intervals.len(), hours);
            assert!(
                intervals
                    .iter()
                    .all(|(from, to, partial)| to - from == 3_600_000 && !partial)
            );
            let daily = calendar_rain_intervals(&request, RainGrouping::Day);
            assert_eq!(daily.len(), 1);
            assert_eq!(daily[0].1 - daily[0].0, hours as i64 * 3_600_000);
        }
    }

    #[test]
    fn rain_weeks_start_monday_and_months_keep_calendar_lengths() {
        let request = date_request("2026-09-30", "2026-10-06", RainGrouping::Week);
        let (_, _, intervals) = rain_intervals(&request);
        assert_eq!(intervals.len(), 2);
        let monday = local_midnight("2026-10-05".parse().unwrap());
        assert_eq!(
            intervals,
            vec![
                (request.from_unix_ms, monday, true),
                (monday, request.to_unix_ms, true)
            ]
        );
        let request = date_request("2028-02-01", "2028-03-31", RainGrouping::Month);
        let (_, _, intervals) = rain_intervals(&request);
        assert_eq!(intervals.len(), 2);
        assert_eq!(intervals[0].1 - intervals[0].0, 29 * 86_400_000);
        assert_eq!(intervals[1].1 - intervals[1].0, 31 * 86_400_000 - 3_600_000);
        assert!(intervals.iter().all(|(_, _, partial)| !partial));
    }

    #[test]
    fn rain_auto_and_oversized_hour_selection_resolve_to_bounded_buckets() {
        for (through, expected) in [
            ("2026-01-02", RainGrouping::Hour),
            ("2026-02-11", RainGrouping::Day),
            ("2026-02-12", RainGrouping::Week),
            ("2026-06-30", RainGrouping::Week),
            ("2026-07-01", RainGrouping::Month),
        ] {
            let request = date_request("2026-01-01", through, RainGrouping::Auto);
            assert_eq!(rain_intervals(&request).0, expected);
        }
        let request = date_request("2026-01-01", "2026-12-31", RainGrouping::Hour);
        let (grouping, available, intervals) = rain_intervals(&request);
        assert_eq!(grouping, RainGrouping::Month);
        assert!(!available.contains(&RainGrouping::Hour));
        assert_eq!(intervals.len(), 12);
        let request = date_request("2026-01-01", "2026-01-25", RainGrouping::Hour);
        assert_eq!(rain_intervals(&request).2.len(), 600);
    }

    #[test]
    fn rain_exact_ranges_clip_edge_periods_without_expanding_them() {
        let midnight = local_midnight("2026-10-05".parse().unwrap());
        let request = SummaryRequest::instants(midnight + 123, midnight + 3_600_456).unwrap();
        assert_eq!(
            rain_intervals(&request).2,
            vec![
                (midnight + 123, midnight + 3_600_000, true),
                (midnight + 3_600_000, midnight + 3_600_456, true),
            ]
        );
    }
}
