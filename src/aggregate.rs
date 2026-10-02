use crate::hourly::{self, HOUR_MS, HourSummary, ceil_hour, floor_hour, round_hour};
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
pub(crate) const MAX_SUMMARY_POINTS: usize = 600;
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
        if through_date < from_date || !supported_date(from_date) || !supported_date(through_date) {
            return None;
        }
        let after_through = through_date.checked_add_days(Days::new(1))?;
        Self {
            mode: SummaryMode::Dates,
            rain_grouping: RainGrouping::Auto,
            from_date,
            through_date,
            from_unix_ms: local_midnight(from_date),
            to_unix_ms: local_midnight(after_through),
        }
        .within_rain_limit()
    }

    pub(crate) fn instants(from_unix_ms: i64, to_unix_ms: i64) -> Option<Self> {
        if to_unix_ms.checked_sub(from_unix_ms)? < 1 {
            return None;
        }
        let from_date = station_date(from_unix_ms).filter(|date| supported_date(*date))?;
        let through_date =
            station_date(to_unix_ms.checked_sub(1)?).filter(|date| supported_date(*date))?;
        Self {
            mode: SummaryMode::Instants,
            rain_grouping: RainGrouping::Auto,
            from_date,
            through_date,
            from_unix_ms,
            to_unix_ms,
        }
        .within_rain_limit()
    }

    // The range length is otherwise unlimited, but some rain grouping must fit into
    // the point limit, which caps ranges at 600 calendar months.
    fn within_rain_limit(self) -> Option<Self> {
        (calendar_rain_intervals(&self, RainGrouping::Month).len() <= MAX_SUMMARY_POINTS)
            .then_some(self)
    }
}

fn supported_date(date: NaiveDate) -> bool {
    (1..=9999).contains(&date.year())
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

pub(crate) struct WeatherSample {
    received_at_unix_ms: i64,
    station_id: u8,
    pub(crate) temperature_celsius: Option<f64>,
    pub(crate) relative_humidity_percent: Option<u8>,
    pub(crate) wind_speed_mps: Option<f64>,
    pub(crate) gust_speed_mps: Option<f64>,
    rain_mm: f64,
}

impl WeatherSample {
    pub(crate) fn rain_point(&self) -> RainPoint {
        RainPoint {
            received_at_unix_ms: self.received_at_unix_ms,
            station_id: self.station_id,
            rain_mm: self.rain_mm,
        }
    }
}

/// The rain counter evidence of one reading.
#[derive(Clone, Copy)]
pub(crate) struct RainPoint {
    pub(crate) received_at_unix_ms: i64,
    pub(crate) station_id: u8,
    pub(crate) rain_mm: f64,
}

#[derive(Default)]
pub(crate) struct RainAccumulator {
    total_mm: f64,
    comparable_pairs: usize,
    excluded_transitions: usize,
    largest_gap_ms: Option<i64>,
    incomplete: bool,
    last_observation_ms: Option<i64>,
}

impl RainAccumulator {
    fn observe(&mut self, previous: Option<RainPoint>, current: RainPoint) {
        self.last_observation_ms = Some(current.received_at_unix_ms);
        match previous {
            Some(previous) => self.pair(previous, current),
            None => self.incomplete = true,
        }
    }

    pub(crate) fn pair(&mut self, previous: RainPoint, current: RainPoint) {
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

    /// Observes an hour as its first reading followed by its pre-aggregated pairs,
    /// which is equivalent to observing each of its readings in order.
    fn observe_hour(&mut self, previous: Option<RainPoint>, hour: &HourSummary) {
        self.observe(previous, hour.first);
        let pairs = &hour.rain;
        self.total_mm += pairs.total_mm;
        self.comparable_pairs += pairs.comparable_pairs;
        self.excluded_transitions += pairs.excluded_transitions;
        self.incomplete |= pairs.incomplete;
        if let Some(gap) = pairs.largest_gap_ms {
            self.largest_gap_ms = Some(self.largest_gap_ms.map_or(gap, |largest| largest.max(gap)));
        }
        self.last_observation_ms = Some(hour.last.received_at_unix_ms);
    }

    pub(crate) fn from_pairs(
        total_mm: f64,
        comparable_pairs: usize,
        excluded_transitions: usize,
        largest_gap_ms: Option<i64>,
    ) -> Self {
        Self {
            total_mm,
            comparable_pairs,
            excluded_transitions,
            largest_gap_ms,
            incomplete: excluded_transitions > 0
                || largest_gap_ms.is_some_and(|gap| gap > RAIN_COMPLETE_GAP_MS),
            last_observation_ms: None,
        }
    }

    pub(crate) fn pair_totals(&self) -> (f64, usize, usize, Option<i64>) {
        (
            self.total_mm,
            self.comparable_pairs,
            self.excluded_transitions,
            self.largest_gap_ms,
        )
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
pub(crate) struct Values {
    pub(crate) count: usize,
    pub(crate) sum: f64,
    pub(crate) min: Option<f64>,
    pub(crate) max: Option<f64>,
}

impl Values {
    pub(crate) fn add(&mut self, value: Option<f64>) {
        let Some(value) = value else { return };
        self.count += 1;
        self.sum += value;
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
    }

    fn merge(&mut self, other: &Values) {
        self.count += other.count;
        self.sum += other.sum;
        if let Some(min) = other.min {
            self.min = Some(self.min.map_or(min, |current| current.min(min)));
        }
        if let Some(max) = other.max {
            self.max = Some(self.max.map_or(max, |current| current.max(max)));
        }
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

impl BucketAccumulator {
    fn add_sample(&mut self, previous: Option<RainPoint>, sample: &WeatherSample) {
        self.sample_count += 1;
        self.temperature.add(sample.temperature_celsius);
        self.humidity
            .add(sample.relative_humidity_percent.map(f64::from));
        self.wind.add(sample.wind_speed_mps);
        self.gust.add(sample.gust_speed_mps);
        self.rain.observe(previous, sample.rain_point());
    }

    fn add_hour(&mut self, previous: Option<RainPoint>, hour: &HourSummary) {
        self.sample_count += hour.sample_count;
        self.temperature.merge(&hour.temperature);
        self.humidity.merge(&hour.humidity);
        self.wind.merge(&hour.wind);
        self.gust.merge(&hour.gust);
        self.rain.observe_hour(previous, hour);
    }
}

/// Accumulates readings, or whole hours of them, in reception order into the
/// overall statistics, chart buckets and rain periods.
struct SummaryFold {
    previous: Option<RainPoint>,
    overall: BucketAccumulator,
    boundaries: Vec<i64>,
    buckets: Vec<BucketAccumulator>,
    rain_ends: Vec<i64>,
    rainfall: Vec<RainAccumulator>,
}

impl SummaryFold {
    fn bucket(&self, time: i64) -> usize {
        self.boundaries
            .partition_point(|&boundary| boundary <= time)
            - 1
    }

    fn rain_period(&self, time: i64) -> usize {
        self.rain_ends
            .partition_point(|&end| end <= time)
            .min(self.rain_ends.len() - 1)
    }

    fn sample(&mut self, sample: &WeatherSample) {
        let time = sample.received_at_unix_ms;
        let (bucket, rain_period) = (self.bucket(time), self.rain_period(time));
        self.overall.add_sample(self.previous, sample);
        self.buckets[bucket].add_sample(self.previous, sample);
        self.rainfall[rain_period].observe(self.previous, sample.rain_point());
        self.previous = Some(sample.rain_point());
    }

    /// Whether the hour lies inside the range, one chart bucket and one rain period.
    fn takes_whole_hour(&self, hour: i64) -> bool {
        let last = hour + HOUR_MS - 1;
        self.boundaries[0] <= hour
            && last < self.boundaries[self.boundaries.len() - 1]
            && self.bucket(hour) == self.bucket(last)
            && self.rain_period(hour) == self.rain_period(last)
    }

    fn hour(&mut self, hour: &HourSummary) {
        let time = hour.hour_start_unix_ms;
        let (bucket, rain_period) = (self.bucket(time), self.rain_period(time));
        self.overall.add_hour(self.previous, hour);
        self.buckets[bucket].add_hour(self.previous, hour);
        self.rainfall[rain_period].observe_hour(self.previous, hour);
        self.previous = Some(hour.last);
    }
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
        let mut rain = RainAccumulator::default();
        let mut previous = weather_predecessor(&transaction, rain_from)?;
        scan_weather(&transaction, rain_from, now, &mut budget, |sample| {
            rain.observe(previous, sample.rain_point());
            previous = Some(sample.rain_point());
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
        self.summarize(request, max_points, true)
    }

    fn summarize(
        &self,
        request: SummaryRequest,
        max_points: usize,
        allow_hourly: bool,
    ) -> Result<WeatherSummary, AggregateError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let (rain_grouping, available_rain_groupings, rain_intervals) = rain_intervals(&request);
        let SummaryRequest {
            mode,
            rain_grouping: _,
            from_date,
            through_date,
            from_unix_ms,
            to_unix_ms,
        } = request;
        let (boundaries, hourly_buckets) = bucket_boundaries(from_unix_ms, to_unix_ms, max_points);
        let mut connection = read_connection(&self.path)?;
        let transaction = connection.transaction()?;
        let mut budget = Budget::new();
        // Establish one read snapshot before checking summary progress and loading
        // the predecessor and range.
        let use_hourly = allow_hourly && hourly_buckets && hourly::complete(&transaction)?;
        let mut fold = SummaryFold {
            previous: weather_predecessor(&transaction, from_unix_ms)?,
            overall: BucketAccumulator::default(),
            buckets: (1..boundaries.len())
                .map(|_| BucketAccumulator::default())
                .collect(),
            boundaries,
            rain_ends: rain_intervals.iter().map(|interval| interval.1).collect(),
            rainfall: rain_intervals
                .iter()
                .map(|_| RainAccumulator::default())
                .collect(),
        };
        let (hours_from, hours_to) = (ceil_hour(from_unix_ms), floor_hour(to_unix_ms));
        if use_hourly && hours_from < hours_to {
            // Partial edge hours and hours split by a bucket or rain period boundary
            // are read from raw readings; every other hour comes from its summary.
            scan_weather(
                &transaction,
                from_unix_ms,
                hours_from,
                &mut budget,
                |sample| fold.sample(sample),
            )?;
            hourly::for_each(&transaction, hours_from, hours_to, |hour| {
                budget.row()?;
                let start = hour.hour_start_unix_ms;
                if fold.takes_whole_hour(start) {
                    fold.hour(&hour);
                    Ok(())
                } else {
                    scan_weather(
                        &transaction,
                        start,
                        start + HOUR_MS,
                        &mut budget,
                        |sample| fold.sample(sample),
                    )
                }
            })?;
            scan_weather(&transaction, hours_to, to_unix_ms, &mut budget, |sample| {
                fold.sample(sample)
            })?;
        } else {
            scan_weather(
                &transaction,
                from_unix_ms,
                to_unix_ms,
                &mut budget,
                |sample| fold.sample(sample),
            )?;
        }
        transaction.commit()?;

        let SummaryFold {
            overall,
            boundaries,
            buckets,
            rainfall,
            ..
        } = fold;
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
            .zip(boundaries.windows(2))
            .map(|(bucket, bounds)| SummaryBucket {
                from_unix_ms: bounds[0],
                to_unix_ms: bounds[1],
                sample_count: bucket.sample_count,
                temperature_celsius: bucket.temperature.min_max_average(),
                relative_humidity_percent: Average {
                    average: bucket.humidity.average(),
                },
                wind_speed_mps: bucket.wind.average_max(),
                gust_speed_mps: Maximum {
                    max: bucket.gust.max,
                },
                rain: bucket.rain.finish(bounds[1]),
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

#[cfg(test)]
impl History {
    /// The summary computed from raw readings only, for comparison in tests.
    pub(crate) fn raw_weather_summary(
        &self,
        request: SummaryRequest,
        max_points: usize,
    ) -> Result<WeatherSummary, AggregateError> {
        self.summarize(request, max_points, false)
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
) -> rusqlite::Result<Option<RainPoint>> {
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
        .map(|sample| sample.map(|sample| sample.rain_point()))
}

fn scan_weather(
    transaction: &Transaction<'_>,
    from_unix_ms: i64,
    to_unix_ms: i64,
    budget: &mut Budget,
    mut observe: impl FnMut(&WeatherSample),
) -> Result<(), AggregateError> {
    weather_samples(transaction, from_unix_ms, to_unix_ms, |sample| {
        budget.row()?;
        observe(sample);
        Ok(())
    })
}

/// Visits the readings received in `[from_unix_ms, to_unix_ms)` in reception order.
pub(crate) fn weather_samples<E: From<rusqlite::Error>>(
    connection: &Connection,
    from_unix_ms: i64,
    to_unix_ms: i64,
    mut observe: impl FnMut(&WeatherSample) -> Result<(), E>,
) -> Result<(), E> {
    let mut statement = connection.prepare_cached(
        "SELECT received_at_unix_ms, station_id, temperature_celsius,
                relative_humidity_percent, wind_speed_mps, gust_speed_mps, rain_mm
         FROM weather_readings
         WHERE received_at_unix_ms >= ?1 AND received_at_unix_ms < ?2
         ORDER BY received_at_unix_ms, id",
    )?;
    let mut rows = statement.query(params![from_unix_ms, to_unix_ms])?;
    while let Some(row) = rows.next()? {
        observe(&weather_sample(row)?)?;
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

// A local time skipped by a daylight-saving gap (such as Vienna midnight on
// 1980-04-06) resolves to the end of the gap, where the local day actually began.
fn local_time(date: NaiveDate, time: NaiveTime) -> i64 {
    let local = date.and_time(time);
    (0..=24 * 60)
        .find_map(|minutes| {
            match Vienna.from_local_datetime(&(local + TimeDelta::minutes(minutes))) {
                LocalResult::Single(value) | LocalResult::Ambiguous(value, _) => {
                    Some(value.timestamp_millis())
                }
                LocalResult::None => None,
            }
        })
        .expect("Vienna daylight-saving gaps are shorter than a day")
}

/// Chart bucket boundaries, `count + 1` values from `from` through `to`. Buckets are
/// equally wide, except that buckets of at least an hour have their inner
/// boundaries rounded to whole UTC hours so they can be built from hourly summaries.
/// Returns whether the boundaries were rounded.
fn bucket_boundaries(from: i64, to: i64, max_points: usize) -> (Vec<i64>, bool) {
    let duration = to - from;
    let count = max_points.min(usize::try_from(duration).unwrap_or(usize::MAX));
    let hourly = duration / count as i64 >= HOUR_MS;
    let boundaries = (0..=count)
        .map(|index| {
            let boundary = from + ((duration as i128 * index as i128) / count as i128) as i64;
            if hourly && 0 < index && index < count {
                round_hour(boundary)
            } else {
                boundary
            }
        })
        .collect();
    (boundaries, hourly)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(time: i64, station: u8, rain: f64) -> RainPoint {
        RainPoint {
            received_at_unix_ms: time,
            station_id: station,
            rain_mm: rain,
        }
    }

    #[test]
    fn rain_distinguishes_flat_pairs_resets_and_trailing_gaps() {
        let previous = sample(0, 1, 5.0);
        let current = sample(60_000, 1, 5.0);
        let mut flat = RainAccumulator::default();
        flat.observe(Some(previous), current);
        let flat = flat.finish(120_000);
        assert_eq!(flat.total_mm, Some(0.0));
        assert!(matches!(flat.coverage, RainCoverage::Complete));

        let reset = sample(60_000, 1, 1.0);
        let after_reset = sample(120_000, 1, 1.5);
        let mut partial = RainAccumulator::default();
        partial.observe(Some(previous), reset);
        partial.observe(Some(reset), after_reset);
        let partial = partial.finish(180_000);
        assert_eq!(partial.total_mm, Some(0.5));
        assert_eq!(partial.excluded_transitions, 1);
        assert!(matches!(partial.coverage, RainCoverage::Partial));

        let mut trailing = RainAccumulator::default();
        trailing.observe(Some(previous), current);
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
    fn ranges_are_limited_only_by_monthly_rain_buckets_and_supported_years() {
        let date = |value: &str| value.parse::<NaiveDate>().unwrap();
        assert!(SummaryRequest::dates(date("2000-01-01"), date("2049-12-31")).is_some());
        assert!(SummaryRequest::dates(date("2000-01-01"), date("2050-01-01")).is_none());
        let from = local_midnight(date("2000-01-01"));
        let to = local_midnight(date("2050-01-01"));
        assert!(SummaryRequest::instants(from, to).is_some());
        assert!(SummaryRequest::instants(from, to + 1).is_none());
        assert!(SummaryRequest::dates(date("0000-12-31"), date("0001-01-01")).is_none());
        assert!(SummaryRequest::instants(i64::MAX - 1, i64::MAX).is_none());
        let maximum_utc = DateTime::<Utc>::MAX_UTC.timestamp_millis();
        assert!(station_date(maximum_utc).is_none());
        assert!(SummaryRequest::instants(maximum_utc - 1, maximum_utc).is_none());
    }

    #[test]
    fn local_midnight_skipped_by_daylight_saving_starts_at_the_gap_end() {
        // Vienna moved from 00:00 CET to 01:00 CEST on 1980-04-06.
        let midnight = local_midnight("1980-04-06".parse().unwrap());
        assert_eq!(
            DateTime::from_timestamp_millis(midnight).unwrap(),
            "1980-04-05T23:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert!(
            SummaryRequest::dates("1980-04-06".parse().unwrap(), "1980-04-06".parse().unwrap())
                .is_some()
        );
    }

    #[test]
    fn long_range_buckets_snap_inner_boundaries_to_whole_hours() {
        let from = local_midnight("2026-01-01".parse().unwrap()) + 123;
        let to = local_midnight("2026-02-01".parse().unwrap()) - 456;
        let (boundaries, hourly) = bucket_boundaries(from, to, 360);
        assert!(hourly);
        assert_eq!(boundaries.len(), 361);
        assert_eq!((boundaries[0], boundaries[360]), (from, to));
        assert!(boundaries[1..360].iter().all(|b| b % HOUR_MS == 0));
        assert!(
            boundaries
                .windows(2)
                .all(|pair| pair[1] - pair[0] >= HOUR_MS / 2)
        );
        assert!(
            boundaries[1..360]
                .windows(2)
                .all(|pair| pair[1] - pair[0] >= HOUR_MS)
        );

        let (short, hourly) = bucket_boundaries(from, from + 360 * HOUR_MS - 1, 360);
        assert!(!hourly);
        assert_eq!(short[1] - short[0], HOUR_MS - 1);
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
