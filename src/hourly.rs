//! Hourly weather summaries that let long summary ranges skip most raw readings.
//!
//! Each row describes the stored readings received in one UTC hour. Rain pairs inside
//! the hour are pre-aggregated; the first and last reading are kept so pairs across
//! hour boundaries can be evaluated when hours are combined. Rows are only trusted
//! while `weather_hourly_through_id` equals the newest weather reading id.
use crate::aggregate::{RainAccumulator, RainPoint, Values, WeatherSample, weather_samples};
use rusqlite::{Connection, OptionalExtension, Row, params};
use std::collections::BTreeSet;

pub(crate) const HOUR_MS: i64 = 3_600_000;
const VERSION: &str = "1";
const VERSION_KEY: &str = "weather_hourly_version";
const THROUGH_ID_KEY: &str = "weather_hourly_through_id";
const BACKFILL_CHUNK_ROWS: i64 = 5_000;
const COLUMNS: &str = "hour_start_unix_ms, sample_count,
    temperature_count, temperature_sum, temperature_min, temperature_max,
    humidity_count, humidity_sum, humidity_min, humidity_max,
    wind_count, wind_sum, wind_min, wind_max,
    gust_count, gust_sum, gust_min, gust_max,
    first_received_at_unix_ms, first_station_id, first_rain_mm,
    last_received_at_unix_ms, last_station_id, last_rain_mm,
    rain_total_mm, rain_comparable_pairs, rain_excluded_transitions, rain_largest_gap_ms";

pub(crate) fn floor_hour(unix_ms: i64) -> i64 {
    unix_ms.div_euclid(HOUR_MS) * HOUR_MS
}

pub(crate) fn ceil_hour(unix_ms: i64) -> i64 {
    floor_hour(unix_ms + HOUR_MS - 1)
}

pub(crate) fn round_hour(unix_ms: i64) -> i64 {
    floor_hour(unix_ms + HOUR_MS / 2)
}

pub(crate) struct HourSummary {
    pub(crate) hour_start_unix_ms: i64,
    pub(crate) sample_count: usize,
    pub(crate) temperature: Values,
    pub(crate) humidity: Values,
    pub(crate) wind: Values,
    pub(crate) gust: Values,
    pub(crate) first: RainPoint,
    pub(crate) last: RainPoint,
    /// Rain pairs between readings of this hour only.
    pub(crate) rain: RainAccumulator,
}

impl HourSummary {
    fn new(hour_start_unix_ms: i64, sample: &WeatherSample) -> Self {
        let mut summary = Self {
            hour_start_unix_ms,
            sample_count: 0,
            temperature: Values::default(),
            humidity: Values::default(),
            wind: Values::default(),
            gust: Values::default(),
            first: sample.rain_point(),
            last: sample.rain_point(),
            rain: RainAccumulator::default(),
        };
        summary.add_values(sample);
        summary
    }

    fn add(&mut self, sample: &WeatherSample) {
        self.rain.pair(self.last, sample.rain_point());
        self.last = sample.rain_point();
        self.add_values(sample);
    }

    fn add_values(&mut self, sample: &WeatherSample) {
        self.sample_count += 1;
        self.temperature.add(sample.temperature_celsius);
        self.humidity
            .add(sample.relative_humidity_percent.map(f64::from));
        self.wind.add(sample.wind_speed_mps);
        self.gust.add(sample.gust_speed_mps);
    }

    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        let count =
            |offset: usize| -> rusqlite::Result<usize> { Ok(row.get::<_, i64>(offset)? as usize) };
        let values = |offset: usize| -> rusqlite::Result<Values> {
            Ok(Values {
                count: count(offset)?,
                sum: row.get(offset + 1)?,
                min: row.get(offset + 2)?,
                max: row.get(offset + 3)?,
            })
        };
        let point = |offset: usize| -> rusqlite::Result<RainPoint> {
            Ok(RainPoint {
                received_at_unix_ms: row.get(offset)?,
                station_id: row.get(offset + 1)?,
                rain_mm: row.get(offset + 2)?,
            })
        };
        Ok(Self {
            hour_start_unix_ms: row.get(0)?,
            sample_count: count(1)?,
            temperature: values(2)?,
            humidity: values(6)?,
            wind: values(10)?,
            gust: values(14)?,
            first: point(18)?,
            last: point(21)?,
            rain: RainAccumulator::from_pairs(row.get(24)?, count(25)?, count(26)?, row.get(27)?),
        })
    }

    fn write(&self, connection: &Connection) -> rusqlite::Result<()> {
        let values = [&self.temperature, &self.humidity, &self.wind, &self.gust];
        let (total_mm, comparable_pairs, excluded_transitions, largest_gap_ms) =
            self.rain.pair_totals();
        let mut statement = connection.prepare_cached(&format!(
            "INSERT OR REPLACE INTO weather_hourly ({COLUMNS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                     ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28)"
        ))?;
        statement.execute(params![
            self.hour_start_unix_ms,
            self.sample_count as i64,
            values[0].count as i64,
            values[0].sum,
            values[0].min,
            values[0].max,
            values[1].count as i64,
            values[1].sum,
            values[1].min,
            values[1].max,
            values[2].count as i64,
            values[2].sum,
            values[2].min,
            values[2].max,
            values[3].count as i64,
            values[3].sum,
            values[3].min,
            values[3].max,
            self.first.received_at_unix_ms,
            self.first.station_id,
            self.first.rain_mm,
            self.last.received_at_unix_ms,
            self.last.station_id,
            self.last.rain_mm,
            total_mm,
            comparable_pairs as i64,
            excluded_transitions as i64,
            largest_gap_ms,
        ])?;
        Ok(())
    }
}

/// Creates the summary table. A changed summary definition, or a progress marker
/// beyond the newest reading (rows removed outside the service), discards all
/// summaries so the backfill rebuilds them.
pub(crate) fn prepare(connection: &mut Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS weather_hourly (
            hour_start_unix_ms INTEGER PRIMARY KEY,
            sample_count INTEGER NOT NULL,
            temperature_count INTEGER NOT NULL,
            temperature_sum REAL NOT NULL,
            temperature_min REAL,
            temperature_max REAL,
            humidity_count INTEGER NOT NULL,
            humidity_sum REAL NOT NULL,
            humidity_min REAL,
            humidity_max REAL,
            wind_count INTEGER NOT NULL,
            wind_sum REAL NOT NULL,
            wind_min REAL,
            wind_max REAL,
            gust_count INTEGER NOT NULL,
            gust_sum REAL NOT NULL,
            gust_min REAL,
            gust_max REAL,
            first_received_at_unix_ms INTEGER NOT NULL,
            first_station_id INTEGER NOT NULL,
            first_rain_mm REAL NOT NULL,
            last_received_at_unix_ms INTEGER NOT NULL,
            last_station_id INTEGER NOT NULL,
            last_rain_mm REAL NOT NULL,
            rain_total_mm REAL NOT NULL,
            rain_comparable_pairs INTEGER NOT NULL,
            rain_excluded_transitions INTEGER NOT NULL,
            rain_largest_gap_ms INTEGER
        ) WITHOUT ROWID;",
    )?;
    let transaction = connection.transaction()?;
    let version = metadata(&transaction, VERSION_KEY)?;
    let through_id = through_id(&transaction)?;
    let newest_id = newest_weather_id(&transaction)?;
    if version.as_deref() != Some(VERSION) || through_id.is_none_or(|id| id > newest_id) {
        transaction.execute("DELETE FROM weather_hourly", [])?;
        set_metadata(&transaction, VERSION_KEY, VERSION)?;
        set_metadata(&transaction, THROUGH_ID_KEY, "0")?;
    }
    transaction.commit()
}

/// Whether the summaries include every stored weather reading. Readers must ask
/// within the same transaction that reads the summaries.
pub(crate) fn complete(connection: &Connection) -> rusqlite::Result<bool> {
    Ok(through_id(connection)? == Some(newest_weather_id(connection)?))
}

/// Number of stored weather readings not yet included in the summaries.
pub(crate) fn pending(connection: &Connection) -> rusqlite::Result<i64> {
    let through_id = through_id(connection)?.unwrap_or(0);
    Ok((newest_weather_id(connection)? - through_id).max(0))
}

/// Folds the next chunk of stored readings into the summaries in one transaction.
/// Returns whether the summaries are complete afterwards.
pub(crate) fn backfill_step(connection: &mut Connection) -> rusqlite::Result<bool> {
    let transaction = connection.transaction()?;
    let through_id = through_id(&transaction)?.unwrap_or(0);
    let newest_id = newest_weather_id(&transaction)?;
    if through_id >= newest_id {
        return Ok(true);
    }
    let chunk_end = newest_id.min(through_id.saturating_add(BACKFILL_CHUNK_ROWS));
    let hours = {
        let mut statement = transaction.prepare_cached(
            "SELECT received_at_unix_ms FROM weather_readings WHERE id > ?1 AND id <= ?2",
        )?;
        statement
            .query_map([through_id, chunk_end], |row| row.get::<_, i64>(0))?
            .map(|time| time.map(floor_hour))
            .collect::<rusqlite::Result<BTreeSet<_>>>()?
    };
    for hour in hours {
        refresh_hour(&transaction, hour)?;
    }
    set_metadata(&transaction, THROUGH_ID_KEY, &chunk_end.to_string())?;
    transaction.commit()?;
    Ok(chunk_end >= newest_id)
}

/// Updates the summaries for a reading inserted in the caller's transaction. Only
/// call this when the summaries were complete before the insert; otherwise the
/// backfill includes the reading later.
pub(crate) fn record(
    connection: &Connection,
    id: i64,
    received_at_unix_ms: i64,
) -> rusqlite::Result<()> {
    refresh_hour(connection, floor_hour(received_at_unix_ms))?;
    set_metadata(connection, THROUGH_ID_KEY, &id.to_string())
}

/// Visits the stored summaries of hours starting in `[from_unix_ms, to_unix_ms)`.
pub(crate) fn for_each<E: From<rusqlite::Error>>(
    connection: &Connection,
    from_unix_ms: i64,
    to_unix_ms: i64,
    mut observe: impl FnMut(HourSummary) -> Result<(), E>,
) -> Result<(), E> {
    let mut statement = connection.prepare_cached(&format!(
        "SELECT {COLUMNS} FROM weather_hourly
         WHERE hour_start_unix_ms >= ?1 AND hour_start_unix_ms < ?2
         ORDER BY hour_start_unix_ms"
    ))?;
    let mut rows = statement.query([from_unix_ms, to_unix_ms])?;
    while let Some(row) = rows.next()? {
        observe(HourSummary::from_row(row)?)?;
    }
    Ok(())
}

fn refresh_hour(connection: &Connection, hour_start_unix_ms: i64) -> rusqlite::Result<()> {
    let mut summary: Option<HourSummary> = None;
    weather_samples(
        connection,
        hour_start_unix_ms,
        hour_start_unix_ms + HOUR_MS,
        |sample| {
            match summary.as_mut() {
                Some(summary) => summary.add(sample),
                None => summary = Some(HourSummary::new(hour_start_unix_ms, sample)),
            }
            Ok::<_, rusqlite::Error>(())
        },
    )?;
    match summary {
        Some(summary) => summary.write(connection),
        None => connection
            .execute(
                "DELETE FROM weather_hourly WHERE hour_start_unix_ms = ?1",
                [hour_start_unix_ms],
            )
            .map(drop),
    }
}

fn newest_weather_id(connection: &Connection) -> rusqlite::Result<i64> {
    connection.query_row(
        "SELECT COALESCE(MAX(id), 0) FROM weather_readings",
        [],
        |row| row.get(0),
    )
}

fn through_id(connection: &Connection) -> rusqlite::Result<Option<i64>> {
    Ok(metadata(connection, THROUGH_ID_KEY)?.and_then(|value| value.parse().ok()))
}

fn metadata(connection: &Connection, key: &str) -> rusqlite::Result<Option<String>> {
    connection
        .query_row(
            "SELECT value FROM service_metadata WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()
}

pub(crate) fn set_metadata(
    connection: &Connection,
    key: &str,
    value: &str,
) -> rusqlite::Result<()> {
    connection
        .prepare_cached(
            "INSERT INTO service_metadata (key, value) VALUES (?1, ?2)
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        )?
        .execute(params![key, value])
        .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{RainGrouping, SummaryRequest};
    use crate::storage::{History, open_database, store_reading};
    use crate::{ClimateReading, LiveWeatherReading};
    use serde_json::Value;

    struct Database {
        _directory: tempfile::TempDir,
        connection: Connection,
        history: History,
    }

    fn database() -> Database {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("climate.sqlite3");
        let connection = open_database(&path).unwrap();
        Database {
            _directory: directory,
            connection,
            history: History { path },
        }
    }

    fn insert(
        connection: &Connection,
        time: i64,
        station: u8,
        values: [Option<f64>; 4],
        rain: f64,
    ) {
        connection
            .execute(
                "INSERT INTO weather_readings (
                    received_at_unix_ms, v, type, boot_id, seq, station_id,
                    temperature_celsius, relative_humidity_percent, wind_speed_mps,
                    gust_speed_mps, rain_mm, battery_low, rssi_dbm, lqi
                 ) VALUES (?1, 1, 'weather', 'boot', 1, ?2, ?3, ?4, ?5, ?6, ?7, 0, -80.0, 20)",
                params![
                    time, station, values[0], values[1], values[2], values[3], rain
                ],
            )
            .unwrap();
    }

    fn store(connection: &mut Connection, time: i64, rain: f64) {
        let reading = serde_json::from_value(serde_json::json!({
            "v": 1, "type": "weather", "boot_id": "boot", "seq": 1, "station_id": 1,
            "temperature_celsius": 12.5, "relative_humidity_percent": 60,
            "wind_direction_degrees": null, "wind_speed_mps": 1.0, "gust_speed_mps": 2.0,
            "rain_mm": rain, "uv_microwatts_per_cm2": null, "uv_index": null,
            "light_lux": null, "battery_low": false, "rssi_dbm": -80.0, "lqi": 20
        }))
        .unwrap();
        let reading = ClimateReading::Weather(LiveWeatherReading {
            reading,
            received_at_unix_ms: time,
        });
        assert!(store_reading(connection, &reading).is_ok());
    }

    fn backfill(connection: &mut Connection) {
        while !backfill_step(connection).unwrap() {}
        assert!(complete(connection).unwrap());
    }

    fn assert_same(left: &Value, right: &Value, path: &str) {
        match (left, right) {
            (Value::Number(left), Value::Number(right)) => {
                let (left, right) = (left.as_f64().unwrap(), right.as_f64().unwrap());
                assert!(
                    (left - right).abs() <= 1e-9 * left.abs().max(1.0),
                    "{path}: {left} != {right}"
                );
            }
            (Value::Array(left), Value::Array(right)) => {
                assert_eq!(left.len(), right.len(), "{path}");
                for (index, (left, right)) in left.iter().zip(right).enumerate() {
                    assert_same(left, right, &format!("{path}[{index}]"));
                }
            }
            (Value::Object(left), Value::Object(right)) => {
                assert_eq!(left.len(), right.len(), "{path}");
                for (key, value) in left {
                    assert_same(value, &right[key], &format!("{path}.{key}"));
                }
            }
            _ => assert_eq!(left, right, "{path}"),
        }
    }

    fn summaries(
        history: &History,
        request: impl Fn() -> SummaryRequest,
        points: usize,
    ) -> (Value, Value) {
        let hourly = history.weather_summary(request(), points).ok().unwrap();
        let raw = history.raw_weather_summary(request(), points).ok().unwrap();
        (
            serde_json::to_value(hourly).unwrap(),
            serde_json::to_value(raw).unwrap(),
        )
    }

    fn dates(from: &str, through: &str, grouping: RainGrouping) -> SummaryRequest {
        let mut request =
            SummaryRequest::dates(from.parse().unwrap(), through.parse().unwrap()).unwrap();
        request.rain_grouping = grouping;
        request
    }

    // Irregular readings around the autumn daylight-saving change, with null values,
    // short and long gaps, counter resets, a station change, equal reception times
    // and readings stored out of reception order.
    fn irregular_readings(connection: &Connection) -> (i64, i64) {
        let start = 1_792_879_200_000; // 2026-10-24 00:00 Vienna
        let end = start + 12 * 24 * HOUR_MS;
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut random = move |limit: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % limit
        };
        let (mut time, mut rain, mut station) = (start - 40_000, 100.0, 1);
        let mut late = Vec::new();
        while time < end {
            time += match random(500) {
                0 => 3 * HOUR_MS + random(HOUR_MS as u64) as i64,
                1..=5 => 300_001 + random(600_000) as i64,
                6..=10 => 0,
                _ => 10_000 + random(30_000) as i64,
            };
            match random(2_000) {
                0 => rain = random(10) as f64,
                1 => station = 3 - station,
                _ => rain += [0.0, 0.0, 0.0, 0.1, 0.3][random(5) as usize],
            }
            let value = |base: f64, spread: u64, random: &mut dyn FnMut(u64) -> u64| {
                (random(20) != 0).then(|| base + random(spread) as f64 / 10.0)
            };
            let values = [
                value(-5.0, 200, &mut random),
                value(30.0, 700, &mut random).map(f64::round),
                value(0.0, 150, &mut random),
                value(0.0, 300, &mut random),
            ];
            if random(300) == 0 {
                late.push((
                    time - random(6 * HOUR_MS as u64) as i64,
                    station,
                    values,
                    rain,
                ));
            } else {
                insert(connection, time, station, values, rain);
            }
        }
        for (time, station, values, rain) in late {
            insert(connection, time, station, values, rain);
        }
        (start, end)
    }

    #[test]
    fn hourly_summaries_reproduce_raw_summaries() {
        let mut database = database();
        let (start, end) = irregular_readings(&database.connection);
        backfill(&mut database.connection);
        let history = &database.history;

        for grouping in [
            RainGrouping::Auto,
            RainGrouping::Hour,
            RainGrouping::Day,
            RainGrouping::Week,
            RainGrouping::Month,
        ] {
            for points in [1, 7, 60, 100, 288] {
                let (hourly, raw) = summaries(
                    history,
                    || dates("2026-10-24", "2026-11-04", grouping),
                    points,
                );
                assert_same(&hourly, &raw, &format!("dates {grouping:?} {points}"));
            }
        }
        for (from, to) in [
            (start + 1_234_567, end - 7_654_321),
            (start - 5 * HOUR_MS + 1, start + 27 * HOUR_MS - 1),
            (start + 30 * HOUR_MS + 17, end + 2 * HOUR_MS),
        ] {
            for points in [3, 25, 99, 600] {
                let (hourly, raw) = summaries(
                    history,
                    || {
                        let mut request = SummaryRequest::instants(from, to).unwrap();
                        request.rain_grouping = RainGrouping::Hour;
                        request
                    },
                    points,
                );
                assert_same(&hourly, &raw, &format!("instants {from}..{to} {points}"));
            }
        }
        let (hourly, _) = summaries(
            history,
            || dates("2026-10-24", "2026-11-04", RainGrouping::Auto),
            100,
        );
        assert!(hourly["sample_count"].as_u64().unwrap() > 10_000);
        assert!(
            hourly["statistics"]["rain"]["excluded_transitions"]
                .as_u64()
                .unwrap()
                > 0
        );

        // The summaries are really used: corrupting them changes the hourly result.
        database
            .connection
            .execute(
                "UPDATE weather_hourly SET sample_count = sample_count + 1",
                [],
            )
            .unwrap();
        let (hourly, raw) = summaries(
            history,
            || dates("2026-10-24", "2026-11-04", RainGrouping::Auto),
            100,
        );
        assert_ne!(hourly["sample_count"], raw["sample_count"]);
    }

    #[test]
    fn stored_readings_keep_summaries_complete_even_out_of_order() {
        let mut database = database();
        let start = 1_792_879_200_000;
        for minute in 0..300 {
            store(
                &mut database.connection,
                start + minute * 60_000,
                f64::from(minute as u32) / 10.0,
            );
        }
        assert!(complete(&database.connection).unwrap());
        // The host clock moved back into an hour that is already summarized.
        store(&mut database.connection, start + 90 * 60_000 + 30_000, 1.0);
        assert!(complete(&database.connection).unwrap());
        let (hourly, raw) = summaries(
            &database.history,
            || dates("2026-10-01", "2026-10-31", RainGrouping::Day),
            360,
        );
        assert_same(&hourly, &raw, "out of order");
        assert_eq!(hourly["statistics"]["rain"]["excluded_transitions"], 1);

        // Rows written by another process are picked up by the backfill.
        insert(
            &database.connection,
            start + 400 * 60_000,
            1,
            [None; 4],
            50.0,
        );
        assert!(!complete(&database.connection).unwrap());
        assert_eq!(pending(&database.connection).unwrap(), 1);
        backfill(&mut database.connection);
        let (hourly, raw) = summaries(
            &database.history,
            || dates("2026-10-01", "2026-10-31", RainGrouping::Day),
            360,
        );
        assert_same(&hourly, &raw, "external row");
        assert_eq!(hourly["sample_count"], 302);
    }

    #[test]
    fn changed_definition_or_removed_readings_rebuild_the_summaries() {
        let mut database = database();
        for minute in 0..10 {
            store(
                &mut database.connection,
                1_792_879_200_000 + minute * 60_000,
                0.0,
            );
        }
        let count = |connection: &Connection| -> i64 {
            connection
                .query_row("SELECT COUNT(*) FROM weather_hourly", [], |row| row.get(0))
                .unwrap()
        };
        assert_eq!(count(&database.connection), 1);

        set_metadata(&database.connection, VERSION_KEY, "0").unwrap();
        prepare(&mut database.connection).unwrap();
        assert_eq!(count(&database.connection), 0);
        assert_eq!(pending(&database.connection).unwrap(), 10);
        backfill(&mut database.connection);
        assert_eq!(count(&database.connection), 1);

        database
            .connection
            .execute("DELETE FROM weather_readings WHERE id > 5", [])
            .unwrap();
        prepare(&mut database.connection).unwrap();
        assert_eq!(count(&database.connection), 0);
        backfill(&mut database.connection);
        assert_eq!(count(&database.connection), 1);
    }
}
