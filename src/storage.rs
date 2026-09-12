use crate::{ClimateReading, LiveIndoorReading, LiveWeatherReading};
use rusqlite::{Connection, OpenFlags, params};
use serde::Serialize;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::mpsc::{self, SyncSender},
    thread::{self, JoinHandle},
    time::Duration,
};

const MINIMUM_SQLITE_VERSION: i32 = 3_051_003;
const STORAGE_QUEUE_CAPACITY: usize = 256;
const INDOOR_COLUMNS: &str = "id, received_at_unix_ms, v, type, boot_id, seq, \
                              temperature_celsius, relative_humidity_percent";

const WEATHER_COLUMNS: &str = "id, received_at_unix_ms, v, type, boot_id, seq, \
    station_id, temperature_celsius, relative_humidity_percent, wind_direction_degrees, \
    wind_speed_mps, gust_speed_mps, rain_mm, uv_microwatts_per_cm2, uv_index, \
    light_lux, battery_low, rssi_dbm, lqi";

#[derive(Clone, Copy)]
pub(crate) enum Stream {
    Indoor,
    Weather,
}

impl Stream {
    fn table(self) -> &'static str {
        match self {
            Self::Indoor => "indoor_readings",
            Self::Weather => "weather_readings",
        }
    }
    fn columns(self) -> &'static str {
        match self {
            Self::Indoor => INDOOR_COLUMNS,
            Self::Weather => WEATHER_COLUMNS,
        }
    }
}

pub(crate) struct RangeCursor {
    pub(crate) received_at_unix_ms: i64,
    pub(crate) id: i64,
}

#[derive(Clone)]
pub(crate) struct History {
    path: PathBuf,
}

pub(crate) struct Storage {
    sender: Option<SyncSender<ClimateReading>>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Serialize)]
pub(crate) struct StoredReading {
    id: i64,
    #[serde(flatten)]
    reading: ClimateReading,
}

impl Storage {
    pub(crate) fn start(path: &Path) -> rusqlite::Result<(Self, History)> {
        if rusqlite::version_number() < MINIMUM_SQLITE_VERSION {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                Some(format!(
                    "SQLite {} is older than required 3.51.3",
                    rusqlite::version()
                )),
            ));
        }
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|error| {
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
                    Some(error.to_string()),
                )
            })?;
        }
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(2))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "wal_autocheckpoint", 1000)?;
        let journal_mode =
            connection.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))?;
        let synchronous =
            connection.pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))?;
        let wal_autocheckpoint =
            connection
                .pragma_query_value(None, "wal_autocheckpoint", |row| row.get::<_, i64>(0))?;
        if journal_mode != "wal" || synchronous != 2 || wal_autocheckpoint != 1_000 {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                Some("SQLite did not apply the required WAL/FULL/checkpoint settings".to_owned()),
            ));
        }
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS indoor_readings (
                id INTEGER PRIMARY KEY,
                received_at_unix_ms INTEGER NOT NULL,
                v INTEGER NOT NULL,
                type TEXT NOT NULL,
                boot_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                temperature_celsius REAL NOT NULL,
                relative_humidity_percent REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS indoor_readings_received_at_id
                ON indoor_readings (received_at_unix_ms, id);
            CREATE TABLE IF NOT EXISTS weather_readings (
                id INTEGER PRIMARY KEY,
                received_at_unix_ms INTEGER NOT NULL,
                v INTEGER NOT NULL,
                type TEXT NOT NULL,
                boot_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                station_id INTEGER NOT NULL,
                temperature_celsius REAL,
                relative_humidity_percent INTEGER,
                wind_direction_degrees INTEGER,
                wind_speed_mps REAL,
                gust_speed_mps REAL,
                rain_mm REAL NOT NULL,
                uv_microwatts_per_cm2 INTEGER,
                uv_index INTEGER,
                light_lux REAL,
                battery_low INTEGER NOT NULL,
                rssi_dbm REAL NOT NULL,
                lqi INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS weather_readings_received_at_id
                ON weather_readings (received_at_unix_ms, id);
            CREATE TABLE IF NOT EXISTS service_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;",
        )?;
        let metadata = connection.transaction()?;
        for (key, value) in [
            ("journal_mode", journal_mode),
            ("sqlite_version", rusqlite::version().to_owned()),
            ("synchronous", synchronous.to_string()),
            ("wal_autocheckpoint", wal_autocheckpoint.to_string()),
        ] {
            metadata.execute(
                "INSERT INTO service_metadata (key, value) VALUES (?1, ?2)
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                params![key, value],
            )?;
        }
        metadata.commit()?;

        let (sender, receiver) = mpsc::sync_channel::<ClimateReading>(STORAGE_QUEUE_CAPACITY);
        let thread = thread::spawn(move || {
            while let Ok(reading) = receiver.recv() {
                let _ = match reading {
                    ClimateReading::Indoor(reading) => connection.execute(
                        "INSERT INTO indoor_readings (
                        received_at_unix_ms, v, type, boot_id, seq,
                        temperature_celsius, relative_humidity_percent
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            reading.received_at_unix_ms,
                            reading.reading.v,
                            reading.reading.record_type,
                            reading.reading.boot_id,
                            reading.reading.seq,
                            reading.reading.temperature_celsius,
                            reading.reading.relative_humidity_percent,
                        ],
                    ),
                    ClimateReading::Weather(reading) => connection.execute(
                        "INSERT INTO weather_readings (
                        received_at_unix_ms, v, type, boot_id, seq, station_id,
                        temperature_celsius, relative_humidity_percent,
                        wind_direction_degrees, wind_speed_mps, gust_speed_mps,
                        rain_mm, uv_microwatts_per_cm2, uv_index, light_lux,
                        battery_low, rssi_dbm, lqi
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                              ?13, ?14, ?15, ?16, ?17, ?18)",
                        params![
                            reading.received_at_unix_ms,
                            reading.reading.v,
                            reading.reading.record_type,
                            reading.reading.boot_id,
                            reading.reading.seq,
                            reading.reading.station_id,
                            reading.reading.temperature_celsius,
                            reading.reading.relative_humidity_percent,
                            reading.reading.wind_direction_degrees,
                            reading.reading.wind_speed_mps,
                            reading.reading.gust_speed_mps,
                            reading.reading.rain_mm,
                            reading.reading.uv_microwatts_per_cm2,
                            reading.reading.uv_index,
                            reading.reading.light_lux,
                            reading.reading.battery_low,
                            reading.reading.rssi_dbm,
                            reading.reading.lqi,
                        ],
                    ),
                };
            }
        });
        Ok((
            Self {
                sender: Some(sender),
                thread: Some(thread),
            },
            History {
                path: path.to_owned(),
            },
        ))
    }

    pub(crate) fn sender(&self) -> SyncSender<ClimateReading> {
        self.sender.as_ref().unwrap().clone()
    }
}

impl History {
    pub(crate) fn range(
        &self,
        stream: Stream,
        from_unix_ms: i64,
        to_unix_ms: i64,
        after: Option<RangeCursor>,
        limit: usize,
    ) -> rusqlite::Result<Vec<StoredReading>> {
        let connection = self.read_connection()?;
        let table = stream.table();
        let columns = stream.columns();
        if let Some(after) = after {
            let sql = format!(
                "SELECT {columns}
                 FROM {table}
                 WHERE received_at_unix_ms >= ?1 AND received_at_unix_ms < ?2
                   AND (received_at_unix_ms > ?3
                        OR (received_at_unix_ms = ?3 AND id > ?4))
                 ORDER BY received_at_unix_ms, id
                 LIMIT ?5"
            );
            let mut statement = connection.prepare(&sql)?;
            statement
                .query_map(
                    params![
                        from_unix_ms,
                        to_unix_ms,
                        after.received_at_unix_ms,
                        after.id,
                        limit as i64
                    ],
                    |row| row_to_reading(row, stream),
                )?
                .collect()
        } else {
            let sql = format!(
                "SELECT {columns}
                 FROM {table}
                 WHERE received_at_unix_ms >= ?1 AND received_at_unix_ms < ?2
                 ORDER BY received_at_unix_ms, id
                 LIMIT ?3"
            );
            let mut statement = connection.prepare(&sql)?;
            statement
                .query_map(params![from_unix_ms, to_unix_ms, limit as i64], |row| {
                    row_to_reading(row, stream)
                })?
                .collect()
        }
    }

    pub(crate) fn after(
        &self,
        stream: Stream,
        after_id: i64,
        limit: usize,
    ) -> rusqlite::Result<Vec<StoredReading>> {
        let connection = self.read_connection()?;
        let table = stream.table();
        let columns = stream.columns();
        let sql = format!(
            "SELECT {columns}
             FROM {table}
             WHERE id > ?1
             ORDER BY id
             LIMIT ?2"
        );
        let mut statement = connection.prepare(&sql)?;
        statement
            .query_map(params![after_id, limit as i64], |row| {
                row_to_reading(row, stream)
            })?
            .collect()
    }

    fn read_connection(&self) -> rusqlite::Result<Connection> {
        let connection = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_millis(100))?;
        Ok(connection)
    }
}

fn row_to_reading(row: &rusqlite::Row<'_>, stream: Stream) -> rusqlite::Result<StoredReading> {
    let reading = match stream {
        Stream::Indoor => ClimateReading::Indoor(LiveIndoorReading {
            received_at_unix_ms: row.get(1)?,
            reading: crate::wire::IndoorReading {
                v: row.get(2)?,
                record_type: row.get(3)?,
                boot_id: row.get(4)?,
                seq: row.get(5)?,
                temperature_celsius: row.get(6)?,
                relative_humidity_percent: row.get(7)?,
            },
        }),
        Stream::Weather => ClimateReading::Weather(LiveWeatherReading {
            received_at_unix_ms: row.get(1)?,
            reading: crate::wire::WeatherReading {
                v: row.get(2)?,
                record_type: row.get(3)?,
                boot_id: row.get(4)?,
                seq: row.get(5)?,
                station_id: row.get(6)?,
                temperature_celsius: row.get(7)?,
                relative_humidity_percent: row.get(8)?,
                wind_direction_degrees: row.get(9)?,
                wind_speed_mps: row.get(10)?,
                gust_speed_mps: row.get(11)?,
                rain_mm: row.get(12)?,
                uv_microwatts_per_cm2: row.get(13)?,
                uv_index: row.get(14)?,
                light_lux: row.get(15)?,
                battery_low: row.get(16)?,
                rssi_dbm: row.get(17)?,
                lqi: row.get(18)?,
            },
        }),
    };
    Ok(StoredReading {
        id: row.get(0)?,
        reading,
    })
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
