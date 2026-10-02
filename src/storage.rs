use crate::{ClimateReading, LiveIndoorReading, LiveWeatherReading};
use log::{debug, error, info, warn};
use rusqlite::{Connection, OpenFlags, params};
use serde::Serialize;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        mpsc::{self, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const MINIMUM_SQLITE_VERSION: i32 = 3_051_003;
const STORAGE_QUEUE_CAPACITY: usize = 256;
const STORAGE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const STORAGE_WRITE_ATTEMPTS: usize = 3;
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
    pub(crate) fn table(self) -> &'static str {
        match self {
            Self::Indoor => "indoor_readings",
            Self::Weather => "weather_readings",
        }
    }
    pub(crate) fn columns(self) -> &'static str {
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
    pub(crate) path: PathBuf,
}

#[derive(Clone, Default, Serialize)]
pub(crate) struct DatabaseStatus {
    pub(crate) available: bool,
    pub(crate) last_error: Option<String>,
}

pub(crate) type DatabaseStatusHandle = Arc<RwLock<DatabaseStatus>>;

pub(crate) struct Storage {
    sender: Option<SyncSender<ClimateReading>>,
    thread: Option<JoinHandle<()>>,
    status: DatabaseStatusHandle,
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
        let status = Arc::new(RwLock::new(DatabaseStatus::default()));
        let initial_connection = match open_database(path) {
            Ok(connection) => {
                info!(
                    "Opened database {} (SQLite {})",
                    path.display(),
                    rusqlite::version()
                );
                mark_available(&status);
                Some(connection)
            }
            Err(error) => {
                mark_failed(&status, &error);
                None
            }
        };

        let (sender, receiver) = mpsc::sync_channel::<ClimateReading>(STORAGE_QUEUE_CAPACITY);
        let worker_path = path.to_owned();
        let worker_status = Arc::clone(&status);
        let thread = thread::spawn(move || {
            let mut connection = initial_connection;
            let mut retry_at = Instant::now();
            loop {
                if connection.is_none() && Instant::now() >= retry_at {
                    connection = reopen_database(&worker_path, &worker_status);
                    retry_at = Instant::now() + STORAGE_RETRY_INTERVAL;
                }
                match receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(reading) => {
                        let Some(mut current) = connection.take() else {
                            let (stream, seq) = reading.describe();
                            warn!("Database unavailable; discarding {stream} reading seq {seq}");
                            continue;
                        };
                        match store_reading(&mut current, &reading) {
                            Ok(()) => {
                                mark_available(&worker_status);
                                connection = Some(current);
                            }
                            Err(error) => {
                                mark_failed(&worker_status, &error);
                                retry_at = Instant::now() + STORAGE_RETRY_INTERVAL;
                            }
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            while let Ok(reading) = receiver.try_recv() {
                let Some(connection) = connection.as_mut() else {
                    warn!("Database unavailable; discarding readings queued at shutdown");
                    break;
                };
                if let Err(error) = store_reading(connection, &reading) {
                    mark_failed(&worker_status, &error);
                    break;
                }
            }
            debug!("Storage worker stopped");
        });
        Ok((
            Self {
                sender: Some(sender),
                thread: Some(thread),
                status,
            },
            History {
                path: path.to_owned(),
            },
        ))
    }

    pub(crate) fn sender(&self) -> SyncSender<ClimateReading> {
        self.sender.as_ref().unwrap().clone()
    }

    pub(crate) fn status(&self) -> DatabaseStatusHandle {
        Arc::clone(&self.status)
    }
}

fn open_database(path: &Path) -> rusqlite::Result<Connection> {
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
        connection.pragma_query_value(None, "wal_autocheckpoint", |row| row.get::<_, i64>(0))?;
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
    Ok(connection)
}

fn reopen_database(path: &Path, status: &DatabaseStatusHandle) -> Option<Connection> {
    match open_database(path) {
        Ok(connection) => {
            info!("Reopened database {}", path.display());
            mark_reopened(status);
            Some(connection)
        }
        Err(error) => {
            mark_failed(status, &error);
            None
        }
    }
}

fn mark_available(status: &DatabaseStatusHandle) {
    let mut status = status.write().unwrap();
    if status.last_error.is_some() {
        info!("Database writes succeeding again");
    }
    status.available = true;
    status.last_error = None;
}

fn mark_reopened(status: &DatabaseStatusHandle) {
    status.write().unwrap().available = true;
}

/// Logs a new or changed failure as an error; repeats of the same failure
/// (such as the once-per-second reopen attempts) only at debug level.
fn mark_failed(status: &DatabaseStatusHandle, error: &impl std::fmt::Display) {
    let mut status = status.write().unwrap();
    let error = error.to_string();
    if status.available || status.last_error.as_ref() != Some(&error) {
        error!("Database unavailable: {error}; retrying every second");
    } else {
        debug!("Database still unavailable: {error}");
    }
    status.available = false;
    status.last_error = Some(error);
}

enum WriteError {
    Retryable(rusqlite::Error),
    Failed(rusqlite::Error),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(error) => write!(formatter, "database write failed: {error}"),
            Self::Failed(error) => write!(formatter, "database write outcome unconfirmed: {error}"),
        }
    }
}

fn store_reading(connection: &mut Connection, reading: &ClimateReading) -> Result<(), WriteError> {
    let (stream, seq) = reading.describe();
    for attempt in 0..STORAGE_WRITE_ATTEMPTS {
        match store_attempt(connection, reading) {
            Ok(id) => {
                debug!("Stored {stream} reading seq {seq} as row {id}");
                return Ok(());
            }
            Err(WriteError::Retryable(error)) if attempt + 1 < STORAGE_WRITE_ATTEMPTS => {
                warn!(
                    "Storing {stream} reading seq {seq} failed (attempt {} of {STORAGE_WRITE_ATTEMPTS}): {error}; retrying",
                    attempt + 1
                );
                thread::sleep(Duration::from_millis(25 * (attempt as u64 + 1)));
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the bounded storage retry loop always returns")
}

fn store_attempt(connection: &mut Connection, reading: &ClimateReading) -> Result<i64, WriteError> {
    let transaction = connection.transaction().map_err(WriteError::Retryable)?;
    let result = match reading {
        ClimateReading::Indoor(reading) => transaction.execute(
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
        ClimateReading::Weather(reading) => transaction.execute(
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
    result.map_err(WriteError::Retryable)?;
    let id = transaction.last_insert_rowid();
    transaction.commit().map_err(WriteError::Failed)?;
    Ok(id)
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

pub(crate) fn row_to_reading(
    row: &rusqlite::Row<'_>,
    stream: Stream,
) -> rusqlite::Result<StoredReading> {
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
            let deadline = Instant::now() + Duration::from_secs(1);
            while !thread.is_finished() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            if !thread.is_finished() {
                warn!("Storage worker did not stop within 1 s; abandoning it");
            } else if thread.join().is_err() {
                error!("Storage worker thread panicked");
            }
        }
    }
}
