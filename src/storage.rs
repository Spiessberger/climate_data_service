use crate::LiveIndoorReading;
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

pub(crate) struct IndoorRangeCursor {
    pub(crate) received_at_unix_ms: i64,
    pub(crate) id: i64,
}

#[derive(Clone)]
pub(crate) struct History {
    path: PathBuf,
}

pub(crate) struct Storage {
    sender: Option<SyncSender<LiveIndoorReading>>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Serialize)]
pub(crate) struct StoredIndoorReading {
    id: i64,
    #[serde(flatten)]
    reading: LiveIndoorReading,
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

        let (sender, receiver) = mpsc::sync_channel::<LiveIndoorReading>(STORAGE_QUEUE_CAPACITY);
        let thread = thread::spawn(move || {
            while let Ok(reading) = receiver.recv() {
                let _ = connection.execute(
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
                );
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

    pub(crate) fn sender(&self) -> SyncSender<LiveIndoorReading> {
        self.sender.as_ref().unwrap().clone()
    }
}

impl History {
    pub(crate) fn indoor_range(
        &self,
        from_unix_ms: i64,
        to_unix_ms: i64,
        after: Option<IndoorRangeCursor>,
        limit: usize,
    ) -> rusqlite::Result<Vec<StoredIndoorReading>> {
        let connection = self.read_connection()?;
        if let Some(after) = after {
            let sql = format!(
                "SELECT {INDOOR_COLUMNS}
                 FROM indoor_readings
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
                    row_to_indoor,
                )?
                .collect()
        } else {
            let sql = format!(
                "SELECT {INDOOR_COLUMNS}
                 FROM indoor_readings
                 WHERE received_at_unix_ms >= ?1 AND received_at_unix_ms < ?2
                 ORDER BY received_at_unix_ms, id
                 LIMIT ?3"
            );
            let mut statement = connection.prepare(&sql)?;
            statement
                .query_map(
                    params![from_unix_ms, to_unix_ms, limit as i64],
                    row_to_indoor,
                )?
                .collect()
        }
    }

    pub(crate) fn indoor_after(
        &self,
        after_id: i64,
        limit: usize,
    ) -> rusqlite::Result<Vec<StoredIndoorReading>> {
        let connection = self.read_connection()?;
        let sql = format!(
            "SELECT {INDOOR_COLUMNS}
             FROM indoor_readings
             WHERE id > ?1
             ORDER BY id
             LIMIT ?2"
        );
        let mut statement = connection.prepare(&sql)?;
        statement
            .query_map(params![after_id, limit as i64], row_to_indoor)?
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

fn row_to_indoor(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredIndoorReading> {
    Ok(StoredIndoorReading {
        id: row.get(0)?,
        reading: LiveIndoorReading {
            received_at_unix_ms: row.get(1)?,
            reading: crate::wire::IndoorReading {
                v: row.get(2)?,
                record_type: row.get(3)?,
                boot_id: row.get(4)?,
                seq: row.get(5)?,
                temperature_celsius: row.get(6)?,
                relative_humidity_percent: row.get(7)?,
            },
        },
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
