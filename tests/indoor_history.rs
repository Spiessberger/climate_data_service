use climate_data_service::Service;
use serde_json::{Value, json};
use serialport::{SerialPort, TTYPort};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;

struct Demo {
    master: TTYPort,
    address: SocketAddr,
    database_path: std::path::PathBuf,
    _service: Service,
    directory: Arc<TempDir>,
}

impl Demo {
    fn start(reception_times: impl IntoIterator<Item = i64>) -> Self {
        Self::start_in(Arc::new(tempfile::tempdir().unwrap()), reception_times)
    }

    fn start_in(directory: Arc<TempDir>, reception_times: impl IntoIterator<Item = i64>) -> Self {
        let database_path = directory.path().join("climate.sqlite3");
        let (master, slave) = TTYPort::pair().unwrap();
        let serial_path = slave.name().unwrap();
        drop(slave);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let reception_times = Arc::new(Mutex::new(
            reception_times.into_iter().collect::<VecDeque<_>>(),
        ));
        let service = Service::start(
            &serial_path,
            listener,
            &database_path,
            move || reception_times.lock().unwrap().pop_front().unwrap(),
            |_| {},
        )
        .unwrap();
        Self {
            master,
            address,
            database_path,
            _service: service,
            directory,
        }
    }

    fn restart(self, reception_times: impl IntoIterator<Item = i64>) -> Self {
        let directory = Arc::clone(&self.directory);
        drop(self);
        Self::start_in(directory, reception_times)
    }

    fn send(&mut self, seq: u32) {
        writeln!(
            self.master,
            "DATA {{\"v\":1,\"type\":\"indoor\",\"boot_id\":\"6a9d3c1f80b24e67a511d92cb837046e\",\"seq\":{seq},\"temperature_celsius\":{},\"relative_humidity_percent\":{}}}",
            20.0 + f64::from(seq),
            40.0 + f64::from(seq),
        )
        .unwrap();
    }

    fn request(&self, method: &str, path: &str) -> (u16, Value) {
        let mut client = TcpStream::connect(self.address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        write!(
            client,
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        )
        .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        let status = response[9..12].parse().unwrap();
        let body = serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        (status, body)
    }

    fn get(&self, path: &str) -> (u16, Value) {
        self.request("GET", path)
    }

    fn wait_for_readings(&self, path: &str, count: usize) -> Value {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let (status, body) = self.get(path);
            assert_eq!(status, 200, "{body}");
            if body["readings"]
                .as_array()
                .is_some_and(|rows| rows.len() == count)
            {
                return body;
            }
            assert!(
                Instant::now() < deadline,
                "history did not reach {count}: {body}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn wait_for_live_seq(&self, seq: u32) -> Value {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let (status, body) = self.get("/live");
            assert_eq!(status, 200, "{body}");
            if body["indoor"]["seq"] == seq {
                return body;
            }
            assert!(
                Instant::now() < deadline,
                "live reading did not reach {seq}: {body}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }
}

#[test]
fn committed_indoor_readings_are_selected_by_half_open_utc_range() {
    let mut demo = Demo::start([999, 1_000, 1_500, 2_000]);
    for seq in 1..=4 {
        demo.send(seq);
    }

    let history = demo.wait_for_readings(
        "/history/indoor?from_unix_ms=1000&to_unix_ms=2000&limit=10",
        2,
    );
    assert_eq!(
        history,
        json!({"readings": [
            {
                "id": 2, "v": 1, "type": "indoor",
                "boot_id": "6a9d3c1f80b24e67a511d92cb837046e", "seq": 2,
                "temperature_celsius": 22.0, "relative_humidity_percent": 42.0,
                "received_at_unix_ms": 1_000,
            },
            {
                "id": 3, "v": 1, "type": "indoor",
                "boot_id": "6a9d3c1f80b24e67a511d92cb837046e", "seq": 3,
                "temperature_celsius": 23.0, "relative_humidity_percent": 43.0,
                "received_at_unix_ms": 1_500,
            }
        ]}),
    );
}

#[test]
fn incremental_pages_follow_row_ids_across_gateway_counter_wrap() {
    let mut demo = Demo::start([1_000, 1_000, 900]);
    for seq in [u32::MAX, 0, 0] {
        demo.send(seq);
    }

    let first = demo.wait_for_readings("/history/indoor/updates?after_id=0&limit=2", 2);
    assert_eq!(first["readings"][0]["id"], 1);
    assert_eq!(first["readings"][0]["seq"], u32::MAX);
    assert_eq!(first["readings"][1]["id"], 2);
    assert_eq!(first["readings"][1]["seq"], 0);

    let second = demo.wait_for_readings("/history/indoor/updates?after_id=2&limit=2", 1);
    assert_eq!(second["readings"][0]["id"], 3);
    assert_eq!(second["readings"][0]["seq"], 0);
    assert_eq!(second["readings"][0]["received_at_unix_ms"], 900);
}

#[test]
fn range_pages_are_stable_with_backward_and_equal_reception_times() {
    let mut demo = Demo::start([1_000, 900, 1_000]);
    for seq in 1..=3 {
        demo.send(seq);
    }

    let first = demo.wait_for_readings("/history/indoor?from_unix_ms=0&to_unix_ms=2000&limit=2", 2);
    assert_eq!(first["readings"][0]["id"], 2);
    assert_eq!(first["readings"][0]["received_at_unix_ms"], 900);
    assert_eq!(first["readings"][1]["id"], 1);
    assert_eq!(first["readings"][1]["received_at_unix_ms"], 1_000);

    let second = demo.wait_for_readings(
        "/history/indoor?from_unix_ms=0&to_unix_ms=2000&after_received_at_unix_ms=1000&after_id=1&limit=2",
        1,
    );
    assert_eq!(second["readings"][0]["id"], 3);
    assert_eq!(second["readings"][0]["received_at_unix_ms"], 1_000);
}

#[test]
fn history_survives_restart_while_live_state_starts_empty() {
    let mut demo = Demo::start([1_234]);
    demo.send(7);
    demo.wait_for_readings("/history/indoor/updates?after_id=0&limit=10", 1);

    let demo = demo.restart([]);
    let (status, live) = demo.get("/live");
    assert_eq!(status, 200);
    assert_eq!(live["indoor"], Value::Null);
    assert_eq!(live["gateway"]["available"], false);
    let history = demo.wait_for_readings("/history/indoor/updates?after_id=0&limit=10", 1);
    assert_eq!(history["readings"][0]["seq"], 7);
    assert_eq!(history["readings"][0]["received_at_unix_ms"], 1_234);
}

#[test]
fn sqlite_schema_and_runtime_preserve_the_required_metadata() {
    let demo = Demo::start([]);
    let connection = rusqlite::Connection::open(&demo.database_path).unwrap();
    let columns = connection
        .prepare("PRAGMA table_info(indoor_readings)")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, bool>(5)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        columns,
        [
            ("id", "INTEGER", false, true),
            ("received_at_unix_ms", "INTEGER", true, false),
            ("v", "INTEGER", true, false),
            ("type", "TEXT", true, false),
            ("boot_id", "TEXT", true, false),
            ("seq", "INTEGER", true, false),
            ("temperature_celsius", "REAL", true, false),
            ("relative_humidity_percent", "REAL", true, false),
        ]
        .map(|(name, kind, not_null, primary_key)| {
            (name.to_owned(), kind.to_owned(), not_null, primary_key)
        })
    );
    let index_sql: String = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = 'indoor_readings_received_at_id'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(index_sql.contains("(received_at_unix_ms, id)"));
    assert!(!index_sql.contains("UNIQUE"));
    assert_eq!(
        connection
            .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    let storage_settings = connection
        .prepare("SELECT key, value FROM service_metadata ORDER BY key")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        storage_settings,
        [
            ("journal_mode", "wal"),
            ("sqlite_version", rusqlite::version()),
            ("synchronous", "2"),
            ("wal_autocheckpoint", "1000"),
            ("weather_hourly_through_id", "0"),
            ("weather_hourly_version", "1"),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
    );
    assert!(rusqlite::version_number() >= 3_051_003);
}

#[test]
fn a_delayed_commit_does_not_delay_live_http_or_leak_into_history() {
    let mut demo = Demo::start([1_000]);
    let blocker = rusqlite::Connection::open(&demo.database_path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    demo.send(9);
    let live = demo.wait_for_live_seq(9);
    assert!(live["indoor"].get("id").is_none());
    assert_eq!(
        demo.get("/history/indoor/updates?after_id=0&limit=10"),
        (200, json!({"readings": []}))
    );

    blocker.execute_batch("COMMIT").unwrap();
    let history = demo.wait_for_readings("/history/indoor/updates?after_id=0&limit=10", 1);
    assert_eq!(history["readings"][0]["seq"], 9);
}

#[test]
fn history_http_is_read_only_and_rejects_unbounded_or_ambiguous_queries() {
    let demo = Demo::start([]);
    let invalid = [
        "/history/indoor",
        "/history/indoor?from_unix_ms=0&to_unix_ms=1&limit=0",
        "/history/indoor?from_unix_ms=0&to_unix_ms=1&limit=1001",
        "/history/indoor?from_unix_ms=0&to_unix_ms=1&after_id=1",
        "/history/indoor/updates?after_id=-1&limit=1",
        "/history/indoor/updates?after_id=0&after_id=1&limit=1",
    ];
    for path in invalid {
        assert_eq!(demo.get(path).0, 400, "{path}");
    }
    assert_eq!(
        demo.request("POST", "/history/indoor/updates?after_id=0&limit=1")
            .0,
        405
    );
}

#[test]
fn a_slow_history_client_does_not_block_live_http() {
    let mut demo = Demo::start([2_000]);
    let mut connection = rusqlite::Connection::open(&demo.database_path).unwrap();
    let transaction = connection.transaction().unwrap();
    {
        let mut insert = transaction
            .prepare(
                "INSERT INTO indoor_readings (
                    received_at_unix_ms, v, type, boot_id, seq,
                    temperature_celsius, relative_humidity_percent
                ) VALUES (?1, 1, 'indoor', ?2, ?3, 20.0, 40.0)",
            )
            .unwrap();
        let large_value = "a".repeat(8_192);
        for id in 1..=1_000 {
            insert
                .execute(rusqlite::params![id, large_value, id])
                .unwrap();
        }
    }
    transaction.commit().unwrap();

    let mut slow = TcpStream::connect(demo.address).unwrap();
    slow.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    slow.write_all(
        b"GET /history/indoor/updates?after_id=0&limit=1000 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    let mut first_byte = [0];
    assert_eq!(slow.peek(&mut first_byte).unwrap(), 1);

    demo.send(1);
    assert_eq!(demo.wait_for_live_seq(1)["indoor"]["seq"], 1);
}
