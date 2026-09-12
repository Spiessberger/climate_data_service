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

    fn send_indoor(&mut self, seq: u32) {
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
fn weather_wire_reaches_independent_live_state() {
    let mut demo = Demo::start([1000, 1001]);
    let initial = demo.get("/live").1;
    assert_eq!(initial["weather"], Value::Null);
    demo.master
        .write_all(include_bytes!("fixtures/weather.data"))
        .unwrap();
    demo.send_indoor(1);
    let live = demo.wait_for_live_seq(1);
    let mut expected: Value = serde_json::from_str(
        include_str!("fixtures/weather.data")
            .trim()
            .strip_prefix("DATA ")
            .unwrap(),
    )
    .unwrap();
    expected["received_at_unix_ms"] = json!(1000);
    expected["light_lux"] = json!(0.0);
    expected["rssi_dbm"] = json!(-90.0);
    assert_eq!(live["weather"], expected);
    assert_eq!(live["indoor"]["temperature_celsius"], 21.0);
    assert_eq!(live["gateway"]["weather"]["last_seq"], 1);
}

#[test]
fn weather_history_retains_nulls_status_and_cumulative_rain_across_restart() {
    let mut demo = Demo::start([1000, 1001, 1002]);
    demo.master
        .write_all(include_bytes!("fixtures/weather.data"))
        .unwrap();
    demo.master
        .write_all(include_bytes!("fixtures/weather-unavailable.data"))
        .unwrap();
    demo.send_indoor(1);
    let live = demo.wait_for_live_seq(1);
    let history = demo.wait_for_readings("/history/weather/updates?after_id=0", 2);
    let mut last = history["readings"][1].clone();
    assert_eq!(last.as_object_mut().unwrap().remove("id"), Some(json!(2)));
    assert_eq!(last, live["weather"]);
    for field in [
        "temperature_celsius",
        "relative_humidity_percent",
        "wind_direction_degrees",
        "wind_speed_mps",
        "gust_speed_mps",
        "uv_microwatts_per_cm2",
        "uv_index",
        "light_lux",
    ] {
        assert_eq!(last[field], Value::Null, "{field}");
    }
    assert_eq!(last["rain_mm"], 19660.5);
    assert_eq!(last["station_id"], 1);
    assert_eq!(last["battery_low"], true);
    assert_eq!(last["rssi_dbm"], -74.0);
    assert_eq!(last["lqi"], 0);
    let first = &history["readings"][0];
    assert_eq!(first["rain_mm"], 22.2);
    assert_eq!(first["temperature_celsius"], 11.8);
    assert_eq!(first["relative_humidity_percent"], 78);
    assert_eq!(first["wind_direction_degrees"], 266);
    assert_eq!(first["wind_speed_mps"], 1.12);
    assert_eq!(first["gust_speed_mps"], 2.24);
    assert_eq!(first["uv_microwatts_per_cm2"], 1);
    assert_eq!(first["uv_index"], 0);
    assert_eq!(first["light_lux"], 0.0);
    assert_eq!(first["station_id"], 191);
    assert_eq!(first["battery_low"], false);
    assert_eq!(first["rssi_dbm"], -90.0);
    assert_eq!(first["lqi"], 42);
    let connection = rusqlite::Connection::open(&demo.database_path).unwrap();
    let types: (String, String, String, String, String) = connection.query_row(
        "SELECT typeof(station_id), typeof(rain_mm), typeof(temperature_celsius), typeof(battery_low), typeof(seq) FROM weather_readings WHERE id = 2",
        [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    ).unwrap();
    assert_eq!(
        types,
        (
            "integer".into(),
            "real".into(),
            "null".into(),
            "integer".into(),
            "integer".into()
        )
    );
    drop(connection);
    let demo = demo.restart([]);
    assert_eq!(demo.get("/live").1["weather"], Value::Null);
    assert_eq!(
        demo.wait_for_readings("/history/weather/updates?after_id=0", 2),
        history
    );
    assert_eq!(
        demo.wait_for_readings("/history/indoor/updates?after_id=0", 1)["readings"][0]["seq"],
        1
    );
}

#[test]
fn weather_pages_follow_time_and_row_identity_across_counter_wrap_and_rain_reset() {
    let mut demo = Demo::start([1000, 900, 1000]);
    for (seq, rain) in [(u32::MAX, 22.2), (0, 0.0), (0, 0.3)] {
        let mut reading: Value = serde_json::from_str(
            include_str!("fixtures/weather.data")
                .trim()
                .strip_prefix("DATA ")
                .unwrap(),
        )
        .unwrap();
        reading["seq"] = json!(seq);
        reading["rain_mm"] = json!(rain);
        writeln!(demo.master, "DATA {reading}").unwrap();
    }
    let all = demo.wait_for_readings("/history/weather/updates?after_id=0", 3);
    assert_eq!(all["readings"][0]["seq"], u32::MAX);
    assert_eq!(all["readings"][1]["seq"], 0);
    assert_eq!(all["readings"][1]["rain_mm"], 0.0);
    assert_eq!(all["readings"][2]["rain_mm"], 0.3);
    let first = demo.wait_for_readings(
        "/history/weather?from_unix_ms=900&to_unix_ms=1001&limit=2",
        2,
    );
    assert_eq!(first["readings"][0]["id"], 2);
    assert_eq!(first["readings"][1]["id"], 1);
    let second = demo.wait_for_readings("/history/weather?from_unix_ms=900&to_unix_ms=1001&after_received_at_unix_ms=1000&after_id=1&limit=2", 1);
    assert_eq!(second["readings"][0]["id"], 3);
    let increment = demo.wait_for_readings("/history/weather/updates?after_id=2&limit=1", 1);
    assert_eq!(increment["readings"][0]["id"], 3);
    let end_excluded =
        demo.wait_for_readings("/history/weather?from_unix_ms=900&to_unix_ms=1000", 1);
    assert_eq!(end_excluded["readings"][0]["id"], 2);
    for path in [
        "/history/weather",
        "/history/weather/updates",
        "/history/weather/updates?after_id=-1",
        "/history/weather/updates?after_id=0&limit=1001",
        "/history/weather/updates?after_id=0&limit=0",
        "/history/weather?from_unix_ms=2&to_unix_ms=1",
        "/history/weather?from_unix_ms=0&to_unix_ms=1&after_id=1",
        "/history/weather/updates?after_id=0&limit=1&limit=2",
    ] {
        assert_eq!(demo.get(path).0, 400, "{path}");
    }
    assert_eq!(
        demo.request("POST", "/history/weather/updates?after_id=0")
            .0,
        405
    );
}

#[test]
fn blocked_weather_writes_leave_both_streams_live_and_history_committed_only() {
    let mut demo = Demo::start([1000, 1001]);
    let blocker = rusqlite::Connection::open(&demo.database_path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    demo.master
        .write_all(include_bytes!("fixtures/weather.data"))
        .unwrap();
    demo.send_indoor(1);
    let live = demo.wait_for_live_seq(1);
    assert_eq!(live["weather"]["seq"], 1);
    assert!(live["weather"].get("id").is_none());
    assert_eq!(
        demo.get("/history/weather/updates?after_id=0"),
        (200, json!({"readings": []}))
    );
    assert_eq!(
        demo.get("/history/indoor/updates?after_id=0"),
        (200, json!({"readings": []}))
    );
    blocker.execute_batch("ROLLBACK").unwrap();
    demo.wait_for_readings("/history/weather/updates?after_id=0", 1);
    demo.wait_for_readings("/history/indoor/updates?after_id=0", 1);
}
