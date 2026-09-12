use climate_data_service::{DiagnosticKind, Service};
use serde_json::Value;
use serialport::{SerialPort, TTYPort};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
        mpsc::{self, Receiver},
    },
    thread,
    time::{Duration, Instant},
};

const BOOT: &str = "6a9d3c1f80b24e67a511d92cb837046e";

struct Demo {
    master: TTYPort,
    address: SocketAddr,
    elapsed_ms: Arc<AtomicU64>,
    utc_ms: Arc<AtomicI64>,
    diagnostics: Receiver<(DiagnosticKind, Vec<u8>)>,
    serial_path: std::path::PathBuf,
    device_path: String,
    _service: Service,
    _directory: tempfile::TempDir,
}

impl Demo {
    fn start() -> Self {
        Self::start_connected(true)
    }

    fn start_connected(connected: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let (master, slave) = TTYPort::pair().unwrap();
        let path = slave.name().unwrap();
        drop(slave);
        let serial_path = directory.path().join("configured-gateway");
        if connected {
            std::os::unix::fs::symlink(&path, &serial_path).unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let elapsed_ms = Arc::new(AtomicU64::new(0));
        let utc_ms = Arc::new(AtomicI64::new(1_800_000_000_123));
        let elapsed = elapsed_ms.clone();
        let utc = utc_ms.clone();
        let (sender, diagnostics) = mpsc::channel();
        let service = Service::start_with_clock(
            serial_path.to_str().unwrap(),
            listener,
            &directory.path().join("climate.sqlite3"),
            move || utc.load(Ordering::SeqCst),
            move || Duration::from_millis(elapsed.load(Ordering::SeqCst)),
            move |event| {
                sender.send((event.kind, event.bytes.to_vec())).unwrap();
            },
        )
        .unwrap();
        Self {
            master,
            address,
            elapsed_ms,
            utc_ms,
            diagnostics,
            serial_path,
            device_path: path,
            _service: service,
            _directory: directory,
        }
    }

    fn live(&self) -> Value {
        let mut client = TcpStream::connect(self.address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client
            .write_all(b"GET /live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    fn wait_for(&self, predicate: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let live = self.live();
            if predicate(&live) {
                return live;
            }
            assert!(Instant::now() < deadline, "unexpected live state: {live}");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn heartbeat(&mut self, boot: &str) {
        writeln!(
            self.master,
            "DATA {{\"v\":1,\"type\":\"heartbeat\",\"boot_id\":\"{boot}\"}}"
        )
        .unwrap();
    }

    fn reading(&mut self, boot: &str, seq: u32) -> Value {
        writeln!(self.master, "DATA {{\"v\":1,\"type\":\"indoor\",\"boot_id\":\"{boot}\",\"seq\":{seq},\"temperature_celsius\":21.5,\"relative_humidity_percent\":48.2}}").unwrap();
        self.wait_for(|live| live["indoor"]["seq"] == seq && live["indoor"]["boot_id"] == boot)
    }

    fn event(&self, name: &str) -> Value {
        loop {
            let (kind, bytes) = self
                .diagnostics
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            if kind == DiagnosticKind::Operational {
                let event: Value = serde_json::from_slice(&bytes).unwrap();
                if event["event"] == name {
                    return event;
                }
            }
        }
    }
}

#[test]
fn absent_device_and_disconnect_reopen_only_the_configured_path() {
    let mut demo = Demo::start_connected(false);
    let initial = demo.live();
    assert_eq!(initial["gateway"]["available"], false);
    assert_eq!(initial["indoor"], Value::Null);
    demo.event("serial_unavailable");
    // An unrelated serial device is present throughout, but is never selected.
    let (mut unrelated, _unrelated_slave) = TTYPort::pair().unwrap();
    writeln!(
        unrelated,
        "DATA {{\"v\":1,\"type\":\"heartbeat\",\"boot_id\":\"{BOOT}\"}}"
    )
    .unwrap();
    assert_eq!(demo.live()["gateway"]["available"], false);
    std::os::unix::fs::symlink(&demo.device_path, &demo.serial_path).unwrap();
    demo.elapsed_ms.store(1_000, Ordering::SeqCst);
    demo.event("serial_connected");
    // Opening a port is not proof of gateway communication.
    assert_eq!(demo.live()["gateway"]["available"], false);
    let original = demo.reading(BOOT, 40);
    demo.master.write_all(b"DATA {\"v\":1").unwrap();
    thread::sleep(Duration::from_millis(30));

    let (replacement, slave) = TTYPort::pair().unwrap();
    let replacement_path = slave.name().unwrap();
    drop(slave);
    std::fs::remove_file(&demo.serial_path).unwrap();
    drop(std::mem::replace(&mut demo.master, replacement));
    // Elapsed time stays at one second: USB loss must not wait for the timeout.
    let lost = demo.wait_for(|live| live["gateway"]["available"] == false);
    assert_eq!(lost["indoor"], original["indoor"]);
    demo.event("serial_disconnected");
    loop {
        let (kind, bytes) = demo
            .diagnostics
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        if kind == DiagnosticKind::PartialLine {
            assert_eq!(bytes, b"DATA {\"v\":1");
            break;
        }
    }

    std::os::unix::fs::symlink(replacement_path, &demo.serial_path).unwrap();
    demo.elapsed_ms.store(2_000, Ordering::SeqCst);
    demo.event("serial_connected");
    assert_eq!(demo.live()["gateway"]["available"], false);
    demo.heartbeat(BOOT);
    let recovered = demo.wait_for(|live| live["gateway"]["available"] == true);
    assert_eq!(recovered["indoor"], original["indoor"]);
    demo.event("gateway_available");
    // The old partial line must not consume the first record after reconnect.
    let next = demo.reading(BOOT, 43);
    assert_eq!(next["gateway"]["restart_count"], 0);
    assert_eq!(next["gateway"]["indoor"]["observed_missing_readings"], 2);
}

#[test]
fn missed_startup_wrap_gaps_and_restarts_have_distinct_meanings() {
    let mut demo = Demo::start();
    let first = demo.reading(BOOT, u32::MAX - 1);
    assert_eq!(first["gateway"]["restart_count"], 0);
    assert_eq!(first["gateway"]["indoor"]["observed_missing_readings"], 0);
    demo.reading(BOOT, u32::MAX);
    let wrapped = demo.reading(BOOT, 0);
    assert_eq!(wrapped["gateway"]["restart_count"], 0);
    assert_eq!(wrapped["gateway"]["indoor"]["observed_missing_readings"], 0);
    let gap = demo.reading(BOOT, 3);
    assert_eq!(gap["gateway"]["indoor"]["observed_missing_readings"], 2);
    let event = demo.event("reading_gap");
    assert_eq!(event["stream"], "indoor");
    assert_eq!(event["previous_seq"], 0);
    assert_eq!(event["seq"], 3);
    assert_eq!(event["observed_missing"], 2);

    let next_boot = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    demo.heartbeat(next_boot);
    let restarted = demo.wait_for(|live| live["gateway"]["boot_id"] == next_boot);
    assert_eq!(restarted["gateway"]["restart_count"], 1);
    assert_eq!(restarted["gateway"]["indoor"]["last_seq"], Value::Null);
    assert_eq!(restarted["indoor"], gap["indoor"]);
    let event = demo.event("gateway_restart");
    assert_eq!(event["previous_boot_id"], BOOT);
    assert_eq!(event["boot_id"], next_boot);
    let reading = demo.reading(next_boot, 800);
    assert_eq!(reading["gateway"]["indoor"]["observed_missing_readings"], 2);
    assert_eq!(reading["gateway"]["restart_count"], 1);
    // Equal counters cannot establish how many readings, if any, were missed.
    demo.reading(next_boot, 800);
    demo.event("reading_sequence_ambiguous");
    assert_eq!(
        demo.live()["gateway"]["indoor"]["observed_missing_readings"],
        2
    );
    // A restart first seen in a reading also resets continuity. A subsequent
    // gap spanning wrap still counts only the two missing produced readings.
    let third_boot = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let restarted = demo.reading(third_boot, u32::MAX - 1);
    assert_eq!(restarted["gateway"]["restart_count"], 2);
    assert_eq!(
        restarted["gateway"]["indoor"]["observed_missing_readings"],
        2
    );
    let wrapped_gap = demo.reading(third_boot, 1);
    assert_eq!(
        wrapped_gap["gateway"]["indoor"]["observed_missing_readings"],
        4
    );
}

#[test]
fn heartbeat_only_traffic_expires_at_fifteen_seconds_independent_of_utc() {
    let mut demo = Demo::start();
    assert_eq!(demo.live()["gateway"]["available"], false);
    demo.heartbeat(BOOT);
    let live = demo.wait_for(|live| live["gateway"]["available"] == true);
    assert_eq!(live["indoor"], Value::Null);
    assert_eq!(live["gateway"]["boot_id"], BOOT);
    demo.elapsed_ms.store(14_999, Ordering::SeqCst);
    demo.utc_ms.store(-123_456, Ordering::SeqCst);
    demo.master.write_all(b"INFO - sensor silent\n").unwrap();
    // A diagnostic proves the serial loop processed traffic at the new time.
    loop {
        let (kind, _) = demo
            .diagnostics
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        if kind == DiagnosticKind::Text {
            break;
        }
    }
    assert_eq!(demo.live()["gateway"]["available"], true);
    demo.elapsed_ms.store(15_000, Ordering::SeqCst);
    demo.wait_for(|live| live["gateway"]["available"] == false);
    demo.heartbeat(BOOT);
    let live = demo.wait_for(|live| live["gateway"]["available"] == true);
    assert_eq!(live["indoor"], Value::Null);
    assert_eq!(live["gateway"]["last_received_at_unix_ms"], -123_456);
}

#[test]
fn only_valid_supported_records_refresh_health_and_readings_keep_their_age() {
    let mut demo = Demo::start();
    let original = demo.reading(BOOT, 7)["indoor"].clone();
    let heartbeat = format!("DATA {{\"v\":1,\"type\":\"heartbeat\",\"boot_id\":\"{BOOT}\"}}\n");
    let rejected = [
        heartbeat.replace("\"v\":1", "\"v\":2"),
        heartbeat.replace("\"v\":1", "\"v\":1.0"),
        heartbeat.replace("heartbeat", "weather"),
        heartbeat.replace("heartbeat", "indoor"),
        heartbeat.replace(BOOT, "bad"),
        heartbeat.replace(BOOT, &BOOT.to_uppercase()),
        heartbeat.replace("\"v\":1", "\"v\":1,\"extra\":{\"x\":1,\"x\":2}"),
        heartbeat.replace("\"v\":1", "\"v\":1,\"extra\":1e999"),
        heartbeat.replace("}\n", "} trailing\n"),
        heartbeat.replace("}\n", "}\r\n"),
        format!(
            "DATA [{}]\n",
            heartbeat.trim().strip_prefix("DATA ").unwrap()
        ),
    ];
    for (index, input) in rejected.iter().enumerate() {
        let base = index as u64 * 15_000;
        demo.elapsed_ms.store(base + 14_999, Ordering::SeqCst);
        demo.master.write_all(input.as_bytes()).unwrap();
        loop {
            let (kind, bytes) = demo
                .diagnostics
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            if kind == DiagnosticKind::RejectedData {
                assert_eq!(bytes, input.as_bytes());
                break;
            }
        }
        assert_eq!(demo.live()["gateway"]["available"], true);
        demo.elapsed_ms.store(base + 15_000, Ordering::SeqCst);
        demo.event("gateway_timeout");
        let lost = demo.live();
        assert_eq!(lost["gateway"]["available"], false);
        assert_eq!(lost["indoor"], original);
        demo.utc_ms.store(index as i64, Ordering::SeqCst);
        // Extra fields are allowed on heartbeat records too.
        demo.master
            .write_all(
                heartbeat
                    .replace("\"v\":1", "\"v\":1,\"extra\":[true,null,42]")
                    .as_bytes(),
            )
            .unwrap();
        demo.event("gateway_available");
        assert_eq!(demo.live()["indoor"], original);
    }
    let base = rejected.len() as u64 * 15_000;
    for tick in 1..=5 {
        demo.elapsed_ms.store(base + tick * 5_000, Ordering::SeqCst);
        demo.utc_ms.store(200 + tick as i64, Ordering::SeqCst);
        demo.heartbeat(BOOT);
        let live =
            demo.wait_for(|live| live["gateway"]["last_received_at_unix_ms"] == 200 + tick as i64);
        assert_eq!(live["gateway"]["available"], true);
        assert_eq!(live["indoor"], original);
    }
    assert!(
        demo.diagnostics
            .try_iter()
            .all(|(kind, _)| kind != DiagnosticKind::Operational),
        "steady heartbeats must not repeat transitions"
    );
}

impl Demo {
    fn weather(&mut self, boot: &str, seq: u32, station: u8) -> Value {
        let mut record: Value = serde_json::from_str(
            include_str!("fixtures/weather.data")
                .trim()
                .strip_prefix("DATA ")
                .unwrap(),
        )
        .unwrap();
        record["boot_id"] = boot.into();
        record["seq"] = seq.into();
        record["station_id"] = station.into();
        writeln!(self.master, "DATA {record}").unwrap();
        self.wait_for(|live| {
            live["weather"]["seq"] == seq
                && live["weather"]["boot_id"] == boot
                && live["weather"]["station_id"] == station
        })
    }
}

#[test]
fn weather_continuity_is_independent_of_indoor_station_identity_and_heartbeats() {
    let mut demo = Demo::start();
    demo.heartbeat(BOOT);
    let first = demo.reading(BOOT, 7);
    assert_eq!(first["weather"], Value::Null);
    demo.weather(BOOT, u32::MAX, 191);
    let wrapped = demo.weather(BOOT, 0, 191);
    assert_eq!(
        wrapped["gateway"]["weather"]["observed_missing_readings"],
        0
    );
    let changed = demo.weather(BOOT, 3, 1);
    assert_eq!(changed["gateway"]["restart_count"], 0);
    assert_eq!(
        changed["gateway"]["weather"]["observed_missing_readings"],
        2
    );
    assert_eq!(changed["gateway"]["indoor"]["observed_missing_readings"], 0);
    let event = demo.event("reading_gap");
    assert_eq!(event["stream"], "weather");
    assert_eq!(event["observed_missing"], 2);
    demo.elapsed_ms.store(15_000, Ordering::SeqCst);
    let lost = demo.wait_for(|live| live["gateway"]["available"] == false);
    assert_eq!(lost["weather"], changed["weather"]);
    demo.heartbeat(BOOT);
    let recovered = demo.wait_for(|live| live["gateway"]["available"] == true);
    assert_eq!(recovered["weather"], changed["weather"]);
    assert_eq!(recovered["indoor"], first["indoor"]);
    let next_boot = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    demo.heartbeat(next_boot);
    let restarted = demo.wait_for(|live| live["gateway"]["boot_id"] == next_boot);
    assert_eq!(restarted["gateway"]["weather"]["last_seq"], Value::Null);
    assert_eq!(restarted["gateway"]["indoor"]["last_seq"], Value::Null);
    assert_eq!(restarted["weather"], changed["weather"]);
    let new = demo.weather(next_boot, 99, 1);
    assert_eq!(new["gateway"]["weather"]["observed_missing_readings"], 2);
    assert_eq!(new["gateway"]["restart_count"], 1);
    let third_boot = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let new = demo.weather(third_boot, 1, 1);
    assert_eq!(new["gateway"]["weather"]["observed_missing_readings"], 2);
    assert_eq!(new["gateway"]["restart_count"], 2);
}

#[test]
fn malformed_weather_never_refreshes_health_or_changes_live_values() {
    let mut demo = Demo::start();
    let original = demo.weather(BOOT, 1, 191);
    let fixture = include_str!("fixtures/weather.data");
    let record: Value =
        serde_json::from_str(fixture.trim().strip_prefix("DATA ").unwrap()).unwrap();
    let mut rejected = Vec::new();
    for field in record.as_object().unwrap().keys() {
        let mut missing = record.clone();
        missing.as_object_mut().unwrap().remove(field);
        rejected.push(format!("DATA {missing}\n"));
        let mut wrong_type = record.clone();
        wrong_type[field] = serde_json::json!([]);
        rejected.push(format!("DATA {wrong_type}\n"));
    }
    for (field, invalid) in [
        ("station_id", "256"),
        ("relative_humidity_percent", "-1"),
        ("wind_direction_degrees", "65536"),
        ("uv_microwatts_per_cm2", "65536"),
        ("uv_index", "256"),
        ("lqi", "1.5"),
        ("seq", "4294967296"),
        ("rain_mm", "null"),
        ("rssi_dbm", "null"),
        ("wind_speed_mps", "1e999"),
    ] {
        let mut candidate = record.clone();
        candidate.as_object_mut().unwrap().remove(field);
        rejected.push(format!(
            "DATA {}\n",
            candidate
                .to_string()
                .replace('}', &format!(",\"{field}\":{invalid}}}"))
        ));
    }
    rejected.push(fixture.replace("\"station_id\":191", "\"station_id\":191,\"station_id\":1"));
    demo.elapsed_ms.store(14_999, Ordering::SeqCst);
    for input in rejected {
        demo.master.write_all(input.as_bytes()).unwrap();
        loop {
            let (kind, bytes) = demo
                .diagnostics
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            if kind == DiagnosticKind::RejectedData {
                assert_eq!(bytes, input.as_bytes());
                break;
            }
        }
        assert_eq!(demo.live(), original);
    }
    demo.elapsed_ms.store(15_000, Ordering::SeqCst);
    let lost = demo.wait_for(|live| live["gateway"]["available"] == false);
    assert_eq!(lost["weather"], original["weather"]);
    // Permitted extensions and values outside usual physical ranges are accepted.
    let mut extended = record;
    extended["seq"] = 2.into();
    extended["station_id"] = 255.into();
    extended["relative_humidity_percent"] = 255.into();
    extended["wind_direction_degrees"] = 65535.into();
    extended["uv_microwatts_per_cm2"] = 65535.into();
    extended["uv_index"] = 255.into();
    extended["lqi"] = 255.into();
    extended["temperature_celsius"] = (-300).into();
    extended["extension"] = serde_json::json!({"future": [1, true, null]});
    writeln!(demo.master, "DATA {extended}").unwrap();
    let live = demo.wait_for(|live| live["weather"]["seq"] == 2);
    assert_eq!(live["weather"]["temperature_celsius"], -300.0);
    assert_eq!(live["weather"]["wind_direction_degrees"], 65535);
    assert_eq!(live["gateway"]["available"], true);
}
