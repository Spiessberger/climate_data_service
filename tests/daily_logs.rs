use climate_data_service::{Service, logs::DailyLogs};
use serde_json::Value;
use serialport::{SerialPort, TTYPort};
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    thread,
    time::{Duration, Instant},
};

const INDOOR: &[u8] = b"DATA {\"v\":1,\"type\":\"indoor\",\"boot_id\":\"6a9d3c1f80b24e67a511d92cb837046e\",\"seq\":42,\"temperature_celsius\":21.5,\"relative_humidity_percent\":48.2}\n";

fn live(address: SocketAddr) -> Value {
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .write_all(b"GET /live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(Instant::now() < deadline, "condition not reached");
        thread::sleep(Duration::from_millis(5));
    }
}

fn records(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

fn raw_bytes(path: &Path) -> Vec<u8> {
    records(path)
        .iter()
        .flat_map(|record| {
            record["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|byte| byte.as_u64().unwrap() as u8)
        })
        .collect()
}

fn start_service(directory: &Path, logs: &DailyLogs) -> (TTYPort, SocketAddr, Service) {
    let sender = logs.sender();
    let (master, slave) = TTYPort::pair().unwrap();
    let serial = slave.name().unwrap();
    drop(slave);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let service = Service::start(
        &serial,
        listener,
        &directory.join("db.sqlite3"),
        || 1_789_171_200_123,
        move |event| sender.record(1_789_171_200_123, event),
    )
    .unwrap();
    (master, address, service)
}

#[test]
fn mixed_serial_input_is_retained_losslessly_while_valid_readings_stay_live() {
    let directory = tempfile::tempdir().unwrap();
    let logs = DailyLogs::start(&directory.path().join("logs"));
    let (mut master, address, service) = start_service(directory.path(), &logs);
    let input = b"INFO - CC1101 ready\nstartup \xff\nPANIC: \x00\xfe\nDATA {broken}\n";
    master.write_all(input).unwrap();
    master.write_all(INDOOR).unwrap();
    wait_until(|| live(address)["indoor"]["seq"] == 42);
    let diagnostics = directory.path().join("logs/2026-09-12.diagnostic.jsonl");
    wait_until(|| raw_bytes(&diagnostics) == input);
    let operational = records(&directory.path().join("logs/2026-09-12.operational.jsonl"));
    assert!(
        operational
            .iter()
            .any(|record| record["message"].as_str().unwrap().contains("CC1101 ready"))
    );
    assert!(operational.iter().any(|record| {
        record["message"]
            .as_str()
            .unwrap()
            .contains("serial_connected")
    }));
    assert!(
        operational
            .iter()
            .any(|record| record["message"].as_str().unwrap().contains("RejectedData"))
    );
    for record in records(&diagnostics).iter().chain(operational.iter()) {
        assert_eq!(record["received_at_unix_ms"], 1_789_171_200_123_i64);
    }
    drop(service);
    drop(logs);
}

#[test]
fn damaged_and_long_lines_recover_at_lf_and_disconnect_preserves_the_partial_tail() {
    let directory = tempfile::tempdir().unwrap();
    let logs = DailyLogs::start(&directory.path().join("logs"));
    let (mut master, address, service) = start_service(directory.path(), &logs);
    let diagnostic_path = directory.path().join("logs/2026-09-12.diagnostic.jsonl");
    let operational_path = directory.path().join("logs/2026-09-12.operational.jsonl");
    let ordinary = [b"INFO - ".as_slice(), &[b'x'; 50_000], b"\n"].concat();
    let oversized = [b"DATA ".as_slice(), &[b'x'; 50_000], INDOOR].concat();
    let damaged = [&INDOOR[..INDOOR.len() - 1], INDOOR].concat();
    let unsupported = String::from_utf8(INDOOR.to_vec())
        .unwrap()
        .replace("\"v\":1", "\"v\":2");
    let duplicate = String::from_utf8(INDOOR.to_vec())
        .unwrap()
        .replace("\"v\":1", "\"v\":1,\"v\":1");
    let mut preserved = Vec::new();
    for input in [
        &ordinary[..],
        &oversized,
        &damaged,
        unsupported.as_bytes(),
        duplicate.as_bytes(),
    ] {
        // Pace the fixture at the persistence boundary; a deliberately saturated
        // handoff is a different (loss-accepting) storage-stall scenario.
        for chunk in input.chunks(1024) {
            master.write_all(chunk).unwrap();
            preserved.extend_from_slice(chunk);
            let complete = preserved.len() - preserved.len() % 1024;
            if input.len() > 1024 {
                wait_until(|| raw_bytes(&diagnostic_path).len() >= complete);
            }
        }
        wait_until(|| raw_bytes(&diagnostic_path) == preserved);
        assert_eq!(live(address)["indoor"], Value::Null);
    }
    let text: String = records(&operational_path)
        .iter()
        .filter(|record| record["source"] == "gateway")
        .map(|record| record["message"].as_str().unwrap())
        .collect();
    assert!(
        text.as_bytes() == ordinary,
        "long operational text was not preserved"
    );
    let extended = String::from_utf8(INDOOR.to_vec())
        .unwrap()
        .replace("\"v\":1", "\"v\":1,\"extra\":{\"ok\":[true,null,1]}");
    master.write_all(extended.as_bytes()).unwrap();
    wait_until(|| live(address)["indoor"]["seq"] == 42);
    // The complete heartbeat proves the preceding bytes were consumed before disconnect.
    master.write_all(b"DATA {\"v\":1,\"type\":\"heartbeat\",\"boot_id\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}\nDATA {\"v\":1").unwrap();
    wait_until(|| live(address)["gateway"]["boot_id"] == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    thread::sleep(Duration::from_millis(30));
    drop(master);
    preserved.extend_from_slice(b"DATA {\"v\":1");
    wait_until(|| raw_bytes(&diagnostic_path) == preserved);
    assert_eq!(live(address)["indoor"]["seq"], 42);
    assert_eq!(
        records(&diagnostic_path).last().unwrap()["kind"],
        "PartialLine"
    );
    drop(service);
    drop(logs);
}

#[test]
fn utc_rollover_clock_rollback_and_restart_append_to_retained_dates() {
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    let directory = tempfile::tempdir().unwrap();
    let log_dir = directory.path().join("logs");
    let clock = Arc::new(AtomicI64::new(1_789_171_199_999)); // 2026-09-11 23:59:59.999 UTC
    let run = |messages: &[(i64, &[u8])]| {
        let logs = DailyLogs::start(&log_dir);
        let sender = logs.sender();
        let (mut master, slave) = TTYPort::pair().unwrap();
        let serial = slave.name().unwrap();
        drop(slave);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let utc = Arc::clone(&clock);
        let log_utc = Arc::clone(&clock);
        let service = Service::start(
            &serial,
            listener,
            &directory.path().join("db.sqlite3"),
            move || utc.load(Ordering::Relaxed),
            move |event| sender.record(log_utc.load(Ordering::Relaxed), event),
        )
        .unwrap();
        assert_eq!(live(address)["indoor"], Value::Null);
        for &(timestamp, input) in messages {
            clock.store(timestamp, Ordering::Relaxed);
            master.write_all(input).unwrap();
            let day = if timestamp < 1_789_171_200_000 {
                "2026-09-11"
            } else {
                "2026-09-12"
            };
            wait_until(|| {
                raw_bytes(&log_dir.join(format!("{day}.diagnostic.jsonl"))).ends_with(input)
            });
        }
        master.write_all(INDOOR).unwrap();
        wait_until(|| live(address)["indoor"]["seq"] == 42);
        drop(service);
        drop(logs);
    };
    run(&[
        (1_789_171_199_999, b"INFO - before midnight\n"),
        (1_789_171_200_000, b"INFO - after midnight\n"),
        (1_789_171_199_000, b"INFO - clock moved backward\n"),
    ]);
    let before = fs::read(log_dir.join("2026-09-11.operational.jsonl")).unwrap();
    run(&[(1_789_171_199_500, b"INFO - service restarted\n")]);
    assert!(
        fs::read(log_dir.join("2026-09-11.operational.jsonl"))
            .unwrap()
            .starts_with(&before)
    );
    assert_eq!(
        raw_bytes(&log_dir.join("2026-09-11.diagnostic.jsonl")),
        b"INFO - before midnight\nINFO - clock moved backward\nINFO - service restarted\n"
    );
    assert_eq!(
        raw_bytes(&log_dir.join("2026-09-12.diagnostic.jsonl")),
        b"INFO - after midnight\n"
    );
    assert_eq!(fs::read_dir(&log_dir).unwrap().count(), 4);
}

#[test]
fn dirty_files_sync_each_minute_and_blocked_sync_does_not_block_live_http() {
    use climate_data_service::logs::LogSync;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    };
    struct ControlledSync {
        observations: Arc<Mutex<Vec<bool>>>,
        block: mpsc::Receiver<()>,
    }
    impl LogSync for ControlledSync {
        fn sync(&mut self, file: &fs::File) -> std::io::Result<()> {
            self.observations
                .lock()
                .unwrap()
                .push(file.metadata()?.is_dir());
            if self.observations.lock().unwrap().len() == 1 {
                self.block.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            file.sync_all()
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let log_dir = directory.path().join("new/nested/logs");
    let elapsed = Arc::new(AtomicU64::new(0));
    let worker_clock = Arc::clone(&elapsed);
    let observations = Arc::new(Mutex::new(Vec::new()));
    let (unblock, blocked) = mpsc::channel();
    let logs = DailyLogs::start_with_clock_and_sync(
        &log_dir,
        move || Duration::from_millis(worker_clock.load(Ordering::Relaxed)),
        ControlledSync {
            observations: Arc::clone(&observations),
            block: blocked,
        },
    );
    let (mut master, address, service) = start_service(directory.path(), &logs);
    master.write_all(b"INFO - dirty\n").unwrap();
    wait_until(|| raw_bytes(&log_dir.join("2026-09-12.diagnostic.jsonl")) == b"INFO - dirty\n");
    elapsed.store(59_999, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(60));
    assert!(observations.lock().unwrap().is_empty());
    elapsed.store(60_000, Ordering::Relaxed);
    wait_until(|| !observations.lock().unwrap().is_empty());
    // Exceed the bounded log handoff while the external sync is stalled.
    master
        .write_all(&b"INFO - still receiving\n".repeat(500))
        .unwrap();
    master.write_all(INDOOR).unwrap();
    wait_until(|| live(address)["indoor"]["seq"] == 42);
    assert_eq!(logs.status().synchronized_batches, 0);
    unblock.send(()).unwrap();
    wait_until(|| logs.status().synchronized_batches == 1);
    assert!(
        observations
            .lock()
            .unwrap()
            .iter()
            .filter(|is_dir| **is_dir)
            .count()
            >= 4,
        "new directory chain must be synchronized too"
    );
    // A marker observes completion of all accepted queued writes before the next tick.
    master.write_all(b"INFO - after stall\n").unwrap();
    wait_until(|| {
        raw_bytes(&log_dir.join("2026-09-12.diagnostic.jsonl")).ends_with(b"INFO - after stall\n")
    });
    elapsed.store(120_000, Ordering::Relaxed);
    wait_until(|| logs.status().synchronized_batches == 2);
    let syncs = observations.lock().unwrap().len();
    elapsed.store(180_000, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(60));
    assert_eq!(
        observations.lock().unwrap().len(),
        syncs,
        "clean files need no sync"
    );
    master.write_all(b"INFO - shutdown flush\n").unwrap();
    wait_until(|| {
        raw_bytes(&log_dir.join("2026-09-12.diagnostic.jsonl"))
            .ends_with(b"INFO - shutdown flush\n")
    });
    drop(service);
    drop(logs);
    assert!(observations.lock().unwrap().len() > syncs);
}

#[test]
fn failed_sync_remains_unconfirmed_and_retries_dirty_files_without_new_input() {
    use climate_data_service::logs::LogSync;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    struct FailOnce(bool);
    impl LogSync for FailOnce {
        fn sync(&mut self, file: &fs::File) -> std::io::Result<()> {
            if self.0 {
                self.0 = false;
                return Err(std::io::Error::other("injected disk sync failure"));
            }
            file.sync_all()
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let elapsed = Arc::new(AtomicU64::new(0));
    let worker_clock = Arc::clone(&elapsed);
    let logs = DailyLogs::start_with_clock_and_sync(
        &directory.path().join("logs"),
        move || Duration::from_secs(worker_clock.load(Ordering::Relaxed)),
        FailOnce(true),
    );
    let (mut master, address, service) = start_service(directory.path(), &logs);
    master.write_all(b"INFO - sync me\n").unwrap();
    wait_until(|| {
        raw_bytes(&directory.path().join("logs/2026-09-12.diagnostic.jsonl")) == b"INFO - sync me\n"
    });
    elapsed.store(60, Ordering::Relaxed);
    wait_until(|| logs.status().last_error.is_some());
    assert_eq!(logs.status().synchronized_batches, 0);
    assert!(
        logs.status()
            .last_error
            .unwrap()
            .contains("injected disk sync failure")
    );
    assert_eq!(live(address)["indoor"], Value::Null);
    elapsed.store(120, Ordering::Relaxed);
    wait_until(|| logs.status().synchronized_batches == 1);
    assert!(logs.status().last_error.is_none());
    master.write_all(INDOOR).unwrap();
    wait_until(|| live(address)["indoor"]["seq"] == 42);
    drop(service);
    drop(logs);
}
