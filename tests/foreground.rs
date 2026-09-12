use serialport::{SerialPort, TTYPort};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn absent_serial_keeps_http_running_and_reports_the_transition_on_stderr() {
    let directory = tempfile::tempdir().unwrap();
    let serial_path = directory.path().join("missing-gateway");
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_climate-data-service"))
            .arg("--serial")
            .arg(&serial_path)
            .current_dir(directory.path())
            .args(["--listen", "127.0.0.1:0", "--database"])
            .arg(directory.path().join("climate.sqlite3"))
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stderr = process.0.stderr.take().unwrap();
    let (send, lines) = std::sync::mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            if send.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let startup = lines.recv_timeout(Duration::from_secs(2)).unwrap();
    let address = startup
        .strip_prefix("Listening on http://")
        .expect(&startup);
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
    let live: serde_json::Value =
        serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(live["gateway"]["available"], false);
    assert_eq!(live["indoor"], serde_json::Value::Null);
    let event = lines
        .recv_timeout(Duration::from_secs(2))
        .expect("missing operational transition");
    assert!(event.contains("serial_unavailable"), "{event}");
    assert!(event.contains(serial_path.to_str().unwrap()), "{event}");
    assert!(process.0.try_wait().unwrap().is_none());
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let retained = std::fs::read_dir(directory.path().join("logs"))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with("operational.jsonl")
            })
            .any(|entry| {
                std::fs::read_to_string(entry.path())
                    .unwrap()
                    .contains("serial_unavailable")
            });
        if retained {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "default log directory did not retain the service event"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn foreground_process_opens_selected_serial_and_serves_read_only_http() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("history.sqlite3");
    let log_dir = directory.path().join("chosen-logs");
    let (mut master, slave) = TTYPort::pair().unwrap();
    let serial = slave.name().unwrap();
    drop(slave);
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_climate-data-service"))
            .args(["--serial", &serial, "--listen", "127.0.0.1:0", "--database"])
            .arg(&database)
            .arg("--log-dir")
            .arg(&log_dir)
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut startup = String::new();
    BufReader::new(process.0.stderr.take().unwrap())
        .read_line(&mut startup)
        .unwrap();
    let address = startup
        .trim()
        .strip_prefix("Listening on http://")
        .expect(&startup);
    let request = |method: &str, path: &str| {
        let mut stream = TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        write!(stream, "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    };
    let response = request("GET", "/live");
    let live: serde_json::Value =
        serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(live["indoor"], serde_json::Value::Null);
    assert_eq!(live["gateway"]["available"], false);
    assert!(request("POST", "/live").starts_with("HTTP/1.1 405"));
    assert!(request("GET", "/missing").starts_with("HTTP/1.1 404"));
    master
        .write_all(b"INFO - foreground log\nPANIC: \xff\n")
        .unwrap();
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    master.write_all(b"DATA {\"v\":1,\"type\":\"indoor\",\"boot_id\":\"6a9d3c1f80b24e67a511d92cb837046e\",\"seq\":1,\"temperature_celsius\":21.5,\"relative_humidity_percent\":48.2}\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let response = request("GET", "/live");
        let value: serde_json::Value =
            serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        if let Some(received) = value["indoor"]["received_at_unix_ms"].as_i64() {
            let after = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!((before..=after).contains(&received));
            assert_eq!(value["indoor"]["temperature_celsius"], 21.5);
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(5));
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let response = request("GET", "/history/indoor/updates?after_id=0&limit=10");
        let value: serde_json::Value =
            serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        if value["readings"]
            .as_array()
            .is_some_and(|rows| rows.len() == 1)
        {
            assert_eq!(value["readings"][0]["seq"], 1);
            break;
        }
        assert!(Instant::now() < deadline, "reading was not stored: {value}");
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        Command::new("kill")
            .args(["-TERM", &process.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "shutdown failed: {status}");
            break;
        }
        assert!(Instant::now() < deadline, "shutdown did not complete");
        thread::sleep(Duration::from_millis(5));
    }
    let diagnostic = std::fs::read_dir(&log_dir)
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .ends_with("diagnostic.jsonl")
        })
        .unwrap();
    let retained: Vec<u8> = std::fs::read_to_string(diagnostic.path())
        .unwrap()
        .lines()
        .flat_map(|line| {
            let record: serde_json::Value = serde_json::from_str(line).unwrap();
            record["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|byte| byte.as_u64().unwrap() as u8)
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(retained, b"INFO - foreground log\nPANIC: \xff\n");
}

#[test]
fn serial_selection_is_required_and_http_defaults_to_loopback() {
    let missing = Command::new(env!("CARGO_BIN_EXE_climate-data-service"))
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(
        String::from_utf8(missing.stderr)
            .unwrap()
            .contains("--serial")
    );
    let help = Command::new(env!("CARGO_BIN_EXE_climate-data-service"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    assert!(help.contains("127.0.0.1:8080"));
    assert!(help.contains("./data/climate.sqlite3"));
    assert!(help.contains("./logs"));
    assert!(help.contains("--log-dir"));
}
