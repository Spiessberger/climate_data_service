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

fn listening_address(line: &str) -> Option<&str> {
    line.split_once("Listening on http://")
        .map(|(_, address)| address.trim())
}

#[test]
fn absent_serial_keeps_http_running_and_reports_the_transition_on_stdout() {
    let directory = tempfile::tempdir().unwrap();
    let serial_path = directory.path().join("missing-gateway");
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_climate-data-service"))
            .arg("--serial")
            .arg(&serial_path)
            .current_dir(directory.path())
            .args(["--listen", "127.0.0.1:0", "--database"])
            .arg(directory.path().join("climate.sqlite3"))
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = process.0.stdout.take().unwrap();
    let (send, lines) = std::sync::mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if send.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let startup = std::iter::from_fn(|| lines.recv_timeout(Duration::from_secs(2)).ok())
        .find(|line| listening_address(line).is_some())
        .expect("missing startup address");
    let address = listening_address(&startup).unwrap();
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
    let event = std::iter::from_fn(|| lines.recv_timeout(Duration::from_secs(2)).ok())
        .find(|line| line.contains("unavailable"))
        .expect("missing operational transition");
    assert!(event.contains("WARN"), "{event}");
    assert!(event.contains(serial_path.to_str().unwrap()), "{event}");
    assert!(process.0.try_wait().unwrap().is_none());
}

#[test]
fn foreground_process_opens_selected_serial_and_serves_read_only_http() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("history.sqlite3");
    let (mut master, slave) = TTYPort::pair().unwrap();
    let serial = slave.name().unwrap();
    drop(slave);
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_climate-data-service"))
            .args(["--serial", &serial, "--listen", "127.0.0.1:0", "--database"])
            .arg(&database)
            .env("RUST_LOG", "debug")
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut stdout = BufReader::new(process.0.stdout.take().unwrap());
    let mut startup = String::new();
    while listening_address(&startup).is_none() {
        startup.clear();
        assert!(
            stdout.read_line(&mut startup).unwrap() > 0,
            "missing startup address"
        );
    }
    let address = listening_address(&startup).unwrap();
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
    let mut output = String::new();
    stdout.read_to_string(&mut output).unwrap();
    assert!(output.contains("INFO  gateway] foreground log"), "{output}");
    assert!(
        output.contains("ERROR gateway] PANIC: \u{fffd}"),
        "{output}"
    );
    assert!(
        output.contains("Indoor reading seq 1: 21.5 °C, 48.2 %RH"),
        "{output}"
    );
    assert!(
        output.contains("Stored indoor reading seq 1 as row 1"),
        "{output}"
    );
    assert!(output.contains("GET /live -> 200"), "{output}");
    assert!(output.contains("Service stopped"), "{output}");
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
    assert!(!help.contains("--log-dir"));
}
