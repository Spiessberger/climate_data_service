use climate_data_service::Service;
use rusqlite::Connection;
use serde_json::Value;
use serialport::{SerialPort, TTYPort};
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};

const BOOT: &str = "6a9d3c1f80b24e67a511d92cb837046e";

struct Demo {
    master: TTYPort,
    address: SocketAddr,
    service: Option<Service>,
}

impl Demo {
    fn start(database_path: std::path::PathBuf) -> Self {
        let (master, slave) = TTYPort::pair().unwrap();
        let serial_path = slave.name().unwrap();
        drop(slave);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let service = Service::start(
            &serial_path,
            listener,
            &database_path,
            || 1_800_000_000_123,
            |_| {},
        )
        .unwrap();
        Self {
            master,
            address,
            service: Some(service),
        }
    }

    fn live(&self) -> Value {
        let mut client = TcpStream::connect(self.address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        client
            .write_all(b"GET /live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    fn history(&self) -> (u16, Value) {
        let mut client = TcpStream::connect(self.address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        client
            .write_all(
                b"GET /history/indoor/updates?after_id=0 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        let status = response[9..12].parse().unwrap();
        let body = serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
        (status, body)
    }

    fn send_reading(&mut self, seq: u32) {
        writeln!(
            self.master,
            "DATA {{\"v\":1,\"type\":\"indoor\",\"boot_id\":\"{BOOT}\",\"seq\":{seq},\"temperature_celsius\":21.5,\"relative_humidity_percent\":48.2}}"
        )
        .unwrap();
    }

    fn send_heartbeat(&mut self) {
        writeln!(
            self.master,
            "DATA {{\"v\":1,\"type\":\"heartbeat\",\"boot_id\":\"{BOOT}\"}}"
        )
        .unwrap();
    }

    fn wait_for(&self, predicate: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            let live = self.live();
            if predicate(&live) {
                return live;
            }
            assert!(Instant::now() < deadline, "unexpected live state: {live}");
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Demo {
    fn drop(&mut self) {
        self.service.take();
    }
}

#[test]
fn unavailable_database_does_not_stop_live_and_recovers_without_replaying_failed_readings() {
    let directory = tempfile::tempdir().unwrap();
    let blocked_parent = directory.path().join("database-parent");
    fs::write(&blocked_parent, b"occupied").unwrap();
    let database_path = blocked_parent.join("climate.sqlite3");
    let mut demo = Demo::start(database_path);

    let initial = demo.wait_for(|live| live["storage"]["database"]["last_error"].is_string());
    assert_eq!(initial["storage"]["database"]["available"], false);
    demo.send_reading(1);
    let live = demo.wait_for(|live| live["indoor"]["seq"] == 1);
    assert_eq!(live["gateway"]["available"], true);
    assert_eq!(demo.history().0, 500);

    fs::remove_file(&blocked_parent).unwrap();
    fs::create_dir(&blocked_parent).unwrap();
    demo.wait_for(|live| live["storage"]["database"]["available"] == true);
    demo.send_reading(2);
    demo.wait_for(|live| live["indoor"]["seq"] == 2);
    let deadline = Instant::now() + Duration::from_secs(3);
    let history = loop {
        let (status, body) = demo.history();
        if status == 200 && body["readings"].as_array().unwrap().len() == 1 {
            break body;
        }
        assert!(
            Instant::now() < deadline,
            "history did not recover: {status} {body}"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(history["readings"][0]["seq"], 2);
}

#[test]
fn blocked_database_keeps_live_and_heartbeat_handling_independent_and_drops_failed_write() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("climate.sqlite3");
    let mut demo = Demo::start(database_path.clone());
    demo.send_reading(1);
    demo.wait_for(|live| live["indoor"]["seq"] == 1);

    let blocker = Connection::open(&database_path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    demo.send_reading(2);
    demo.send_heartbeat();
    let live = demo.wait_for(|live| {
        live["indoor"]["seq"] == 2
            && live["gateway"]["available"] == true
            && live["storage"]["database"]["available"] == false
    });
    assert_eq!(live["indoor"]["seq"], 2);
    assert_eq!(demo.history().0, 200);
    assert_eq!(demo.history().1["readings"][0]["seq"], 1);
    blocker.execute_batch("ROLLBACK").unwrap();

    demo.wait_for(|live| live["storage"]["database"]["available"] == true);
    demo.send_reading(3);
    demo.wait_for(|live| live["indoor"]["seq"] == 3);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let (status, body) = demo.history();
        if status == 200 && body["readings"].as_array().unwrap().len() == 2 {
            assert_eq!(body["readings"][1]["seq"], 3);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "recovered write did not persist: {status} {body}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}
