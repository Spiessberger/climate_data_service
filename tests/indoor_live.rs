use climate_data_service::{DiagnosticKind, Service};
use serde_json::{Value, json};
use serialport::{SerialPort, TTYPort};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

const INDOOR: &str = concat!(
    "DATA {\"v\":1,\"type\":\"indoor\",",
    "\"boot_id\":\"6a9d3c1f80b24e67a511d92cb837046e\",\"seq\":42,",
    "\"temperature_celsius\":21.5,\"relative_humidity_percent\":48.2}\n"
);

struct Demo {
    master: TTYPort,
    address: SocketAddr,
    _service: Service,
    diagnostics: Receiver<(DiagnosticKind, Vec<u8>)>,
}

impl Demo {
    fn start() -> Self {
        let (master, slave) = TTYPort::pair().unwrap();
        let serial_path = slave.name().unwrap();
        drop(slave);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (send, diagnostics) = mpsc::channel();
        let service = Service::start(
            &serial_path,
            listener,
            || 1_800_000_000_123,
            move |event| {
                let _ = send.send((event.kind, event.bytes.to_vec()));
            },
        )
        .unwrap();
        Self {
            master,
            address,
            _service: service,
            diagnostics,
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
        serde_json::from_str(response.split("\r\n\r\n").nth(1).unwrap()).unwrap()
    }

    fn wait_for_seq(&self, seq: u32) -> Value {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let live = self.live();
            if live["indoor"]["seq"] == seq {
                return live;
            }
            assert!(
                Instant::now() < deadline,
                "reading {seq} not received: {live}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }
}

#[test]
fn serial_indoor_reading_is_live_with_utc_reception_time() {
    let mut demo = Demo::start();
    assert_eq!(demo.live(), json!({"indoor": null}));
    demo.master.write_all(b"INFO - CC1101 ready\n").unwrap();
    demo.master.write_all(INDOOR.as_bytes()).unwrap();
    assert_eq!(
        demo.wait_for_seq(42),
        json!({"indoor": {
            "v": 1, "type": "indoor", "boot_id": "6a9d3c1f80b24e67a511d92cb837046e", "seq": 42,
            "temperature_celsius": 21.5, "relative_humidity_percent": 48.2,
            "received_at_unix_ms": 1_800_000_000_123_i64
        }})
    );
}

#[test]
fn invalid_or_unsupported_records_cannot_replace_live_values() {
    let mut demo = Demo::start();
    demo.master.write_all(INDOOR.as_bytes()).unwrap();
    let original = demo.wait_for_seq(42);
    let invalid = [
        "DATA [1,\"indoor\",\"6a9d3c1f80b24e67a511d92cb837046e\",42,21.5,48.2]\n".to_owned(),
        INDOOR.replace("\"v\":1", "\"v\":2"),
        INDOOR.replace("\"v\":1", "\"v\":1.0"),
        INDOOR.replace("\"v\":1", "\"v\":true"),
        INDOOR.replace("indoor", "heartbeat"),
        INDOOR.replace("indoor", "weather"),
        INDOOR.replace("indoor", "future"),
        INDOOR.replace(
            "6a9d3c1f80b24e67a511d92cb837046e",
            "6A9D3C1F80B24E67A511D92CB837046E",
        ),
        INDOOR.replace("6a9d3c1f80b24e67a511d92cb837046e", "abcd"),
        INDOOR.replace("\"seq\":42", "\"seq\":-1"),
        INDOOR.replace("\"seq\":42", "\"seq\":4294967296"),
        INDOOR.replace("\"seq\":42", "\"seq\":42.0"),
        INDOOR.replace("\"seq\":42,", ""),
        INDOOR.replace(
            "\"temperature_celsius\":21.5",
            "\"temperature_celsius\":null",
        ),
        INDOOR.replace(
            "\"temperature_celsius\":21.5",
            "\"temperature_celsius\":\"21.5\"",
        ),
        INDOOR.replace(
            "\"temperature_celsius\":21.5",
            "\"temperature_celsius\":1e999",
        ),
        INDOOR.replace(
            "\"temperature_celsius\":21.5",
            "\"temperature_celsius\":NaN",
        ),
        INDOOR.replace(
            "\"relative_humidity_percent\":48.2",
            "\"relative_humidity_percent\":false",
        ),
        INDOOR.replace("\"v\":1", "\"v\":1,\"v\":1"),
        INDOOR.replace("\"v\":1", "\"v\":1,\"future\":1,\"future\":2"),
        INDOOR.replace("\"v\":1", "\"v\":1,\"future\":{\"x\":1,\"x\":2}"),
        INDOOR.replace("\"v\":1", "\"v\":1,\"future\":[{\"x\":1,\"x\":2}]"),
        INDOOR.replace("}\n", "} trailing\n"),
        INDOOR.replace("}\n", "}\r\n"),
        format!("DATA [{}]\n", INDOOR.trim().strip_prefix("DATA ").unwrap()),
    ];
    for input in invalid {
        demo.master.write_all(input.as_bytes()).unwrap();
        let (kind, bytes) = demo
            .diagnostics
            .recv_timeout(Duration::from_secs(2))
            .expect(&input);
        assert_eq!(kind, DiagnosticKind::RejectedData, "{input}");
        assert_eq!(bytes, input.as_bytes());
        assert_eq!(demo.live(), original, "invalid input changed live: {input}");
    }
}

#[test]
fn extra_fields_and_numeric_extremes_are_accepted_without_range_filtering() {
    let mut demo = Demo::start();
    let input = INDOOR
        .replace(
            "\"v\":1",
            "\"v\":1,\"future\":{\"text\":\"Grüße\\n\",\"values\":[true,null,1,-2,1.5]}",
        )
        .replace("\"seq\":42", "\"seq\":4294967295")
        .replace("21.5", "-300")
        .replace("48.2", "150");
    demo.master.write_all(input.as_bytes()).unwrap();
    let live = demo.wait_for_seq(u32::MAX);
    assert_eq!(live["indoor"]["temperature_celsius"], -300.0);
    assert_eq!(live["indoor"]["relative_humidity_percent"], 150.0);
    assert!(live["indoor"].get("future").is_none());
    demo.master
        .write_all(INDOOR.replace("\"seq\":42", "\"seq\":0").as_bytes())
        .unwrap();
    assert_eq!(demo.wait_for_seq(0)["indoor"]["temperature_celsius"], 21.5);
}

#[test]
fn split_records_wait_for_lf_and_damaged_lines_recover_without_salvage() {
    let mut demo = Demo::start();
    for chunk in INDOOR.as_bytes()[..INDOOR.len() - 1].chunks(7) {
        demo.master.write_all(chunk).unwrap();
    }
    assert_eq!(demo.live(), json!({"indoor": null}));
    demo.master.write_all(b"\n").unwrap();
    let original = demo.wait_for_seq(42);
    let damaged = format!("{}{}", INDOOR.trim_end(), INDOOR);
    demo.master.write_all(damaged.as_bytes()).unwrap();
    assert_eq!(
        demo.diagnostics
            .recv_timeout(Duration::from_secs(2))
            .unwrap(),
        (DiagnosticKind::RejectedData, damaged.into_bytes())
    );
    assert_eq!(demo.live(), original);
    demo.master
        .write_all(INDOOR.replace("\"seq\":42", "\"seq\":44").as_bytes())
        .unwrap();
    demo.wait_for_seq(44);
}

#[test]
fn record_limit_includes_prefix_and_terminator_and_recovers_after_oversize() {
    let mut demo = Demo::start();
    let prefix = INDOOR.trim_end().strip_suffix('}').unwrap();
    let line = format!(
        "{prefix},\"extra\":\"{}\"}}\n",
        "x".repeat(1024 - prefix.len() - 13)
    );
    assert_eq!(line.len(), 1024);
    demo.master.write_all(line.as_bytes()).unwrap();
    let original = demo.wait_for_seq(42);
    let oversized = line.replace("\"extra\":\"", "\"extra\":\"x");
    demo.master.write_all(oversized.as_bytes()).unwrap();
    let mut preserved = Vec::new();
    while preserved.len() < oversized.len() {
        let (kind, bytes) = demo
            .diagnostics
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        assert_eq!(kind, DiagnosticKind::OversizedLine);
        assert!(bytes.len() <= 1024);
        preserved.extend(bytes);
    }
    assert_eq!(preserved, oversized.as_bytes());
    assert_eq!(demo.live(), original);
    demo.master
        .write_all(INDOOR.replace("\"seq\":42", "\"seq\":43").as_bytes())
        .unwrap();
    demo.wait_for_seq(43);
}

#[test]
fn binary_text_and_long_rejected_input_remain_preservable_in_bounded_chunks() {
    let mut demo = Demo::start();
    let mut inputs = vec![
        b"startup \xff\n".to_vec(),
        b"DATA {\"future\":\"\xff\"}\n".to_vec(),
    ];
    let mut invalid_extension = INDOOR
        .replace("\"v\":1", "\"v\":1,\"extra\":\"X\"")
        .into_bytes();
    *invalid_extension
        .iter_mut()
        .find(|byte| **byte == b'X')
        .unwrap() = 0xff;
    inputs.push(invalid_extension);
    inputs.push([b"DATA ".as_slice(), &[b'x'; 50_000], b"\n"].concat());
    inputs.push([&[b'x'; 50_000], b"\n".as_slice()].concat());
    for input in inputs {
        demo.master.write_all(&input).unwrap();
        let mut preserved = Vec::new();
        while preserved.len() < input.len() {
            let (_, bytes) = demo
                .diagnostics
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            assert!(bytes.len() <= 1024);
            preserved.extend(bytes);
        }
        assert_eq!(preserved, input);
        assert_eq!(demo.live(), json!({"indoor": null}));
    }
    demo.master.write_all(INDOOR.as_bytes()).unwrap();
    demo.wait_for_seq(42);
}

#[test]
fn disconnect_offers_unterminated_bytes_to_diagnostics() {
    let mut demo = Demo::start();
    demo.master.write_all(b"DATA {\"v\":1").unwrap();
    // Allow the OS to deliver the partial bytes before destroying the PTY.
    thread::sleep(Duration::from_millis(30));
    assert_eq!(demo.live(), json!({"indoor": null}));
    drop(demo.master);
    assert_eq!(
        demo.diagnostics
            .recv_timeout(Duration::from_secs(2))
            .unwrap(),
        (DiagnosticKind::PartialLine, b"DATA {\"v\":1".to_vec())
    );
}

#[test]
fn a_client_with_an_incomplete_request_body_does_not_block_live_polling() {
    let mut demo = Demo::start();
    let mut slow = TcpStream::connect(demo.address).unwrap();
    slow.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    slow.write_all(b"POST /live HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000000\r\n\r\n")
        .unwrap();
    let mut response = [0; 256];
    assert!(slow.read(&mut response).unwrap() > 0);
    demo.master.write_all(INDOOR.as_bytes()).unwrap();
    demo.wait_for_seq(42);
}
