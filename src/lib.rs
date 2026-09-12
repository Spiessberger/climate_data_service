mod health;
mod http;
mod storage;
mod wire;

use serde::Serialize;
use std::{
    io::{self, Read},
    net::TcpListener,
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use wire::{Framer, IndoorReading, Record};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Raw input in bounded chunks, or one JSON service operational event.
/// Offered synchronously. A future persistence adapter
/// must use a bounded, nonblocking handoff; it must not write files on this thread.
#[derive(Debug)]
pub struct Diagnostic<'a> {
    pub kind: DiagnosticKind,
    pub bytes: &'a [u8],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnosticKind {
    Text,
    RejectedData,
    OversizedLine,
    PartialLine,
    Operational,
}

#[derive(Clone, Serialize)]
struct LiveIndoorReading {
    #[serde(flatten)]
    reading: IndoorReading,
    received_at_unix_ms: i64,
}

#[derive(Clone, Default, Serialize)]
struct Live {
    indoor: Option<LiveIndoorReading>,
    gateway: health::Gateway,
}

fn operational(event: health::Event, diagnostic: &mut impl FnMut(Diagnostic<'_>)) {
    let bytes = serde_json::to_vec(&event).expect("operational event contains serializable fields");
    diagnostic(Diagnostic {
        kind: DiagnosticKind::Operational,
        bytes: &bytes,
    });
}

/// One explicitly selected serial device with read-only live and history HTTP.
pub struct Service {
    stopping: Arc<AtomicBool>,
    serial_thread: Option<JoinHandle<()>>,
    http: Option<http::HttpServer>,
    storage: Option<storage::Storage>,
}

impl Service {
    pub fn start(
        serial_path: &str,
        listener: TcpListener,
        database_path: &Path,
        utc_unix_ms: impl Fn() -> i64 + Send + 'static,
        diagnostic: impl FnMut(Diagnostic<'_>) + Send + 'static,
    ) -> Result<Self, Error> {
        let started = Instant::now();
        Self::start_with_clock(
            serial_path,
            listener,
            database_path,
            utc_unix_ms,
            move || started.elapsed(),
            diagnostic,
        )
    }

    /// `elapsed` is a monotonic clock, independent of UTC reception timestamps.
    /// Diagnostic callbacks must remain nonblocking, including operational events.
    pub fn start_with_clock(
        serial_path: &str,
        listener: TcpListener,
        database_path: &Path,
        utc_unix_ms: impl Fn() -> i64 + Send + 'static,
        elapsed: impl Fn() -> Duration + Send + 'static,
        mut diagnostic: impl FnMut(Diagnostic<'_>) + Send + 'static,
    ) -> Result<Self, Error> {
        let serial_path = serial_path.to_owned();
        let initial_serial = serialport::new(&serial_path, 115_200)
            .timeout(Duration::from_millis(100))
            .open();
        let (storage, history) = storage::Storage::start(database_path)?;
        let storage_sender = storage.sender();
        let live = Arc::new(RwLock::new(Live::default()));
        let http = http::HttpServer::start(listener, Arc::clone(&live), history)?;
        let stopping = Arc::new(AtomicBool::new(false));
        let serial_live = Arc::clone(&live);
        let serial_stopping = Arc::clone(&stopping);
        let serial_thread = thread::spawn(move || {
            let mut framer = Framer::default();
            let mut bytes = [0; 512];
            let mut retry_at = elapsed() + Duration::from_secs(1);
            let mut serial = match initial_serial {
                Ok(serial) => {
                    operational(
                        health::Event::SerialConnected {
                            path: serial_path.clone(),
                        },
                        &mut diagnostic,
                    );
                    Some(serial)
                }
                Err(error) => {
                    operational(
                        health::Event::SerialUnavailable {
                            path: serial_path.clone(),
                            error: error.to_string(),
                        },
                        &mut diagnostic,
                    );
                    None
                }
            };
            while !serial_stopping.load(Ordering::Relaxed) {
                if serial.is_none() {
                    if elapsed() >= retry_at {
                        match serialport::new(&serial_path, 115_200)
                            .timeout(Duration::from_millis(100))
                            .open()
                        {
                            Ok(port) => {
                                serial = Some(port);
                                operational(
                                    health::Event::SerialConnected {
                                        path: serial_path.clone(),
                                    },
                                    &mut diagnostic,
                                );
                            }
                            Err(_) => retry_at = elapsed() + Duration::from_secs(1),
                        }
                    }
                    if serial.is_none() {
                        thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                }
                let result = serial.as_mut().unwrap().read(&mut bytes);
                let now = elapsed();
                let transition = serial_live.write().unwrap().gateway.expire(now);
                if let Some(event) = transition {
                    operational(event, &mut diagnostic);
                }
                let mut transitions = Vec::new();
                match result {
                    Ok(len) if len > 0 => framer.feed(
                        &bytes[..len],
                        &mut |record| {
                            let received_at_unix_ms = utc_unix_ms();
                            let mut live = serial_live.write().unwrap();
                            transitions.extend(live.gateway.receive(
                                record.boot_id(),
                                now,
                                received_at_unix_ms,
                            ));
                            if let Record::Indoor(reading) = record {
                                transitions.extend(live.gateway.indoor(reading.seq));
                                let reading = LiveIndoorReading {
                                    reading,
                                    received_at_unix_ms,
                                };
                                live.indoor = Some(reading.clone());
                                drop(live);
                                let _ = storage_sender.try_send(reading);
                            }
                        },
                        &mut diagnostic,
                    ),
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
                        ) => {}
                    result => {
                        serial_live.write().unwrap().gateway.disconnect();
                        let error = match result {
                            Ok(_) => "end of serial stream".to_owned(),
                            Err(error) => error.to_string(),
                        };
                        operational(
                            health::Event::SerialDisconnected {
                                path: serial_path.clone(),
                                error,
                            },
                            &mut diagnostic,
                        );
                        framer.finish(&mut diagnostic);
                        serial = None;
                        retry_at = now + Duration::from_secs(1);
                    }
                }
                for event in transitions {
                    operational(event, &mut diagnostic);
                }
            }
            framer.finish(&mut diagnostic);
        });
        Ok(Self {
            stopping,
            serial_thread: Some(serial_thread),
            http: Some(http),
            storage: Some(storage),
        })
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Relaxed);
        self.http.take();
        if let Some(thread) = self.serial_thread.take() {
            let _ = thread.join();
        }
        self.storage.take();
    }
}
