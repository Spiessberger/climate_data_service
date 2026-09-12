mod health;
mod http;
pub mod logs;
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
use wire::{Framer, IndoorReading, Record, WeatherReading};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Raw input in bounded chunks, or one JSON service operational event.
/// Offered synchronously. Persistence adapters must use a bounded, nonblocking
/// handoff (such as `logs::LogSender`); never write files on this thread.
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

#[derive(Clone, Serialize)]
struct LiveWeatherReading {
    #[serde(flatten)]
    reading: WeatherReading,
    received_at_unix_ms: i64,
}

#[derive(Clone, Serialize)]
#[serde(untagged)]
enum ClimateReading {
    Indoor(LiveIndoorReading),
    Weather(LiveWeatherReading),
}

#[derive(Clone, Default, Serialize)]
struct LogHealth {
    available: bool,
    last_error: Option<String>,
}

#[derive(Clone, Default, Serialize)]
struct StorageHealth {
    database: storage::DatabaseStatus,
    logs: LogHealth,
}

#[derive(Clone, Default, Serialize)]
struct Live {
    indoor: Option<LiveIndoorReading>,
    weather: Option<LiveWeatherReading>,
    gateway: health::Gateway,
    storage: StorageHealth,
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

    /// Starts the service with the daily-log worker's shared health state.
    pub fn start_with_log_status(
        serial_path: &str,
        listener: TcpListener,
        database_path: &Path,
        log_status: logs::LogStatusHandle,
        utc_unix_ms: impl Fn() -> i64 + Send + 'static,
        diagnostic: impl FnMut(Diagnostic<'_>) + Send + 'static,
    ) -> Result<Self, Error> {
        let started = Instant::now();
        Self::start_with_clock_and_status(
            serial_path,
            listener,
            database_path,
            Some(log_status),
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
        diagnostic: impl FnMut(Diagnostic<'_>) + Send + 'static,
    ) -> Result<Self, Error> {
        Self::start_with_clock_and_status(
            serial_path,
            listener,
            database_path,
            None,
            utc_unix_ms,
            elapsed,
            diagnostic,
        )
    }

    pub fn start_with_clock_and_status(
        serial_path: &str,
        listener: TcpListener,
        database_path: &Path,
        log_status: Option<logs::LogStatusHandle>,
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
        let database_status = storage.status();
        let live = Arc::new(RwLock::new(Live::default()));
        let http = http::HttpServer::start(
            listener,
            Arc::clone(&live),
            history,
            database_status,
            log_status,
        )?;
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
                            match record {
                                Record::Weather(reading) => {
                                    transitions.extend(live.gateway.weather(reading.seq));
                                    let reading = LiveWeatherReading {
                                        reading,
                                        received_at_unix_ms,
                                    };
                                    live.weather = Some(reading.clone());
                                    drop(live);
                                    let _ =
                                        storage_sender.try_send(ClimateReading::Weather(reading));
                                }
                                Record::Heartbeat { .. } => {}
                                Record::Indoor(reading) => {
                                    transitions.extend(live.gateway.indoor(reading.seq));
                                    let reading = LiveIndoorReading {
                                        reading,
                                        received_at_unix_ms,
                                    };
                                    live.indoor = Some(reading.clone());
                                    drop(live);
                                    let _ =
                                        storage_sender.try_send(ClimateReading::Indoor(reading));
                                }
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
