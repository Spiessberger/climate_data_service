mod aggregate;
mod health;
mod http;
mod storage;
mod wire;

use log::{Level, debug, error, log, trace, warn};
use serde::Serialize;
use std::{
    io::{self, Read},
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::{SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use wire::{Framer, IndoorReading, Record, WeatherReading};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Raw input in bounded chunks, or one JSON service operational event.
/// Offered synchronously on the serial thread, so callbacks should return quickly.
/// The service also logs every diagnostic through the `log` crate.
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

impl ClimateReading {
    fn describe(&self) -> (&'static str, u32) {
        match self {
            Self::Indoor(reading) => ("indoor", reading.reading.seq),
            Self::Weather(reading) => ("weather", reading.reading.seq),
        }
    }
}

#[derive(Clone, Default, Serialize)]
struct StorageHealth {
    database: storage::DatabaseStatus,
}

#[derive(Clone, Default, Serialize)]
struct Live {
    indoor: Option<LiveIndoorReading>,
    weather: Option<LiveWeatherReading>,
    gateway: health::Gateway,
    storage: StorageHealth,
}

fn operational(event: health::Event, diagnostic: &mut impl FnMut(Diagnostic<'_>)) {
    event.log();
    let bytes = serde_json::to_vec(&event).expect("operational event contains serializable fields");
    diagnostic(Diagnostic {
        kind: DiagnosticKind::Operational,
        bytes: &bytes,
    });
}

fn log_diagnostic(diagnostic: &Diagnostic<'_>) {
    let text = String::from_utf8_lossy(diagnostic.bytes);
    let text = text.trim_end();
    match diagnostic.kind {
        // Logged at the event's own level by `operational`.
        DiagnosticKind::Operational => {}
        DiagnosticKind::Text if text.is_empty() => {}
        DiagnosticKind::Text => {
            let (level, message) = gateway_log_level(text);
            log!(target: "gateway", level, "{message}");
        }
        DiagnosticKind::RejectedData => warn!("Rejected invalid DATA record: {text}"),
        DiagnosticKind::OversizedLine => warn!(
            "Discarding {} bytes of an oversized DATA record",
            diagnostic.bytes.len()
        ),
        DiagnosticKind::PartialLine => warn!("Discarding unterminated serial input: {text}"),
    }
}

/// Gateway firmware logs through esp-println as `LEVEL - message`.
fn gateway_log_level(text: &str) -> (Level, &str) {
    for (prefix, level) in [
        ("ERROR - ", Level::Error),
        ("WARN - ", Level::Warn),
        ("INFO - ", Level::Info),
        ("DEBUG - ", Level::Debug),
        ("TRACE - ", Level::Trace),
    ] {
        if let Some(message) = text.strip_prefix(prefix) {
            return (level, message);
        }
    }
    if text.contains("PANIC") {
        (Level::Error, text)
    } else {
        (Level::Info, text)
    }
}

fn queue_for_storage(sender: &SyncSender<ClimateReading>, reading: ClimateReading) {
    let (stream, seq) = reading.describe();
    match sender.try_send(reading) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            warn!("Storage queue full; dropping {stream} reading seq {seq}")
        }
        Err(TrySendError::Disconnected(_)) => {
            error!("Storage worker stopped; dropping {stream} reading seq {seq}")
        }
    }
}

/// Formats optional weather values, which the station omits when a sensor has no value.
struct Optional<T>(Option<T>);

impl<T: std::fmt::Display> std::fmt::Display for Optional<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(value) => value.fmt(formatter),
            None => formatter.write_str("-"),
        }
    }
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

    /// Starts the service and serves a built web application from `web_root`.
    pub fn start_with_web_root(
        serial_path: &str,
        listener: TcpListener,
        database_path: &Path,
        web_root: PathBuf,
        utc_unix_ms: impl Fn() -> i64 + Send + 'static,
        diagnostic: impl FnMut(Diagnostic<'_>) + Send + 'static,
    ) -> Result<Self, Error> {
        let started = Instant::now();
        Self::start_with_clock_and_web_root(
            serial_path,
            listener,
            database_path,
            Some(web_root),
            utc_unix_ms,
            move || started.elapsed(),
            diagnostic,
        )
    }

    /// `elapsed` is a monotonic clock, independent of UTC reception timestamps.
    /// Diagnostic callbacks run on the serial thread, including operational events.
    pub fn start_with_clock(
        serial_path: &str,
        listener: TcpListener,
        database_path: &Path,
        utc_unix_ms: impl Fn() -> i64 + Send + 'static,
        elapsed: impl Fn() -> Duration + Send + 'static,
        diagnostic: impl FnMut(Diagnostic<'_>) + Send + 'static,
    ) -> Result<Self, Error> {
        Self::start_with_clock_and_web_root(
            serial_path,
            listener,
            database_path,
            None,
            utc_unix_ms,
            elapsed,
            diagnostic,
        )
    }

    fn start_with_clock_and_web_root(
        serial_path: &str,
        listener: TcpListener,
        database_path: &Path,
        web_root: Option<PathBuf>,
        utc_unix_ms: impl Fn() -> i64 + Send + 'static,
        elapsed: impl Fn() -> Duration + Send + 'static,
        mut diagnostic: impl FnMut(Diagnostic<'_>) + Send + 'static,
    ) -> Result<Self, Error> {
        let mut diagnostic = move |event: Diagnostic<'_>| {
            log_diagnostic(&event);
            diagnostic(event);
        };
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
            web_root,
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
                            Err(error) => {
                                debug!("Serial device {serial_path} still unavailable: {error}");
                                retry_at = elapsed() + Duration::from_secs(1);
                            }
                        }
                    }
                    if serial.is_none() {
                        thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                }
                let result = serial.as_mut().unwrap().read(&mut bytes);
                if let Ok(len) = result
                    && len > 0
                {
                    trace!("Read {len} bytes from serial device");
                }
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
                                    debug!(
                                        "Weather reading seq {} from station {}: {} °C, {} %RH, wind {} m/s (gust {}) from {}°, rain {} mm, {} lx, UV {}, battery {}, RSSI {} dBm",
                                        reading.seq,
                                        reading.station_id,
                                        Optional(reading.temperature_celsius),
                                        Optional(reading.relative_humidity_percent),
                                        Optional(reading.wind_speed_mps),
                                        Optional(reading.gust_speed_mps),
                                        Optional(reading.wind_direction_degrees),
                                        reading.rain_mm,
                                        Optional(reading.light_lux),
                                        Optional(reading.uv_index),
                                        if reading.battery_low { "low" } else { "ok" },
                                        reading.rssi_dbm,
                                    );
                                    transitions.extend(live.gateway.weather(reading.seq));
                                    let reading = LiveWeatherReading {
                                        reading,
                                        received_at_unix_ms,
                                    };
                                    live.weather = Some(reading.clone());
                                    drop(live);
                                    queue_for_storage(
                                        &storage_sender,
                                        ClimateReading::Weather(reading),
                                    );
                                }
                                Record::Heartbeat { boot_id } => {
                                    trace!("Gateway heartbeat (boot {boot_id})")
                                }
                                Record::Indoor(reading) => {
                                    debug!(
                                        "Indoor reading seq {}: {} °C, {} %RH",
                                        reading.seq,
                                        reading.temperature_celsius,
                                        reading.relative_humidity_percent,
                                    );
                                    transitions.extend(live.gateway.indoor(reading.seq));
                                    let reading = LiveIndoorReading {
                                        reading,
                                        received_at_unix_ms,
                                    };
                                    live.indoor = Some(reading.clone());
                                    drop(live);
                                    queue_for_storage(
                                        &storage_sender,
                                        ClimateReading::Indoor(reading),
                                    );
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
            debug!("Serial reader stopped");
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
        debug!("Stopping HTTP server");
        self.http.take();
        debug!("Stopping serial reader");
        if let Some(thread) = self.serial_thread.take()
            && thread.join().is_err()
        {
            error!("Serial reader thread panicked");
        }
        debug!("Stopping storage worker");
        self.storage.take();
    }
}
