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
    time::Duration,
};
use wire::{Framer, IndoorReading};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Raw input offered synchronously in bounded chunks. A future persistence adapter
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
}

#[derive(Clone, Serialize)]
struct LiveIndoorReading {
    #[serde(flatten)]
    reading: IndoorReading,
    received_at_unix_ms: i64,
}

#[derive(Default, Serialize)]
struct Live {
    indoor: Option<LiveIndoorReading>,
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
        mut diagnostic: impl FnMut(Diagnostic<'_>) + Send + 'static,
    ) -> Result<Self, Error> {
        let mut serial = serialport::new(serial_path, 115_200)
            .timeout(Duration::from_millis(100))
            .open()?;
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
            while !serial_stopping.load(Ordering::Relaxed) {
                match serial.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(len) => framer.feed(
                        &bytes[..len],
                        &mut |reading| {
                            let reading = LiveIndoorReading {
                                reading,
                                received_at_unix_ms: utc_unix_ms(),
                            };
                            serial_live.write().unwrap().indoor = Some(reading.clone());
                            let _ = storage_sender.try_send(reading);
                        },
                        &mut diagnostic,
                    ),
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
                        ) => {}
                    Err(_) => break,
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
