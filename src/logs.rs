//! Bounded, nonblocking handoff to daily append-only operational and diagnostic files.
use crate::{Diagnostic, DiagnosticKind};
use chrono::{DateTime, Utc};
use serde_json::json;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

struct Entry {
    received_at_unix_ms: i64,
    kind: DiagnosticKind,
    bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct LogSender(SyncSender<Entry>);

impl LogSender {
    /// Never waits for disk. Saturation drops chunks; there is no replay queue.
    pub fn record(&self, received_at_unix_ms: i64, diagnostic: Diagnostic<'_>) {
        for bytes in diagnostic.bytes.chunks(1024) {
            let _ = self.0.try_send(Entry {
                received_at_unix_ms,
                kind: diagnostic.kind,
                bytes: bytes.to_vec(),
            });
        }
    }
}

/// Successful batches count disk synchronization, never merely successful appends.
#[derive(Clone, Default, Debug)]
pub struct LogStatus {
    pub synchronized_batches: u64,
    pub last_error: Option<String>,
}

/// External filesystem synchronization boundary; also used for directory handles.
pub trait LogSync: Send + 'static {
    fn sync(&mut self, file: &File) -> io::Result<()>;
}

pub struct FileSync;
impl LogSync for FileSync {
    fn sync(&mut self, file: &File) -> io::Result<()> {
        file.sync_all()
    }
}

pub struct DailyLogs {
    stopping: Arc<AtomicBool>,
    status: Arc<Mutex<LogStatus>>,
    sender: Option<LogSender>,
    thread: Option<JoinHandle<()>>,
}

impl DailyLogs {
    /// Filesystem work, including opening the directory, happens on the worker.
    pub fn start(directory: &Path) -> Self {
        let started = Instant::now();
        Self::start_with_clock_and_sync(directory, move || started.elapsed(), FileSync)
    }

    /// The elapsed clock is monotonic and independent of entry reception dates.
    pub fn start_with_clock_and_sync(
        directory: &Path,
        elapsed: impl Fn() -> Duration + Send + 'static,
        mut sync: impl LogSync,
    ) -> Self {
        let directory = directory.to_owned();
        let (sender, receiver) = mpsc::sync_channel::<Entry>(128);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let status = Arc::new(Mutex::new(LogStatus::default()));
        let worker_status = Arc::clone(&status);
        let mut next_sync = elapsed() + Duration::from_secs(60);
        let thread = thread::spawn(move || {
            let mut files: Option<DayFiles> = None;
            loop {
                let entry = if worker_stopping.load(Ordering::Relaxed) {
                    match receiver.try_recv() {
                        Ok(entry) => Some(entry),
                        Err(_) => break,
                    }
                } else {
                    match receiver.recv_timeout(Duration::from_millis(20)) {
                        Ok(entry) => Some(entry),
                        Err(mpsc::RecvTimeoutError::Timeout) => None,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                };
                if let Some(entry) = entry {
                    let result = (|| {
                        let date =
                            DateTime::<Utc>::from_timestamp_millis(entry.received_at_unix_ms)
                                .ok_or_else(|| {
                                    io::Error::other("reception time outside supported UTC range")
                                })?
                                .format("%Y-%m-%d")
                                .to_string();
                        if files.as_ref().is_none_or(|files| files.date != date) {
                            if let Some(mut previous) = files.take() {
                                synchronize(&mut previous, &mut sync, &worker_status);
                            }
                            files = Some(DayFiles::open(&directory, date)?);
                        }
                        files.as_mut().unwrap().append(&entry)
                    })();
                    if let Err(error) = result {
                        worker_status.lock().unwrap().last_error = Some(error.to_string());
                    }
                }
                let now = elapsed();
                if now >= next_sync {
                    if let Some(files) = files.as_mut() {
                        synchronize(files, &mut sync, &worker_status);
                    }
                    next_sync = now + Duration::from_secs(60);
                }
            }
            if let Some(mut files) = files {
                synchronize(&mut files, &mut sync, &worker_status);
            }
        });
        Self {
            sender: Some(LogSender(sender)),
            thread: Some(thread),
            stopping,
            status,
        }
    }

    pub fn status(&self) -> LogStatus {
        self.status.lock().unwrap().clone()
    }

    pub fn sender(&self) -> LogSender {
        self.sender.as_ref().unwrap().clone()
    }
}

impl Drop for DailyLogs {
    fn drop(&mut self) {
        self.sender.take();
        self.stopping.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let deadline = Instant::now() + Duration::from_secs(1);
            while !thread.is_finished() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            if thread.is_finished() {
                let _ = thread.join();
            }
        }
    }
}

struct DirtyFile {
    file: File,
    dirty: bool,
}

impl DirtyFile {
    fn open(path: PathBuf) -> io::Result<Self> {
        Ok(Self {
            file: OpenOptions::new().create(true).append(true).open(path)?,
            dirty: true,
        })
    }

    fn append(&mut self, value: &serde_json::Value) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(value)?;
        bytes.push(b'\n');
        // A failed write may still have changed the file.
        self.dirty = true;
        self.file.write_all(&bytes)
    }

    fn sync(&mut self, sync: &mut impl LogSync) -> io::Result<()> {
        if self.dirty {
            sync.sync(&self.file)?;
            self.dirty = false;
        }
        Ok(())
    }
}

struct DayFiles {
    date: String,
    directory: PathBuf,
    directory_dirty: bool,
    operational: DirtyFile,
    diagnostic: DirtyFile,
}

impl DayFiles {
    fn open(directory: &Path, date: String) -> io::Result<Self> {
        fs::create_dir_all(directory)?;
        Ok(Self {
            directory: directory.to_owned(),
            directory_dirty: true,
            operational: DirtyFile::open(directory.join(format!("{date}.operational.jsonl")))?,
            diagnostic: DirtyFile::open(directory.join(format!("{date}.diagnostic.jsonl")))?,
            date,
        })
    }

    fn append(&mut self, entry: &Entry) -> io::Result<()> {
        if entry.kind != DiagnosticKind::Operational {
            self.diagnostic.append(&json!({
                "received_at_unix_ms": entry.received_at_unix_ms,
                "kind": format!("{:?}", entry.kind),
                "bytes": entry.bytes,
            }))?;
        }
        let message = match entry.kind {
            DiagnosticKind::Operational | DiagnosticKind::Text => {
                String::from_utf8_lossy(&entry.bytes).into_owned()
            }
            kind => format!(
                "{kind:?}: {} input bytes preserved in diagnostics",
                entry.bytes.len()
            ),
        };
        self.operational.append(&json!({
            "received_at_unix_ms": entry.received_at_unix_ms,
            "source": if entry.kind == DiagnosticKind::Text { "gateway" } else { "service" },
            "message": message,
        }))
    }

    fn sync(&mut self, sync: &mut impl LogSync) -> io::Result<bool> {
        let dirty = self.operational.dirty || self.diagnostic.dirty || self.directory_dirty;
        // Attempt both files even if one fails. Failed files remain dirty.
        let operational = self.operational.sync(sync);
        let diagnostic = self.diagnostic.sync(sync);
        operational?;
        diagnostic?;
        if self.directory_dirty {
            // Synchronize leaf before parents, including ancestors potentially
            // created by create_dir_all. Canonicalization handles relative paths.
            let directory = self.directory.canonicalize()?;
            for ancestor in directory.ancestors() {
                sync.sync(&File::open(ancestor)?)?;
            }
            self.directory_dirty = false;
        }
        Ok(dirty)
    }
}

fn synchronize(files: &mut DayFiles, sync: &mut impl LogSync, status: &Mutex<LogStatus>) {
    let result = files.sync(sync);
    let mut status = status.lock().unwrap();
    match result {
        Ok(true) => {
            status.synchronized_batches += 1;
            status.last_error = None;
        }
        Ok(false) => {}
        Err(error) => status.last_error = Some(error.to_string()),
    }
}
