use clap::Parser;
use climate_data_service::{DiagnosticKind, Service};
use std::{
    io::Write,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::mpsc,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Parser)]
#[command(
    version,
    about = "Receive gateway indoor readings over USB and serve live and historical HTTP"
)]
struct Config {
    /// Explicit Linux serial device (or stable device symlink); never auto-selected
    #[arg(long)]
    serial: PathBuf,
    /// Read-only, unauthenticated HTTP address on a trusted network
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    /// Local SQLite database containing retained climate history
    #[arg(long, default_value = "./data/climate.sqlite3")]
    database: PathBuf,
}

fn utc_unix_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(error) => -(error.duration().as_millis() as i64),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::parse();
    let listener = TcpListener::bind(config.listen)?;
    let address = listener.local_addr()?;
    let (shutdown, receive_shutdown) = mpsc::sync_channel(1);
    ctrlc::set_handler(move || {
        let _ = shutdown.try_send(());
    })?;
    let (log_sender, log_events) = mpsc::sync_channel(128);
    let _service = Service::start(
        config
            .serial
            .to_str()
            .ok_or("serial path must be valid UTF-8")?,
        listener,
        &config.database,
        utc_unix_ms,
        // Daily raw diagnostic persistence is supplied by the log slice.
        // A blocked stderr must not stall serial ingestion or health deadlines.
        move |diagnostic| {
            if diagnostic.kind == DiagnosticKind::Operational {
                let _ = log_sender.try_send((utc_unix_ms(), diagnostic.bytes.to_vec()));
            }
        },
    )?;
    eprintln!("Listening on http://{address}");
    // Intentionally not joined: a stalled output sink cannot prevent shutdown.
    std::thread::spawn(move || {
        for (received_at_unix_ms, bytes) in log_events {
            let _ = writeln!(
                std::io::stderr().lock(),
                "INFO - {received_at_unix_ms} {}",
                String::from_utf8_lossy(&bytes)
            );
        }
    });
    receive_shutdown.recv()?;
    Ok(())
}
