use clap::Parser;
use climate_data_service::Service;
use log::{error, info};
use std::{
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
    /// Built web application directory (the contents of weatherstation_web/dist)
    #[arg(long)]
    web_root: Option<PathBuf>,
}

fn utc_unix_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(error) => -(error.duration().as_millis() as i64),
    }
}

fn main() {
    let config = Config::parse();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .target(env_logger::Target::Stdout)
        .format_timestamp_millis()
        .init();
    if let Err(error) = run(config) {
        error!("Service failed: {error}");
        std::process::exit(1);
    }
}

fn run(config: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(
        "climate-data-service {} starting (serial {}, database {})",
        env!("CARGO_PKG_VERSION"),
        config.serial.display(),
        config.database.display()
    );
    let listener = TcpListener::bind(config.listen)?;
    let address = listener.local_addr()?;
    let (shutdown, receive_shutdown) = mpsc::sync_channel(1);
    ctrlc::set_handler(move || {
        let _ = shutdown.try_send(());
    })?;
    let serial = config
        .serial
        .to_str()
        .ok_or("serial path must be valid UTF-8")?;
    // The socket is already bound, so clients may connect before the service starts.
    info!("Listening on http://{address}");
    let service = match config.web_root {
        Some(web_root) => Service::start_with_web_root(
            serial,
            listener,
            &config.database,
            web_root,
            utc_unix_ms,
            // The service logs every diagnostic itself.
            |_| {},
        )?,
        None => Service::start(
            serial,
            listener,
            &config.database,
            utc_unix_ms,
            // The service logs every diagnostic itself.
            |_| {},
        )?,
    };
    receive_shutdown.recv()?;
    info!("Shutdown requested");
    drop(service);
    info!("Service stopped");
    Ok(())
}
