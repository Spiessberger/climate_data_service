use crate::{
    Error, Live, StorageHealth,
    aggregate::{AggregateError, MAX_SUMMARY_POINTS, SummaryRequest},
    storage::{DatabaseStatusHandle, History, RangeCursor, StoredReading, Stream},
};
use chrono::NaiveDate;
use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use log::{debug, error, info, warn};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs,
    io::Read,
    net::{SocketAddr, TcpListener},
    path::{Component, Path, PathBuf},
    sync::{Arc, RwLock},
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Semaphore, oneshot},
    task::JoinSet,
};

const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 1_000;
const DEFAULT_SUMMARY_POINTS: usize = 360;
const MAX_AGGREGATE_JOBS: usize = 2;
const MAX_STATIC_JOBS: usize = 4;
const MAX_STATIC_FILE_BYTES: u64 = 16 * 1024 * 1024;

struct RangeQuery {
    from_unix_ms: i64,
    to_unix_ms: i64,
    after: Option<RangeCursor>,
    limit: usize,
}

struct UpdatesQuery {
    after_id: i64,
    limit: usize,
}

struct SummaryQuery {
    request: SummaryRequest,
    max_points: usize,
}

#[derive(Clone)]
struct HttpContext {
    live: Arc<RwLock<Live>>,
    history: History,
    database_status: DatabaseStatusHandle,
    aggregates: Arc<Semaphore>,
    web_root: Option<PathBuf>,
    web_files: Arc<Semaphore>,
}

pub(crate) struct HttpServer {
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl HttpServer {
    pub(crate) fn start(
        listener: TcpListener,
        live: Arc<RwLock<Live>>,
        history: History,
        database_status: DatabaseStatusHandle,
        web_root: Option<PathBuf>,
    ) -> Result<Self, Error> {
        let web_root = web_root.map(fs::canonicalize).transpose()?;
        if web_root.as_ref().is_some_and(|path| !path.is_dir()) {
            return Err("web root must be a directory".into());
        }
        match &web_root {
            Some(root) => info!("Serving web application from {}", root.display()),
            None => info!("No web root configured; serving APIs only"),
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        listener.set_nonblocking(true)?;
        let listener = {
            let _runtime = runtime.enter();
            tokio::net::TcpListener::from_std(listener)?
        };
        let aggregates = Arc::new(Semaphore::new(MAX_AGGREGATE_JOBS));
        let web_files = Arc::new(Semaphore::new(MAX_STATIC_JOBS));
        let context = HttpContext {
            live,
            history,
            database_status,
            aggregates,
            web_root,
            web_files,
        };
        let (shutdown, mut stopping) = oneshot::channel();
        let thread = thread::spawn(move || {
            runtime.block_on(async move {
                let mut clients = JoinSet::new();
                loop {
                    tokio::select! {
                        _ = &mut stopping => break,
                        Some(_) = clients.join_next(), if !clients.is_empty() => {},
                        accepted = listener.accept(), if clients.len() < 64 => {
                            let (socket, peer) = match accepted {
                                Ok(accepted) => accepted,
                                Err(error) => {
                                    error!("HTTP accept failed: {error}; HTTP server stopping");
                                    break;
                                }
                            };
                            let context = context.clone();
                            clients.spawn(async move {
                                let service = service_fn(move |request| {
                                    logged_respond(request, context.clone(), peer)
                                });
                                let mut builder = http1::Builder::new();
                                builder.keep_alive(false).max_buf_size(8192);
                                let connection = builder.serve_connection(TokioIo::new(socket), service);
                                match tokio::time::timeout(Duration::from_secs(5), connection).await {
                                    Ok(Ok(())) => {}
                                    Ok(Err(error)) => debug!("HTTP connection from {peer} failed: {error}"),
                                    Err(_) => debug!("HTTP connection from {peer} closed at the 5 s deadline"),
                                }
                            });
                        },
                    }
                }
            });
            debug!("HTTP server stopped");
        });
        Ok(Self {
            shutdown: Some(shutdown),
            thread: Some(thread),
        })
    }
}

async fn logged_respond(
    request: Request<Incoming>,
    context: HttpContext,
    peer: SocketAddr,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let started = Instant::now();
    let method = request.method().clone();
    let uri = request.uri().clone();
    let response = respond(request, context).await;
    if let Ok(response) = &response {
        debug!(
            "{peer} {method} {uri} -> {} in {} ms",
            response.status().as_u16(),
            started.elapsed().as_millis()
        );
    }
    response
}

async fn respond(
    request: Request<Incoming>,
    context: HttpContext,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = request.uri().path();
    let known_path = matches!(
        path,
        "/live"
            | "/history/indoor"
            | "/history/indoor/updates"
            | "/history/weather"
            | "/history/weather/updates"
            | "/api/v1/dashboard"
            | "/api/v1/history/weather/summary"
    );
    if !known_path
        && !is_api_path(path)
        && let Some(root) = context.web_root.clone()
    {
        return Ok(static_response(&request, root, Arc::clone(&context.web_files)).await);
    }
    if !known_path {
        return Ok(json_response(
            StatusCode::NOT_FOUND,
            "{\"error\":\"not found\"}".to_owned(),
        ));
    }
    if request.method() != Method::GET {
        let mut response = json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "{\"error\":\"read-only endpoint\"}".to_owned(),
        );
        response
            .headers_mut()
            .insert("Allow", "GET".parse().unwrap());
        return Ok(response);
    }

    let (status, body) = match path {
        "/live" => {
            if request.uri().query().is_some() {
                (
                    StatusCode::NOT_FOUND,
                    "{\"error\":\"not found\"}".to_owned(),
                )
            } else {
                let mut snapshot = context.live.read().unwrap().clone();
                snapshot.storage = StorageHealth {
                    database: context.database_status.read().unwrap().clone(),
                };
                (StatusCode::OK, serde_json::to_string(&snapshot).unwrap())
            }
        }
        "/api/v1/dashboard" => {
            if request.uri().query().is_some() {
                invalid_aggregate_query("invalid dashboard query")
            } else {
                dashboard(context.history, context.aggregates).await
            }
        }
        "/api/v1/history/weather/summary" => {
            summary(request.uri().query(), context.history, context.aggregates).await
        }
        path if path.ends_with("/updates") => {
            let stream = if path.starts_with("/history/weather") {
                Stream::Weather
            } else {
                Stream::Indoor
            };
            history_updates(request.uri().query(), context.history, stream).await
        }
        path => {
            let stream = if path.starts_with("/history/weather") {
                Stream::Weather
            } else {
                Stream::Indoor
            };
            range_history(request.uri().query(), context.history, stream).await
        }
    };
    Ok(json_response(status, body))
}

async fn dashboard(history: History, aggregates: Arc<Semaphore>) -> (StatusCode, String) {
    let Ok(permit) = aggregates.try_acquire_owned() else {
        return aggregate_busy();
    };
    let now = utc_unix_ms();
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        history.dashboard(now)
    })
    .await;
    aggregate_response(result)
}

async fn summary(
    query: Option<&str>,
    history: History,
    aggregates: Arc<Semaphore>,
) -> (StatusCode, String) {
    let Ok(query) = parse_summary(query) else {
        return invalid_aggregate_query("invalid weather summary query");
    };
    let Ok(permit) = aggregates.try_acquire_owned() else {
        return aggregate_busy();
    };
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        history.weather_summary(query.request, query.max_points)
    })
    .await;
    aggregate_response(result)
}

fn aggregate_response<T: serde::Serialize>(
    result: Result<Result<T, AggregateError>, tokio::task::JoinError>,
) -> (StatusCode, String) {
    match result {
        Ok(Ok(value)) => (StatusCode::OK, serde_json::to_string(&value).unwrap()),
        Ok(Err(AggregateError::RowBudget)) => {
            info!("Aggregate query rejected: range exceeds the row budget");
            error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "row_budget_exceeded",
                "aggregate range exceeds the row budget",
            )
        }
        Ok(Err(AggregateError::Timeout)) => {
            warn!("Aggregate query exceeded its time budget");
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "aggregate_timeout",
                "aggregate query exceeded its time budget",
            )
        }
        Ok(Err(AggregateError::Database(error))) => {
            error!("Aggregate query failed: {error}");
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "aggregate_unavailable",
                "aggregate data is temporarily unavailable",
            )
        }
        Err(error) => {
            error!("Aggregate task failed: {error}");
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "aggregate_unavailable",
                "aggregate data is temporarily unavailable",
            )
        }
    }
}

fn parse_summary(query: Option<&str>) -> Result<SummaryQuery, ()> {
    let mut parameters = query_parameters(
        query,
        &[
            "from_date",
            "through_date",
            "from_unix_ms",
            "to_unix_ms",
            "max_points",
        ],
    )?;
    let from_date = parameters.remove("from_date");
    let through_date = parameters.remove("through_date");
    let from_unix_ms = parameters.remove("from_unix_ms");
    let to_unix_ms = parameters.remove("to_unix_ms");
    let max_points = parameters
        .remove("max_points")
        .map(|value| value.parse::<usize>().map_err(|_| ()))
        .transpose()?
        .unwrap_or(DEFAULT_SUMMARY_POINTS);
    if !(1..=MAX_SUMMARY_POINTS).contains(&max_points) {
        return Err(());
    }
    let request = match (from_date, through_date, from_unix_ms, to_unix_ms) {
        (Some(from), Some(through), None, None) => SummaryRequest::dates(
            from.parse::<NaiveDate>().map_err(|_| ())?,
            through.parse::<NaiveDate>().map_err(|_| ())?,
        )
        .ok_or(())?,
        (None, None, Some(from), Some(to)) => SummaryRequest::instants(
            from.parse::<i64>().map_err(|_| ())?,
            to.parse::<i64>().map_err(|_| ())?,
        )
        .ok_or(())?,
        _ => return Err(()),
    };
    Ok(SummaryQuery {
        request,
        max_points,
    })
}

async fn history_updates(
    query: Option<&str>,
    history: History,
    stream: Stream,
) -> (StatusCode, String) {
    let Ok(query) = parse_history_updates(query) else {
        return invalid_history_query();
    };
    let result =
        tokio::task::spawn_blocking(move || history.after(stream, query.after_id, query.limit))
            .await;
    history_response(result)
}

async fn range_history(
    query: Option<&str>,
    history: History,
    stream: Stream,
) -> (StatusCode, String) {
    let Ok(query) = parse_history_range(query) else {
        return invalid_history_query();
    };
    let result = tokio::task::spawn_blocking(move || {
        history.range(
            stream,
            query.from_unix_ms,
            query.to_unix_ms,
            query.after,
            query.limit,
        )
    })
    .await;
    history_response(result)
}

fn parse_history_updates(query: Option<&str>) -> Result<UpdatesQuery, ()> {
    let mut parameters = query_parameters(query, &["after_id", "limit"])?;
    let after_id = required_i64(&mut parameters, "after_id")?;
    if after_id < 0 {
        return Err(());
    }
    Ok(UpdatesQuery {
        after_id,
        limit: page_limit(&mut parameters)?,
    })
}

fn parse_history_range(query: Option<&str>) -> Result<RangeQuery, ()> {
    let mut parameters = query_parameters(
        query,
        &[
            "from_unix_ms",
            "to_unix_ms",
            "after_received_at_unix_ms",
            "after_id",
            "limit",
        ],
    )?;
    let from_unix_ms = required_i64(&mut parameters, "from_unix_ms")?;
    let to_unix_ms = required_i64(&mut parameters, "to_unix_ms")?;
    if from_unix_ms > to_unix_ms {
        return Err(());
    }
    let after = match (
        optional_i64(&mut parameters, "after_received_at_unix_ms")?,
        optional_i64(&mut parameters, "after_id")?,
    ) {
        (None, None) => None,
        (Some(received_at_unix_ms), Some(id)) if id >= 0 => Some(RangeCursor {
            received_at_unix_ms,
            id,
        }),
        _ => return Err(()),
    };
    Ok(RangeQuery {
        from_unix_ms,
        to_unix_ms,
        after,
        limit: page_limit(&mut parameters)?,
    })
}

fn query_parameters<'a>(
    query: Option<&'a str>,
    allowed: &[&str],
) -> Result<BTreeMap<&'a str, &'a str>, ()> {
    let mut parameters = BTreeMap::new();
    for parameter in query.ok_or(())?.split('&') {
        let (name, value) = parameter.split_once('=').ok_or(())?;
        if !allowed.contains(&name) || parameters.insert(name, value).is_some() {
            return Err(());
        }
    }
    Ok(parameters)
}

fn required_i64(parameters: &mut BTreeMap<&str, &str>, name: &str) -> Result<i64, ()> {
    optional_i64(parameters, name)?.ok_or(())
}

fn optional_i64(parameters: &mut BTreeMap<&str, &str>, name: &str) -> Result<Option<i64>, ()> {
    parameters
        .remove(name)
        .map(|value| value.parse().map_err(|_| ()))
        .transpose()
}

fn page_limit(parameters: &mut BTreeMap<&str, &str>) -> Result<usize, ()> {
    let limit = parameters
        .remove("limit")
        .map(|value| value.parse::<usize>().map_err(|_| ()))
        .transpose()?
        .unwrap_or(DEFAULT_PAGE_LIMIT);
    if (1..=MAX_PAGE_LIMIT).contains(&limit) {
        Ok(limit)
    } else {
        Err(())
    }
}

fn invalid_history_query() -> (StatusCode, String) {
    (
        StatusCode::BAD_REQUEST,
        "{\"error\":\"invalid history query\"}".to_owned(),
    )
}

fn invalid_aggregate_query(message: &str) -> (StatusCode, String) {
    error_response(StatusCode::UNPROCESSABLE_ENTITY, "invalid_query", message)
}

fn aggregate_busy() -> (StatusCode, String) {
    warn!("Aggregate workers busy; rejecting request");
    error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "aggregate_busy",
        "aggregate workers are busy",
    )
}

fn error_response(status: StatusCode, code: &str, message: &str) -> (StatusCode, String) {
    (
        status,
        serde_json::json!({"error": {"code": code, "message": message}}).to_string(),
    )
}

fn history_response(
    result: Result<rusqlite::Result<Vec<StoredReading>>, tokio::task::JoinError>,
) -> (StatusCode, String) {
    match result {
        Ok(Ok(readings)) => (
            StatusCode::OK,
            serde_json::to_string(&serde_json::json!({"readings": readings})).unwrap(),
        ),
        Ok(Err(error)) => {
            error!("History query failed: {error}");
            history_unavailable()
        }
        Err(error) => {
            error!("History task failed: {error}");
            history_unavailable()
        }
    }
}

fn history_unavailable() -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "{\"error\":\"history unavailable\"}".to_owned(),
    )
}

fn json_response(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-store")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn is_api_path(path: &str) -> bool {
    path == "/live"
        || path.starts_with("/live/")
        || path == "/history"
        || path.starts_with("/history/")
        || path == "/api"
        || path.starts_with("/api/")
}

async fn static_response(
    request: &Request<Incoming>,
    root: PathBuf,
    web_files: Arc<Semaphore>,
) -> Response<Full<Bytes>> {
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("Allow", "GET, HEAD")
            .header("Content-Type", "text/plain; charset=utf-8")
            .header("Cache-Control", "no-store")
            .body(Full::new(Bytes::from_static(b"method not allowed")))
            .unwrap();
    }
    let Ok(permit) = web_files.try_acquire_owned() else {
        warn!("Static file workers busy; rejecting request");
        return static_unavailable();
    };
    let method = request.method().clone();
    let raw_path = request.uri().path().to_owned();
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        static_response_blocking(method, &raw_path, &root)
    })
    .await
    {
        Ok(response) => response,
        Err(error) => {
            error!("Static file task failed: {error}");
            static_unavailable()
        }
    }
}

fn static_response_blocking(method: Method, raw_path: &str, root: &Path) -> Response<Full<Bytes>> {
    let lowercase = raw_path.to_ascii_lowercase();
    if raw_path.contains('\\')
        || lowercase.contains("%2e")
        || lowercase.contains("%2f")
        || lowercase.contains("%5c")
    {
        return static_not_found();
    }
    let relative = raw_path.trim_start_matches('/');
    let candidate = Path::new(relative);
    if candidate
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
        && !relative.is_empty()
    {
        return static_not_found();
    }
    let direct = root.join(candidate);
    let asset_request = raw_path.starts_with("/assets/");
    let selected = if direct.is_file() {
        direct
    } else if asset_request {
        return static_not_found();
    } else {
        root.join("index.html")
    };
    let Ok(canonical) = fs::canonicalize(&selected) else {
        return static_not_found();
    };
    if !canonical.starts_with(root) || !canonical.is_file() {
        return static_not_found();
    }
    let Ok(file) = fs::File::open(&canonical) else {
        return static_not_found();
    };
    let Ok(metadata) = file.metadata() else {
        return static_not_found();
    };
    if metadata.len() > MAX_STATIC_FILE_BYTES {
        warn!(
            "Static file {} exceeds the {MAX_STATIC_FILE_BYTES}-byte limit",
            canonical.display()
        );
        return Response::builder()
            .status(StatusCode::PAYLOAD_TOO_LARGE)
            .header("Content-Type", "text/plain; charset=utf-8")
            .header("Cache-Control", "no-store")
            .body(Full::new(Bytes::from_static(b"static file too large")))
            .unwrap();
    }
    let mut contents = Vec::with_capacity(metadata.len() as usize);
    let Ok(_) = file
        .take(MAX_STATIC_FILE_BYTES + 1)
        .read_to_end(&mut contents)
    else {
        return static_not_found();
    };
    if contents.len() as u64 > MAX_STATIC_FILE_BYTES {
        return Response::builder()
            .status(StatusCode::PAYLOAD_TOO_LARGE)
            .header("Content-Type", "text/plain; charset=utf-8")
            .header("Cache-Control", "no-store")
            .body(Full::new(Bytes::from_static(b"static file too large")))
            .unwrap();
    }
    let length = contents.len();
    let body = if method == Method::HEAD {
        Bytes::new()
    } else {
        Bytes::from(contents)
    };
    let immutable = canonical
        .strip_prefix(root)
        .is_ok_and(|path| path.starts_with("assets"));
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", content_type(&canonical))
        .header(
            "Cache-Control",
            if immutable {
                "public, max-age=31536000, immutable"
            } else {
                "no-store"
            },
        )
        .header("Content-Length", length.to_string())
        .body(Full::new(body))
        .unwrap()
}

fn static_unavailable() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header("Content-Type", "text/plain; charset=utf-8")
        .header("Cache-Control", "no-store")
        .body(Full::new(Bytes::from_static(b"static files busy")))
        .unwrap()
}

fn static_not_found() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Type", "text/plain; charset=utf-8")
        .header("Cache-Control", "no-store")
        .body(Full::new(Bytes::from_static(b"not found")))
        .unwrap()
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn utc_unix_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(error) => -(error.duration().as_millis() as i64),
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
