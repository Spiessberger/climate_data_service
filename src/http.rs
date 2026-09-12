use crate::{
    Error, Live,
    storage::{History, RangeCursor, StoredReading, Stream},
};
use http_body_util::Full;
use hyper::{
    Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    net::TcpListener,
    sync::{Arc, RwLock},
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinSet};

const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 1_000;

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

pub(crate) struct HttpServer {
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl HttpServer {
    pub(crate) fn start(
        listener: TcpListener,
        live: Arc<RwLock<Live>>,
        history: History,
    ) -> Result<Self, Error> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        listener.set_nonblocking(true)?;
        let listener = {
            let _runtime = runtime.enter();
            tokio::net::TcpListener::from_std(listener)?
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
                        let Ok((socket, _)) = accepted else { break };
                        let live = Arc::clone(&live);
                        let client_history = history.clone();
                        clients.spawn(async move {
                            let service = service_fn(move |request| {
                                respond(request, Arc::clone(&live), client_history.clone())
                            });
                            let mut builder = http1::Builder::new();
                            builder.keep_alive(false).max_buf_size(8192);
                            let connection = builder.serve_connection(TokioIo::new(socket), service);
                            let _ = tokio::time::timeout(Duration::from_secs(5), connection).await;
                        });
                    },
                }
            }
            // Dropping JoinSet cancels clients, including incomplete requests.
        })
        });
        Ok(Self {
            shutdown: Some(shutdown),
            thread: Some(thread),
        })
    }
}

async fn respond(
    request: Request<Incoming>,
    live: Arc<RwLock<Live>>,
    history: History,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let known_path = matches!(
        request.uri().path(),
        "/live"
            | "/history/indoor"
            | "/history/indoor/updates"
            | "/history/weather"
            | "/history/weather/updates"
    );
    let (status, body) = if !known_path {
        (
            StatusCode::NOT_FOUND,
            "{\"error\":\"not found\"}".to_owned(),
        )
    } else if request.method() != hyper::Method::GET {
        (
            StatusCode::METHOD_NOT_ALLOWED,
            "{\"error\":\"read-only endpoint\"}".to_owned(),
        )
    } else if request.uri().path() == "/live" {
        if request.uri().query().is_some() {
            (
                StatusCode::NOT_FOUND,
                "{\"error\":\"not found\"}".to_owned(),
            )
        } else {
            // Clone the small snapshot and release its lock before socket I/O.
            let snapshot = live.read().unwrap().clone();
            (StatusCode::OK, serde_json::to_string(&snapshot).unwrap())
        }
    } else {
        let stream = if request.uri().path().starts_with("/history/weather") {
            Stream::Weather
        } else {
            Stream::Indoor
        };
        if request.uri().path().ends_with("/updates") {
            history_updates(request.uri().query(), history, stream).await
        } else {
            range_history(request.uri().query(), history, stream).await
        }
    };
    let mut response = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-store");
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response = response.header("Allow", "GET");
    }
    Ok(response.body(Full::new(Bytes::from(body))).unwrap())
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

fn history_response(
    result: Result<rusqlite::Result<Vec<StoredReading>>, tokio::task::JoinError>,
) -> (StatusCode, String) {
    match result {
        Ok(Ok(readings)) => (
            StatusCode::OK,
            serde_json::to_string(&serde_json::json!({"readings": readings})).unwrap(),
        ),
        Ok(Err(_)) | Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "{\"error\":\"history unavailable\"}".to_owned(),
        ),
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
