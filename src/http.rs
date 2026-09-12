use crate::{Error, Live, storage::History};
use http_body_util::Full;
use hyper::{
    Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    net::TcpListener,
    sync::{Arc, RwLock},
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinSet};

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
        "/live" | "/history/indoor" | "/history/indoor/updates"
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
            let indoor = live.read().unwrap().indoor.clone();
            (
                StatusCode::OK,
                serde_json::to_string(&Live { indoor }).unwrap(),
            )
        }
    } else if request.uri().path() == "/history/indoor" {
        indoor_history(request.uri().query(), history).await
    } else {
        indoor_updates(request.uri().query(), history).await
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

async fn indoor_updates(query: Option<&str>, history: History) -> (StatusCode, String) {
    let parsed = (|| {
        let mut after_id = None;
        let mut limit = None;
        for parameter in query.ok_or(())?.split('&') {
            let (name, value) = parameter.split_once('=').ok_or(())?;
            match name {
                "after_id" if after_id.is_none() => {
                    after_id = Some(value.parse::<i64>().map_err(|_| ())?)
                }
                "limit" if limit.is_none() => limit = Some(value.parse::<usize>().map_err(|_| ())?),
                _ => return Err(()),
            }
        }
        let (after_id, limit) = (after_id.ok_or(())?, limit.unwrap_or(100));
        if after_id < 0 || !(1..=1000).contains(&limit) {
            return Err(());
        }
        Ok((after_id, limit))
    })();
    let Ok((after_id, limit)) = parsed else {
        return (
            StatusCode::BAD_REQUEST,
            "{\"error\":\"invalid history query\"}".to_owned(),
        );
    };
    match tokio::task::spawn_blocking(move || history.indoor_after(after_id, limit)).await {
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

async fn indoor_history(query: Option<&str>, history: History) -> (StatusCode, String) {
    let parsed = (|| {
        let mut from = None;
        let mut to = None;
        let mut after_time = None;
        let mut after_id = None;
        let mut limit = None;
        for parameter in query.ok_or(())?.split('&') {
            let (name, value) = parameter.split_once('=').ok_or(())?;
            match name {
                "from_unix_ms" if from.is_none() => {
                    from = Some(value.parse::<i64>().map_err(|_| ())?)
                }
                "to_unix_ms" if to.is_none() => to = Some(value.parse::<i64>().map_err(|_| ())?),
                "after_received_at_unix_ms" if after_time.is_none() => {
                    after_time = Some(value.parse::<i64>().map_err(|_| ())?)
                }
                "after_id" if after_id.is_none() => {
                    after_id = Some(value.parse::<i64>().map_err(|_| ())?)
                }
                "limit" if limit.is_none() => limit = Some(value.parse::<usize>().map_err(|_| ())?),
                _ => return Err(()),
            }
        }
        let (from, to, limit) = (from.ok_or(())?, to.ok_or(())?, limit.unwrap_or(100));
        let after = match (after_time, after_id) {
            (None, None) => None,
            (Some(time), Some(id)) if id >= 0 => Some((time, id)),
            _ => return Err(()),
        };
        if from > to || !(1..=1000).contains(&limit) {
            return Err(());
        }
        Ok((from, to, after, limit))
    })();
    let Ok((from, to, after, limit)) = parsed else {
        return (
            StatusCode::BAD_REQUEST,
            "{\"error\":\"invalid history query\"}".to_owned(),
        );
    };
    match tokio::task::spawn_blocking(move || history.indoor_range(from, to, after, limit)).await {
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
