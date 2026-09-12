use crate::{Error, Live};
use http_body_util::Full;
use hyper::{
    Request, Response,
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
    pub(crate) fn start(listener: TcpListener, live: Arc<RwLock<Live>>) -> Result<Self, Error> {
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
                        clients.spawn(async move {
                            let service = service_fn(move |request| respond(request, Arc::clone(&live)));
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
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (status, body) = if request.uri().path() != "/live" || request.uri().query().is_some() {
        (404, "{\"error\":\"not found\"}".to_owned())
    } else if request.method() != hyper::Method::GET {
        (405, "{\"error\":\"read-only endpoint\"}".to_owned())
    } else {
        // Clone the small snapshot and release its lock before socket I/O.
        let indoor = live.read().unwrap().indoor.clone();
        (200, serde_json::to_string(&Live { indoor }).unwrap())
    };
    let mut response = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-store");
    if status == 405 {
        response = response.header("Allow", "GET");
    }
    Ok(response.body(Full::new(Bytes::from(body))).unwrap())
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
