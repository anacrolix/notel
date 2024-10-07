#[cfg(test)]
mod tests;

mod conn;
mod stream_id;

use conn::*;
use stream_id::StreamId;

use anyhow::{anyhow, Context, Result};
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::WebSocketUpgrade;
use axum::http::{HeaderMap, StatusCode};
use chrono::SecondsFormat;
use clap::Parser;
use futures::FutureExt;
use futures::{future, select_biased, TryFutureExt};
use futures::{Stream, StreamExt};
use std::fmt::{Debug, Display, Formatter};
use std::future::{poll_fn, Future, IntoFuture};
use std::io::Write;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::signal::ctrl_c;
use tokio::signal::unix::SignalKind;
use tokio::sync::Mutex;
use tracing::*;
use Error::*;

#[derive(clap::Parser)]
struct Args {
    #[command(subcommand)]
    storage: Storage,
}

#[derive(Clone, clap::Subcommand)]
enum Storage {
    Sqlite(SqliteOpen),
    DuckDB(DuckDbOpen),
    JsonFiles(JsonFilesOpen),
    Postgres(PostgresOpener),
}

impl Storage {
    pub(crate) async fn open(self) -> Result<Box<dyn Connection + Send>> {
        // Moving the box/dyn stuff into the trait doesn't seem better. Rust doesn't let me dispatch
        // over enums that all implement the same trait?
        match self {
            Storage::Sqlite(open) => Self::do_open(open).await,
            Storage::DuckDB(open) => Self::do_open(open).await,
            Storage::JsonFiles(open) => Self::do_open(open).await,
            Storage::Postgres(open) => Self::do_open(open).await,
        }
    }

    async fn do_open<O>(opener: O) -> Result<Box<dyn Connection + Send>>
    where
        O: StorageOpen,
        <O as StorageOpen>::Conn: Connection + 'static,
    {
        Ok(Box::new(opener.open().await?))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_default_env()
        .format(|fmt, record| {
            let level_style = fmt.default_level_style(record.level());
            let localtime = chrono::Local::now();
            writeln!(
                fmt,
                "[{} {level_style}{}{level_style:#} {}] {}",
                localtime,
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();
    debug!(test_arg = "hi mum", "debug level test");
    let args = Args::parse();
    let db_conn = args.storage.open().await?;
    let commit_on_sigint = db_conn.commit_on_sigint();
    let db_conn = Arc::new(Mutex::new(db_conn));

    // This catches signals that trigger commit. Spin it up even if not committing on sigint to
    // ensure all behaviours are handled correctly.
    tokio::spawn({
        let db_conn = db_conn.clone();
        async move {
            if !db_conn.lock().await.commit_on_sigint() {
                std::future::pending::<()>().await;
                return;
            }
            loop {
                ctrl_c().await.unwrap();
                log_commit(&mut **db_conn.lock().await).await.unwrap();
            }
        }
    });

    let server = Arc::new(Server { db_conn });
    // TODO: Catch a signal or handle an endpoint that triggers the db conn to be committed. Also do
    // this on a timer.
    let tower_layer = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(tower_http::trace::DefaultMakeSpan::new().include_headers(true))
        .on_request(())
        .on_body_chunk(());
    let app = axum::Router::new()
        .route(
            "/",
            axum::routing::post({
                let server = Arc::clone(&server);
                move |body| async move { server.post_handler(body).await }
            }),
        )
        .route(
            "/",
            axum::routing::get({
                let server = Arc::clone(&server);
                |ws_upgrade: WebSocketUpgrade, headers: HeaderMap| async move {
                    ws_upgrade.on_upgrade(move |ws| async move {
                        server.websocket_handler(ws, &headers).await
                    })
                }
            }),
        )
        .layer(tower_layer);
    // This is just the OTLP/HTTP port, because if we're using this we're probably not using OTLP. I
    // want this to bind dual stack, but I don't see any obvious way to do it with one call.
    let listener = tokio::net::TcpListener::bind("[::]:4318").await?;
    let listener_local_addr = listener.local_addr()?;
    info!(?listener_local_addr, "serving http");
    let http_server = axum::serve(listener, app)
        .into_future()
        .map_err(anyhow::Error::from);
    let term_sigs = pin!(handle_main_signals(commit_on_sigint)?);
    let either = future::select(http_server, term_sigs).await;
    either.factor_first().0
}

fn handle_main_signals(commit_on_sigint: bool) -> Result<impl Future<Output = Result<()>>> {
    let mut signals = vec![];
    if !commit_on_sigint {
        signals.push(Box::pin(signal("SIGINT", SignalKind::interrupt())?));
    }
    for (name, kind) in [
        // On Windows we probably wouldn't have this one. Also what about the fact this is normally
        // for user detected errors?
        ("SIGQUIT", SignalKind::quit()),
        ("SIGTERM", SignalKind::terminate()),
    ] {
        signals.push(Box::pin(signal(name, kind)?));
    }
    Ok(async move {
        let signal_name = future::select_all(signals).await.0 .0;
        warn!(signal_name, "received terminating main signal");
        Ok(())
    })
}

fn signal(name: &str, kind: SignalKind) -> Result<impl Future<Output = (&str, Option<()>)>> {
    let mut signal = tokio::signal::unix::signal(kind)?;
    Ok(async move { signal.recv().map(|maybe_sig| (name, maybe_sig)).await })
}

async fn log_commit(conn: &mut (impl Connection + ?Sized)) -> Result<()> {
    let res = conn.commit().await;
    match &res {
        Ok(()) => info!("committed"),
        Err(err) => error!(%err, "committing"),
    };
    res
}

type SerializedHeaders = serde_json::Value;

type StreamEventIndex = u64;

struct Server {
    db_conn: Arc<Mutex<Box<dyn Connection + Send>>>,
}

async fn iter_json_stream<F>(
    mut body_data_stream: impl Stream<Item = Result<Bytes, axum::Error>> + Unpin,
    mut on_payload: impl FnMut(Vec<u8>) -> F,
) -> Result<(), (anyhow::Error, StatusCode)>
where
    F: Future<Output = Result<()>>,
{
    let mut bytes = vec![];
    let mut last_eof_error = None;
    while let Some(result) = body_data_stream.next().await {
        let new_bytes = match result {
            Err(err) => {
                return Err((
                    anyhow::Error::from(err).context("error in body data stream"),
                    StatusCode::INTERNAL_SERVER_ERROR,
                ));
            }
            Ok(ok) => ok,
        };
        bytes.extend_from_slice(&new_bytes);
        // Iterate through JSON objects without allocating anything.
        let mut json_stream_deserializer = serde_json::Deserializer::from_slice(bytes.as_slice())
            .into_iter::<serde::de::IgnoredAny>();
        let mut last_offset = 0;
        while let Some(result) = json_stream_deserializer.next() {
            match result {
                Err(err) if err.is_eof() => {
                    last_eof_error = Some(err);
                    break;
                }
                Err(err) => {
                    error!(?err, "error deserializing json value");
                    return Err((
                        anyhow!(err).context("deserializing json value"),
                        StatusCode::BAD_REQUEST,
                    ));
                }
                Ok(serde::de::IgnoredAny) => {
                    last_eof_error = None;
                    let value_end_offset = json_stream_deserializer.byte_offset();
                    let payload = bytes[last_offset..value_end_offset].to_vec();
                    if let Err(err) = on_payload(payload).await {
                        return Err((
                            err.context("handling payload"),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        ));
                    }
                    last_offset = value_end_offset;
                }
            }
        }
        trace!(last_offset, "draining bytes to offset");
        bytes.drain(..last_offset);
    }
    last_eof_error
        .map(|eof_err| Err((anyhow!(eof_err), StatusCode::BAD_REQUEST)))
        .unwrap_or(Ok(()))
}

enum StreamRetry {
    More,
    Stop,
}

#[derive(Debug)]
enum Error {
    Recv(axum::Error),
    Handle(anyhow::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Recv(err) => Display::fmt(err, f),
            Handle(err) => Display::fmt(err, f),
        }
    }
}

impl Server {
    async fn websocket_handler(&self, websocket: WebSocket, headers: &HeaderMap) {
        if let Err(err) = self.websocket_handler_err(websocket, headers).await {
            match err {
                Recv(err) => {
                    debug!(?err, "receiving message");
                }
                Handle(err) => {
                    error!(?err, "handling message");
                }
            }
        }
    }

    async fn handle_message(
        &self,
        message: Message,
        stream_id: StreamId,
        stream_event_index: &mut StreamEventIndex,
    ) -> Result<StreamRetry> {
        match message {
            Message::Close(reason) => {
                debug!(%stream_id, ?reason, "websocket closed");
                // Not sure if we should act on this or let the next recv return None?
                Ok(StreamRetry::More)
            }
            // Pings and pongs are apparently are handled for us by the library.
            Message::Ping(_) | Message::Pong(_) => Ok(StreamRetry::More),
            // That should leave text and binary types, which we won't discriminate.
            Message::Binary(vec) if vec.is_empty() => Ok(StreamRetry::Stop),
            _ => {
                let payload = message.to_text().context("converting payload to text")?;
                self.insert_event(payload, stream_id, *stream_event_index)
                    .await
                    .context("inserting event")?;
                Ok(StreamRetry::More)
            }
        }
    }

    /// Only returns receive errors. Logs acknowledgement errors (but still returns).
    async fn websocket_handler_err(
        &self,
        mut websocket: WebSocket,
        headers: &HeaderMap,
    ) -> Result<(), Error> {
        let stream_id = self
            .new_stream(headers)
            .await
            .context("creating new stream")
            .map_err(Handle)?;
        // TODO: Flush streams
        let mut total_events = 0;
        let mut stream_event_index = 0;
        let result = loop {
            let (batch_count, last_recv_result) = Self::receive_consecutive_websocket_messages(
                &mut websocket,
                |message| async move {
                    // TODO: Take db_conn lock on first event.
                    self.handle_message(message, stream_id, &mut stream_event_index)
                        .await
                },
            )
            .await;
            info!(batch_count, %stream_id, "inserted consecutive payloads");
            if batch_count != 0 {
                // Just flush the events.
                self.db_conn
                    .lock()
                    .await
                    .flush()
                    .await
                    .context("flushing consecutive payloads")
                    .map_err(Handle)?;
                total_events += batch_count;
                if let Err(err) = Self::acknowledge_inserted(&mut websocket, total_events).await {
                    // Report the acknowledgment error, which is pretty important, and return with
                    // whatever the recv result was.
                    error!(?err, "acknowledging received");
                    break last_recv_result.map(|_| ());
                }
            }
            match last_recv_result {
                Err(err) => {
                    break Err(err);
                }
                Ok(StreamRetry::Stop) => {
                    break Ok(());
                }
                Ok(StreamRetry::More) => {}
            }
        };
        match &result {
            Ok(()) => {
                info!(%stream_id, total_events, "stream ended");
            }
            Err(err) => {
                info!(%stream_id, total_events, %err, "stream ended");
            }
        }
        result
    }

    async fn receive_consecutive_websocket_messages<F>(
        websocket: &mut WebSocket,
        mut handle: impl FnMut(Message) -> F,
    ) -> (u64, Result<StreamRetry, Error>)
    where
        F: Future<Output = Result<StreamRetry>>,
    {
        let mut count = 0;
        let result = loop {
            let mut nonblocking = poll_fn(|_cx| {
                if count == 0 {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .fuse();
            let option_recv = select_biased! {
                a = websocket.recv().fuse() => a,
                () = nonblocking => {
                    assert!(count > 0);
                    break Ok(StreamRetry::More);
                }
            };
            match match option_recv {
                Some(Ok(message)) => match handle(message).await {
                    Ok(more) => {
                        count += 1;
                        more
                    }
                    Err(err) => break Err(Handle(err)),
                },
                Some(Err(err)) => {
                    break Err(Recv(err));
                }
                None => break Ok(StreamRetry::Stop),
            } {
                StreamRetry::More => {}
                StreamRetry::Stop => break Ok(StreamRetry::Stop),
            }
        };
        (count, result)
    }

    async fn acknowledge_inserted(
        websocket: &mut WebSocket,
        counter: u64,
    ) -> Result<(), axum::Error> {
        websocket.send(Message::Text(counter.to_string())).await
    }

    // Eventually this might return a list of items added, or a count, so that callers can throw
    // away what they know was committed.
    async fn post_handler(
        &self,
        req: axum::http::Request<axum::body::Body>,
    ) -> (StatusCode, String) {
        let mut payloads_inserted = 0;
        let status_code = self
            .post_handler_status_code(req, &mut payloads_inserted)
            .await;
        info!(payloads_inserted, "submit handled ok");
        (status_code, format!("{}", payloads_inserted))
    }

    async fn new_stream(&self, headers: &HeaderMap) -> anyhow::Result<StreamId> {
        let headers_value = headers_to_json_value(headers)?;
        let mut conn = self.db_conn.lock().await;
        let stream_id = conn.new_stream(headers_value).await?;
        info!(%stream_id, "started new stream");
        Ok(stream_id)
    }

    async fn insert_event(
        &self,
        payload: &str,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
    ) -> Result<()> {
        // Down the track this could be done in a separate thread, or under a transaction each time
        // we read a chunk.
        debug!(payload, "inserting payload into store");
        let mut conn = self.db_conn.lock().await;
        conn.insert_event(stream_id, stream_event_index, payload)
            .await
            .context("inserting payload into store")
    }

    async fn post_handler_status_code(
        &self,
        req: axum::http::Request<axum::body::Body>,
        payloads_inserted: &mut u64,
    ) -> StatusCode {
        let stream_id = match self.new_stream(req.headers()).await {
            Err(err) => {
                error!(?err, "creating new stream");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            Ok(ok) => ok,
        };
        let body_data_stream = req.into_body().into_data_stream();
        let mut stream_event_index = 0;
        let result = iter_json_stream(body_data_stream, move |payload| {
            *payloads_inserted += 1;
            stream_event_index += 1;
            assert_eq!(stream_event_index, *payloads_inserted);
            async move {
                // sqlite needs to be given text.
                let payload = std::str::from_utf8(&payload).unwrap();
                self.insert_event(payload, stream_id, stream_event_index)
                    .await?;
                Ok(())
            }
        })
        .await;
        match result {
            Ok(()) => {}
            Err((err, code)) => {
                error!(?err, "error while iterating json stream");
                return code;
            }
        }
        StatusCode::OK
    }
}

fn headers_to_json_value(headers: &HeaderMap) -> serde_json::Result<serde_json::Value> {
    // This converts duplicate header values to an array, and seems to leave single header values
    // alone. This is needed to fix JSON containing backslashes for some values when those should be
    // valid objects.
    http_serde::header_map::serialize(headers, serde_json::value::Serializer)
}

#[allow(dead_code)]
fn sqlite_local_datetime_now_string() -> String {
    chrono::Local::now().to_rfc3339_opts(SecondsFormat::Millis, false)
}

// enum Payload {
//     Binary,
//     Text,
//     Json,
// }
