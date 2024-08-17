mod conn;
use conn::Connection;

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
use std::future::{poll_fn, IntoFuture};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use tracing::*;

#[derive(clap::Parser)]
struct Args {
    #[arg(long)]
    #[clap(value_enum, default_value_t=Storage::JsonFiles)]
    storage: Storage,
}

#[derive(Clone, clap::ValueEnum)]
enum Storage {
    Sqlite,
    DuckDB,
    JsonFiles,
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
    let server = Arc::new(Server {
        db_conn: Mutex::new(<dyn Connection>::open(args.storage)?),
    });
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
                move |body| async move { server.submit(body).await }
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
    let ctrl_c = async {
        // TODO: Catch SIGTERM too?
        tokio::signal::ctrl_c().await?;
        info!("ctrl_c");
        Ok(())
    };
    tokio::pin!(ctrl_c);
    let either = future::select(http_server, ctrl_c).await;
    either.factor_first().0
}

type SerializedHeaders = serde_json::Value;

// Let's see if u32 is enough. I might have to create a newtype here so I can get nicer hex
// formatting and stuff.
type StreamId = u32;

type StreamEventIndex = u64;

struct Server {
    db_conn: Mutex<Box<dyn Connection + Send>>,
}

async fn iter_json_stream(
    mut body_data_stream: impl Stream<Item = Result<Bytes, axum::Error>> + Unpin,
    mut on_payload: impl FnMut(&[u8]) -> Result<()>,
) -> Result<(), (anyhow::Error, StatusCode)> {
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
                    let payload = &bytes[last_offset..value_end_offset];
                    if let Err(err) = on_payload(payload) {
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

use Error::*;

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

    fn handle_message(
        &self,
        message: Message,
        stream_id: StreamId,
        stream_event_index: &mut StreamEventIndex,
    ) -> Result<StreamRetry> {
        match message {
            Message::Close(reason) => {
                debug!(stream_id, ?reason, "websocket closed");
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
            .context("creating new stream")
            .map_err(Handle)?;
        // TODO: Flush streams
        info!(%stream_id, "started new stream");
        let mut total_events = 0;
        let mut stream_event_index = 0;
        let result = loop {
            let (batch_count, last_recv_result) =
                Self::receive_consecutive_websocket_messages(&mut websocket, |message| {
                    // TODO: Take db_conn lock on first event.
                    self.handle_message(message, stream_id, &mut stream_event_index)
                })
                .await;
            info!(batch_count, %stream_id, "inserted consecutive payloads");
            if batch_count != 0 {
                // Just flush the events.
                self.db_conn
                    .lock()
                    .unwrap()
                    .flush()
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
                info!(stream_id, total_events, "stream ended");
            }
            Err(err) => {
                info!(%stream_id, total_events, %err, "stream ended");
            }
        }
        result
    }

    async fn receive_consecutive_websocket_messages(
        websocket: &mut WebSocket,
        mut handle: impl FnMut(Message) -> Result<StreamRetry>,
    ) -> (u64, Result<StreamRetry, Error>) {
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
                Some(Ok(message)) => match handle(message) {
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
    async fn submit(&self, req: axum::http::Request<axum::body::Body>) -> (StatusCode, String) {
        let mut payloads_inserted = 0;
        let status_code = self.submit_inner(req, &mut payloads_inserted).await;
        info!(payloads_inserted, "submit handled ok");
        (status_code, format!("{}", payloads_inserted))
    }

    fn new_stream(&self, headers: &HeaderMap) -> anyhow::Result<StreamId> {
        let headers_value = headers_to_json_value(headers)?;
        let mut conn = self.db_conn.lock().unwrap();
        conn.new_stream(headers_value)
    }

    fn insert_event(
        &self,
        payload: &str,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
    ) -> Result<()> {
        // Down the track this could be done in a separate thread, or under a
        // transaction each time we read a chunk.
        debug!(payload, "inserting payload into store");
        let mut conn = self.db_conn.lock().unwrap();
        conn.insert_event(stream_id, stream_event_index, payload)
            .context("inserting payload into store")
    }

    async fn submit_inner(
        &self,
        req: axum::http::Request<axum::body::Body>,
        payloads_inserted: &mut u64,
    ) -> StatusCode {
        let stream_id = match self.new_stream(req.headers()) {
            Err(err) => {
                error!(?err, "creating new stream");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            Ok(ok) => ok,
        };
        let body_data_stream = req.into_body().into_data_stream();
        let mut stream_event_index = 0;
        let result = iter_json_stream(body_data_stream, move |payload| {
            // sqlite needs to be given text.
            let payload = std::str::from_utf8(payload).unwrap();
            self.insert_event(payload, stream_id, stream_event_index)?;
            *payloads_inserted += 1;
            stream_event_index += 1;
            assert_eq!(stream_event_index, *payloads_inserted);
            Ok(())
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

#[cfg(test)]
mod tests {
    use crate::{headers_to_json_value, iter_json_stream};
    use axum::http::HeaderMap;
    use serde_json::json;

    #[test]
    fn test_headers_to_json() -> anyhow::Result<()> {
        let mut headers = HeaderMap::new();
        let path = "c:\\herp/some/path";
        let object_value = json!({"cwd": path});
        headers.append("x-client", object_value.to_string().parse()?);
        let json = headers_to_json_value(&headers)?;
        println!("{}", &json);
        let conn = rusqlite::Connection::open_in_memory()?;
        conn.execute_batch("create table a(b)")?;
        conn.execute("insert into a values (jsonb(?))", [&json])?;
        let cwd: String =
            conn.query_row("select b->>'x-client'->>'cwd' from a", [], |row| row.get(0))?;
        assert_eq!(cwd, path);
        Ok(())
    }

    #[test]
    fn test_duplicated_header_names_to_json() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let mut headers = HeaderMap::new();
        let path1 = "c:\\path1";
        let path2 = "c:\\path2";
        headers.append("x-client", json!({"herp": path1}).to_string().parse()?);
        headers.append("x-client", json!({"derp": path2}).to_string().parse()?);
        let json = headers_to_json_value(&headers)?;
        println!("{}", &json);
        let conn = rusqlite::Connection::open_in_memory()?;
        conn.execute_batch("create table a(b)")?;
        conn.execute("insert into a values (jsonb(?))", [&json])?;
        let first_value: String =
            conn.query_row("select b->>'x-client'->>0->>'herp' from a", [], |row| {
                row.get(0)
            })?;
        assert_eq!(first_value, path1);
        let second_value: String =
            conn.query_row("select b->>'x-client'->>1->>'derp' from a", [], |row| {
                row.get(0)
            })?;
        assert_eq!(second_value, path2);
        Ok(())
    }

    #[tokio::test]
    async fn test_chunked_json_stream() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let inputs = [
            r#""#,
            r#"{"msg"#,
            "\": \"hi\", \"context\": 3}\n",
            r#"
{"type": "span", "id": "a"}
{"type": "span", "id": "b", "parentSpan":"#,
            r#""#,
            r#" "a"}"#,
            r#"   "#,
        ];
        let mut outputs = vec![];
        iter_json_stream(
            futures::stream::iter(inputs.map(|str| Ok(str.into()))),
            |payload| {
                outputs.push(payload.to_owned());
                Ok(())
            },
        )
        .await
        .unwrap();
        let output_strings = outputs
            .into_iter()
            .map(|bytes| std::str::from_utf8(&bytes).unwrap().to_string())
            .collect::<Vec<_>>();
        let expected_eq = vec![
            r#"{"msg": "hi", "context": 3}"#,
            r#"

{"type": "span", "id": "a"}"#,
            r#"
{"type": "span", "id": "b", "parentSpan": "a"}"#,
        ];
        assert_eq!(output_strings.len(), expected_eq.len());
        assert_eq!(output_strings, expected_eq);
        Ok(())
    }

    #[tokio::test]
    /// Ensure that starting a valid JSON value doesn't result in success.
    async fn test_chunked_json_stream_trailing_garbage() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let inputs = [
            r#""#,
            r#"{"msg"#,
            "\": \"hi\", \"context\": 3}\n",
            r#"
{"type": "span", "id": "a"}
{"type": "span", "id": "b", "parentSpan":"#,
            r#""#,
            r#" "a"}"#,
            r#" {  "#,
        ];
        let mut outputs = vec![];
        let result = iter_json_stream(
            futures::stream::iter(inputs.map(|str| Ok(str.into()))),
            |payload| {
                outputs.push(payload.to_owned());
                Ok(())
            },
        )
        .await;
        result
            .as_ref()
            .expect_err("should error on trailing json value");
        dbg!(&result);
        let output_strings = outputs
            .into_iter()
            .map(|bytes| std::str::from_utf8(&bytes).unwrap().to_string())
            .collect::<Vec<_>>();
        let expected_eq = vec![
            r#"{"msg": "hi", "context": 3}"#,
            r#"

{"type": "span", "id": "a"}"#,
            r#"
{"type": "span", "id": "b", "parentSpan": "a"}"#,
        ];
        assert_eq!(output_strings.len(), expected_eq.len());
        assert_eq!(output_strings, expected_eq);
        Ok(())
    }
}
