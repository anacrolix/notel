use anyhow::{anyhow, Context};
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::WebSocketUpgrade;
use axum::http::{HeaderMap, StatusCode};
use chrono::SecondsFormat;
use futures::select;
use futures::FutureExt;
use futures::{Stream, StreamExt};
use std::sync::{Arc, Mutex};
use tracing::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut sqlite_conn = rusqlite::Connection::open("telemetry.db")?;
    let tx = sqlite_conn.transaction()?;
    let user_version: u64 =
        tx.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if user_version == 0 {
        tx.execute_batch(include_str!("../proposed-schema.sql"))?;
        tx.pragma_update(None, "user_version", 1)?;
    }
    tx.commit()?;
    let sqlite_conn = Mutex::new(sqlite_conn);
    let server = Arc::new(Server { sqlite_conn });
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
    Ok(axum::serve(listener, app).await?)
}

struct Server {
    sqlite_conn: Mutex<rusqlite::Connection>,
}

async fn iter_json_stream(
    mut body_data_stream: impl Stream<Item = Result<Bytes, axum::Error>> + Unpin,
    mut on_payload: impl FnMut(&[u8]) -> anyhow::Result<()>,
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

impl Server {
    async fn websocket_handler(&self, websocket: WebSocket, headers: &HeaderMap) {
        if let Err(err) = self.websocket_handler_err(websocket, headers).await {
            error!(?err, "error handling websocket");
        }
    }

    fn handle_message(&self, message: Message, stream_id: i64) -> anyhow::Result<()> {
        match message {
            Message::Close(reason) => {
                debug!(stream_id, ?reason, "websocket closed");
            }
            // Pings and pongs are apparently are handled for us by the library.
            Message::Ping(_) | Message::Pong(_) => {}
            // That should leave text and binary types, which we won't discriminate.
            _ => {
                let payload = message.to_text().context("converting payload to text")?;
                self.insert_event(payload, stream_id)?;
            }
        }
        Ok(())
    }

    async fn websocket_handler_err(
        &self,
        mut websocket: WebSocket,
        headers: &HeaderMap,
    ) -> anyhow::Result<()> {
        let stream_id = self.new_stream(headers)?;
        let mut counter = 0;
        loop {
            let (batch_count, last_recv_result) =
                Self::receive_consecutive_websocket_messages(&mut websocket, |message| {
                    self.handle_message(message, stream_id)
                })
                .await;
            let ack_result = if batch_count != 0 {
                counter += batch_count;
                Self::acknowledge_inserted(&mut websocket, counter).await
            } else {
                Ok(())
            };
            match last_recv_result {
                Err(err) => {
                    return Err(err);
                }
                Ok(false) => {
                    return Ok(());
                }
                Ok(true) => {}
            }
            ack_result.context("acknowledging received")?;
        }
    }

    async fn receive_consecutive_websocket_messages(
        websocket: &mut WebSocket,
        handle: impl Fn(Message) -> anyhow::Result<()>,
    ) -> (u64, anyhow::Result<bool>) {
        let message = match websocket.recv().await {
            Some(Ok(message)) => message,
            None => {
                return (0, Ok(false));
            }
            Some(Err(err)) => {
                return (0, Err(err.into()));
            }
        };
        match handle(message) {
            Err(err) => {
                return (0, Err(err));
            }
            Ok(()) => {}
        };
        let mut count = 1;
        loop {
            let option_recv = select! {
                a = websocket.recv().fuse() => a,
                default => break,
            };
            match option_recv {
                Some(Ok(message)) => match handle(message) {
                    Ok(()) => count += 1,
                    Err(err) => {
                        return (count, Err(err));
                    }
                },
                None => return (count, Ok(false)),
                Some(Err(err)) => {
                    return (count, Err(err.into()));
                }
            }
        }
        (count, Ok(true))
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

    fn new_stream(&self, headers: &HeaderMap) -> anyhow::Result<i64> {
        let conn = self.sqlite_conn.lock().unwrap();
        // This is a multimap, so we might need to do our own serialization elsewhere.
        let headers_string = format!("{:?}", headers);
        debug!(?headers_string, "headers string");
        let headers_value: serde_json::Value =
            serde_json::from_str(&headers_string).context(headers_string)?;
        Ok(conn.query_row(
            "insert into streams (headers, start_datetime) values (jsonb(?), datetime('now')) returning stream_id",
            rusqlite::params![headers_value],
            |row| row.get(0),
        )?)
    }

    fn insert_event(&self, payload: &str, stream_id: i64) -> anyhow::Result<()> {
        // Down the track this could be done in a separate thread, or under a
        // transaction each time we read a chunk.
        debug!(payload, "inserting payload into store");
        let conn = self.sqlite_conn.lock().unwrap();
        // I expect down the track we'll have a collector stream ID and submit stuff like headers
        // only once.
        conn.execute(
            "\
            insert into events (insert_datetime, payload, stream_id) \
            values (datetime('now'), jsonb(?), ?)",
            rusqlite::params![payload, stream_id],
        )
        .context("inserting payload into store")?;
        Ok(())
    }

    async fn submit_inner(
        &self,
        req: axum::http::Request<axum::body::Body>,
        payloads_inserted: &mut usize,
    ) -> StatusCode {
        let stream_id = match self.new_stream(req.headers()) {
            Err(err) => {
                error!(?err, "creating new stream");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            Ok(ok) => ok,
        };
        let body_data_stream = req.into_body().into_data_stream();
        let result = iter_json_stream(body_data_stream, move |payload| {
            // sqlite needs to be given text.
            let payload = std::str::from_utf8(payload).unwrap();
            self.insert_event(payload, stream_id)?;
            *payloads_inserted += 1;
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

#[allow(dead_code)]
fn sqlite_local_datetime_now_string() -> String {
    chrono::Local::now().to_rfc3339_opts(SecondsFormat::Millis, false)
}

#[cfg(test)]
mod tests {
    use crate::iter_json_stream;

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
