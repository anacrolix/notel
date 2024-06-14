use anyhow::{anyhow, Context};
use axum::body::Bytes;
use axum::http::StatusCode;
use futures::{Stream, StreamExt};
use std::sync::{Arc, Mutex};
use tracing::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let sqlite_conn = Mutex::new(rusqlite::Connection::open("telemetry.db")?);
    let server = Arc::new(Server { sqlite_conn });
    let tower_layer = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(tower_http::trace::DefaultMakeSpan::new().include_headers(true))
        .on_request(())
        .on_body_chunk(());
    let app = axum::Router::new()
        .route(
            "/",
            axum::routing::post(|body| async move { server.submit(body).await }),
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
    // Eventually this might return a list of items added, or a count, so that callers can throw
    // away what they know was committed.
    async fn submit(&self, req: axum::http::Request<axum::body::Body>) -> (StatusCode, String) {
        let mut payloads_inserted = 0;
        let status_code = self.submit_inner(req, &mut payloads_inserted).await;
        info!(payloads_inserted, "submit handled ok");
        (status_code, format!("{}", payloads_inserted))
    }

    async fn submit_inner(
        &self,
        req: axum::http::Request<axum::body::Body>,
        payloads_inserted: &mut usize,
    ) -> StatusCode {
        let body_data_stream = req.into_body().into_data_stream();
        let result = iter_json_stream(body_data_stream, move |payload| {
            // sqlite needs to be given text.
            let payload = std::str::from_utf8(payload).unwrap();
            // Down the track this could be done in a separate thread, or under a
            // transaction each time we read a chunk.
            self.sqlite_conn
                .lock()
                .unwrap()
                .execute("insert into data (payload) values (jsonb(?))", [payload])
                .context("inserting payload into store")?;
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
