use super::*;
use crate::{headers_to_json_value, iter_json_stream};
use axum::http::HeaderMap;
use pgtemp::PgTempDB;
use serde_json::json;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_postgres_new_stream_and_event() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db = PgTempDB::async_new().await;
    let db_conn = Arc::new(Mutex::new(
        <dyn Connection>::open(
            Storage::Postgres,
            Some("sql/postgres.sql".to_owned()),
            Some(db.connection_uri()),
            None,
            None,
        )
        .await
        .expect("opening test postgres db"),
    ));

    let headers = json!({
        "some": "headers",
        "some_more": "headers",
    });
    let stream_id = db_conn
        .lock()
        .await
        .new_stream(headers)
        .await
        .expect("new stream");
    assert_eq!(stream_id.0, 1);

    // Insert new event to that stream
    let payload = json!(
    {"this": "is", "a": "test", "payload": "for", "the": "new", "stream": "id"}
    );
    db_conn
        .lock()
        .await
        .insert_event(stream_id, 0, &payload.to_string())
        .await
        .expect("inserting event");

    // Assert that the event was inserted
    let (client, conn) = tokio_postgres::connect(&db.connection_uri(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            error!(%err, "postgres connection failed");
        }
    });
    let events = client
        .query("SELECT * FROM events", &[])
        .await
        .expect("querying events");
    assert_eq!(events.len(), 1);
    let event = events.get(0).expect("event row but found none");
    assert_eq!(event.get::<_, i32>("stream_event_index"), 0);
    assert_eq!(event.get::<_, i32>("stream_id"), 1);
    assert_eq!(event.get::<_, serde_json::Value>("payload"), payload);
    Ok(())
}

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
            async move { Ok(()) }
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
            async { Ok(()) }
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
