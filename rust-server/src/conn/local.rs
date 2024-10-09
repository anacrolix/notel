use super::*;
use crate::conn::Connection;
use crate::stream_id::StreamId;
use crate::{log_commit, SerializedHeaders, StreamEventIndex};
use anyhow::Context;
use axum::async_trait;
use chrono::Utc;
use rand::random;
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use tracing::warn;

struct JsonFileWriter {
    w: Option<zstd::Encoder<'static, NamedTempFile>>,
    table: String,
}

impl JsonFileWriter {
    fn take(&mut self) -> Self {
        Self {
            w: self.w.take(),
            table: std::mem::take(&mut self.table),
        }
    }
    fn new(table: String) -> anyhow::Result<Self> {
        Ok(Self { w: None, table })
    }
    /// Flushes the compressed stream but keeps the file open for the next stream.
    fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(file) = self.finish_stream()? {
            self.w = Some(Self::new_encoder(file)?)
        }
        Ok(())
    }
    fn finish_file(&mut self) -> anyhow::Result<()> {
        self.finish_stream()?;
        Ok(())
    }
    fn finish_stream(&mut self) -> anyhow::Result<Option<NamedTempFile>> {
        let Some(w) = self.w.take() else {
            return Ok(None);
        };
        Ok(Some(w.finish()?))
    }
    fn new_encoder(file: NamedTempFile) -> anyhow::Result<zstd::Encoder<'static, NamedTempFile>> {
        Ok(zstd::Encoder::new(file, 0)?)
    }
    fn open(&mut self) -> anyhow::Result<()> {
        self.finish_file()?;
        let dir_path = "json_files";
        std::fs::create_dir_all(dir_path)?;
        let temp_file = tempfile::Builder::new()
            .prefix(&format!("{}.file.", self.table))
            .append(true)
            .suffix(".json.zst")
            .keep(true)
            .tempfile_in(dir_path)
            .context("opening temp file")?;
        self.w = Some(Self::new_encoder(temp_file)?);
        Ok(())
    }
    fn write(&mut self) -> anyhow::Result<impl Write + '_> {
        if self.w.is_none() {
            self.open()?;
        }
        Ok(self.w.as_mut().unwrap())
    }
}

impl Drop for JsonFileWriter {
    fn drop(&mut self) {
        self.finish_file().unwrap();
    }
}

#[async_trait]
impl Connection for rusqlite::Connection {
    async fn new_stream(&mut self, headers_value: SerializedHeaders) -> anyhow::Result<StreamId> {
        Ok(self.query_row(
            "\
            insert into streams\
                (headers, start_datetime)\
                values (jsonb(?), datetime('now'))\
                returning stream_id",
            rusqlite::params![headers_value],
            |row| row.get(0),
        )?)
    }
    async fn insert_event(
        &mut self,
        stream_id: StreamId,
        _stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> anyhow::Result<()> {
        self.execute(
            "\
            insert into events (insert_datetime, payload, stream_id) \
            values (datetime('now'), jsonb(?), ?)",
            rusqlite::params![payload, stream_id],
        )?;
        Ok(())
    }
}

#[async_trait]
impl Connection for duckdb::Connection {
    async fn new_stream(&mut self, headers_value: SerializedHeaders) -> anyhow::Result<StreamId> {
        Ok(self.query_row(
            "insert into streams (headers) values (?) returning stream_id",
            duckdb::params![headers_value],
            |row| row.get(0),
        )?)
    }
    async fn insert_event(
        &mut self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> anyhow::Result<()> {
        self.execute(
            "\
            insert into events (stream_event_index, payload, stream_id) \
            values (?, ?, ?)",
            duckdb::params![stream_event_index, payload, stream_id],
        )?;
        Ok(())
    }
}

pub struct JsonFiles {
    streams: JsonFileWriter,
    events: JsonFileWriter,
}

impl JsonFiles {
    fn take(&mut self) -> Self {
        Self {
            streams: self.streams.take(),
            events: self.events.take(),
        }
    }
}

fn json_datetime_now() -> serde_json::Value {
    json!(Utc::now().to_rfc3339())
}

#[async_trait]
impl Connection for JsonFiles {
    async fn new_stream(&mut self, headers: SerializedHeaders) -> anyhow::Result<StreamId> {
        let stream_id: StreamId = StreamId(random());
        let start_datetime = Utc::now().to_rfc3339();
        let json_value = json!({
            "stream_id": stream_id.0,
            "start_datetime": start_datetime,
            "headers": headers,
        });
        let mut writer = self.streams.write()?;
        serde_json::to_writer(&mut writer, &json_value)?;
        writer.write_all(b"\n")?;
        Ok(stream_id)
    }

    async fn insert_event(
        &mut self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> anyhow::Result<()> {
        let payload_value: serde_json::Value = serde_json::from_str(payload)?;
        let line_json = json!({
            "insert_datetime": json_datetime_now(),
            "stream_id": stream_id.0,
            "stream_event_index": stream_event_index,
            "payload": payload_value,
        });
        let mut writer = self.events.write()?;
        serde_json::to_writer(&mut writer, &line_json)?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.streams.flush()?;
        self.events.flush()?;
        Ok(())
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        self.streams.finish_file()?;
        self.events.finish_file()?;
        Ok(())
    }

    fn commit_on_sigint(&self) -> bool {
        true
    }
}

impl Drop for JsonFiles {
    fn drop(&mut self) {
        let mut conn = self.take();
        tokio::spawn(async move { log_commit(&mut conn).await.unwrap() });
    }
}

#[derive(Clone, clap::Args)]
struct LocalStorageArgs {
    #[arg(long)]
    schema_path: Option<String>,
    // I'd rather have a default for duck DB and sqlite that are exposed here, but LocalStorageArgs
    // is shared by them.
    #[arg(long)]
    db_path: Option<PathBuf>,
}

impl LocalStorageArgs {
    fn open_schema_path_or_embedded(&self, embedded: &str) -> anyhow::Result<String> {
        if let Some(schema_path) = &self.schema_path {
            Ok(fs::read_to_string(schema_path)?)
        } else {
            Ok(embedded.to_owned())
        }
    }
}

#[derive(Clone, clap::Args)]
pub struct SqliteOpen {
    #[command(flatten)]
    args: LocalStorageArgs,
}

impl StorageOpen for SqliteOpen {
    type Conn = rusqlite::Connection;

    async fn open(self) -> anyhow::Result<Self::Conn> {
        let db_path = self
            .args
            .db_path
            .clone()
            .unwrap_or_else(|| "telemetry.sqlite.db".to_owned().into());
        let schema_contents = self
            .args
            .open_schema_path_or_embedded(include_str!("../../sql/sqlite.sql"))?;
        let mut conn = rusqlite::Connection::open(db_path)?;
        conn.pragma_update(None, "foreign_keys", "on")?;
        if !conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))? {
            warn!("foreign keys not enabled");
        }
        let tx = conn.transaction()?;
        let user_version: u64 = tx.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if user_version == 0 {
            tx.execute_batch(&schema_contents)?;
            tx.pragma_update(None, "user_version", 1)?;
        }
        tx.commit()?;
        Ok(conn)
    }
}

#[derive(Clone, clap::Args)]
pub struct DuckDbOpen {
    #[command(flatten)]
    args: LocalStorageArgs,
}

impl StorageOpen for DuckDbOpen {
    type Conn = duckdb::Connection;

    async fn open(self) -> anyhow::Result<Self::Conn> {
        let db_path = self
            .args
            .db_path
            .clone()
            .unwrap_or_else(|| "duck.db".into());
        let schema_contents = self
            .args
            .open_schema_path_or_embedded(include_str!("../../sql/duckdb.sql"))?;
        let mut conn = duckdb::Connection::open(db_path)?;
        let tx = conn.transaction()?;
        if let Err(err) = tx.execute_batch(&schema_contents) {
            warn!(%err, "initing duckdb schema (haven't figured out user_version yet)");
        }
        tx.commit()?;
        Ok(conn)
    }
}

#[derive(Clone, clap::Args)]
pub struct JsonFilesOpen {}

impl StorageOpen for JsonFilesOpen {
    type Conn = JsonFiles;

    async fn open(self) -> anyhow::Result<Self::Conn> {
        let streams = JsonFileWriter::new("streams".to_owned()).context("opening streams")?;
        let events = JsonFileWriter::new("events".to_owned()).context("opening events")?;
        Ok(JsonFiles { streams, events })
    }
}

impl rusqlite::types::FromSql for StreamId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(Self(rusqlite::types::FromSql::column_result(value)?))
    }
}

impl rusqlite::types::ToSql for StreamId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        rusqlite::types::ToSql::to_sql(&self.0)
    }
}

impl duckdb::types::FromSql for StreamId {
    fn column_result(value: duckdb::types::ValueRef<'_>) -> duckdb::types::FromSqlResult<Self> {
        Ok(Self(duckdb::types::FromSql::column_result(value)?))
    }
}

impl duckdb::types::ToSql for StreamId {
    fn to_sql(&self) -> duckdb::Result<duckdb::types::ToSqlOutput<'_>> {
        duckdb::types::ToSql::to_sql(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "local")]
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

    #[cfg(feature = "local")]
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
}
