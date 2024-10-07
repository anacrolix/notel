use super::*;
use axum::async_trait;
use chrono::Utc;
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use rand::random;
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use tokio_postgres::{Client, NoTls};

#[async_trait]
pub(crate) trait Connection: Send {
    async fn new_stream(&mut self, headers: SerializedHeaders) -> Result<StreamId>;
    async fn insert_event(
        &mut self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        // TODO: Could use payload type here to let implementation decide what to do.
        payload: &str,
    ) -> Result<()>;
    // Write stuff to disk
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    // Make data available to observers.
    async fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, clap::Args)]
struct LocalStorageArgs {
    #[arg(long)]
    schema_path: String,
    #[arg(long)]
    db_dir_path: PathBuf,
}

#[derive(Clone, clap::Args)]
pub struct SqliteOpen {
    #[command(flatten)]
    args: LocalStorageArgs,
}

impl StorageOpen for SqliteOpen {
    type Conn = rusqlite::Connection;

    async fn open(self) -> Result<Self::Conn> {
        let LocalStorageArgs {
            db_dir_path,
            schema_path,
        } = self.args;
        let db_path = db_dir_path.join("telemetry.sqlite.db");
        let schema_contents = fs::read_to_string(schema_path)?;
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

    async fn open(self) -> Result<Self::Conn> {
        let LocalStorageArgs {
            db_dir_path,
            schema_path,
        } = self.args;
        let db_path = db_dir_path.join("duck.db");
        let schema_contents = fs::read_to_string(schema_path)?;
        let mut conn = duckdb::Connection::open(db_path)?;
        let tx = conn.transaction()?;
        if let Err(err) = tx.execute_batch(&schema_contents) {
            warn!(%err, "initing duckdb schema (haven't figured out user_version yet)");
        }
        tx.commit()?;
        Ok(conn)
    }
}

pub trait StorageOpen {
    type Conn;
    async fn open(self) -> Result<Self::Conn>;
}

#[derive(Clone, clap::Args)]
pub struct JsonFilesOpen {}

impl StorageOpen for JsonFilesOpen {
    type Conn = JsonFiles;

    async fn open(self) -> Result<Self::Conn> {
        let streams = JsonFileWriter::new("streams".to_owned()).context("opening streams")?;
        let events = JsonFileWriter::new("events".to_owned()).context("opening events")?;
        Ok(JsonFiles { streams, events })
    }
}

#[derive(Clone, clap::Args)]
pub(crate) struct PostgresOpener {
    #[arg(long)]
    pub schema_path: Option<String>,
    #[arg(long)]
    pub conn_str: Option<String>,
    #[arg(long)]
    pub tls_cert_path: Option<String>,
}

impl StorageOpen for PostgresOpener {
    type Conn = Postgres;

    async fn open(self) -> Result<Self::Conn> {
        let PostgresOpener {
            tls_cert_path,
            conn_str,
            schema_path,
        } = self;
        Ok({
            let client = match tls_cert_path {
                None => {
                    debug!("Initializing postgres storage without TLS");
                    let (client, conn) = tokio_postgres::connect(&conn_str.unwrap(), NoTls).await?;
                    tokio::spawn(async move {
                        if let Err(err) = conn.await {
                            error!(%err, "postgres connection failed");
                        }
                    });
                    client
                }
                Some(tls_cert_path) => {
                    debug!("Initializing postgres storage with TLS");
                    let cert = fs::read(tls_cert_path)?;
                    let cert = Certificate::from_pem(&cert)?;
                    let connector = TlsConnector::builder().add_root_certificate(cert).build()?;
                    let connector = MakeTlsConnector::new(connector);
                    let (client, conn) =
                        tokio_postgres::connect(&conn_str.unwrap(), connector).await?;
                    tokio::spawn(async move {
                        if let Err(err) = conn.await {
                            error!(%err, "postgres connection failed");
                        }
                    });
                    client
                }
            };
            // Init DB schema
            client
                .batch_execute(fs::read_to_string(schema_path.unwrap())?.as_str())
                .await?;
            Postgres { client }
        })
    }
}

pub struct Postgres {
    client: Client,
}

#[async_trait]
impl Connection for Postgres {
    async fn new_stream(&mut self, headers_value: SerializedHeaders) -> Result<StreamId> {
        let stmt = self
            .client
            .prepare(
                "INSERT INTO streams (headers, start_datetime) VALUES ($1, NOW()) RETURNING stream_id",
            )
            .await?;
        let stream_id: i32 = self
            .client
            .query_one(&stmt, &[&headers_value])
            .await?
            .get(0);
        Ok(StreamId(stream_id as u32))
    }

    async fn insert_event(
        &mut self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> Result<()> {
        let payload_value: serde_json::Value = serde_json::from_str(payload)?;
        let stmt = self
            .client
            .prepare(
                "INSERT INTO events (insert_datetime, stream_event_index, payload, stream_id) VALUES (NOW(), $1, $2, $3)",
            )
            .await?;
        self.client
            .execute(
                &stmt,
                &[
                    &(stream_event_index as i32),
                    &payload_value,
                    &(stream_id.0 as i32),
                ],
            )
            .await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        // Not necessary for Postgres
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        // Not necessary for Postgres
        Ok(())
    }
}

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
    fn new(table: String) -> Result<Self> {
        Ok(Self { w: None, table })
    }
    /// Flushes the compressed stream but keeps the file open for the next stream.
    fn flush(&mut self) -> Result<()> {
        if let Some(file) = self.finish_stream()? {
            self.w = Some(Self::new_encoder(file)?)
        }
        Ok(())
    }
    fn finish_file(&mut self) -> Result<()> {
        self.finish_stream()?;
        Ok(())
    }
    fn finish_stream(&mut self) -> Result<Option<NamedTempFile>> {
        let Some(w) = self.w.take() else {
            return Ok(None);
        };
        Ok(Some(w.finish()?))
    }
    fn new_encoder(file: NamedTempFile) -> Result<zstd::Encoder<'static, NamedTempFile>> {
        Ok(zstd::Encoder::new(file, 0)?)
    }
    fn open(&mut self) -> Result<()> {
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
    fn write(&mut self) -> Result<impl Write + '_> {
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
    async fn new_stream(&mut self, headers_value: SerializedHeaders) -> Result<StreamId> {
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
    ) -> Result<()> {
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
    async fn new_stream(&mut self, headers_value: SerializedHeaders) -> Result<StreamId> {
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
    ) -> Result<()> {
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
    async fn new_stream(&mut self, headers: SerializedHeaders) -> Result<StreamId> {
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
    ) -> Result<()> {
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

    async fn flush(&mut self) -> Result<()> {
        self.streams.flush()?;
        self.events.flush()?;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.streams.finish_file()?;
        self.events.finish_file()?;
        Ok(())
    }
}

impl Drop for JsonFiles {
    fn drop(&mut self) {
        let mut conn = self.take();
        tokio::spawn(async move {
            if let Err(err) = conn.commit().await {
                error!(%err, "logging commit");
            } else {
                info!("committed");
            }
        });
    }
}
