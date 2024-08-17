use super::*;
use chrono::Utc;
use rand::random;
use serde_json::json;
use tempfile::NamedTempFile;

pub(crate) trait Connection {
    fn new_stream(&mut self, headers: SerializedHeaders) -> Result<StreamId>;
    fn insert_event(
        &mut self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> Result<()>;
    // Write stuff to disk
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    // Make data available to observers.
    fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}

impl dyn Connection {
    pub(crate) fn open(storage: Storage) -> Result<Box<dyn Connection + Send>> {
        Ok(match storage {
            Storage::Sqlite => Box::new({
                let mut conn = rusqlite::Connection::open("telemetry.db")?;
                conn.pragma_update(None, "foreign_keys", "on")?;
                if !conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))? {
                    warn!("foreign keys not enabled");
                }
                let tx = conn.transaction()?;
                let user_version: u64 =
                    tx.pragma_query_value(None, "user_version", |row| row.get(0))?;
                if user_version == 0 {
                    tx.execute_batch(include_str!("../sql/sqlite.sql"))?;
                    tx.pragma_update(None, "user_version", 1)?;
                }
                tx.commit()?;
                conn
            }),
            Storage::DuckDB => Box::new({
                let mut conn = duckdb::Connection::open("duck.db")?;
                let tx = conn.transaction()?;
                if let Err(err) = tx.execute_batch(include_str!("../sql/duckdb.sql")) {
                    warn!(%err, "initing duckdb schema (haven't figured out user_version yet)");
                }
                tx.commit()?;
                conn
            }),
            Storage::JsonFiles => Box::new({
                let streams =
                    JsonFileWriter::new("streams".to_owned()).context("opening streams")?;
                let events = JsonFileWriter::new("events".to_owned()).context("opening events")?;
                JsonFiles { streams, events }
            }),
        })
    }
}

struct JsonFileWriter {
    w: Option<zstd::Encoder<'static, NamedTempFile>>,
    table: String,
}

impl JsonFileWriter {
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

impl Connection for rusqlite::Connection {
    fn new_stream(&mut self, headers_value: SerializedHeaders) -> Result<StreamId> {
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
    fn insert_event(
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

impl Connection for duckdb::Connection {
    fn new_stream(&mut self, headers_value: SerializedHeaders) -> Result<StreamId> {
        Ok(self.query_row(
            "insert into streams (headers) values (?) returning stream_id",
            duckdb::params![headers_value],
            |row| row.get(0),
        )?)
    }
    fn insert_event(
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

struct JsonFiles {
    streams: JsonFileWriter,
    events: JsonFileWriter,
}

fn json_datetime_now() -> serde_json::Value {
    json!(Utc::now().to_rfc3339())
}

impl Connection for JsonFiles {
    fn new_stream(&mut self, headers: SerializedHeaders) -> Result<StreamId> {
        let stream_id: StreamId = random();
        let start_datetime = Utc::now().to_rfc3339();
        let json_value = json!({
            "stream_id": stream_id,
            "start_datetime": start_datetime,
            "headers": headers,
        });
        let mut writer = self.streams.write()?;
        serde_json::to_writer(&mut writer, &json_value)?;
        writer.write_all(b"\n")?;
        Ok(stream_id)
    }

    fn insert_event(
        &mut self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> Result<()> {
        let line_json = json!({
            "insert_datetime": json_datetime_now(),
            "stream_id": stream_id,
            "stream_event_index": stream_event_index,
            "payload": payload,
        });
        let mut writer = self.events.write()?;
        serde_json::to_writer(&mut writer, &line_json)?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.streams.flush()?;
        self.events.flush()?;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.streams.finish_file()?;
        self.events.finish_file()?;
        Ok(())
    }
}

impl Drop for JsonFiles {
    fn drop(&mut self) {
        log_commit(self).unwrap()
    }
}
