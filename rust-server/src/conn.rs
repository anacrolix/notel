use super::*;

pub(crate) trait Connection {
    fn new_stream(&self, headers: SerializedHeaders) -> anyhow::Result<StreamId>;
    fn insert_event(
        &self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> Result<()>;
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
                    tx.execute_batch(include_str!("../schemas/sqlite.sql"))?;
                    tx.pragma_update(None, "user_version", 1)?;
                }
                tx.commit()?;
                conn
            }),
            Storage::DuckDB => Box::new({
                let mut conn = duckdb::Connection::open("duck.db")?;
                let tx = conn.transaction()?;
                if let Err(err) = tx.execute_batch(include_str!("../schemas/duckdb.sql")) {
                    warn!(%err, "initing duckdb schema (haven't figured out user_version yet)");
                }
                tx.commit()?;
                conn
            }),
        })
    }
}

impl Connection for rusqlite::Connection {
    fn new_stream(&self, headers_value: SerializedHeaders) -> anyhow::Result<StreamId> {
        Ok(self.query_row(
            "insert into streams (headers, start_datetime) values (jsonb(?), datetime('now')) returning stream_id",
            rusqlite::params![headers_value],
            |row| row.get(0),
        )?)
    }
    fn insert_event(
        &self,
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
    fn new_stream(&self, headers_value: SerializedHeaders) -> anyhow::Result<StreamId> {
        Ok(self.query_row(
            "insert into streams (headers) values (?) returning stream_id",
            duckdb::params![headers_value],
            |row| row.get(0),
        )?)
    }
    fn insert_event(
        &self,
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
