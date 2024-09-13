use std::fmt::{Display, Formatter};
use std::ops::Deref;

/// Let's see if u32 is enough. Newtype for nicer formatting.
#[derive(Copy, Clone)]
pub(crate) struct StreamId(pub u32);

impl Display for StreamId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

impl Deref for StreamId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
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
