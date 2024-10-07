use super::*;
use crate::conn::{JsonFiles, Postgres};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use std::path::PathBuf;
use tokio_postgres::NoTls;
use tracing::{debug, error, warn};

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

    async fn open(self) -> anyhow::Result<Self::Conn> {
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

    async fn open(self) -> anyhow::Result<Self::Conn> {
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
    async fn open(self) -> anyhow::Result<Self::Conn>;
}

#[derive(Clone, clap::Args)]
pub struct JsonFilesOpen {}

impl StorageOpen for JsonFilesOpen {
    type Conn = JsonFiles;

    async fn open(self) -> anyhow::Result<Self::Conn> {
        let streams =
            crate::conn::JsonFileWriter::new("streams".to_owned()).context("opening streams")?;
        let events =
            crate::conn::JsonFileWriter::new("events".to_owned()).context("opening events")?;
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

    async fn open(self) -> anyhow::Result<Self::Conn> {
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
