use super::*;
use crate::conn::Connection;
use crate::stream_id::StreamId;
use crate::{SerializedHeaders, StreamEventIndex};
use axum::async_trait;
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error};

pub struct Postgres {
    client: Client,
}

#[async_trait]
impl Connection for Postgres {
    async fn new_stream(&mut self, headers_value: SerializedHeaders) -> anyhow::Result<StreamId> {
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
    ) -> anyhow::Result<()> {
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
}

#[derive(Clone, clap::Args)]
pub(crate) struct PostgresOpener {
    #[arg(long)]
    pub schema_path: String,
    #[arg(long)]
    pub conn_str: String,
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
                    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
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
                    let (client, conn) = tokio_postgres::connect(&conn_str, connector).await?;
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
                .batch_execute(fs::read_to_string(schema_path)?.as_str())
                .await?;
            Postgres { client }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgtemp::PgTempDB;
    use serde_json::json;

    #[tokio::test]
    async fn test_postgres_new_stream_and_event() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let db = PgTempDB::async_new().await;
        let connection_uri = db.connection_uri();
        let db_conn = Arc::new(Mutex::new(
            PostgresOpener {
                schema_path: "sql/postgres.sql".to_owned(),
                conn_str: connection_uri.to_owned(),
                tls_cert_path: None,
            }
            .open()
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
        let (client, conn) = tokio_postgres::connect(&connection_uri, NoTls).await?;
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
        let event = events.first().expect("event row but found none");
        assert_eq!(event.get::<_, i32>("stream_event_index"), 0);
        assert_eq!(event.get::<_, i32>("stream_id"), 1);
        assert_eq!(event.get::<_, serde_json::Value>("payload"), payload);
        Ok(())
    }
}
