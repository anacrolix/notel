#[cfg(feature = "local")]
pub(crate) mod local;
#[cfg(feature = "postgres")]
pub(crate) mod postgres;

use super::*;
use axum::async_trait;

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
    /// Whether sigint should be hooked to trigger a commit. Some storage types buffer output and
    /// need to be committed to ensure observability of data up to the point of the commit.
    fn commit_on_sigint(&self) -> bool {
        false
    }
}

pub trait StorageOpen {
    type Conn;
    async fn open(self) -> anyhow::Result<Self::Conn>;
}
