//! The join queue, for sending a join back to wait for the open.

use aws_sdk_sqs::Client;
use jiff::SignedDuration;

use crate::{Queue, StoreError};

/// The join queue the event source mapping drains.
pub struct JoinQueue {
    client: Client,
    queue_url: String,
}

impl JoinQueue {
    #[must_use]
    pub fn new(client: Client, queue_url: String) -> Self {
        Self { client, queue_url }
    }
}

impl Queue for JoinQueue {
    async fn defer(&self, receipt_handle: &str, delay: SignedDuration) -> Result<(), StoreError> {
        let seconds = i32::try_from(delay.as_secs()).unwrap_or(i32::MAX);
        self.client
            .change_message_visibility()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt_handle)
            .visibility_timeout(seconds)
            .send()
            .await
            .map_err(|e| StoreError(format!("change_message_visibility: {e}")))?;
        Ok(())
    }
}
