//! The `aws-sdk-sqs`-backed [`Backlog`]: the join queue's waiting count.

use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::QueueAttributeName;

use crate::{Backlog, StoreError};

/// The join queue, read for how many messages are waiting.
pub struct JoinBacklog {
    client: Client,
    queue_url: String,
}

impl JoinBacklog {
    #[must_use]
    pub fn new(client: Client, queue_url: String) -> Self {
        Self { client, queue_url }
    }
}

impl Backlog for JoinBacklog {
    async fn waiting(&self) -> Result<u64, StoreError> {
        let out = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
            .map_err(|e| StoreError(format!("get_queue_attributes: {e}")))?;
        out.attributes()
            .and_then(|a| a.get(&QueueAttributeName::ApproximateNumberOfMessages))
            .and_then(|n| n.parse::<u64>().ok())
            .ok_or_else(|| StoreError("join queue reported no message count".to_owned()))
    }
}
