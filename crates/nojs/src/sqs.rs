//! The SDK-backed [`JoinQueue`]: the same join queue `/v1/join` writes to.

use aws_sdk_sqs::Client;

use crate::{JoinQueue, QueueError};

pub struct SqsJoinQueue {
    client: Client,
    queue_url: String,
}

impl SqsJoinQueue {
    #[must_use]
    pub const fn new(client: Client, queue_url: String) -> Self {
        Self { client, queue_url }
    }
}

impl JoinQueue for SqsJoinQueue {
    async fn send(&self, body: String) -> Result<(), QueueError> {
        self.client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(body)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| QueueError(e.to_string()))
    }
}
