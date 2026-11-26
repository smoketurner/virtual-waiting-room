//! The SQS event handler: a batch in, the ids to retry out. The runtime in
//! `main.rs` calls it with the live store, and the local simulation with its
//! own.

use aws_lambda_events::sqs::{BatchItemFailure, SqsBatchResponse, SqsEvent, SqsMessage};
use jiff::Timestamp;
use wr_common::Shard;

use crate::{BatchRecord, Store, process_batch};

// The SqsBatchResponse / BatchItemFailure event structs are #[non_exhaustive],
// so they cannot be built with struct-literal syntax from outside their crate;
// Default + field assignment is the only construction path.
/// Assigns one SQS batch of joins and returns the ids to retry.
pub async fn handle<S: Store>(store: &S, event_id: &str, event: SqsEvent) -> SqsBatchResponse {
    let records: Vec<BatchRecord> = event
        .records
        .into_iter()
        .filter_map(record_from_sqs)
        .collect();

    // Drawn once per invocation (issue #59): server-random rather than
    // derived from request_id, so a batch that takes the pre-queue path
    // claims one contiguous block on this one shard. A draw failure fails the
    // whole batch for retry rather than falling back to a fixed shard, which
    // would reintroduce the attacker-steerable behaviour this replaces.
    let shard = match Shard::random() {
        Ok(shard) => shard,
        Err(err) => {
            tracing::error!(error = %err, "failed to draw a random shard; retrying batch");
            let mut response = SqsBatchResponse::default();
            response.batch_item_failures = records
                .into_iter()
                .map(|record| {
                    let mut failure = BatchItemFailure::default();
                    failure.item_identifier = record.message_id;
                    failure
                })
                .collect();
            return response;
        }
    };

    let outcome = process_batch(store, event_id, shard, &records).await;

    let batch_item_failures = outcome
        .failures
        .into_iter()
        .map(|id| {
            let mut failure = BatchItemFailure::default();
            failure.item_identifier = id;
            failure
        })
        .collect();

    let mut response = SqsBatchResponse::default();
    response.batch_item_failures = batch_item_failures;
    response
}

/// Maps an SQS message to a batch record, dropping any message with no id, no
/// body or no `SentTimestamp` — such a message cannot be processed or
/// reported, so it is skipped.
fn record_from_sqs(msg: SqsMessage) -> Option<BatchRecord> {
    let sent_ms: i64 = msg.attributes.get("SentTimestamp")?.parse().ok()?;
    Some(BatchRecord {
        message_id: msg.message_id?,
        body: msg.body?,
        sent_at: Timestamp::from_millisecond(sent_ms).ok()?,
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    fn message(sent: Option<&str>) -> SqsMessage {
        let mut msg = SqsMessage::default();
        msg.message_id = Some("m1".to_owned());
        msg.body = Some("{}".to_owned());
        if let Some(sent) = sent {
            msg.attributes
                .insert("SentTimestamp".to_owned(), sent.to_owned());
        }
        msg
    }

    #[test]
    fn a_record_is_stamped_with_when_sqs_accepted_it() {
        let record = record_from_sqs(message(Some("1788000000250"))).unwrap();
        assert_eq!(
            record.sent_at,
            Timestamp::from_millisecond(1_788_000_000_250).unwrap()
        );
    }

    #[test]
    fn a_record_without_a_usable_sent_time_is_skipped() {
        assert!(record_from_sqs(message(None)).is_none());
        assert!(record_from_sqs(message(Some("yesterday"))).is_none());
    }
}
