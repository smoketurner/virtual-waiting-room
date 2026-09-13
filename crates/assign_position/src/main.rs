//! Lambda entry point: an SQS event source mapping delivers a batch of join
//! messages; each is assigned a queue position and the ids to retry are
//! returned as partial-batch failures.

use assign_position::dynamo::DynamoStore;
use assign_position::{BatchRecord, process_batch};
use aws_lambda_events::sqs::{
    BatchItemFailure, SqsBatchResponse, SqsEvent, SqsMessage, SqsMessageAttribute,
};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use wr_common::{Shard, Telemetry};

/// Resolved once at cold start and shared across invocations.
struct AppState {
    store: DynamoStore,
    event_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    let state = init().await?;

    lambda_runtime::run(service_fn(|event: LambdaEvent<SqsEvent>| {
        handle(&state, event)
    }))
    .await
}

async fn init() -> Result<AppState, Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_dynamodb::Client::new(&config);
    let counters_table = std::env::var("COUNTERS_TABLE")?;
    let prequeue_table = std::env::var("PREQUEUE_TABLE")?;
    let positions_table = std::env::var("POSITIONS_TABLE")?;
    let event_id = std::env::var("EVENT_ID")?;
    let store = DynamoStore::new(client, counters_table, prequeue_table, positions_table);

    Ok(AppState { store, event_id })
}

// The SqsBatchResponse / BatchItemFailure event structs are #[non_exhaustive],
// so they cannot be built with struct-literal syntax from outside their crate;
// Default + field assignment is the only construction path.
async fn handle(state: &AppState, event: LambdaEvent<SqsEvent>) -> Result<SqsBatchResponse, Error> {
    let records: Vec<BatchRecord> = event
        .payload
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
            return Ok(response);
        }
    };

    let outcome = process_batch(&state.store, &state.event_id, shard, &records).await;

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
    Ok(response)
}

/// Maps an SQS message to a batch record, dropping any message with no id or
/// no body — such a message cannot be processed or reported, so it is
/// skipped. Viewer telemetry rides as message attributes (issue #59) rather
/// than in the body, so it is lifted here rather than parsed from JSON.
fn record_from_sqs(msg: SqsMessage) -> Option<BatchRecord> {
    let telemetry = telemetry_from_attributes(&msg.message_attributes);
    Some(BatchRecord {
        message_id: msg.message_id?,
        body: msg.body?,
        telemetry,
    })
}

/// Reads the six `CloudFront`-viewer message attributes the SQS `SendMessage`
/// request template attaches. Every attribute is optional: the template omits
/// one whose source header was empty (SQS rejects an empty `StringValue`), so
/// a missing key here means "not reported", not corruption.
fn telemetry_from_attributes(
    attrs: &std::collections::HashMap<String, SqsMessageAttribute>,
) -> Telemetry {
    let value = |name: &str| attrs.get(name).and_then(|a| a.string_value.clone());
    Telemetry {
        a: value("va"),
        n: value("vn"),
        c: value("vc"),
        j: value("vj"),
        // Truncated to 256 bytes: a customer's User-Agent has no length
        // bound, and this is stored, not just logged.
        u: value("vu").map(|ua| truncate_bytes(&ua, 256)),
        q: value("vq"),
    }
}

/// Truncates `s` to at most `max_bytes` bytes on a UTF-8 character boundary,
/// so a multi-byte character at the cut point is dropped whole rather than
/// split into invalid UTF-8.
fn truncate_bytes(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_owned();
    }
    let boundary = s.floor_char_boundary(max_bytes);
    s[..boundary].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SqsMessageAttribute` is `#[non_exhaustive]`, so it is built by mutation
    /// rather than a struct literal.
    fn string_attribute(value: &str) -> SqsMessageAttribute {
        let mut attribute = SqsMessageAttribute::default();
        attribute.string_value = Some(value.to_owned());
        attribute.data_type = Some("String".to_owned());
        attribute
    }

    #[test]
    fn telemetry_from_attributes_reads_present_keys_and_leaves_absent_ones_none() {
        let mut attrs = std::collections::HashMap::new();
        attrs.insert("vc".to_owned(), string_attribute("US"));
        let telemetry = telemetry_from_attributes(&attrs);
        assert_eq!(telemetry.c, Some("US".to_owned()));
        assert_eq!(telemetry.a, None);
        assert_eq!(telemetry.n, None);
        assert_eq!(telemetry.j, None);
        assert_eq!(telemetry.u, None);
        assert_eq!(telemetry.q, None);
    }

    #[test]
    fn a_missing_header_does_not_fail_the_mapping() {
        // The VTL template omits an attribute whose source header was empty;
        // this must degrade to None, never panic or error.
        let telemetry = telemetry_from_attributes(&std::collections::HashMap::new());
        assert_eq!(telemetry, Telemetry::default());
    }

    #[test]
    fn a_long_user_agent_is_truncated_not_rejected() {
        let ua = "a".repeat(300);
        assert_eq!(truncate_bytes(&ua, 256).len(), 256);
        let short = "short-ua";
        assert_eq!(truncate_bytes(short, 256), short);
    }

    #[test]
    fn truncation_lands_on_a_char_boundary() {
        // A 3-byte UTF-8 character straddling the cut point must be dropped
        // whole, never split into invalid UTF-8.
        let s = format!("{}{}", "a".repeat(255), '€');
        let truncated = truncate_bytes(&s, 256);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(String::from_utf8(truncated.into_bytes()).is_ok());
    }
}
