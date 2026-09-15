//! End-to-end check of the parse-error report path against a real `DynamoDB`,
//! via `DynamoDB` Local (`docker run -p 8000:8000 amazon/dynamodb-local:2.5.1`).
//!
//! `#[ignore]`'d so `cargo test` and CI never need the emulator: run it with
//! `cargo test -p seal_event -- --ignored e2e_dynamodb_local_writes_the_configured_mode` after
//! starting `DynamoDB` Local, with `DDB_LOCAL_ENDPOINT` pointing at it
//! (default `http://127.0.0.1:8000`). It exercises the *production* `DynamoStore`
//! — the same `read_shard_counts`/`write_seal`/`write_report` that the Lambda
//! runs — not the `FakeStore` the unit tests use, so it confirms the persisted
//! `EVT#{event_id}#DM` item carries the configured mode.

#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "e2e test panics on setup failure"
)]

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use seal_event::dynamo::DynamoStore;
use seal_event::{DemotionConfig, seal_event};
use wr_common::DemotionMode;

const COUNTERS: &str = "vwr-e2e-counters";
const PREQUEUE: &str = "vwr-e2e-prequeue";

fn client() -> Client {
    let endpoint =
        std::env::var("DDB_LOCAL_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:8000".to_owned());
    let cfg = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "e2e"))
        .build();
    Client::from_conf(cfg)
}

async fn recreate_counters(client: &Client) {
    // Ignore "table not found" on the drop; the create is the real setup.
    let _ = client.delete_table().table_name(COUNTERS).send().await;
    client
        .create_table()
        .table_name(COUNTERS)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("event_id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("event_id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create Counters table");
}

/// Pre-seeds shard 0 with `count` registrations, so the cohort is nonzero —
/// matching the unit-test scenario the report mislabels in the bug.
async fn seed_shard_zero(client: &Client, event_id: &str, count: u64) {
    client
        .put_item()
        .table_name(COUNTERS)
        .item(
            "event_id",
            AttributeValue::S(format!("EVT#{event_id}#PQ#0")),
        )
        .item("s", AttributeValue::N("0".to_owned()))
        .item("n", AttributeValue::N(count.to_string()))
        .send()
        .await
        .expect("seed shard 0");
}

async fn read_report(client: &Client, event_id: &str) -> HashMap<String, AttributeValue> {
    client
        .get_item()
        .table_name(COUNTERS)
        .key("event_id", AttributeValue::S(format!("EVT#{event_id}#DM")))
        .consistent_read(true)
        .send()
        .await
        .expect("get report item")
        .item()
        .cloned()
        .unwrap_or_default()
}

fn mode_of(item: &HashMap<String, AttributeValue>) -> String {
    match item.get("mode") {
        Some(AttributeValue::S(s)) => s.clone(),
        other => format!("unexpected mode attribute: {other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs DynamoDB Local on $DDB_LOCAL_ENDPOINT (default http://127.0.0.1:8000)"]
async fn e2e_dynamodb_local_writes_the_configured_mode() {
    let client = client();
    recreate_counters(&client).await;

    // Enforce: the bug recorded "observe". The fix records the configured mode.
    seed_shard_zero(&client, "evt-force", 10).await;
    let store = DynamoStore::new(client.clone(), COUNTERS.to_owned(), PREQUEUE.to_owned());
    seal_event(
        &store,
        "evt-force",
        [1u8; 32],
        [0u8; 8],
        &DemotionConfig::parse("address:lots", DemotionMode::Enforce),
        5,
    )
    .await
    .expect("seal enforce-path event");
    let report = read_report(&client, "evt-force").await;
    assert_eq!(mode_of(&report), "enforce");
    assert!(matches!(report.get("error"), Some(AttributeValue::S(_))));
    assert!(matches!(report.get("rules"), Some(AttributeValue::S(s)) if s == "address:lots"));
    assert!(matches!(report.get("cohort"), Some(AttributeValue::N(_))));

    // Observe: must stay observe (no relabel regression), forwarded verbatim.
    seed_shard_zero(&client, "evt-observe", 10).await;
    seal_event(
        &store,
        "evt-observe",
        [2u8; 32],
        [0u8; 8],
        &DemotionConfig::parse("address:lots", DemotionMode::Observe),
        5,
    )
    .await
    .expect("seal observe-path event");
    let report = read_report(&client, "evt-observe").await;
    assert_eq!(mode_of(&report), "observe");
    assert!(matches!(report.get("error"), Some(AttributeValue::S(_))));

    // The seal itself landed on the Counters event item (shuffle_seed present).
    let sealed = client
        .get_item()
        .table_name(COUNTERS)
        .key("event_id", AttributeValue::S("EVT#evt-force".to_owned()))
        .consistent_read(true)
        .send()
        .await
        .expect("get event item")
        .item()
        .cloned()
        .unwrap_or_default();
    assert!(
        sealed.contains_key("shuffle_seed"),
        "seal wrote the event item"
    );
    assert!(matches!(sealed.get("phase"), Some(AttributeValue::S(p)) if p == "active"));
}
