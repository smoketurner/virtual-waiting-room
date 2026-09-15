//! Integration test against `DynamoDB` Local: verifies the `DynamoStore` scan
//! folds every row in the shared `PreQueue` table into the cohort (the
//! production `scan_segment` has no `FilterExpression`), and that the seal's
//! `cohort > participant_count` guard refuses to write when foreign rows from
//! a different event contaminate the scan.
//!
//! Requires `DynamoDB` Local on `http://localhost:8000`. Start it with:
//! `docker run -d --name dynamodb-local -p 8000:8000 amazon/dynamodb-local`

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    reason = "integration test panics on setup failure and prints skip reasons"
)]

use std::collections::HashMap;

use aws_sdk_dynamodb::config::BehaviorVersion;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use seal_event::dynamo::DynamoStore;
use seal_event::{DemotionConfig, SealResult, seal_event};
use wr_common::DemotionMode;
use wr_common::PreQueueItem;
use wr_common::Telemetry;
use wr_common::expr::{Key, SHARD_COUNT_ATTR, SHARD_INDEX_ATTR};
use wr_common::{SHARDS, Shard};

const COUNTERS_TABLE: &str = "test-seal-counters";
const PREQUEUE_TABLE: &str = "test-seal-prequeue";
const NONCE: [u8; 8] = [0x0b, 0xad, 0xca, 0xfe, 0x00, 0x11, 0x22, 0x33];
const ENDPOINT: &str = "http://localhost:8000";

fn local_client() -> aws_sdk_dynamodb::Client {
    let creds =
        aws_sdk_dynamodb::config::Credentials::new("LOCAL", "LOCAL", None, None, "dynamodb-local");
    let config = aws_sdk_dynamodb::Config::builder()
        .endpoint_url(ENDPOINT)
        .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
        .credentials_provider(creds)
        .behavior_version(BehaviorVersion::latest())
        .build();
    aws_sdk_dynamodb::Client::from_conf(config)
}

async fn ensure_tables(client: &aws_sdk_dynamodb::Client) {
    for (table, key_attr) in [(COUNTERS_TABLE, "event_id"), (PREQUEUE_TABLE, "r")] {
        let _ = client
            .create_table()
            .table_name(table)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(key_attr)
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(key_attr)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;
        // If the table already exists that is fine; the purge below clears it.
    }
    for table in [COUNTERS_TABLE, PREQUEUE_TABLE] {
        loop {
            let desc = client.describe_table().table_name(table).send().await;
            if let Ok(out) = desc
                && let Some(table_desc) = out.table()
                && table_desc.table_status() == Some(&aws_sdk_dynamodb::types::TableStatus::Active)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

async fn purge_tables(client: &aws_sdk_dynamodb::Client) {
    for (table, key_attr) in [(COUNTERS_TABLE, "event_id"), (PREQUEUE_TABLE, "r")] {
        let scan = client
            .scan()
            .table_name(table)
            .send()
            .await
            .expect("scan for purge");
        for item in scan.items() {
            let key_val = item.get(key_attr).cloned();
            if let Some(kv) = key_val {
                client
                    .delete_item()
                    .table_name(table)
                    .set_key(Some(HashMap::from([(key_attr.to_string(), kv)])))
                    .send()
                    .await
                    .expect("delete for purge");
            }
        }
    }
}

async fn write_shard_count(
    client: &aws_sdk_dynamodb::Client,
    event_id: &str,
    shard: u8,
    count: u64,
) {
    let shard = Shard::new(usize::from(shard)).unwrap();
    let key = Key::PrequeueShard { event_id, shard };
    let mut item = key.build();
    item.insert(
        SHARD_INDEX_ATTR.to_string(),
        AttributeValue::N(shard.index().to_string()),
    );
    item.insert(
        SHARD_COUNT_ATTR.to_string(),
        AttributeValue::N(count.to_string()),
    );
    client
        .put_item()
        .table_name(COUNTERS_TABLE)
        .set_item(Some(item))
        .send()
        .await
        .expect("write shard count");
}

async fn write_prequeue_row(
    client: &aws_sdk_dynamodb::Client,
    request_id: &str,
    shard: u8,
    local_index: u64,
    address: &str,
) {
    let item = PreQueueItem {
        r: request_id.to_owned(),
        s: shard,
        l: local_index,
        t: 1_788_000_000,
        v: Some(Telemetry {
            a: Some(format!("{address}:4433")),
            n: Some("64500".to_owned()),
            c: None,
            j: None,
            u: None,
            q: None,
        }),
    };
    let av: HashMap<String, AttributeValue> = serde_dynamo::to_item(&item).unwrap();
    client
        .put_item()
        .table_name(PREQUEUE_TABLE)
        .set_item(Some(av))
        .send()
        .await
        .expect("write prequeue row");
}

async fn get_event_item(
    client: &aws_sdk_dynamodb::Client,
    event_id: &str,
) -> Option<HashMap<String, AttributeValue>> {
    let key = Key::Event { event_id }.build();
    let out = client
        .get_item()
        .table_name(COUNTERS_TABLE)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .expect("get event item");
    out.item().cloned()
}

fn shard_counts_for(counts: [u64; SHARDS]) -> Vec<(u8, u64)> {
    counts
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(s, &c)| (u8::try_from(s).unwrap(), c))
        .collect()
}

async fn seal_with(
    client: &aws_sdk_dynamodb::Client,
    event_id: &str,
    counts: [u64; SHARDS],
    mode: DemotionMode,
) -> Result<SealResult, seal_event::StoreError> {
    for (shard, count) in shard_counts_for(counts) {
        write_shard_count(client, event_id, shard, count).await;
    }
    let store = DynamoStore::new(
        client.clone(),
        COUNTERS_TABLE.to_owned(),
        PREQUEUE_TABLE.to_owned(),
    );
    seal_event(
        &store,
        event_id,
        [1u8; 32],
        NONCE,
        &DemotionConfig::parse("address:5", mode),
        1,
    )
    .await
}

/// Serializes the tests: they share one `DynamoDB` Local instance and one pair
/// of tables, so a parallel run would see another test's rows in its scan.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Returns `true` when `DynamoDB` Local is reachable on `http://localhost:8000`,
/// so `cargo test --workspace` skips these tests in environments that do not
/// run the container (`docker run -d -p 8000:8000 amazon/dynamodb-local`).
fn dynamodb_local_available() -> bool {
    std::net::TcpStream::connect("localhost:8000").is_ok()
}

async fn setup() -> aws_sdk_dynamodb::Client {
    let client = local_client();
    ensure_tables(&client).await;
    purge_tables(&client).await;
    client
}

#[tokio::test]
async fn a_clean_event_seals_against_dynamodb_local() {
    if !dynamodb_local_available() {
        eprintln!("skipping: DynamoDB Local not on localhost:8000");
        return;
    }
    let _guard = TEST_LOCK.lock().await;
    let client = setup().await;

    // 5 event-B rows on shard 0, all from one address. Threshold 5 means
    // 5 is not > 5, so nothing is demoted.
    for i in 0..5u64 {
        write_prequeue_row(&client, &format!("B{i}"), 0, i, "198.51.100.1").await;
    }

    let result = seal_with(
        &client,
        "evt-B",
        [5, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        DemotionMode::Enforce,
    )
    .await
    .expect("clean event should seal");

    let SealResult::Sealed(values) = result else {
        panic!("expected a first seal, got {result:?}");
    };
    assert_eq!(values.participant_count, 5);
    assert_eq!(values.demoted_count, 0);
    assert_eq!(values.queue_counter_start().unwrap(), 5);

    let item = get_event_item(&client, "evt-B").await;
    let item = item.expect("event item should exist after seal");
    assert!(item.contains_key("shuffle_seed"));
    assert_eq!(
        item.get("participant_count")
            .and_then(|v| v.as_n().ok())
            .map(String::as_str),
        Some("5")
    );
}

#[tokio::test]
async fn foreign_event_rows_are_refused_against_dynamodb_local() {
    if !dynamodb_local_available() {
        eprintln!("skipping: DynamoDB Local not on localhost:8000");
        return;
    }
    let _guard = TEST_LOCK.lock().await;
    let client = setup().await;

    // Seed 5 event-B rows on shard 0.
    for i in 0..5u64 {
        write_prequeue_row(&client, &format!("B{i}"), 0, i, "198.51.100.1").await;
    }

    // Seed 3 foreign rows that a prior event A would have written to the same
    // shared `PreQueue` table. Different request_ids, but the same shard and
    // local indices — the unfiltered scan picks them up.
    for i in 0..3u64 {
        write_prequeue_row(&client, &format!("A{i}"), 0, i, "198.51.100.1").await;
    }

    let result = seal_with(
        &client,
        "evt-B",
        [5, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        DemotionMode::Enforce,
    )
    .await;

    assert!(
        result.is_err(),
        "seal should refuse when the scan folds foreign rows: {result:?}"
    );
    let err = result.unwrap_err().to_string();
    assert!(err.contains('8'), "error should name the cohort: {err}");
    assert!(
        err.contains('5'),
        "error should name participant_count: {err}"
    );
    assert!(
        err.contains("foreign"),
        "error should point at the cause: {err}"
    );

    let item = get_event_item(&client, "evt-B").await;
    match item {
        None => { /* no item at all — correct */ }
        Some(item) => {
            assert!(
                !item.contains_key("shuffle_seed"),
                "event item must not be sealed on a refused seal: {item:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_clean_event_with_a_cohort_below_n_seals_against_dynamodb_local() {
    if !dynamodb_local_available() {
        eprintln!("skipping: DynamoDB Local not on localhost:8000");
        return;
    }
    let _guard = TEST_LOCK.lock().await;
    let client = setup().await;

    // Seed only 3 of 5 possible registrations (simulating request_id reuse or
    // stragglers classified LiveJoin). cohort = 3 < participant_count = 5.
    for i in 0..3u64 {
        write_prequeue_row(&client, &format!("C{i}"), 0, i, "198.51.100.1").await;
    }

    let result = seal_with(
        &client,
        "evt-C",
        [5, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        DemotionMode::Enforce,
    )
    .await
    .expect("a cohort below N should seal");

    let SealResult::Sealed(values) = result else {
        panic!("expected a first seal, got {result:?}");
    };
    assert_eq!(values.participant_count, 5);
    assert_eq!(values.demoted_count, 0);
}
