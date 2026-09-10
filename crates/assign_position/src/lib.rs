//! Live-join batch processing for the `assign_position` Lambda.
//!
//! Consumes a batch of enqueued join messages, allocates a contiguous block of
//! queue positions with one counter increment, and writes one `Positions` row
//! per valid record. The batch logic is generic over the [`Store`] port so it
//! runs without AWS; the SDK-backed implementation lives in `dynamo`.

use std::future::Future;

use serde::Deserialize;

pub mod dynamo;

/// A join message body enqueued by the ingest API. Only the two fields the
/// request validator already required are read.
#[derive(Debug, Clone, Deserialize)]
pub struct JoinMessage {
    pub request_id: String,
    pub event_id: String,
}

/// One record from the SQS batch: the message id (for failure reporting) and
/// the raw body to parse.
#[derive(Debug, Clone)]
pub struct BatchRecord {
    pub message_id: String,
    pub body: String,
}

/// A position write to attempt: the request and the queue position it claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionWrite {
    pub request_id: String,
    pub event_id: String,
    pub position: u64,
}

/// The persistence port the batch logic drives. Two operations: claim a
/// contiguous block of positions from the counter, and write the rows.
pub trait Store {
    /// `ADD queue_counter :n` with `ALL_NEW` for one event, returning the block
    /// end. The claimed block is `[end - n + 1, end]`. `n` is the count of
    /// valid records and is always `>= 1` when called.
    fn claim_block(
        &self,
        event_id: &str,
        n: u64,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Writes one position row with `attribute_not_exists(request_id)`. A
    /// conditional-check failure (a duplicate request id) is reported as
    /// `Ok(WriteOutcome::Duplicate)`, not an error — the position is simply
    /// abandoned, which is a permitted gap.
    fn put_position(
        &self,
        write: &PositionWrite,
    ) -> impl Future<Output = Result<WriteOutcome, StoreError>> + Send;
}

/// The result of a single conditional position write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Written,
    /// The `attribute_not_exists` guard rejected a duplicate request id.
    Duplicate,
}

/// A store failure that should return the record to the queue for retry.
#[derive(Debug, thiserror::Error)]
#[error("store error: {0}")]
pub struct StoreError(pub String);

/// The message ids that must be returned to the queue, in the shape the Lambda
/// event source mapping expects for partial-batch failure reporting.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BatchOutcome {
    pub failures: Vec<String>,
}

/// Parses `body` and returns the join message if `request_id` is a valid
/// `UUIDv7`; `None` marks the record invalid (its position is never claimed).
#[must_use]
pub fn parse_valid(body: &str) -> Option<JoinMessage> {
    let msg: JoinMessage = serde_json::from_str(body).ok()?;
    if is_uuid_v7(&msg.request_id) {
        Some(msg)
    } else {
        None
    }
}

/// Checks the canonical `8-4-4-4-12` hex form with version nibble `7`. Rejects
/// anything else so a malformed or spoofed id claims no position.
#[must_use]
pub fn is_uuid_v7(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate() {
        let ok = match i {
            8 | 13 | 18 | 23 => b == b'-',
            14 => b == b'7', // version nibble
            _ => b.is_ascii_hexdigit(),
        };
        if !ok {
            return false;
        }
    }
    true
}

/// Processes one SQS batch: partition valid/invalid, claim one block for the
/// valid records, write their rows, and collect the message ids to retry.
///
/// A record is a failure (returned to the queue) when its body is invalid, its
/// row write errors, or the whole block claim errors. A duplicate request id is
/// not a failure — its position is abandoned as a permitted gap.
pub async fn process_batch<S: Store>(store: &S, records: &[BatchRecord]) -> BatchOutcome {
    let mut outcome = BatchOutcome::default();
    let mut valid = Vec::new();
    for record in records {
        if let Some(msg) = parse_valid(&record.body) {
            valid.push((record.message_id.clone(), msg));
        } else {
            tracing::warn!(message_id = %record.message_id, "invalid join record");
            outcome.failures.push(record.message_id.clone());
        }
    }

    if valid.is_empty() {
        return outcome;
    }

    // One event per deployment in the MVP; the block is claimed against the
    // first record's event.
    let event_id = valid[0].1.event_id.clone();
    let n = valid.len() as u64;
    let end = match store.claim_block(&event_id, n).await {
        Ok(end) => end,
        Err(err) => {
            tracing::error!(error = %err, "queue_counter claim failed; retrying batch");
            for (message_id, _) in &valid {
                outcome.failures.push(message_id.clone());
            }
            return outcome;
        }
    };
    // Saturating, not bare: the release profile has no overflow checks, and a
    // wrapped start would hand out positions from the top of the u64 range.
    // `end >= n` always holds for a counter that only moves forward, so this is
    // a guard against a counter that was reset, never normal arithmetic.
    let start = end.saturating_sub(n).saturating_add(1);

    for (offset, (message_id, msg)) in valid.into_iter().enumerate() {
        let write = PositionWrite {
            request_id: msg.request_id,
            event_id: msg.event_id,
            position: start + offset as u64,
        };
        if let Err(err) = store.put_position(&write).await {
            tracing::error!(error = %err, message_id = %message_id, "position write failed");
            outcome.failures.push(message_id);
        }
    }

    outcome
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::Mutex;

    use super::*;

    const VALID_ID: &str = "018f3a2b-7c9d-7e1f-abcd-0123456789ab";
    const V4_ID: &str = "018f3a2b-7c9d-4e1f-abcd-0123456789ab";

    #[derive(Default)]
    struct FakeStore {
        counter: Mutex<u64>,
        writes: Mutex<Vec<PositionWrite>>,
        seen: Mutex<Vec<String>>,
        claim_fails: bool,
        /// Forces the block end the claim reports, standing in for a counter
        /// that was reset below the block size.
        claim_end: Option<u64>,
        fail_write_for: Option<String>,
    }

    impl Store for FakeStore {
        fn claim_block(
            &self,
            _event_id: &str,
            n: u64,
        ) -> impl std::future::Future<Output = Result<u64, StoreError>> + Send {
            let result = if self.claim_fails {
                Err(StoreError("counter down".to_owned()))
            } else if let Some(end) = self.claim_end {
                Ok(end)
            } else {
                let mut c = self.counter.lock().unwrap();
                *c += n;
                Ok(*c)
            };
            std::future::ready(result)
        }

        fn put_position(
            &self,
            write: &PositionWrite,
        ) -> impl std::future::Future<Output = Result<WriteOutcome, StoreError>> + Send {
            let result = if self.fail_write_for.as_deref() == Some(write.request_id.as_str()) {
                Err(StoreError("write down".to_owned()))
            } else {
                let mut seen = self.seen.lock().unwrap();
                if seen.contains(&write.request_id) {
                    Ok(WriteOutcome::Duplicate)
                } else {
                    seen.push(write.request_id.clone());
                    self.writes.lock().unwrap().push(write.clone());
                    Ok(WriteOutcome::Written)
                }
            };
            std::future::ready(result)
        }
    }

    fn rec(message_id: &str, request_id: &str) -> BatchRecord {
        BatchRecord {
            message_id: message_id.to_owned(),
            body: format!(r#"{{"request_id":"{request_id}","event_id":"evt-1"}}"#),
        }
    }

    #[test]
    fn uuid_v7_validation() {
        assert!(is_uuid_v7(VALID_ID));
        assert!(!is_uuid_v7(V4_ID)); // wrong version nibble
        assert!(!is_uuid_v7("not-a-uuid"));
        assert!(!is_uuid_v7("018f3a2b7c9d7e1fabcd0123456789ab")); // no dashes
        assert!(!is_uuid_v7("")); // empty
        assert!(!is_uuid_v7("018f3a2b-7c9d-7e1f-abcd-0123456789ab-extra"));
    }

    #[tokio::test]
    async fn valid_batch_claims_contiguous_block_no_duplicates() {
        let store = FakeStore::default();
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-0001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-0002-0123456789ab"),
            rec("m3", "018f3a2b-7c9d-7e1f-0003-0123456789ab"),
        ];
        let outcome = process_batch(&store, &records).await;
        assert!(outcome.failures.is_empty());
        let writes = store.writes.lock().unwrap();
        let mut positions: Vec<u64> = writes.iter().map(|w| w.position).collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn invalid_records_are_failures_and_dont_consume_positions() {
        let store = FakeStore::default();
        let records = vec![
            rec("m1", VALID_ID),
            BatchRecord {
                message_id: "m2".to_owned(),
                body: "not json".to_owned(),
            },
            rec("m3", V4_ID), // valid json, wrong UUID version
        ];
        let outcome = process_batch(&store, &records).await;
        assert_eq!(outcome.failures, vec!["m2".to_owned(), "m3".to_owned()]);
        // Only the one valid record claimed a position: counter incremented by 1.
        assert_eq!(*store.counter.lock().unwrap(), 1);
        assert_eq!(store.writes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn all_invalid_claims_nothing() {
        let store = FakeStore::default();
        let records = vec![BatchRecord {
            message_id: "m1".to_owned(),
            body: "garbage".to_owned(),
        }];
        let outcome = process_batch(&store, &records).await;
        assert_eq!(outcome.failures, vec!["m1".to_owned()]);
        assert_eq!(*store.counter.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn counter_claim_failure_retries_whole_valid_set() {
        let store = FakeStore {
            claim_fails: true,
            ..FakeStore::default()
        };
        let records = vec![rec("m1", VALID_ID)];
        let outcome = process_batch(&store, &records).await;
        assert_eq!(outcome.failures, vec!["m1".to_owned()]);
        assert!(store.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn single_write_failure_is_reported_others_succeed() {
        let store = FakeStore {
            fail_write_for: Some("018f3a2b-7c9d-7e1f-0002-0123456789ab".to_owned()),
            ..FakeStore::default()
        };
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-0001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-0002-0123456789ab"),
            rec("m3", "018f3a2b-7c9d-7e1f-0003-0123456789ab"),
        ];
        let outcome = process_batch(&store, &records).await;
        assert_eq!(outcome.failures, vec!["m2".to_owned()]);
        assert_eq!(store.writes.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn live_joins_after_a_seal_land_above_the_pre_queue_cohort() {
        // The seal starts queue_counter at the cohort size N. A live join
        // arriving afterwards must be numbered outside the pre-queue cohort's
        // [0, N), or two visitors hold the same position.
        const COHORT: u64 = 1_000;
        let store = FakeStore {
            counter: Mutex::new(COHORT),
            ..FakeStore::default()
        };
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-0001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-0002-0123456789ab"),
        ];
        let outcome = process_batch(&store, &records).await;
        assert!(outcome.failures.is_empty());
        let writes = store.writes.lock().unwrap();
        let mut positions: Vec<u64> = writes.iter().map(|w| w.position).collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![COHORT + 1, COHORT + 2]);
        for position in positions {
            assert!(
                position >= COHORT,
                "live position {position} collides with the pre-queue cohort [0, {COHORT})"
            );
        }
    }

    #[tokio::test]
    async fn a_block_end_below_the_block_size_does_not_wrap() {
        // A counter reset below the block it just claimed reports an end lower
        // than n. `end - n + 1` would wrap into the top of the u64 range under
        // the release profile's absent overflow checks, handing out positions
        // near u64::MAX. Saturating keeps the block at the bottom instead.
        let store = FakeStore {
            claim_end: Some(1),
            ..FakeStore::default()
        };
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-0001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-0002-0123456789ab"),
            rec("m3", "018f3a2b-7c9d-7e1f-0003-0123456789ab"),
        ];
        let outcome = process_batch(&store, &records).await;
        assert!(outcome.failures.is_empty());
        let writes = store.writes.lock().unwrap();
        for write in writes.iter() {
            assert!(
                write.position < 1_000,
                "position {} wrapped instead of saturating",
                write.position
            );
        }
    }

    #[tokio::test]
    async fn duplicate_request_id_is_not_a_failure() {
        let store = FakeStore::default();
        let dup = "018f3a2b-7c9d-7e1f-0009-0123456789ab";
        // Same id twice in one batch: second write is a Duplicate, not a failure.
        let records = vec![rec("m1", dup), rec("m2", dup)];
        let outcome = process_batch(&store, &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.writes.lock().unwrap().len(), 1);
    }
}
