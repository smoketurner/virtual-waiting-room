//! The tables in memory, for running the Lambdas' own logic without AWS.
//!
//! One [`Memory`] holds every item the functions read and write, and
//! implements each function's `Store` with the conditions its `dynamo.rs`
//! writes under: a join's row is written only where none exists, a unit of
//! budget is taken only below the shard's cap, the open lands once and only
//! from `pre_queue`, the cursor advances only from the value the pass read.
//! Every write, including one whose condition fails, counts against its key's
//! [`WRITES_PER_KEY_PER_SECOND`] on the run clock, and the excess is refused
//! the way `DynamoDB` throttles it.
//!
//! What it does not model: eventually consistent reads (every read sees the
//! latest write), a table's own throughput ceiling, and the `Counters` cache
//! each `read` and `generate_token` environment holds. The stack runs the
//! real `dynamo.rs` against an emulator for those.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use controller::{
    AdmissionLog, ControllerState, PassDecision, PassInputs, ReleaseOutcome, ShardBudget,
};
use jiff::{SignedDuration, Timestamp};
use open_event::OpenValues;
use wr_common::{Counters, Phase, PositionStatus, PreQueueItem, SHARDS, SecretDigest, Shard};

/// `DynamoDB`'s write ceiling for one partition key.
pub const WRITES_PER_KEY_PER_SECOND: u32 = 1_000;

/// A `Positions` row.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Position {
    position: u64,
    status: PositionStatus,
    digest: Option<SecretDigest>,
    admitted_at: Option<Timestamp>,
    entry_time: Timestamp,
}

/// An event: its `Counters` item and the items keyed beside it.
#[derive(Debug, Clone)]
struct Event {
    counters: Counters,
    prequeue_shards: [u64; SHARDS],
    arrivals_shards: [ShardBudget; SHARDS],
    log: AdmissionLog,
}

#[derive(Debug, Default)]
struct Tables {
    events: HashMap<String, Event>,
    prequeue: HashMap<String, PreQueueItem>,
    positions: HashMap<String, Position>,
    /// Per key: the second its writes are being counted in, and how many.
    writes: HashMap<String, (i64, u32)>,
}

/// The tables, and the session length the controller counts with. Clones
/// share the tables, as every function shares the real ones.
#[derive(Debug, Clone)]
pub struct Memory {
    tables: Arc<Mutex<Tables>>,
    session: SignedDuration,
}

/// A key took more writes this second than `DynamoDB` allows it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("throttled: {key} took {WRITES_PER_KEY_PER_SECOND} writes this second")]
pub struct Throttled {
    key: String,
}

impl Memory {
    /// Empty tables for sessions of `session`.
    #[must_use]
    pub fn new(session: SignedDuration) -> Self {
        Self {
            tables: Arc::new(Mutex::new(Tables::default())),
            session,
        }
    }

    /// Writes the event's `Counters` item, as the apply seeds it.
    pub fn seed(&self, counters: Counters) {
        self.with(|t| {
            t.events.insert(
                counters.event_id.clone(),
                Event {
                    counters,
                    prequeue_shards: [0; SHARDS],
                    arrivals_shards: [ShardBudget::default(); SHARDS],
                    log: AdmissionLog::default(),
                },
            );
        });
    }

    /// Changes the event's `Counters` item, as an operator action does.
    pub fn update_counters(&self, event_id: &str, change: impl FnOnce(&mut Counters)) {
        self.with(|t| {
            if let Some(event) = t.events.get_mut(event_id) {
                change(&mut event.counters);
            }
        });
    }

    /// The event's `Counters` item.
    #[must_use]
    pub fn counters(&self, event_id: &str) -> Option<Counters> {
        self.with(|t| t.events.get(event_id).map(|e| e.counters.clone()))
    }

    /// Every `PreQueue` row, for the report's recomputation of the order.
    #[must_use]
    pub fn prequeue_rows(&self) -> Vec<PreQueueItem> {
        self.with(|t| t.prequeue.values().cloned().collect())
    }

    fn with<R>(&self, f: impl FnOnce(&mut Tables) -> R) -> R {
        // A poisoned lock means a test panicked holding it; the tables are
        // still the tables.
        let mut tables = match self.tables.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        f(&mut tables)
    }
}

impl Tables {
    /// Counts one write to `key`, refusing it past the per-key ceiling.
    fn write(&mut self, key: String) -> Result<(), Throttled> {
        let second = crate::clock::now().as_second();
        let slot = self.writes.entry(key.clone()).or_insert((second, 0));
        if slot.0 != second {
            *slot = (second, 0);
        }
        if slot.1 >= WRITES_PER_KEY_PER_SECOND {
            return Err(Throttled { key });
        }
        slot.1 += 1;
        Ok(())
    }

    fn event(&mut self, event_id: &str) -> Result<&mut Event, String> {
        self.events
            .get_mut(event_id)
            .ok_or_else(|| format!("no event {event_id}"))
    }
}

fn event_key(event_id: &str) -> String {
    format!("EVT#{event_id}")
}

fn prequeue_shard_key(event_id: &str, shard: Shard) -> String {
    format!("EVT#{event_id}#PQ#{}", shard.index())
}

fn arrivals_shard_key(event_id: &str, shard: usize) -> String {
    format!("EVT#{event_id}#AR#{shard}")
}

fn position_key(request_id: &str) -> String {
    format!("Positions/{request_id}")
}

fn prequeue_row_key(request_id: &str) -> String {
    format!("PreQueue/{request_id}")
}

fn log_key(event_id: &str) -> String {
    format!("EVT#{event_id}#LOG")
}

impl read::Store for Memory {
    async fn load_counters(&self, event_id: &str) -> Result<Option<Counters>, read::StoreError> {
        Ok(self.counters(event_id))
    }

    async fn load_prequeue(
        &self,
        request_id: &str,
    ) -> Result<Option<PreQueueItem>, read::StoreError> {
        Ok(self.with(|t| t.prequeue.get(request_id).cloned()))
    }

    async fn load_position(&self, request_id: &str) -> Result<Option<u64>, read::StoreError> {
        Ok(self.with(|t| t.positions.get(request_id).map(|p| p.position)))
    }
}

impl generate_token::Store for Memory {
    async fn load_counters(
        &self,
        event_id: &str,
    ) -> Result<Option<Counters>, generate_token::StoreError> {
        Ok(self.counters(event_id))
    }

    async fn load_prequeue(
        &self,
        request_id: &str,
    ) -> Result<Option<PreQueueItem>, generate_token::StoreError> {
        Ok(self.with(|t| t.prequeue.get(request_id).cloned()))
    }

    async fn load_position(
        &self,
        request_id: &str,
    ) -> Result<Option<generate_token::PositionRow>, generate_token::StoreError> {
        Ok(self.with(|t| {
            t.positions
                .get(request_id)
                .map(|p| generate_token::PositionRow {
                    position: p.position,
                    status: p.status,
                    digest: p.digest.clone(),
                    admitted_at: p.admitted_at,
                })
        }))
    }

    async fn take_slot(
        &self,
        event_id: &str,
        shard: Shard,
    ) -> Result<bool, generate_token::StoreError> {
        self.with(|t| {
            t.write(arrivals_shard_key(event_id, shard.index()))
                .map_err(|e| generate_token::StoreError(e.to_string()))?;
            let event = t.event(event_id).map_err(generate_token::StoreError)?;
            let Some(budget) = event.arrivals_shards.get_mut(shard.index()) else {
                return Err(generate_token::StoreError(format!(
                    "shard {} out of range",
                    shard.index()
                )));
            };
            // `n < c`: a shard never granted a cap admits nobody.
            if budget.taken < budget.cap {
                budget.taken += 1;
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    async fn mark_admitted(
        &self,
        request_id: &str,
        position: u64,
        digest: &SecretDigest,
        now: Timestamp,
    ) -> Result<(), generate_token::StoreError> {
        self.with(|t| {
            t.write(position_key(request_id))
                .map_err(|e| generate_token::StoreError(e.to_string()))?;
            // Lands once per row: on no row, or on one still `issued`. A row
            // already admitted keeps its first admission.
            match t.positions.get_mut(request_id) {
                Some(row) if row.status == PositionStatus::Admitted => {}
                Some(row) => {
                    row.status = PositionStatus::Admitted;
                    row.position = position;
                    row.admitted_at = Some(now);
                    if row.digest.is_none() {
                        row.digest = Some(digest.clone());
                    }
                }
                None => {
                    t.positions.insert(
                        request_id.to_owned(),
                        Position {
                            position,
                            status: PositionStatus::Admitted,
                            digest: Some(digest.clone()),
                            admitted_at: Some(now),
                            entry_time: now,
                        },
                    );
                }
            }
            Ok(())
        })
    }
}

impl assign_position::Store for Memory {
    async fn claim_block(
        &self,
        event_id: &str,
        n: u64,
    ) -> Result<u64, assign_position::StoreError> {
        self.with(|t| {
            t.write(event_key(event_id))
                .map_err(|e| assign_position::StoreError(e.to_string()))?;
            let event = t.event(event_id).map_err(assign_position::StoreError)?;
            event.counters.queue_counter = event.counters.queue_counter.saturating_add(n);
            Ok(event.counters.queue_counter)
        })
    }

    async fn put_position(
        &self,
        write: &assign_position::PositionWrite,
    ) -> Result<assign_position::WriteOutcome, assign_position::StoreError> {
        self.with(|t| {
            t.write(position_key(&write.request_id))
                .map_err(|e| assign_position::StoreError(e.to_string()))?;
            if t.positions.contains_key(&write.request_id) {
                return Ok(assign_position::WriteOutcome::Duplicate);
            }
            t.positions.insert(
                write.request_id.clone(),
                Position {
                    position: write.position,
                    status: PositionStatus::Issued,
                    digest: Some(write.digest.clone()),
                    admitted_at: None,
                    entry_time: write.at,
                },
            );
            Ok(assign_position::WriteOutcome::Written)
        })
    }

    async fn load_counters(
        &self,
        event_id: &str,
    ) -> Result<Option<Counters>, assign_position::StoreError> {
        Ok(self.counters(event_id))
    }

    async fn claim_prequeue_block(
        &self,
        event_id: &str,
        shard: Shard,
        count: u64,
    ) -> Result<u64, assign_position::StoreError> {
        self.with(|t| {
            t.write(prequeue_shard_key(event_id, shard))
                .map_err(|e| assign_position::StoreError(e.to_string()))?;
            let event = t.event(event_id).map_err(assign_position::StoreError)?;
            let Some(issued) = event.prequeue_shards.get_mut(shard.index()) else {
                return Err(assign_position::StoreError(format!(
                    "shard {} out of range",
                    shard.index()
                )));
            };
            let first = *issued;
            *issued = issued.saturating_add(count);
            Ok(first)
        })
    }

    async fn put_prequeue(
        &self,
        write: &assign_position::PreQueueWrite,
    ) -> Result<assign_position::WriteOutcome, assign_position::StoreError> {
        self.with(|t| {
            t.write(prequeue_row_key(&write.request_id))
                .map_err(|e| assign_position::StoreError(e.to_string()))?;
            if t.prequeue.contains_key(&write.request_id) {
                return Ok(assign_position::WriteOutcome::Duplicate);
            }
            let shard = u8::try_from(write.shard.index()).map_err(|_| {
                assign_position::StoreError(format!("shard {} out of range", write.shard.index()))
            })?;
            t.prequeue.insert(
                write.request_id.clone(),
                PreQueueItem {
                    r: write.request_id.clone(),
                    s: shard,
                    l: write.local_index,
                    t: write.at,
                    h: Some(write.digest.clone()),
                },
            );
            Ok(assign_position::WriteOutcome::Written)
        })
    }

    async fn registered_ids(
        &self,
        request_ids: &[String],
    ) -> Result<HashSet<String>, assign_position::StoreError> {
        Ok(self.with(|t| {
            request_ids
                .iter()
                .filter(|id| t.prequeue.contains_key(*id))
                .cloned()
                .collect()
        }))
    }
}

impl open_event::Store for Memory {
    async fn read_shard_counts(
        &self,
        event_id: &str,
    ) -> Result<[u64; SHARDS], open_event::StoreError> {
        self.with(|t| {
            t.event(event_id)
                .map(|e| e.prequeue_shards)
                .map_err(open_event::StoreError)
        })
    }

    async fn write_open(
        &self,
        event_id: &str,
        values: &OpenValues,
    ) -> Result<bool, open_event::StoreError> {
        self.with(|t| {
            t.write(event_key(event_id))
                .map_err(|e| open_event::StoreError(e.to_string()))?;
            let counters = &mut t.event(event_id).map_err(open_event::StoreError)?.counters;
            // `attribute_not_exists(shuffle_seed) AND phase = pre_queue`.
            if counters.shuffle_seed.is_some() || counters.phase != Phase::PreQueue {
                return Ok(false);
            }
            counters.shuffle_seed = Some(values.seed);
            counters.participant_count = Some(values.participant_count);
            counters.queue_counter = values.participant_count;
            counters.prequeue_offsets = Some(values.offsets);
            counters.phase = Phase::Active;
            Ok(true)
        })
    }

    async fn is_already_open(&self, event_id: &str) -> Result<bool, open_event::StoreError> {
        Ok(self
            .counters(event_id)
            .is_some_and(|c| c.shuffle_seed.is_some()))
    }
}

impl controller::Store for Memory {
    async fn read_state(&self, event_id: &str) -> Result<ControllerState, controller::StoreError> {
        let now = crate::clock::now();
        let shards_read_at = Timestamp::from_second(now.as_second()).unwrap_or(now);
        let session = self.session;
        self.with(|t| {
            let event = t.event(event_id).map_err(controller::StoreError)?;
            let c = &event.counters;
            Ok(ControllerState {
                phase: c.phase,
                stored_control: c.stored_control,
                fail_open_until: c.fail_open_until,
                inputs: PassInputs {
                    shards: event.arrivals_shards,
                    serving_counter: c.serving_counter,
                    queue_counter: c.queue_counter,
                    origin_concurrency: c.origin_concurrency.unwrap_or(0),
                    session,
                    log: event.log.clone(),
                    shards_read_at,
                },
            })
        })
    }

    async fn write_release(
        &self,
        event_id: &str,
        decision: &PassDecision,
        expected_serving_counter: u64,
    ) -> Result<ReleaseOutcome, controller::StoreError> {
        self.with(|t| {
            t.write(event_key(event_id))
                .map_err(|e| controller::StoreError(e.to_string()))?;
            let counters = &mut t.event(event_id).map_err(controller::StoreError)?.counters;
            if counters.serving_counter != expected_serving_counter {
                return Ok(ReleaseOutcome::LostRace);
            }
            counters.serving_counter = decision.next_serving_counter;
            Ok(ReleaseOutcome::Advanced)
        })
    }

    async fn write_log(
        &self,
        event_id: &str,
        log: &AdmissionLog,
    ) -> Result<(), controller::StoreError> {
        self.with(|t| {
            t.write(log_key(event_id))
                .map_err(|e| controller::StoreError(e.to_string()))?;
            t.event(event_id).map_err(controller::StoreError)?.log = log.clone();
            Ok(())
        })
    }

    async fn write_caps(
        &self,
        event_id: &str,
        caps: &[u64; SHARDS],
    ) -> Result<(), controller::StoreError> {
        self.with(|t| {
            for (index, cap) in caps.iter().enumerate() {
                t.write(arrivals_shard_key(event_id, index))
                    .map_err(|e| controller::StoreError(e.to_string()))?;
                if let Some(budget) = t
                    .event(event_id)
                    .map_err(controller::StoreError)?
                    .arrivals_shards
                    .get_mut(index)
                {
                    budget.cap = *cap;
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use generate_token::{Admission, Denied, Minting};
    use wr_common::{PossessionSecret, SigningKey, StoredControl};

    use super::*;

    const EVENT: &str = "evt";

    fn memory() -> Memory {
        let memory = Memory::new(SignedDuration::from_mins(15));
        memory.seed(Counters {
            event_id: EVENT.to_owned(),
            phase: Phase::PreQueue,
            queue_counter: 0,
            serving_counter: 0,
            shuffle_seed: None,
            participant_count: None,
            prequeue_offsets: None,
            message: None,
            origin_concurrency: Some(1),
            stored_control: StoredControl::Open,
            fail_open_until: None,
            starts_at: None,
        });
        memory
    }

    fn visitor(n: u8) -> (String, PossessionSecret) {
        let id = format!("018f3a2b-7c9d-7e1f-abcd-0123456789{n:02x}");
        let secret = PossessionSecret::parse(&format!("{}{n:02}", "A".repeat(41))).unwrap();
        (id, secret)
    }

    fn join(id: &str, secret: &PossessionSecret) -> assign_position::BatchRecord {
        assign_position::BatchRecord {
            message_id: id.to_owned(),
            body: serde_json::json!({
                "request_id": id,
                "event_id": EVENT,
                "h": secret.digest().as_str(),
            })
            .to_string(),
            sent_at: crate::clock::now(),
        }
    }

    async fn admit(memory: &Memory, id: &str, secret: &PossessionSecret) -> Admission {
        let key = SigningKey::new(b"a-test-signing-key");
        let minting = Minting {
            key: &key,
            event_id: EVENT,
            session: SignedDuration::from_mins(15),
        };
        generate_token::admit(memory, &minting, id, secret, None, crate::clock::now())
            .await
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn a_registration_is_opened_metered_and_admitted_once() {
        let memory = memory();
        let (first, first_secret) = visitor(1);
        let (second, second_secret) = visitor(2);
        let shard = Shard::new(0).unwrap();
        let records = [join(&first, &first_secret), join(&second, &second_secret)];
        let outcome = assign_position::process_batch(&memory, EVENT, shard, &records).await;
        assert!(outcome.failures.is_empty());
        // The same joins again claim nothing more.
        assign_position::process_batch(&memory, EVENT, shard, &records).await;
        assert_eq!(memory.prequeue_rows().len(), 2);

        let opened = open_event::open_event(&memory, EVENT, [7; 32])
            .await
            .unwrap();
        assert!(matches!(opened, open_event::OpenResult::Opened(_)));
        assert_eq!(
            open_event::open_event(&memory, EVENT, [8; 32])
                .await
                .unwrap(),
            open_event::OpenResult::AlreadyOpen
        );
        assert_eq!(memory.counters(EVENT).unwrap().queue_counter, 2);

        // Before the controller grants anything, nobody is let in.
        assert!(matches!(
            admit(&memory, &first, &first_secret).await,
            Admission::Refused(Denied::StillQueued { .. })
        ));

        // A pass grants a unit on every shard and reaches both positions. Each
        // visitor takes one unit, however often they ask.
        memory.update_counters(EVENT, |c| c.origin_concurrency = Some(10));
        let pass = controller::run_pass(&memory, EVENT, crate::clock::now())
            .await
            .unwrap();
        // One past the last position issued: the cursor is exclusive.
        assert_eq!(pass, controller::PassOutcome::Ran { released: 3 });
        let taken = |memory: &Memory| {
            memory.with(|t| {
                t.events[EVENT]
                    .arrivals_shards
                    .iter()
                    .map(|s| s.taken)
                    .sum::<u64>()
            })
        };
        for _ in 0..3 {
            assert!(matches!(
                admit(&memory, &first, &first_secret).await,
                Admission::Admitted { .. }
            ));
        }
        assert_eq!(taken(&memory), 1);
        assert!(matches!(
            admit(&memory, &second, &second_secret).await,
            Admission::Admitted { .. }
        ));
        assert_eq!(taken(&memory), 2);
        // Knowing the id is not enough.
        assert_eq!(
            admit(&memory, &second, &first_secret).await,
            Admission::Refused(Denied::NotHolder)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_mark_keeps_the_first_admission() {
        // Two requests racing past the admitted check both mark the row; the
        // session still ends a session length after the first.
        let memory = memory();
        let (id, secret) = visitor(1);
        let first = crate::clock::now();
        generate_token::Store::mark_admitted(&memory, &id, 0, &secret.digest(), first)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        generate_token::Store::mark_admitted(
            &memory,
            &id,
            0,
            &secret.digest(),
            crate::clock::now(),
        )
        .await
        .unwrap();
        let row = generate_token::Store::load_position(&memory, &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.admitted_at, Some(first));
    }

    #[tokio::test(start_paused = true)]
    async fn the_open_lands_only_from_pre_queue() {
        let memory = memory();
        memory.update_counters(EVENT, |c| c.phase = Phase::Idle);
        let values = OpenValues {
            seed: [1; 32],
            participant_count: 0,
            offsets: [0; SHARDS],
        };
        assert!(
            !open_event::Store::write_open(&memory, EVENT, &values)
                .await
                .unwrap()
        );
        assert!(
            !open_event::Store::is_already_open(&memory, EVENT)
                .await
                .unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_unit_is_taken_only_below_the_cap() {
        let memory = memory();
        let shard = Shard::new(3).unwrap();
        assert!(
            !generate_token::Store::take_slot(&memory, EVENT, shard)
                .await
                .unwrap()
        );
        let mut caps = [0; SHARDS];
        caps[3] = 1;
        controller::Store::write_caps(&memory, EVENT, &caps)
            .await
            .unwrap();
        assert!(
            generate_token::Store::take_slot(&memory, EVENT, shard)
                .await
                .unwrap()
        );
        assert!(
            !generate_token::Store::take_slot(&memory, EVENT, shard)
                .await
                .unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_cursor_moves_only_from_the_value_the_pass_read() {
        let memory = memory();
        let state = controller::Store::read_state(&memory, EVENT).await.unwrap();
        let decision = controller::compute_pass(&state.inputs, crate::clock::now());
        assert_eq!(
            controller::Store::write_release(&memory, EVENT, &decision, 0)
                .await
                .unwrap(),
            ReleaseOutcome::Advanced
        );
        assert_eq!(
            controller::Store::write_release(&memory, EVENT, &decision, 0)
                .await
                .unwrap(),
            ReleaseOutcome::LostRace
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_key_past_its_write_ceiling_is_throttled_until_the_next_second() {
        let memory = memory();
        let shard = Shard::new(0).unwrap();
        for _ in 0..WRITES_PER_KEY_PER_SECOND {
            generate_token::Store::take_slot(&memory, EVENT, shard)
                .await
                .unwrap();
        }
        let refused = generate_token::Store::take_slot(&memory, EVENT, shard).await;
        assert!(refused.unwrap_err().0.contains("throttled"));
        // Another key is unaffected, and the next second the ceiling resets.
        generate_token::Store::take_slot(&memory, EVENT, Shard::new(1).unwrap())
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        generate_token::Store::take_slot(&memory, EVENT, shard)
            .await
            .unwrap();
    }
}
