//! Shared domain types for the Virtual Waiting Room Lambdas: identifier
//! newtypes, the `DynamoDB` item shapes for the `Counters`, `PreQueue`, and
//! `Positions` tables, and the counter/condition expression fragments the
//! handlers write. AWS-free — the handlers own the SDK client and pass values
//! through these types.

pub mod expr;
pub mod ids;
pub mod items;

pub use ids::{EventId, Phase, RequestId};
pub use items::{Counters, PositionItem, PositionStatus, PreQueueItem};
pub use wr_permutation::{Assignment, SHARDS, SealError, SealedOffsets, Seed, prp, shard_for};
