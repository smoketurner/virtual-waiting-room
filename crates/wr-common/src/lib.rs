//! Shared logic for the Virtual Waiting Room Lambdas: the seeded permutation
//! that assigns queue positions, the domain types and `DynamoDB` item shapes the
//! handlers read and write, the expression fragments they write them with, and
//! the credential signing.
//!
//! AWS-free apart from the `AttributeValue` type used to build item maps: the
//! handlers own the SDK clients and pass values through these types, so every
//! rule here is testable without AWS.
//!
//! The modules are layered — `permutation` depends on nothing, `items` builds on
//! it, `crypto` stands alone — but the whole public surface is re-exported flat
//! below, because a caller wants `wr_common::Phase`, not a path that encodes
//! which layer a type happens to live in.

pub mod crypto;
pub mod expr;
pub mod ids;
pub mod items;
pub mod permutation;

pub use crypto::{AdmissionToken, Session, SigningKey, VerifyError};
pub use ids::{
    AdmissionControl, EventId, IllegalControl, Phase, RequestId, ServingState, UnknownControl,
    UnknownPhase, serving_state,
};
pub use items::{
    Counters, PositionItem, PositionStatus, PreQueueItem, ResolveError, ResolvedPosition,
};
pub use permutation::{Assignment, SHARDS, SealError, SealedOffsets, Seed, prp, shard_for};
