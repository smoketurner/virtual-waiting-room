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
pub mod rules;

pub use crypto::{AdmissionToken, PLACEHOLDER_SIGNING_KEY, Session, SigningKey, VerifyError};
pub use ids::{
    AdmissionControl, EventId, IllegalControl, Phase, RequestId, ServingState, StoredControl,
    UnknownControl, UnknownPhase, resolve, serving_state,
};
pub use items::{
    Counters, PositionItem, PositionStatus, PreQueueItem, ResolveError, ResolvedPosition, Sealed,
};
pub use permutation::{Assignment, SHARDS, SealError, SealedOffsets, Seed, prp, shard_for};
pub use rules::{
    MAX_COOKIE_NAME_BYTES, MAX_HEADER_NAME_BYTES, MAX_HEADER_VALUE_BYTES, MAX_PATH_PREFIX_BYTES,
    MAX_USER_AGENT_BYTES, ProtectionRule, RequestView, RuleFieldError, RuleWire, matches_any,
    validate_rule_fields,
};
