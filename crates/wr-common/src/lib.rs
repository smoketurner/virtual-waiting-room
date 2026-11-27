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
pub mod http;
pub mod ids;
pub mod items;
pub mod permutation;
pub mod rules;
pub mod turns;

pub use crypto::{
    MalformedPossession, PossessionSecret, SecretDigest, Session, SignError, SigningKey,
    VerifyError, ip_network,
};
pub use expr::{STARTS_AT_ATTR, STARTS_AT_TZ_ATTR};
pub use ids::{
    AdmissionControl, EventId, IllegalControl, Phase, RequestId, ServingState, StoredControl,
    UnknownControl, UnknownPhase, is_uuid_shape, resolve, serving_state,
};
pub use items::{
    Counters, DRAW_GRACE, OpenOutputs, PositionItem, PositionStatus, PreQueueItem, ResolveError,
    ResolvedPosition, shard_cap_of, shard_count_of, shard_index_of, stored_control_of,
};
pub use permutation::{
    Assignment, CohortError, CohortOffsets, RandError, SHARDS, Seed, Shard, prp,
};
pub use rules::{
    MAX_COOKIE_NAME_BYTES, MAX_HEADER_NAME_BYTES, MAX_HEADER_VALUE_BYTES, MAX_PATH_PREFIX_BYTES,
    MAX_USER_AGENT_BYTES, ProtectionRule, RequestView, RuleFieldError, RuleWire, matches_any,
    validate_rule_fields,
};
pub use turns::{
    CALLS_ATTR, Call, OPEN_SETTLE, SHELVES, Shelf, TURN_HOLD, TURN_MARGIN, TurnState, calls_of,
    calls_to_attr, hold_ends, turn_of,
};
