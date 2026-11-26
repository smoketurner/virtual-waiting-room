//! The models the local tools share: a visitor following the waiting page's
//! protocol, the edge between it and the functions, the join queue and its
//! event source mapping, and the report that checks a run's claims.
//!
//! They run on tokio's clock, so a run can use real time or paused time. What
//! answers the edge's calls is a [`backend::Backend`]: the shipped binaries in
//! `stack`.

pub mod backend;
pub mod clock;
pub mod edge;
pub mod metrics;
pub mod queue;
pub mod report;
pub mod rng;
pub mod visitor;
