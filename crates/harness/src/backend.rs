//! What answers the edge: the functions behind API Gateway, the gate on the
//! protected behaviour, and the consumer the join queue delivers to.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;

/// A JSON answer, boxed so the traits below can be held as trait objects.
pub type Reply<'a> = Pin<Box<dyn Future<Output = Result<serde_json::Value>> + Send + 'a>>;

/// The functions behind API Gateway's Lambda proxy integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Api {
    /// `read`: `/status` and `/queue_num`.
    Read,
    /// `generate_token`.
    GenerateToken,
}

/// What the edge calls.
pub trait Backend: Send + Sync {
    /// Invokes `api` with a REST proxy event and returns its proxy response.
    fn invoke(&self, api: Api, event: serde_json::Value) -> Reply<'_>;

    /// The gate's decision for one protected viewer request: `pass`, and when
    /// it refuses, the `status` it answers with.
    fn gate(&self, viewer: serde_json::Value) -> Reply<'_>;
}

/// What the join queue's event source mapping delivers batches to.
pub trait Consumer: Send + Sync {
    /// Delivers one SQS event and returns the function's batch response.
    fn deliver(&self, event: serde_json::Value) -> Reply<'_>;
}
