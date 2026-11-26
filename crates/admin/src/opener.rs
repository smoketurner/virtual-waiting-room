//! The `aws-sdk-lambda`-backed [`Opener`]: opens the event now, by invoking
//! the same function the schedule invokes.
//!
//! Deliberately an invoke rather than a reimplementation. The open is one
//! conditional `UpdateItem` guarded by `attribute_not_exists(shuffle_seed)`
//! that writes the seed, the prefix offsets, the cohort size and the active
//! phase together — and the guard is what makes a double-fire safe. A second
//! copy of that write living in the admin Lambda would be a second thing to
//! keep in step with `open_event`, and the failure if they drifted is a cohort
//! ordered by one permutation and resolved by another.
//!
//! `Event` rather than `RequestResponse`: the open waits for the join queue to
//! drain before it counts the cohort, which can outlast API Gateway's 29 s
//! (issue #239). Lambda retries a failed asynchronous open, the open's error
//! alarm reports one that keeps failing, and the dashboard's phase shows when
//! it lands.

use aws_sdk_lambda::Client;
use aws_sdk_lambda::types::InvocationType;

use crate::{OpenError, Opener};

/// A live Lambda-backed [`Opener`] bound to one event's open function.
pub struct LambdaOpener {
    client: Client,
    function_name: String,
    event_id: String,
}

impl LambdaOpener {
    #[must_use]
    pub fn new(client: Client, function_name: String, event_id: String) -> Self {
        Self {
            client,
            function_name,
            event_id,
        }
    }
}

impl Opener for LambdaOpener {
    async fn open_now(&self) -> Result<(), OpenError> {
        // The scheduler's payload shape, so the function is entered exactly as
        // a scheduled open enters it.
        let payload = serde_json::json!({ "event_id": self.event_id }).to_string();

        let out = self
            .client
            .invoke()
            .function_name(&self.function_name)
            .invocation_type(InvocationType::Event)
            .payload(aws_sdk_lambda::primitives::Blob::new(payload.into_bytes()))
            .send()
            .await
            .map_err(|e| OpenError(format!("invoke {}: {e}", self.function_name)))?;

        // Lambda answers 202 once it has queued the event for the function.
        if out.status_code() != 202 {
            return Err(OpenError(format!(
                "invoke {}: answered {}",
                self.function_name,
                out.status_code()
            )));
        }
        Ok(())
    }
}
