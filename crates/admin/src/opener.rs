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
//! `RequestResponse` rather than `Event`: the operator pressed a button and is
//! owed the answer. A fire-and-forget invoke would report success for an open
//! that failed on its shard-count read.

use aws_sdk_lambda::Client;
use aws_sdk_lambda::types::InvocationType;

use crate::{OpenError, OpenNow, Opener};

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
    async fn open_now(&self) -> Result<OpenNow, OpenError> {
        // The scheduler's payload shape, so the function is entered exactly as
        // a scheduled open enters it.
        let payload = serde_json::json!({ "event_id": self.event_id }).to_string();

        let out = self
            .client
            .invoke()
            .function_name(&self.function_name)
            .invocation_type(InvocationType::RequestResponse)
            .payload(aws_sdk_lambda::primitives::Blob::new(payload.into_bytes()))
            .send()
            .await
            .map_err(|e| OpenError(format!("invoke {}: {e}", self.function_name)))?;

        // A handler that returned Err is reported in `function_error`, not as a
        // transport failure: without this check a panicking open reads as a
        // successful one.
        if let Some(kind) = out.function_error() {
            let detail = out
                .payload()
                .and_then(|b| std::str::from_utf8(b.as_ref()).ok())
                .unwrap_or("<no payload>")
                .to_owned();
            return Err(OpenError(format!("open failed ({kind}): {detail}")));
        }

        // `{"opened": bool}`: the open's once-only guard rejected this call if
        // the event was already open, and the operator is owed that difference.
        // An unreadable body is not a failure -- the open landed either way --
        // so it reports the conservative answer rather than erroring on a
        // response it could not parse.
        let opened = out
            .payload()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(b.as_ref()).ok())
            .and_then(|v| v.get("opened").and_then(serde_json::Value::as_bool))
            .unwrap_or(false);
        Ok(if opened {
            OpenNow::Opened
        } else {
            OpenNow::AlreadyOpen
        })
    }
}
