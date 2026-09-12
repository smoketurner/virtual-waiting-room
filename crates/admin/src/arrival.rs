//! The instant a request arrived, stamped once and passed down explicitly.
//!
//! Every clock comparison serving one operator action reads the same instant:
//! [`arrival_layer`] stamps it at the outermost layer of the router and
//! handlers receive it through the [`ArrivalTime`] extractor. Without a shared
//! origin the debounce check and the audit stamp it writes are two different
//! readings separated by however long the `DynamoDB` round trip between them
//! took, and the row then records an action at an instant the guard never
//! evaluated.
//!
//! Construction is private to this module, so an `ArrivalTime` parameter is
//! evidence that the value came from the middleware rather than from a fresh
//! clock reading at the call site — which is what keeps a second, disagreeing
//! instant out of a mutation that has already been guarded against the first.

use std::future::Future;

use axum::extract::{FromRequestParts, Request};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::{IntoResponse as _, Response};
use jiff::Timestamp;

/// The instant a request arrived at the control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArrivalTime {
    at: Timestamp,
    /// The same instant in epoch seconds and milliseconds, converted once in
    /// the constructor where both are checked against the epoch.
    epoch_seconds: u64,
    epoch_millis: u64,
}

impl ArrivalTime {
    /// An arrival at `at`, or `None` if that instant is before the epoch.
    ///
    /// A request cannot arrive before 1970, and the two things this feeds read
    /// a pre-epoch value in opposite directions — a fail-open deadline becomes
    /// one that has already lapsed, a session expiry becomes one nothing has
    /// reached yet — so there is no substitute value that is safe for both.
    /// The request is refused instead.
    fn new(at: Timestamp) -> Option<Self> {
        let epoch_seconds = u64::try_from(at.as_second()).ok()?;
        let epoch_millis = u64::try_from(at.as_millisecond()).ok()?;
        Some(Self {
            at,
            epoch_seconds,
            epoch_millis,
        })
    }

    /// The arrival instant, at full precision.
    #[must_use]
    pub fn timestamp(self) -> Timestamp {
        self.at
    }

    /// The arrival instant in epoch seconds, for the places that work in them
    /// rather than in instants: the fail-open deadline the edge gate compares
    /// against its own clock, the start-time validation, and the session and
    /// login-transaction expiries.
    #[must_use]
    pub fn epoch_seconds(self) -> u64 {
        self.epoch_seconds
    }

    /// The arrival instant in epoch milliseconds, the unit a `UUIDv7` carries in
    /// its leading 48 bits.
    #[must_use]
    pub fn epoch_millis(self) -> u64 {
        self.epoch_millis
    }

    /// An arrival at a fixed instant, for tests that call an action directly
    /// instead of driving it through the router.
    #[cfg(test)]
    pub(crate) fn for_test_millis(millis: i64) -> Self {
        #[expect(
            clippy::expect_used,
            reason = "test-only constructor; an out-of-range literal is a test bug"
        )]
        Timestamp::from_millisecond(millis)
            .ok()
            .and_then(Self::new)
            .expect("test timestamp in range and at or after the epoch")
    }
}

/// Stamps the arrival instant into request extensions.
///
/// Mounted as the outermost layer of the router, so the stamp is taken before
/// any other layer can await. A clock a request cannot have arrived at answers
/// the request here rather than handing a handler something to stamp: reading
/// it with `Timestamp::now()` would panic on such a clock instead.
pub async fn arrival_layer(mut request: Request, next: Next) -> Response {
    let arrival = Timestamp::try_from(std::time::SystemTime::now())
        .ok()
        .and_then(ArrivalTime::new);
    let Some(arrival) = arrival else {
        tracing::error!("system clock is not readable as an instant at or after the epoch");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    request.extensions_mut().insert(arrival);
    next.run(request).await
}

/// Generic over router state and rejecting with a bare [`StatusCode`], so the
/// actions and the store can take an `ArrivalTime` without depending on the
/// handler layer's error types.
impl<S: Send + Sync> FromRequestParts<S> for ArrivalTime {
    type Rejection = StatusCode;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(parts.extensions.get::<Self>().copied().ok_or_else(|| {
            tracing::error!(
                "arrival_layer is not mounted on this router; an operator action \
                 has no instant to be guarded and stamped with"
            );
            StatusCode::INTERNAL_SERVER_ERROR
        }))
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use axum::Router;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use axum::routing::get;
    use tower::ServiceExt as _;

    use super::*;

    /// Echoes the arrival instant, so a test can place it against the wall
    /// clock the request was made at.
    async fn echo_arrival(arrival: ArrivalTime) -> String {
        arrival.timestamp().as_millisecond().to_string()
    }

    async fn get_t(app: Router) -> (StatusCode, String) {
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/t")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn the_mounted_layer_supplies_the_request_instant() {
        let app = Router::new()
            .route("/t", get(echo_arrival))
            .layer(axum::middleware::from_fn(arrival_layer));

        let before = Timestamp::try_from(std::time::SystemTime::now())
            .unwrap()
            .as_millisecond();
        let (status, body) = get_t(app).await;
        let after = Timestamp::try_from(std::time::SystemTime::now())
            .unwrap()
            .as_millisecond();

        assert_eq!(status, StatusCode::OK);
        let stamped: i64 = body.parse().unwrap();
        assert!(
            (before..=after).contains(&stamped),
            "arrival {stamped} must fall inside the request's wall-clock window {before}..={after}"
        );
    }

    #[tokio::test]
    async fn a_router_missing_the_layer_fails_closed() {
        // A handler that cannot be told when its request arrived must not fall
        // back to reading the clock itself: the two would disagree, and the
        // point of the stamp is that one request is one instant.
        let app = Router::new().route("/t", get(echo_arrival));
        let (status, _) = get_t(app).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn an_instant_before_the_epoch_is_not_an_arrival() {
        // Nothing can have arrived before 1970, and the seconds form would
        // read in opposite directions for the two things it feeds: a deadline
        // already lapsed, an expiry nothing has reached. The layer answers the
        // request instead of picking one.
        assert!(ArrivalTime::new(Timestamp::from_second(-1).unwrap()).is_none());
        assert_eq!(
            ArrivalTime::new(Timestamp::UNIX_EPOCH).map(ArrivalTime::epoch_seconds),
            Some(0)
        );
    }

    #[tokio::test]
    async fn each_request_carries_its_own_stamp() {
        let app = Router::new()
            .route("/t", get(echo_arrival))
            .layer(axum::middleware::from_fn(arrival_layer));

        let (_, first) = get_t(app.clone()).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let (_, second) = get_t(app).await;

        let first: i64 = first.parse().unwrap();
        let second: i64 = second.parse().unwrap();
        assert!(
            second > first,
            "the layer must stamp per request, not once per process: {first} then {second}"
        );
    }
}
