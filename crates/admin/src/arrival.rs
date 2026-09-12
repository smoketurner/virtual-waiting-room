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
pub struct ArrivalTime(Timestamp);

impl ArrivalTime {
    /// The arrival instant, at full precision.
    #[must_use]
    pub fn timestamp(self) -> Timestamp {
        self.0
    }

    /// The arrival instant in epoch seconds, for the two places that work in
    /// them rather than in instants: the fail-open deadline the edge gate
    /// compares against its own clock, and the start-time validation.
    ///
    /// A pre-epoch instant clamps to 0, which both read as already past — the
    /// safe direction for a deadline, since it withholds a fail-open window
    /// rather than granting an unbounded one.
    #[must_use]
    pub fn epoch_seconds(self) -> u64 {
        u64::try_from(self.0.as_second()).unwrap_or(0)
    }

    /// An arrival at a fixed instant, for tests that call an action directly
    /// instead of driving it through the router.
    #[cfg(test)]
    pub(crate) fn for_test_millis(millis: i64) -> Self {
        #[expect(
            clippy::expect_used,
            reason = "test-only constructor; an out-of-range literal is a test bug"
        )]
        Self(Timestamp::from_millisecond(millis).expect("test timestamp in range"))
    }
}

/// Stamps the arrival instant into request extensions.
///
/// Mounted as the outermost layer of the router, so the stamp is taken before
/// any other layer can await. A clock outside the range of an instant answers
/// the request here rather than handing a handler something to stamp: reading
/// it with `Timestamp::now()` would panic on such a clock instead.
pub async fn arrival_layer(mut request: Request, next: Next) -> Response {
    let Ok(at) = Timestamp::try_from(std::time::SystemTime::now()) else {
        tracing::error!("system clock is not readable as an instant");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    request.extensions_mut().insert(ArrivalTime(at));
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
