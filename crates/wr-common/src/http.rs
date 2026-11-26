//! What the API Gateway handlers share about a request.

use jiff::Timestamp;
use lambda_http::request::RequestContext;
use lambda_http::{Request, RequestExt};

/// When API Gateway received the request: the time it is answered at, set by
/// API Gateway rather than by the visitor.
#[must_use]
pub fn received_at(req: &Request) -> Option<Timestamp> {
    let Some(RequestContext::ApiGatewayV1(context)) = req.request_context_ref() else {
        return None;
    };
    Timestamp::from_millisecond(context.request_time_epoch).ok()
}

/// `req` stamped the way API Gateway delivers it, arriving at `at`.
#[must_use]
pub fn received(req: Request, at: Timestamp) -> Request {
    let mut context =
        lambda_http::aws_lambda_events::apigw::ApiGatewayProxyRequestContext::default();
    context.request_time_epoch = at.as_millisecond();
    req.with_request_context(RequestContext::ApiGatewayV1(context))
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use lambda_http::Body;

    use super::*;

    #[test]
    fn the_request_time_is_read_back_as_stamped() {
        let at = Timestamp::from_millisecond(1_788_000_000_250).unwrap();
        let req = received(Request::new(Body::Empty), at);
        assert_eq!(received_at(&req), Some(at));
    }

    #[test]
    fn a_request_api_gateway_did_not_stamp_has_no_time() {
        assert_eq!(received_at(&Request::new(Body::Empty)), None);
    }
}
