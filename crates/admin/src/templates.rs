//! The askama template binding for the operator dashboard. Compile-time
//! templates render server-side HTML; no runtime templating, no JS required for
//! the core actions (each is a plain `<form method="post">`).

use askama::Template;

use crate::ControlState;

/// The dashboard view. Optional fields render as an em dash when absent so the
/// operator sees "not set" rather than a blank. Also serialized as JSON by the
/// `/admin/state` poller endpoint.
#[derive(Template, serde::Serialize)]
#[template(path = "dashboard.html")]
pub struct Dashboard {
    pub event_id: String,
    pub phase: String,
    pub serving_counter: u64,
    pub queue_counter: u64,
    pub participant_count: String,
    pub target_rate: String,
    pub message: String,
    /// Andon cord (ADR-0017): whether admission is currently paused, and a
    /// human "last changed by X at T" line for the audit trail.
    pub admission_paused: bool,
    pub last_action_line: String,
    /// Signed-in operator email for the top nav (set by the handler, not from
    /// control state). Not part of the JSON state view.
    #[serde(skip)]
    pub operator_email: String,
    /// Per-render CSP nonce for the inline poller script. Not part of the JSON
    /// state view (the poller endpoint reuses this struct).
    #[serde(skip)]
    pub csp_nonce: String,
}

impl Dashboard {
    /// Builds the view from control state, formatting optionals for display.
    #[must_use]
    pub fn from_state(state: &ControlState) -> Self {
        let dash = |s: Option<String>| s.unwrap_or_else(|| "not set".to_owned());
        Self {
            event_id: state.event_id.clone(),
            phase: format!("{:?}", state.phase).to_lowercase(),
            serving_counter: state.serving_counter,
            queue_counter: state.queue_counter,
            participant_count: dash(state.participant_count.map(|n| n.to_string())),
            target_rate: dash(state.target_rate.map(|n| n.to_string())),
            message: dash(state.message.clone()),
            admission_paused: state.admission_paused,
            last_action_line: match (
                &state.last_action,
                &state.last_action_by,
                &state.last_action_at,
            ) {
                (Some(a), Some(by), Some(at)) => format!("{a} by {by} at {at}"),
                _ => "none yet".to_owned(),
            },
            csp_nonce: String::new(),
            operator_email: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use askama::Template;
    use wr_domain::Phase;

    use super::*;

    fn state() -> ControlState {
        ControlState {
            event_id: "launch".to_owned(),
            phase: Phase::Active,
            serving_counter: 42,
            queue_counter: 1000,
            participant_count: Some(1000),
            target_rate: Some(500),
            message: Some("Doors open at noon".to_owned()),
            admission_paused: false,
            last_action: Some("set_rate".to_owned()),
            last_action_by: Some("op@example.com".to_owned()),
            last_action_at: Some("2026-09-09T22:00:00Z".to_owned()),
            last_action_epoch_ms: Some(1_788_000_000_000),
        }
    }

    #[test]
    fn renders_current_state() {
        let html = Dashboard::from_state(&state()).render().unwrap();
        assert!(html.contains("launch"));
        assert!(html.contains(">active<"));
        assert!(html.contains("Doors open at noon"));
        assert!(html.contains("500"));
    }

    #[test]
    fn core_actions_are_plain_form_posts_no_js() {
        let html = Dashboard::from_state(&state()).render().unwrap();
        // Every operator action is a POST form to its /admin route — works with
        // JavaScript disabled.
        for action in [
            "action=\"/admin/phase\"",
            "action=\"/admin/rate\"",
            "action=\"/admin/message\"",
            "action=\"/admin/reset\"",
        ] {
            assert!(html.contains(action), "missing form {action}");
        }
        assert!(html.contains("method=\"post\""));
        // The only script is the additive, nonce-guarded live-stats poller — it
        // enhances the read-only view and drives none of the operator actions.
        assert_eq!(
            html.matches("<script").count(),
            1,
            "exactly one script (the poller) expected"
        );
        assert!(html.contains("<script nonce="));
        assert!(html.contains("/admin/state"));
    }

    #[test]
    fn absent_optionals_render_as_dash() {
        let mut s = state();
        s.participant_count = None;
        s.target_rate = None;
        s.message = None;
        let html = Dashboard::from_state(&s).render().unwrap();
        assert!(html.contains("not set"));
    }

    #[test]
    fn deferred_features_are_labeled_not_faked() {
        let html = Dashboard::from_state(&state()).render().unwrap();
        assert!(html.contains("Not yet available"));
    }
}
