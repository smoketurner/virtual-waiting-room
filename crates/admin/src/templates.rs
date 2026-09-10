//! The askama template binding for the operator dashboard. Compile-time
//! templates render server-side HTML; no runtime templating, no JS required for
//! the core actions (each is a plain `<form method="post">`).

use askama::Template;

use crate::ControlState;

/// One selectable phase transition in the Phase control's dropdown: the target
/// phase value plus a self-describing label.
#[derive(serde::Serialize)]
pub struct PhaseOption {
    pub value: String,
    pub label: String,
}

/// The dashboard view. Optional fields render as "not set" when absent so the
/// operator sees a label rather than a blank. Also serialized as JSON by the
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
    /// The raw broadcast message (empty string when unset), for pre-filling the
    /// input value so the operator sees the current message is still set. The
    /// `message` field above is the "not set"-dashed display form. Not part of
    /// the JSON state view.
    #[serde(skip)]
    pub message_raw: String,
    /// The operator's admission override as its wire string — `open`, `paused`,
    /// or `fail_open`. The template branches on it and the poller sends it back
    /// as JSON, so all three states are visible rather than collapsing the two
    /// non-open ones together.
    pub admission_control: String,
    /// The visitor-facing serving state, derived from the phase and the
    /// admission control. Human label for display: "Running" / "Paused" /
    /// "Closed" / "Fail open".
    pub serving_state: String,
    pub last_action_line: String,
    /// The phase transitions the operator may select right now (legal forward
    /// steps only). Empty when the event is in a terminal or halted phase.
    pub allowed_transitions: Vec<PhaseOption>,
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
            message_raw: state.message.clone().unwrap_or_default(),
            admission_control: state.admission_control.as_wire_str().to_owned(),
            serving_state: {
                use wr_domain::ServingState::{Closed, FailOpen, Paused, Running};
                match wr_domain::serving_state(state.phase, state.admission_control) {
                    Running => "Running",
                    Paused => "Paused",
                    Closed => "Closed",
                    FailOpen => "Fail open",
                }
                .to_owned()
            },
            allowed_transitions: crate::next_phases(state.phase)
                .into_iter()
                .map(|p| PhaseOption {
                    value: p.as_wire_str().to_owned(),
                    label: phase_label(p),
                })
                .collect(),
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

/// A self-describing dropdown label for a phase transition.
fn phase_label(p: wr_domain::Phase) -> String {
    use wr_domain::Phase::{Active, Idle, Maintenance, PostEvent, PreQueue};
    match p {
        Idle => "idle: reset before the event starts",
        PreQueue => "pre_queue: open the countdown page for early arrivals",
        Active => "active: assign positions and start admitting",
        PostEvent => "post_event: close the event and drain the queue",
        Maintenance => "maintenance: halt the room",
    }
    .to_owned()
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
            admission_control: wr_domain::AdmissionControl::Open,
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
    fn each_admission_state_renders_its_own_banner_and_badge() {
        use wr_domain::AdmissionControl::{FailOpen, Open, Paused};

        let render = |control| {
            let mut s = state();
            s.admission_control = control;
            Dashboard::from_state(&s).render().unwrap()
        };

        // Open: no banner, and the action offered is Pause.
        let open = render(Open);
        assert!(!open.contains("pause-banner"));
        assert!(open.contains("action=\"/admin/pause\""));
        assert!(open.contains(">open<"));

        // Paused: banner, and the action offered is Resume, not Pause again.
        let paused = render(Paused);
        assert!(paused.contains("Admission is PAUSED"));
        assert!(paused.contains("action=\"/admin/resume\""));
        assert!(!paused.contains("action=\"/admin/pause\""));
        assert!(paused.contains(">paused<"));

        // Fail open: its own banner and badge, never rendered as the normal
        // admitting state — the operator must see the room is being bypassed.
        let failed_open = render(FailOpen);
        assert!(failed_open.contains("FAIL OPEN"));
        assert!(failed_open.contains(">fail open<"));
        assert!(!failed_open.contains("Admission is PAUSED"));
    }

    #[test]
    fn serving_state_reports_fail_open_to_the_operator() {
        let mut s = state();
        s.admission_control = wr_domain::AdmissionControl::FailOpen;
        assert_eq!(Dashboard::from_state(&s).serving_state, "Fail open");
    }

    #[test]
    fn deferred_features_are_labeled_not_faked() {
        let html = Dashboard::from_state(&state()).render().unwrap();
        assert!(html.contains("Not yet available"));
    }
}
