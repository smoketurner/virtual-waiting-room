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
    /// The current phase as its wire string — `idle`, `pre_queue`, `active`,
    /// `post_event`, or `maintenance`. The badge renders it as both the label
    /// and the `status-{phase}` CSS class, and the poller sends it back as
    /// JSON to re-render the same badge, so it must be the canonical form the
    /// stylesheet and the dropdown values already use.
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
            phase: state.phase.as_wire_str().to_owned(),
            serving_counter: state.serving_counter,
            queue_counter: state.queue_counter,
            participant_count: dash(state.participant_count.map(|n| n.to_string())),
            target_rate: dash(state.target_rate.map(|n| n.to_string())),
            message: dash(state.message.clone()),
            message_raw: state.message.clone().unwrap_or_default(),
            admission_control: state.admission_control.as_wire_str().to_owned(),
            serving_state: {
                use wr_common::ServingState::{Closed, FailOpen, Paused, Running};
                match wr_common::serving_state(state.phase, state.admission_control) {
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
fn phase_label(p: wr_common::Phase) -> String {
    use wr_common::Phase::{Active, Idle, Maintenance, PostEvent, PreQueue};
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
    use wr_common::Phase;

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
            admission_control: wr_common::AdmissionControl::Open,
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
    fn phase_renders_as_its_wire_string() {
        // The badge label, the `status-{phase}` CSS class, and the /admin/state
        // JSON all read this one field, so it must be the wire string the
        // stylesheet and the transition dropdown already use. A `Debug`-derived
        // lowercase form drops the underscore on the two-word variants, giving
        // "prequeue"/"postevent" — a misnamed label and a class no rule matches.
        let mut s = state();
        for (phase, wire) in [
            (Phase::Idle, "idle"),
            (Phase::PreQueue, "pre_queue"),
            (Phase::Active, "active"),
            (Phase::PostEvent, "post_event"),
            (Phase::Maintenance, "maintenance"),
        ] {
            s.phase = phase;
            assert_eq!(Dashboard::from_state(&s).phase, wire);
        }
    }

    #[test]
    fn the_phase_badge_class_matches_a_stylesheet_rule() {
        // The rendered badge carries `status-pre_queue`, the selector
        // admin.css defines; `status-prequeue` would render unstyled.
        let mut s = state();
        s.phase = Phase::PreQueue;
        let html = Dashboard::from_state(&s).render().unwrap();
        assert!(html.contains("status-pre_queue"), "{html}");
        assert!(html.contains(">pre_queue<"), "{html}");
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
        use wr_common::AdmissionControl::{FailOpen, Open, Paused};

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
        s.admission_control = wr_common::AdmissionControl::FailOpen;
        assert_eq!(Dashboard::from_state(&s).serving_state, "Fail open");
    }

    #[test]
    fn deferred_features_are_labeled_not_faked() {
        let html = Dashboard::from_state(&state()).render().unwrap();
        assert!(html.contains("Not yet available"));
    }

    #[test]
    fn poller_refreshes_serving_state() {
        // #s-serving_state is the visitor-facing projection rendered in the
        // "Live. Refreshes automatically." card next to the phase and admission
        // badges. The poller must write it on every tick so an out-of-band
        // change (pause/resume, phase transition) flips it alongside its
        // sibling badges, instead of leaving "Visitors see: Running" stale next
        // to a "paused" / "post_event" badge until a full page reload.
        let html = Dashboard::from_state(&state()).render().unwrap();
        assert!(
            html.contains(r#"id="s-serving_state""#),
            "#s-serving_state must be present in the current-state card"
        );
        assert!(
            nums_array(&html).contains(&"serving_state"),
            "the poller's nums array must include \"serving_state\" so the \
             getElementById(\"s-\" + f) loop updates #s-serving_state live"
        );
    }

    #[test]
    fn poller_refreshes_every_s_prefixed_state_element() {
        // Every server-rendered element with an `s-`-prefixed id lives in the
        // "Live. Refreshes automatically." card, so the poller must refresh each
        // — either through the nums array (plain textContent) or a dedicated
        // getElementById handler (badges that swap className/innerHTML). This
        // guards the whole class of "serialised field gets an s- id but is left
        // out of the poller" regressions; #s-serving_state was one instance.
        let html = Dashboard::from_state(&state()).render().unwrap();
        let nums = nums_array(&html);
        // Badge elements change className/innerHTML, so they use dedicated
        // handlers rather than the nums textContent loop — by design.
        let dedicated = ["s-phase-badge", "s-admission-badge"];
        for id in dedicated {
            assert!(
                html.contains(&format!("getElementById(\"{id}\")")),
                "dedicated handler for #{id} must call getElementById so the badge refreshes live"
            );
        }
        for id in s_prefixed_ids(&html) {
            let in_nums = id
                .strip_prefix("s-")
                .is_some_and(|bare| nums.contains(&bare));
            let in_dedicated = dedicated.contains(&id);
            assert!(
                in_nums || in_dedicated,
                "poller does not refresh #{id}: add its bare name to the nums \
                 array or a dedicated getElementById handler (otherwise the Live \
                 card shows stale data until a full page reload)"
            );
        }
    }

    #[test]
    fn admin_state_json_includes_serving_state_for_the_poller() {
        // The poller can only refresh #s-serving_state if /admin/state actually
        // serializes it. Guard the other half of the contract: serving_state is
        // NOT #[serde(skip)], unlike the render-only inputs it must stay apart
        // from.
        let view = Dashboard::from_state(&state());
        let json = serde_json::to_string(&view).unwrap();
        assert!(
            json.contains(r#""serving_state":"Running""#),
            "/admin/state must serialise serving_state so the poller can write it; got: {json}"
        );
        for render_only in [r#""message_raw""#, r#""operator_email""#, r#""csp_nonce""#] {
            assert!(
                !json.contains(render_only),
                "{render_only} is a render-only input and must stay out of the JSON state view"
            );
        }
    }

    /// The poller's plain-text refresh list, parsed from the inline `<script>`
    /// `var nums = [...]` literal in the rendered dashboard.
    fn nums_array(html: &str) -> Vec<&str> {
        let start = html.find("var nums = [").unwrap();
        let end = html[start..].find("];").unwrap();
        let literal = &html[start + "var nums = [".len()..start + end];
        literal
            .split(',')
            .map(|s| s.trim().trim_matches('"'))
            .collect()
    }

    /// Every `id="s-..."` value in the rendered dashboard — the set of elements
    /// the poller's `s-` id convention places in its live-refresh namespace.
    fn s_prefixed_ids(html: &str) -> Vec<&str> {
        html.match_indices(r#"id="s-"#)
            .map(|(i, _)| {
                let rest = &html[i + r#"id=""#.len()..];
                &rest[..rest.find('"').unwrap()]
            })
            .collect()
    }
}
