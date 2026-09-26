//! The server-rendered waiting page (issue #67). Escaped by askama, styled by
//! the same stylesheet as the scripted page, and refreshed with a meta tag.

use askama::Template;

use crate::WaitPage;

/// A known place in line, formatted for display.
pub struct Queued {
    pub position: String,
    pub ahead: String,
}

#[derive(Template)]
#[template(path = "wait.html")]
pub struct WaitHtml {
    pub title: &'static str,
    pub sub: &'static str,
    pub queued: Option<Queued>,
    pub show_form: bool,
    pub refresh: Option<u64>,
    /// Where an admitted visitor goes, percent-encoded for the form's URL.
    pub next_encoded: String,
}

impl WaitHtml {
    #[must_use]
    pub fn of(page: &WaitPage, next: &str) -> Self {
        let (title, sub) = match page {
            WaitPage::NotInLine => (
                "You are not in line yet",
                "Join below. This works without JavaScript; the page refreshes itself while you wait.",
            ),
            WaitPage::Joining => ("Getting your place in line…", "Just a moment."),
            WaitPage::Queued { ahead: 0, .. } => ("You're next", "Letting you through any moment."),
            WaitPage::Queued { .. } => (
                "You're in line",
                "Keep this page open. You'll be let through automatically.",
            ),
            WaitPage::Holding => (
                "You're in line",
                "The line isn't moving yet. You keep your place; this page updates on its own.",
            ),
            WaitPage::Lost => (
                "We couldn't confirm this place is yours",
                "Join again to take a new place in line.",
            ),
            WaitPage::Unavailable => (
                "The waiting room is having trouble",
                "You keep your place. This page will try again on its own.",
            ),
        };
        let queued = match page {
            WaitPage::Queued { position, ahead } => Some(Queued {
                position: grouped(*position),
                ahead: grouped(*ahead),
            }),
            WaitPage::NotInLine
            | WaitPage::Joining
            | WaitPage::Holding
            | WaitPage::Lost
            | WaitPage::Unavailable => None,
        };
        Self {
            title,
            sub,
            queued,
            show_form: matches!(page, WaitPage::NotInLine | WaitPage::Lost),
            refresh: page.refresh_secs(),
            next_encoded: encode_component(next),
        }
    }
}

/// `1234567` as `1,234,567`.
fn grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Percent-encodes everything but the unreserved characters.
#[must_use]
pub fn encode_component(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    #[test]
    fn a_queued_visitor_sees_their_place_and_the_page_refreshes() {
        let html = WaitHtml::of(
            &WaitPage::Queued {
                position: 1_234_567,
                ahead: 1_000,
            },
            "/",
        )
        .render()
        .unwrap();
        assert!(html.contains("1,234,567"));
        assert!(html.contains("1,000"));
        assert!(html.contains(r#"http-equiv="refresh" content="20""#));
        assert!(!html.contains("<form"), "no second join while in line");
        assert!(
            !html.contains("<script"),
            "the whole point is working without one"
        );
    }

    #[test]
    fn a_visitor_not_in_line_gets_the_form_carrying_next() {
        let html = WaitHtml::of(&WaitPage::NotInLine, "/checkout?id=1")
            .render()
            .unwrap();
        assert!(
            html.contains(r#"action="/v1/enter?next=%2Fcheckout%3Fid%3D1""#),
            "{html}"
        );
        assert!(!html.contains("http-equiv"), "nothing to wait for yet");
    }

    #[test]
    fn next_cannot_break_out_of_the_form_attribute() {
        let html = WaitHtml::of(&WaitPage::NotInLine, r#"/"><script>x</script>"#)
            .render()
            .unwrap();
        assert!(!html.contains("<script>"));
    }

    #[test]
    fn grouping_is_by_thousands() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1_000), "1,000");
        assert_eq!(grouped(12_345_678), "12,345,678");
    }
}
