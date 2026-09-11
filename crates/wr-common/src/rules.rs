//! Protection rules: which requests the gate covers.
//!
//! Shared by two callers that must never disagree — `authorizer`, which
//! matches rules against a real request, and `admin`'s edge-config writer,
//! which serializes them into the `CloudFront` Function's `KeyValueStore` value —
//! and mirrored a third time by `infra/modules/edge/functions/gate.js`, which
//! is why the wire encoding here is frozen by conformance vectors
//! (`crates/wr-common/tests/vectors/`) rather than left to drift.

use serde::{Deserialize, Serialize};

/// A minimal view over a request, so [`ProtectionRule::matches`] does not
/// depend on any one crate's request shape. Header names are matched
/// case-insensitively by the implementation, mirroring how HTTP header names
/// are compared everywhere else in this codebase.
pub trait RequestView {
    fn path(&self) -> &str;
    /// The value of a header by case-insensitive name.
    fn header(&self, name: &str) -> Option<&str>;
    /// The value of a cookie by name.
    fn cookie(&self, name: &str) -> Option<&str>;
}

/// One local protection rule: a request matches when the named request
/// attribute contains (or equals, or is present) the configured value. A
/// request no rule matches is unprotected and forwarded without a credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtectionRule {
    /// The request path starts with this prefix.
    PathPrefix(String),
    /// A request header equals this `(name, value)` pair (case-insensitive
    /// name and value).
    Header { name: String, value: String },
    /// A cookie of this name is present.
    Cookie(String),
    /// The user agent contains this substring.
    UserAgent(String),
}

impl ProtectionRule {
    /// Whether this rule matches the request.
    #[must_use]
    pub fn matches<R: RequestView + ?Sized>(&self, req: &R) -> bool {
        match self {
            Self::PathPrefix(prefix) => req.path().starts_with(prefix.as_str()),
            Self::Header { name, value } => req
                .header(name)
                .is_some_and(|actual| actual.eq_ignore_ascii_case(value)),
            Self::Cookie(name) => req.cookie(name).is_some(),
            Self::UserAgent(needle) => req
                .header("user-agent")
                .is_some_and(|ua| ua.contains(needle.as_str())),
        }
    }
}

/// Whether any rule protects this request.
#[must_use]
pub fn matches_any<R: RequestView + ?Sized>(rules: &[ProtectionRule], req: &R) -> bool {
    rules.iter().any(|r| r.matches(req))
}

/// Per-field byte caps on rule input (issue #71): sized so one worst-case
/// rule costs about a fifth of the `KeyValueStore`'s value budget rather than
/// all of it, while a realistic ruleset still reaches roughly 40 rules.
pub const MAX_PATH_PREFIX_BYTES: usize = 128;
pub const MAX_HEADER_NAME_BYTES: usize = 64;
pub const MAX_HEADER_VALUE_BYTES: usize = 128;
pub const MAX_COOKIE_NAME_BYTES: usize = 64;
pub const MAX_USER_AGENT_BYTES: usize = 64;

/// A rule field that failed input validation. Named by rule index and field,
/// not just "rule N is invalid", so the admin can point the operator at the
/// exact box to fix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuleFieldError {
    #[error("rule {rule_index}: {field} is {actual} bytes, over the {limit}-byte limit")]
    TooLong {
        rule_index: usize,
        field: &'static str,
        limit: usize,
        actual: usize,
    },
    /// Anything outside printable ASCII `0x20..=0x7E`: rejects newlines and
    /// control characters that could break the `KeyValueStore` JSON or the
    /// gate's parse, and — since one byte is one character under ASCII —
    /// makes the bytes-vs-characters ambiguity in AWS's docs moot. It also
    /// closes the `eq_ignore_ascii_case` (Rust) vs `toLowerCase()` (JS)
    /// case-folding divergence: the two agree exactly on ASCII input.
    #[error("rule {rule_index}: {field} contains a non-printable-ASCII byte")]
    NotAscii {
        rule_index: usize,
        field: &'static str,
    },
}

fn check_field(
    rule_index: usize,
    field: &'static str,
    value: &str,
    limit: usize,
) -> Result<(), RuleFieldError> {
    if !value.bytes().all(|b| (0x20..=0x7E).contains(&b)) {
        return Err(RuleFieldError::NotAscii { rule_index, field });
    }
    if value.len() > limit {
        return Err(RuleFieldError::TooLong {
            rule_index,
            field,
            limit,
            actual: value.len(),
        });
    }
    Ok(())
}

/// Validates every rule's fields against the per-field bounds above. Shared
/// by the admin input boundary and the edge-config encoder so the two never
/// disagree about what is acceptable — the authorizer's env-parsed rules are
/// deliberately out of scope (a Terraform variable set by an engineer is not
/// the untrusted boundary this exists for, and it has no JavaScript evaluator
/// for the case-folding divergence to arise in).
///
/// # Errors
///
/// The first [`RuleFieldError`] encountered, in rule order.
pub fn validate_rule_fields(rules: &[ProtectionRule]) -> Result<(), RuleFieldError> {
    for (i, rule) in rules.iter().enumerate() {
        match rule {
            ProtectionRule::PathPrefix(p) => {
                check_field(i, "path prefix", p, MAX_PATH_PREFIX_BYTES)?;
            }
            ProtectionRule::Cookie(name) => {
                check_field(i, "cookie name", name, MAX_COOKIE_NAME_BYTES)?;
            }
            ProtectionRule::UserAgent(ua) => {
                check_field(i, "user agent substring", ua, MAX_USER_AGENT_BYTES)?;
            }
            ProtectionRule::Header { name, value } => {
                check_field(i, "header name", name, MAX_HEADER_NAME_BYTES)?;
                check_field(i, "header value", value, MAX_HEADER_VALUE_BYTES)?;
            }
        }
    }
    Ok(())
}

/// The compact tuple wire form the 1 KB `KeyValueStore` value ceiling requires:
/// `["p",prefix]` | `["c",name]` | `["u",substring]` | `["h",name,value]`. The
/// derived struct/enum form (`{"PathPrefix":"/checkout"}`) runs 30-45 bytes a
/// rule and blows the budget; this is 15-25.
///
/// Two arities disambiguate under `#[serde(untagged)]`: a two-element array
/// tries `Two` first, a three-element array falls through to `Three`. The tag
/// (first element) then disambiguates within an arity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RuleWire {
    Two((String, String)),
    Three((String, String, String)),
}

/// A `RuleWire` tuple whose tag names no known rule kind.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown protection rule tag: {0}")]
pub struct UnknownRuleTag(pub String);

impl TryFrom<RuleWire> for ProtectionRule {
    type Error = UnknownRuleTag;

    fn try_from(wire: RuleWire) -> Result<Self, Self::Error> {
        match wire {
            RuleWire::Two((tag, a)) => match tag.as_str() {
                "p" => Ok(Self::PathPrefix(a)),
                "c" => Ok(Self::Cookie(a)),
                "u" => Ok(Self::UserAgent(a)),
                _ => Err(UnknownRuleTag(tag)),
            },
            RuleWire::Three((tag, name, value)) => match tag.as_str() {
                "h" => Ok(Self::Header { name, value }),
                _ => Err(UnknownRuleTag(tag)),
            },
        }
    }
}

impl From<ProtectionRule> for RuleWire {
    fn from(rule: ProtectionRule) -> Self {
        match rule {
            ProtectionRule::PathPrefix(prefix) => Self::Two(("p".to_owned(), prefix)),
            ProtectionRule::Cookie(name) => Self::Two(("c".to_owned(), name)),
            ProtectionRule::UserAgent(needle) => Self::Two(("u".to_owned(), needle)),
            ProtectionRule::Header { name, value } => Self::Three(("h".to_owned(), name, value)),
        }
    }
}

impl Serialize for ProtectionRule {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        RuleWire::from(self.clone()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProtectionRule {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = RuleWire::deserialize(deserializer)?;
        Self::try_from(wire).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    struct TestRequest {
        path: String,
        headers: Vec<(String, String)>,
        cookies: Vec<(String, String)>,
    }

    impl RequestView for TestRequest {
        fn path(&self) -> &str {
            &self.path
        }

        fn header(&self, name: &str) -> Option<&str> {
            let name = name.to_ascii_lowercase();
            self.headers
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.as_str())
        }

        fn cookie(&self, name: &str) -> Option<&str> {
            self.cookies
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        }
    }

    fn req(path: &str) -> TestRequest {
        TestRequest {
            path: path.to_owned(),
            headers: Vec::new(),
            cookies: Vec::new(),
        }
    }

    #[test]
    fn each_rule_kind_matches_its_own_attribute() {
        let path = ProtectionRule::PathPrefix("/buy".to_owned());
        let header = ProtectionRule::Header {
            name: "x-protect".to_owned(),
            value: "yes".to_owned(),
        };
        let cookie = ProtectionRule::Cookie("gate".to_owned());
        let ua = ProtectionRule::UserAgent("BadBot".to_owned());

        let mut r = req("/buy/thing");
        r.headers.push(("x-protect".to_owned(), "yes".to_owned()));
        r.headers
            .push(("user-agent".to_owned(), "Mozilla BadBot/1".to_owned()));
        r.cookies.push(("gate".to_owned(), "1".to_owned()));

        assert!(path.matches(&r));
        assert!(header.matches(&r));
        assert!(cookie.matches(&r));
        assert!(ua.matches(&r));

        let empty = req("/other");
        assert!(!path.matches(&empty));
        assert!(!header.matches(&empty));
        assert!(!cookie.matches(&empty));
        assert!(!ua.matches(&empty));
    }

    #[test]
    fn header_match_is_case_insensitive_on_name_and_value() {
        let rule = ProtectionRule::Header {
            name: "X-Internal-Monitor".to_owned(),
            value: "True".to_owned(),
        };
        let mut r = req("/");
        r.headers
            .push(("x-internal-monitor".to_owned(), "true".to_owned()));
        assert!(rule.matches(&r));
    }

    #[test]
    fn matches_any_is_false_for_an_empty_ruleset() {
        assert!(!matches_any(&[], &req("/anything")));
    }

    #[test]
    fn wire_round_trips_every_kind() {
        let rules = vec![
            ProtectionRule::PathPrefix("/checkout".to_owned()),
            ProtectionRule::Cookie("loyalty_member".to_owned()),
            ProtectionRule::UserAgent("HeadlessChrome".to_owned()),
            ProtectionRule::Header {
                name: "x-internal-monitor".to_owned(),
                value: "true".to_owned(),
            },
        ];
        let json = serde_json::to_string(&rules).unwrap();
        let back: Vec<ProtectionRule> = serde_json::from_str(&json).unwrap();
        assert_eq!(rules, back);
    }

    #[test]
    fn wire_form_is_the_compact_tuple_not_the_derived_struct() {
        let json =
            serde_json::to_string(&ProtectionRule::PathPrefix("/checkout".to_owned())).unwrap();
        assert_eq!(json, r#"["p","/checkout"]"#);
        let json = serde_json::to_string(&ProtectionRule::Header {
            name: "x-internal-monitor".to_owned(),
            value: "true".to_owned(),
        })
        .unwrap();
        assert_eq!(json, r#"["h","x-internal-monitor","true"]"#);
    }

    #[test]
    fn an_unknown_tag_is_rejected_not_defaulted() {
        assert!(serde_json::from_str::<ProtectionRule>(r#"["z","x"]"#).is_err());
        assert!(serde_json::from_str::<ProtectionRule>(r#"["z","x","y"]"#).is_err());
    }

    #[test]
    fn validate_rule_fields_accepts_a_well_formed_ruleset() {
        let rules = vec![
            ProtectionRule::PathPrefix("/checkout".to_owned()),
            ProtectionRule::Header {
                name: "x-internal-monitor".to_owned(),
                value: "true".to_owned(),
            },
            ProtectionRule::Cookie("loyalty_member".to_owned()),
            ProtectionRule::UserAgent("HeadlessChrome".to_owned()),
        ];
        assert_eq!(validate_rule_fields(&rules), Ok(()));
    }

    #[test]
    fn validate_rule_fields_rejects_a_field_over_its_limit() {
        let rules = vec![ProtectionRule::PathPrefix(
            "/".repeat(MAX_PATH_PREFIX_BYTES + 1),
        )];
        assert_eq!(
            validate_rule_fields(&rules),
            Err(RuleFieldError::TooLong {
                rule_index: 0,
                field: "path prefix",
                limit: MAX_PATH_PREFIX_BYTES,
                actual: MAX_PATH_PREFIX_BYTES + 1,
            })
        );
    }

    #[test]
    fn validate_rule_fields_accepts_exactly_the_limit() {
        let rules = vec![ProtectionRule::Cookie("a".repeat(MAX_COOKIE_NAME_BYTES))];
        assert_eq!(validate_rule_fields(&rules), Ok(()));
    }

    #[test]
    fn validate_rule_fields_rejects_a_newline() {
        let rules = vec![ProtectionRule::UserAgent("bad\nbot".to_owned())];
        assert_eq!(
            validate_rule_fields(&rules),
            Err(RuleFieldError::NotAscii {
                rule_index: 0,
                field: "user agent substring",
            })
        );
    }

    #[test]
    fn validate_rule_fields_rejects_non_ascii() {
        let rules = vec![ProtectionRule::Cookie("café".to_owned())];
        assert_eq!(
            validate_rule_fields(&rules),
            Err(RuleFieldError::NotAscii {
                rule_index: 0,
                field: "cookie name",
            })
        );
    }

    #[test]
    fn validate_rule_fields_checks_both_header_fields() {
        let long_value = "v".repeat(MAX_HEADER_VALUE_BYTES + 1);
        let rules = vec![ProtectionRule::Header {
            name: "x-ok".to_owned(),
            value: long_value,
        }];
        assert!(matches!(
            validate_rule_fields(&rules),
            Err(RuleFieldError::TooLong {
                field: "header value",
                ..
            })
        ));
    }

    #[test]
    fn validate_rule_fields_reports_the_first_offending_rule_in_order() {
        let rules = vec![
            ProtectionRule::PathPrefix("/ok".to_owned()),
            ProtectionRule::Cookie("bad\t".to_owned()),
            ProtectionRule::PathPrefix("/also-fine".to_owned()),
        ];
        assert_eq!(
            validate_rule_fields(&rules),
            Err(RuleFieldError::NotAscii {
                rule_index: 1,
                field: "cookie name",
            })
        );
    }
}
