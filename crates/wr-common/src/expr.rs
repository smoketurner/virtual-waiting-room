//! `DynamoDB` keys, update expressions, and condition fragments, kept in one
//! place so the key shape and the attribute names live beside the item shapes
//! rather than scattered across handlers.
//!
//! [`Key`] returns the complete primary key rather than a string or a bare
//! value, so the key attribute's own name is written once too. Call sites pass
//! the result straight to `set_key`, or to `BatchGetItem`, which takes exactly
//! this type.
//!
//! [`Update`] and [`Condition`] build an expression and its placeholder
//! bindings together. The call that names an attribute is the call that binds
//! its value, so an expression cannot reference a placeholder nothing supplies
//! — a mistake that compiles perfectly and fails only once the request reaches
//! `DynamoDB`. The two allocate from disjoint placeholder namespaces (`#u`/`:u`
//! and `#c`/`:c`), so an update and a condition on the same request can never
//! collide.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::permutation::Shard;

/// The partition key attribute of the `Counters` table.
const KEY_ATTR: &str = "event_id";

/// The partition key attribute of the `Tokens` table.
///
/// Named `request_id` for the admission tokens it was built for, which is a
/// misnomer for the other two things it now holds — an OIDC session id is not a
/// request id, and neither is a PKCE state. Renaming it changes the table's hash
/// key, which replaces the table.
const TOKENS_KEY_ATTR: &str = "request_id";

/// The partition key attribute of the `PreQueue` table.
const PREQUEUE_KEY_ATTR: &str = "r";

/// The partition key attribute of the `Positions` table.
pub const POSITIONS_KEY_ATTR: &str = "request_id";

/// The expiry attribute of every `Tokens` row — admission-token reservations,
/// operator OIDC sessions, and pending PKCE logins alike.
///
/// This name is the `Tokens` table's `ttl { attribute_name }` in
/// `infra/modules/core/main.tf`: `DynamoDB` reclaims a row only when the
/// attribute it was told to watch is the one the writers actually set. The two
/// sides are a single name in two layers that nothing checks against each
/// other, so every writer takes it from here rather than spelling it out, and
/// the Terraform side carries a comment pointing back at this constant.
///
/// Reclamation is lazy: an expired row keeps being returned by reads and writes
/// until the background sweep removes it, so a reader that cares whether the
/// row is still valid compares this attribute to the current time itself.
pub const TOKENS_TTL_ATTR: &str = "expires_at";

/// The attribute a shard item records its own index in, so a reader that
/// fetched a batch of shards knows which is which without taking the key apart
/// again.
pub const SHARD_INDEX_ATTR: &str = "s";

/// The attribute a shard item holds its count in.
///
/// One letter on purpose. `UpdateItem` is billed on the size of the whole item
/// including its attribute names, so a verbose name is paid for on every single
/// increment, forever, for no benefit — nothing queries by attribute name.
pub const SHARD_COUNT_ATTR: &str = "n";

/// The attribute the event's scheduled start is stored in, epoch seconds.
///
/// Named here because two crates must agree on it and neither can see the
/// other's spelling: `admin` writes it, and `wr-common`'s own `Counters` parser
/// reads it back for `read` to publish. Absence means unscheduled, so clearing
/// a start time removes the attribute rather than zeroing it.
pub const STARTS_AT_ATTR: &str = "starts_at";

/// The attribute the scheduled start's IANA timezone is stored in.
///
/// Kept beside the epoch rather than derived from it: the epoch is the
/// absolute instant every reader needs, while this is what the operator
/// actually chose, and only the latter renders their form back the way they
/// filled it in.
pub const STARTS_AT_TZ_ATTR: &str = "starts_at_tz";

/// The `status` attribute of a `Positions` row. A `DynamoDB` reserved word, so
/// it can only be referenced through a name placeholder — which [`Condition`]
/// allocates for every attribute, reserved or not, so a caller never has to
/// know which words are reserved.
pub const STATUS_ATTR: &str = "status";

/// The `status` value of a row the controller has expired.
pub const STATUS_EXPIRED: &str = "expired";

/// Which item a key addresses.
///
/// One type rather than a constructor per item kind, so the tag prefixes and
/// the per-table key attribute stay together and a call site cannot pair the
/// wrong two. The uppercase tags mark the structural part of a key so it cannot
/// be confused with the id itself.
///
/// Tags appear only where a table holds more than one kind of item. `Counters`
/// holds the event plus its shard counters, and `Tokens` holds admission-token
/// reservations, operator OIDC sessions and pending PKCE logins — without the
/// tag a session id and a token for the same string would be one row.
/// `Positions` and `PreQueue` hold one kind each and take bare ids: a tag there
/// disambiguates nothing and costs bytes in the partition key of every row, of
/// which there is one per visitor.
///
/// `event_id` may not contain `#`, or one event's shard key could collide with
/// another event's item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key<'a> {
    /// The event's own item, holding the sequences, phase, seal outputs, and
    /// operator state.
    Event { event_id: &'a str },
    /// One pre-queue registration shard.
    ///
    /// A shard is its own ITEM, not an attribute on a shared one. `DynamoDB`
    /// caps throughput at 1,000 writes per second per partition key, so ten
    /// attributes on one item share one budget and distribute nothing; ten
    /// items are ten partition keys and ten budgets. The item is tiny, so every
    /// increment costs exactly one write unit rather than the size of a growing
    /// shared item.
    PrequeueShard { event_id: &'a str, shard: Shard },
    /// One arrivals shard, incremented when a visitor claims their admission
    /// and summed by the controller to measure the no-show rate.
    ArrivalsShard { event_id: &'a str, shard: Shard },
    /// A single-use admission token reservation.
    AdmissionToken { request_id: &'a str },
    /// An operator's OIDC session.
    OidcSession { session_id: &'a str },
    /// A pending OIDC login, keyed by its CSRF state.
    PkceTransaction { state: &'a str },
    /// A visitor's own `PreQueue` row.
    Prequeue { request_id: &'a str },
    /// A visitor's own `Positions` row.
    Position { request_id: &'a str },
}

impl Key<'_> {
    /// The partition key attribute this key is written under.
    #[must_use]
    pub fn attr(self) -> &'static str {
        match self {
            Key::Event { .. } | Key::PrequeueShard { .. } | Key::ArrivalsShard { .. } => KEY_ATTR,
            Key::AdmissionToken { .. } | Key::OidcSession { .. } | Key::PkceTransaction { .. } => {
                TOKENS_KEY_ATTR
            }
            Key::Prequeue { .. } => PREQUEUE_KEY_ATTR,
            Key::Position { .. } => POSITIONS_KEY_ATTR,
        }
    }

    /// The key's value, without the attribute wrapper.
    #[must_use]
    pub fn value(self) -> String {
        match self {
            Key::Event { event_id } => format!("EVT#{event_id}"),
            Key::PrequeueShard { event_id, shard } => {
                format!("EVT#{event_id}#PQ#{}", shard.index())
            }
            Key::ArrivalsShard { event_id, shard } => {
                format!("EVT#{event_id}#AR#{}", shard.index())
            }
            Key::AdmissionToken { request_id } => format!("TKN#{request_id}"),
            Key::OidcSession { session_id } => format!("SESS#{session_id}"),
            Key::PkceTransaction { state } => format!("PKCE#{state}"),
            // Untagged: one kind of item per table, so a tag would
            // disambiguate nothing and cost bytes in every visitor's key.
            Key::Prequeue { request_id } | Key::Position { request_id } => request_id.to_owned(),
        }
    }

    /// The complete primary key, ready for `set_key` or `BatchGetItem`.
    #[must_use]
    pub fn build(self) -> HashMap<String, AttributeValue> {
        HashMap::from([(self.attr().to_owned(), AttributeValue::S(self.value()))])
    }
}

/// A built expression and everything it refers to.
///
/// Returned as one value so the three parts cannot be separated on the way to
/// the request builder, which is how a binding goes missing.
#[derive(Debug, Clone, PartialEq)]
pub struct Expression {
    /// The expression text, referring only to placeholders bound below.
    pub expression: String,
    /// `ExpressionAttributeNames`.
    pub names: HashMap<String, String>,
    /// `ExpressionAttributeValues`.
    pub values: HashMap<String, AttributeValue>,
}

impl Expression {
    /// The names, or `None` when there are none. `DynamoDB` rejects an empty
    /// `ExpressionAttributeNames` map rather than ignoring it, so a builder
    /// that produced no names must send the field absent, not empty.
    #[must_use]
    pub fn names_or_none(&self) -> Option<HashMap<String, String>> {
        (!self.names.is_empty()).then(|| self.names.clone())
    }

    /// The values, or `None` when there are none — see [`Expression::names_or_none`].
    /// A condition such as a bare `attribute_not_exists` binds no values at all.
    #[must_use]
    pub fn values_or_none(&self) -> Option<HashMap<String, AttributeValue>> {
        (!self.values.is_empty()).then(|| self.values.clone())
    }
}

/// Builds an `UpdateExpression` together with its bindings.
///
/// Every attribute is referenced through an allocated `#u<n>` name placeholder,
/// so a reserved word cannot silently break an expression, and every value
/// through the matching `:u<n>`. Two calls naming the same attribute get two
/// placeholders; that costs a few bytes and removes any question of one clause
/// overwriting another's binding.
#[derive(Debug, Clone, Default)]
pub struct Update {
    set: Vec<String>,
    add: Vec<String>,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
    next: usize,
}

impl Update {
    /// An empty update.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn bind(&mut self, attr: &str, value: AttributeValue) -> (String, String) {
        let slot = self.next;
        self.next = self.next.saturating_add(1);
        let name = format!("#u{slot}");
        let placeholder = format!(":u{slot}");
        self.names.insert(name.clone(), attr.to_owned());
        self.values.insert(placeholder.clone(), value);
        (name, placeholder)
    }

    /// `SET <attr> = <value>`.
    #[must_use]
    pub fn set(mut self, attr: &str, value: AttributeValue) -> Self {
        let (name, placeholder) = self.bind(attr, value);
        self.set.push(format!("{name} = {placeholder}"));
        self
    }

    /// `ADD <attr> <value>` — the atomic counter increment.
    #[must_use]
    pub fn add(mut self, attr: &str, value: AttributeValue) -> Self {
        let (name, placeholder) = self.bind(attr, value);
        self.add.push(format!("{name} {placeholder}"));
        self
    }

    /// The expression and its bindings.
    #[must_use]
    pub fn build(self) -> Expression {
        let mut expression = String::new();
        if !self.set.is_empty() {
            expression.push_str("SET ");
            expression.push_str(&self.set.join(", "));
        }
        if !self.add.is_empty() {
            if !expression.is_empty() {
                expression.push(' ');
            }
            expression.push_str("ADD ");
            expression.push_str(&self.add.join(", "));
        }
        Expression {
            expression,
            names: self.names,
            values: self.values,
        }
    }
}

/// Builds a `ConditionExpression` together with its bindings.
#[derive(Debug, Clone)]
pub struct Condition {
    expression: String,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
    next: usize,
}

impl Condition {
    fn bind_name(&mut self, attr: &str) -> String {
        let slot = self.next;
        self.next = self.next.saturating_add(1);
        let name = format!("#c{slot}");
        self.names.insert(name.clone(), attr.to_owned());
        name
    }

    fn bind_value(&mut self, value: AttributeValue) -> String {
        let slot = self.next;
        self.next = self.next.saturating_add(1);
        let placeholder = format!(":c{slot}");
        self.values.insert(placeholder.clone(), value);
        placeholder
    }

    /// `attribute_not_exists(<attr>)` — the idempotency guard on every position
    /// and pre-queue write, so a retried request id is rejected rather than
    /// duplicated.
    #[must_use]
    pub fn attribute_not_exists(attr: &str) -> Self {
        let mut condition = Self {
            expression: String::new(),
            names: HashMap::new(),
            values: HashMap::new(),
            next: 0,
        };
        let name = condition.bind_name(attr);
        condition.expression = format!("attribute_not_exists({name})");
        condition
    }

    /// Widens the condition with `OR <attr> = <value>`.
    #[must_use]
    pub fn or_equals(mut self, attr: &str, value: AttributeValue) -> Self {
        let name = self.bind_name(attr);
        let placeholder = self.bind_value(value);
        self.expression = format!("{} OR {name} = {placeholder}", self.expression);
        self
    }

    /// The expression and its bindings.
    #[must_use]
    pub fn build(self) -> Expression {
        Expression {
            expression: self.expression,
            names: self.names,
            values: self.values,
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::collections::BTreeSet;

    use super::*;
    use crate::permutation::SHARDS;

    fn shard(index: usize) -> Shard {
        Shard::new(index).unwrap()
    }

    #[test]
    fn key_values_are_frozen_wire_shapes() {
        assert_eq!(Key::Event { event_id: "evt" }.value(), "EVT#evt");
        assert_eq!(
            Key::PrequeueShard {
                event_id: "evt",
                shard: shard(3)
            }
            .value(),
            "EVT#evt#PQ#3"
        );
        assert_eq!(
            Key::ArrivalsShard {
                event_id: "evt",
                shard: shard(3)
            }
            .value(),
            "EVT#evt#AR#3"
        );
        assert_eq!(Key::AdmissionToken { request_id: "abc" }.value(), "TKN#abc");
        assert_eq!(Key::OidcSession { session_id: "abc" }.value(), "SESS#abc");
        assert_eq!(Key::PkceTransaction { state: "abc" }.value(), "PKCE#abc");
        // One kind per table, so no tag: the bytes would be paid per visitor.
        assert_eq!(Key::Prequeue { request_id: "abc" }.value(), "abc");
        assert_eq!(Key::Position { request_id: "abc" }.value(), "abc");
    }

    #[test]
    fn shard_keys_are_distinct_partition_keys() {
        // The whole point: distinct KEYS, not distinct attributes on one item.
        // Ten attributes on a shared item share that item's 1,000 writes per
        // second; ten keys do not.
        let prequeue: BTreeSet<String> = (0..SHARDS)
            .map(|s| {
                Key::PrequeueShard {
                    event_id: "evt",
                    shard: shard(s),
                }
                .value()
            })
            .collect();
        assert_eq!(prequeue.len(), SHARDS);
        assert!(
            !prequeue.contains("EVT#evt"),
            "a shard must not collide with the Counters item"
        );

        let arrivals: BTreeSet<String> = (0..SHARDS)
            .map(|s| {
                Key::ArrivalsShard {
                    event_id: "evt",
                    shard: shard(s),
                }
                .value()
            })
            .collect();
        assert_eq!(arrivals.len(), SHARDS);
        // The two counter families must not collide with each other either.
        assert!(prequeue.is_disjoint(&arrivals));
    }

    #[test]
    fn the_tokens_table_key_space_does_not_collide_across_kinds() {
        // Three kinds of item share this table's key. Without the tags an
        // operator session id and an admission token reservation for the same
        // string would be the same row.
        let keys = [
            Key::AdmissionToken { request_id: "abc" }.value(),
            Key::OidcSession { session_id: "abc" }.value(),
            Key::PkceTransaction { state: "abc" }.value(),
        ];
        let distinct: BTreeSet<&String> = keys.iter().collect();
        assert_eq!(distinct.len(), 3, "two kinds share a key: {keys:?}");
    }

    #[test]
    fn shard_keys_are_scoped_to_their_event() {
        // Two events in one table must not share a counter.
        assert_ne!(
            Key::PrequeueShard {
                event_id: "evt-a",
                shard: shard(3)
            }
            .value(),
            Key::PrequeueShard {
                event_id: "evt-b",
                shard: shard(3)
            }
            .value()
        );
    }

    #[test]
    fn a_key_carries_its_own_table_attribute() {
        assert_eq!(Key::Event { event_id: "e" }.attr(), KEY_ATTR);
        assert_eq!(Key::OidcSession { session_id: "s" }.attr(), TOKENS_KEY_ATTR);
        assert_eq!(Key::Prequeue { request_id: "r" }.attr(), PREQUEUE_KEY_ATTR);
        assert_eq!(Key::Position { request_id: "r" }.attr(), POSITIONS_KEY_ATTR);

        let built = Key::Event { event_id: "evt" }.build();
        assert_eq!(built.len(), 1);
        assert_eq!(
            built.get(KEY_ATTR).and_then(|v| v.as_s().ok()),
            Some(&"EVT#evt".to_owned())
        );
    }

    /// Every placeholder an expression names must be bound, and nothing else.
    /// This is the property the builder exists to guarantee: the previous
    /// split between an expression function and a separate values function
    /// could not hold it, because nothing tied the two together.
    fn assert_self_consistent(built: &Expression) {
        for name in extract(&built.expression, '#') {
            assert!(
                built.names.contains_key(&name),
                "{name} referenced but not bound in {:?}",
                built.names
            );
        }
        for placeholder in extract(&built.expression, ':') {
            assert!(
                built.values.contains_key(&placeholder),
                "{placeholder} referenced but not bound in {:?}",
                built.values
            );
        }
        for name in built.names.keys() {
            assert!(
                built.expression.contains(name.as_str()),
                "{name} bound but never referenced"
            );
        }
        for placeholder in built.values.keys() {
            assert!(
                built.expression.contains(placeholder.as_str()),
                "{placeholder} bound but never referenced"
            );
        }
    }

    /// Pulls every `#name` or `:value` token out of an expression.
    fn extract(expression: &str, sigil: char) -> Vec<String> {
        let mut found = Vec::new();
        let mut rest = expression;
        while let Some(at) = rest.find(sigil) {
            let tail = &rest[at..];
            let end = tail
                .char_indices()
                .position(|(i, c)| i > 0 && !c.is_ascii_alphanumeric())
                .unwrap_or(tail.len());
            found.push(tail[..end].to_owned());
            rest = &tail[end..];
        }
        found
    }

    #[test]
    fn an_update_binds_every_placeholder_it_names() {
        let built = Update::new()
            .set(SHARD_INDEX_ATTR, AttributeValue::N("7".to_owned()))
            .add(SHARD_COUNT_ATTR, AttributeValue::N("1".to_owned()))
            .build();
        assert!(built.expression.starts_with("SET "));
        assert!(built.expression.contains(" ADD "));
        assert_eq!(built.names.len(), 2);
        assert_eq!(built.values.len(), 2);
        assert_self_consistent(&built);
    }

    #[test]
    fn an_update_with_only_one_clause_emits_only_that_clause() {
        let add_only = Update::new()
            .add("queue_counter", AttributeValue::N("5".to_owned()))
            .build();
        assert!(add_only.expression.starts_with("ADD "));
        assert!(!add_only.expression.contains("SET"));
        assert_self_consistent(&add_only);

        let set_only = Update::new()
            .set("phase", AttributeValue::S("active".to_owned()))
            .build();
        assert!(set_only.expression.starts_with("SET "));
        assert!(!set_only.expression.contains("ADD"));
        assert_self_consistent(&set_only);
    }

    #[test]
    fn repeating_an_attribute_binds_each_clause_separately() {
        // The seal writes the cohort size to two attributes at once. Two
        // clauses, two placeholders, no shared binding to get out of step.
        let built = Update::new()
            .set("participant_count", AttributeValue::N("9".to_owned()))
            .set("queue_counter", AttributeValue::N("9".to_owned()))
            .build();
        assert_eq!(built.values.len(), 2);
        assert_self_consistent(&built);
    }

    #[test]
    fn an_empty_update_is_empty_rather_than_malformed() {
        let built = Update::new().build();
        assert_eq!(built.expression, "");
        assert!(built.names.is_empty());
        assert!(built.values.is_empty());
    }

    #[test]
    fn a_condition_binds_every_placeholder_it_names() {
        let plain = Condition::attribute_not_exists(POSITIONS_KEY_ATTR).build();
        assert!(plain.expression.starts_with("attribute_not_exists("));
        assert!(plain.values.is_empty());
        assert_self_consistent(&plain);

        let widened = Condition::attribute_not_exists(POSITIONS_KEY_ATTR)
            .or_equals(STATUS_ATTR, AttributeValue::S(STATUS_EXPIRED.to_owned()))
            .build();
        assert!(widened.expression.contains(" OR "));
        assert_eq!(widened.values.len(), 1);
        assert_self_consistent(&widened);
    }

    #[test]
    fn a_reserved_word_is_referenced_through_a_name_placeholder() {
        // `status` is reserved, so it must never appear literally.
        let built = Condition::attribute_not_exists(POSITIONS_KEY_ATTR)
            .or_equals(STATUS_ATTR, AttributeValue::S(STATUS_EXPIRED.to_owned()))
            .build();
        assert!(
            !built.expression.contains(STATUS_ATTR),
            "reserved word written literally: {}",
            built.expression
        );
        assert!(built.names.values().any(|v| v == STATUS_ATTR));
    }

    #[test]
    fn an_update_and_a_condition_never_share_a_placeholder() {
        // Both land on one request, so their namespaces must be disjoint.
        let update = Update::new()
            .add(SHARD_COUNT_ATTR, AttributeValue::N("1".to_owned()))
            .build();
        let condition = Condition::attribute_not_exists(POSITIONS_KEY_ATTR)
            .or_equals(STATUS_ATTR, AttributeValue::S(STATUS_EXPIRED.to_owned()))
            .build();
        for name in condition.names.keys() {
            assert!(!update.names.contains_key(name), "name collision: {name}");
        }
        for placeholder in condition.values.keys() {
            assert!(
                !update.values.contains_key(placeholder),
                "value collision: {placeholder}"
            );
        }
    }
}
