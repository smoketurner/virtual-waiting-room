//! Turns. Each controller pass calls the next places in line, as many as
//! there are free spots, and holds one spot for each of them for
//! [`TURN_HOLD`]. Nobody else can take a held spot. A visitor who does not
//! take theirs in time has missed their turn, and the spot is called for the
//! next place in line.
//!
//! The batch of places one pass calls is a [`Call`]. Its held spots are
//! counted on its own [`Shelf`] of arrivals counters, one of [`SHELVES`]
//! reused in turn, so a visitor takes a spot from their own call and never
//! from anyone else's. The `Counters` item keeps the recent calls, so
//! `generate_token` can tell which call a place in line belongs to, and
//! whether its turn has passed, from the one item it already reads.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use jiff::{SignedDuration, Timestamp};

/// How long a spot is held for the visitor whose turn it is. A waiting page
/// takes its turn within seconds; a hidden tab can be slowed to checking once
/// a minute.
pub const TURN_HOLD: SignedDuration = SignedDuration::from_mins(2);

/// How long after the draw closes a waiting page may take to ask for its place
/// in line: each page waits a random slice of up to this long, so the whole
/// cohort does not ask at once (`FIRST_ASK_MAX_SPREAD_MS` in `waiting.js`). A
/// visitor called in that time cannot take their turn before they know it is
/// theirs, so their spot is held [`TURN_HOLD`] from the end of it.
pub const OPEN_SETTLE: SignedDuration = SignedDuration::from_mins(2);

/// How long after a turn has passed the controller still counts its spot as
/// held: one pass, so a request that took the spot just before the turn
/// passed has landed before the spot is called for anyone else.
pub const TURN_MARGIN: SignedDuration = SignedDuration::from_secs(10);

/// Shelves, reused in turn: enough that a shelf comes round again only once
/// the call it counted is past its hold and margin, at one call per
/// ten-second pass, the longest hold being one made as the draw closes.
pub const SHELVES: usize = 26;

/// The `Counters` attribute holding the recent calls.
pub const CALLS_ATTR: &str = "calls";

/// One of the [`SHELVES`] sets of arrivals counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Shelf(usize);

impl Shelf {
    /// A shelf from a stored index, or `None` if it is out of range.
    #[must_use]
    pub fn new(index: usize) -> Option<Self> {
        (index < SHELVES).then_some(Self(index))
    }

    /// The shelf after this one, wrapping round.
    #[must_use]
    pub fn next(self) -> Self {
        Self((self.0 + 1) % SHELVES)
    }

    /// The raw index, always `< SHELVES`.
    #[must_use]
    pub fn index(self) -> usize {
        self.0
    }

    /// Every shelf, in order.
    pub fn all() -> impl Iterator<Item = Self> {
        (0..SHELVES).map(Self)
    }
}

/// One pass's call: the places in line from `first` up to the next call's
/// `first` (or now serving), their spots held until `until`, counted on
/// `shelf`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Call {
    /// The first place in line called.
    pub first: u64,
    /// When the turns this call gave out pass.
    pub until: Timestamp,
    /// Where the call's held spots are counted.
    pub shelf: Shelf,
}

/// When the turns of a call made at `called_at` pass: [`TURN_HOLD`] after it,
/// or after the waiting pages have all learned their place in line, if the
/// draw closed less than [`OPEN_SETTLE`] before.
#[must_use]
pub fn hold_ends(called_at: Timestamp, draw_cutoff: Option<Timestamp>) -> Timestamp {
    let settled = draw_cutoff
        .and_then(|cutoff| cutoff.checked_add(OPEN_SETTLE).ok())
        .map_or(called_at, |settled| settled.max(called_at));
    settled.checked_add(TURN_HOLD).unwrap_or(Timestamp::MAX)
}

impl Call {
    /// Whether the visitors this call called may still take their turn at
    /// `now`.
    #[must_use]
    pub fn is_held(&self, now: Timestamp) -> bool {
        now < self.until
    }

    /// Whether the controller still counts this call's untaken spots as held
    /// at `now`: for the hold, and one pass more.
    #[must_use]
    pub fn is_counted(&self, now: Timestamp) -> bool {
        now < self
            .until
            .checked_add(TURN_MARGIN)
            .unwrap_or(Timestamp::MAX)
    }
}

/// Where a place in line stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    /// Its number has not been called.
    NotCalled,
    /// It is this visitor's turn, with their spot held on this shelf.
    Held(Shelf),
    /// Its number was called and the turn has passed.
    Missed,
}

/// Where `place` stands, given now serving and the recent calls, oldest
/// first. A called place older than every call kept has long missed its
/// turn.
#[must_use]
pub fn turn_of(calls: &[Call], serving_counter: u64, place: u64, now: Timestamp) -> TurnState {
    if place >= serving_counter {
        return TurnState::NotCalled;
    }
    match calls.iter().rev().find(|c| c.first <= place) {
        Some(call) if call.is_held(now) => TurnState::Held(call.shelf),
        Some(_) | None => TurnState::Missed,
    }
}

/// The calls as stored: a list of `[first, until, shelf]`, `until` in epoch
/// seconds.
#[must_use]
pub fn calls_to_attr(calls: &[Call]) -> AttributeValue {
    AttributeValue::L(
        calls
            .iter()
            .map(|c| {
                AttributeValue::L(vec![
                    AttributeValue::N(c.first.to_string()),
                    AttributeValue::N(c.until.as_second().to_string()),
                    AttributeValue::N(c.shelf.index().to_string()),
                ])
            })
            .collect(),
    )
}

/// The calls stored on an item. Absent is none; anything unreadable is also
/// none, which lets nobody in until the controller writes them again.
#[must_use]
pub fn calls_of<S: std::hash::BuildHasher>(item: &HashMap<String, AttributeValue, S>) -> Vec<Call> {
    let Some(list) = item.get(CALLS_ATTR).and_then(|v| v.as_l().ok()) else {
        return Vec::new();
    };
    let parsed: Option<Vec<Call>> = list
        .iter()
        .map(|entry| {
            let fields = entry.as_l().ok()?;
            let [first, until, shelf] = fields.as_slice() else {
                return None;
            };
            let num = |v: &AttributeValue| v.as_n().ok().and_then(|s| s.parse::<i64>().ok());
            Some(Call {
                first: u64::try_from(num(first)?).ok()?,
                until: Timestamp::from_second(num(until)?).ok()?,
                shelf: Shelf::new(usize::try_from(num(shelf)?).ok()?)?,
            })
        })
        .collect();
    parsed.unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: Timestamp = Timestamp::constant(1_000_000, 0);

    fn shelf(i: usize) -> Shelf {
        Shelf::new(i).unwrap_or(Shelf(0))
    }

    fn calls() -> Vec<Call> {
        vec![
            Call {
                first: 0,
                until: T0 + TURN_HOLD,
                shelf: shelf(0),
            },
            Call {
                first: 100,
                until: T0 + SignedDuration::from_secs(10) + TURN_HOLD,
                shelf: shelf(1),
            },
        ]
    }

    #[test]
    fn a_called_place_is_held_on_its_own_calls_shelf_until_the_turn_passes() {
        let calls = calls();
        let soon = T0 + SignedDuration::from_secs(30);
        assert_eq!(turn_of(&calls, 150, 99, soon), TurnState::Held(shelf(0)));
        assert_eq!(turn_of(&calls, 150, 100, soon), TurnState::Held(shelf(1)));
        assert_eq!(turn_of(&calls, 150, 150, soon), TurnState::NotCalled);
        // The first call's turn passes ten seconds before the second's.
        let between = T0 + TURN_HOLD + SignedDuration::from_secs(5);
        assert_eq!(turn_of(&calls, 150, 99, between), TurnState::Missed);
        assert_eq!(
            turn_of(&calls, 150, 100, between),
            TurnState::Held(shelf(1))
        );
    }

    #[test]
    fn a_called_place_before_every_kept_call_has_missed_its_turn() {
        let later = &calls()[1..];
        assert_eq!(turn_of(later, 150, 50, T0), TurnState::Missed);
        assert_eq!(turn_of(&[], 150, 50, T0), TurnState::Missed);
        assert_eq!(turn_of(&[], 0, 0, T0), TurnState::NotCalled);
    }

    #[test]
    fn a_call_is_counted_one_pass_past_its_hold() {
        let call = calls()[0];
        assert!(call.is_held(T0 + TURN_HOLD - SignedDuration::from_secs(1)));
        assert!(!call.is_held(T0 + TURN_HOLD));
        assert!(call.is_counted(T0 + TURN_HOLD));
        assert!(!call.is_counted(T0 + TURN_HOLD + TURN_MARGIN));
    }

    #[test]
    fn a_call_as_the_draw_closes_holds_until_every_page_knows_its_place() {
        let cutoff = T0;
        // Called as the draw closes: held two minutes past the pages' spread.
        assert_eq!(
            hold_ends(cutoff, Some(cutoff)),
            cutoff + OPEN_SETTLE + TURN_HOLD
        );
        // Called a minute later: the same end.
        assert_eq!(
            hold_ends(cutoff + SignedDuration::from_mins(1), Some(cutoff)),
            cutoff + OPEN_SETTLE + TURN_HOLD
        );
        // Called once every page knows its place, or with no draw: the hold.
        let later = cutoff + SignedDuration::from_mins(10);
        assert_eq!(hold_ends(later, Some(cutoff)), later + TURN_HOLD);
        assert_eq!(hold_ends(later, None), later + TURN_HOLD);
        assert_eq!(hold_ends(Timestamp::MAX, None), Timestamp::MAX);
    }

    #[test]
    fn shelves_wrap_round_and_reject_out_of_range() {
        assert_eq!(shelf(SHELVES - 1).next(), shelf(0));
        assert_eq!(shelf(3).next(), shelf(4));
        assert_eq!(Shelf::new(SHELVES), None);
        assert_eq!(Shelf::all().count(), SHELVES);
    }

    #[test]
    fn the_calls_round_trip_through_their_attribute() {
        let calls = calls();
        let mut item = HashMap::new();
        item.insert(CALLS_ATTR.to_owned(), calls_to_attr(&calls));
        assert_eq!(calls_of(&item), calls);
        assert_eq!(calls_of(&HashMap::new()), Vec::new());
    }

    #[test]
    fn an_unreadable_call_reads_as_no_calls_at_all() {
        let mut item = HashMap::new();
        let bad_shelf = AttributeValue::L(vec![AttributeValue::L(vec![
            AttributeValue::N("0".to_owned()),
            AttributeValue::N("1000".to_owned()),
            AttributeValue::N(SHELVES.to_string()),
        ])]);
        item.insert(CALLS_ATTR.to_owned(), bad_shelf);
        assert_eq!(calls_of(&item), Vec::new());
        let short = AttributeValue::L(vec![AttributeValue::L(vec![AttributeValue::N(
            "0".to_owned(),
        )])]);
        item.insert(CALLS_ATTR.to_owned(), short);
        assert_eq!(calls_of(&item), Vec::new());
    }
}
