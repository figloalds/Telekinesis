//! Local legacy-transfer ownership extension (CCR-WP6.3-4). Whole-file hashes,
//! peer messages and the old opaque session field keep their meanings.
use crate::model::{errors::ErrorWire, protocol::LaneSpec};

const MARKER: &str = "~w6.";
pub const MAX_RUN_FENCES: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Generation {
    pub incarnation: u64,
    pub run: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PullRun {
    pub generation: Generation,
    /// Counter allocated when the peer pool is constructed, before its lanes.
    pub layout: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushRun {
    pub generation: Generation,
    pub layout: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunFence {
    pub generation: Generation,
    pub layout: Option<u64>,
    pub frame: Option<u64>,
}

#[derive(Debug)]
pub struct Owned<'a> {
    /// Pull slots share a parent fence and retain independent round floors.
    pub scope: &'a str,
    pub family: &'a str,
    pub peer: &'a str,
    pub lane: LaneSpec,
    /// Push/foreground frame or background round nonce. Counters from
    /// different pull slots are never compared for background takeover.
    pub frame: Option<u64>,
    /// Background a/b slot; foreground c and push frames have no round slot.
    pub round_slot: Option<usize>,
    pub layout: Option<u64>,
    pub generation: Generation,
}

fn invalid() -> ErrorWire {
    ErrorWire::coded("INVALID_REQUEST", "malformed local lease generation")
}

fn hex(part: &str) -> Result<u64, ErrorWire> {
    if part.len() != 16
        || !part
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    u64::from_str_radix(part, 16).map_err(|_| invalid())
}

pub fn qualify(base: &str, generation: Generation) -> Result<String, ErrorWire> {
    let session = format!(
        "{base}{MARKER}{:016x}.{:016x}",
        generation.incarnation, generation.run
    );
    parse(&session)?.ok_or_else(invalid)?;
    Ok(session)
}

pub fn qualify_pull(base: &str, run: PullRun) -> Result<String, ErrorWire> {
    let session = format!("{}.{:016x}", qualify(base, run.generation)?, run.layout);
    let owned = parse(&session)?.ok_or_else(invalid)?;
    if !owned.family.starts_with("pull:") {
        return Err(invalid());
    }
    Ok(session)
}

pub fn qualify_push(base: &str, run: PushRun, frame: u64) -> Result<String, ErrorWire> {
    let session = format!(
        "{}.{:016x}",
        qualify(&format!("{base}:c{frame:016x}"), run.generation)?,
        run.layout
    );
    let owned = parse(&session)?.ok_or_else(invalid)?;
    if !owned.family.starts_with("push:") {
        return Err(invalid());
    }
    Ok(session)
}

/// Two stable IDs within one download round, distinct from every later round.
pub fn qualify_pull_round(base: &str, run: PullRun, round: u64) -> Result<String, ErrorWire> {
    if round == 0 || !matches!(base.rsplit(':').next(), Some("a" | "b")) {
        return Err(invalid());
    }
    qualify_pull(&format!("{base}{round:016x}"), run)
}

pub fn parse(session: &str) -> Result<Option<Owned<'_>>, ErrorWire> {
    let Some((base, suffix)) = session.split_once(MARKER) else {
        return Ok(None);
    };
    if session.len() > 128 {
        return Err(invalid());
    }
    let mut numbers = suffix.split('.');
    let generation = Generation {
        incarnation: hex(numbers.next().ok_or_else(invalid)?)?,
        run: hex(numbers.next().ok_or_else(invalid)?)?,
    };
    let layout = numbers.next().map(hex).transpose()?;
    if numbers.next().is_some() || layout.is_some_and(|n| n == 0 || n > generation.run) {
        return Err(invalid());
    }
    if generation.run == 0 {
        return Err(invalid());
    }
    parse_base(base, generation, layout).map(Some)
}

/// Recognize canonical old transfer IDs for fencing after an upgraded pool
/// has claimed this worker. Arbitrary historical opaque sessions stay opaque.
pub fn parse_transfer(session: &str) -> Result<Option<Owned<'_>>, ErrorWire> {
    if session.contains(MARKER) {
        return parse(session);
    }
    Ok(parse_base(
        session,
        Generation {
            incarnation: 0,
            run: 0,
        },
        None,
    )
    .ok())
}

fn parse_base(
    base: &str,
    generation: Generation,
    layout: Option<u64>,
) -> Result<Owned<'_>, ErrorWire> {
    let mut parts = base.split(':');
    let kind = parts.next().ok_or_else(invalid)?;
    let peer = parts
        .next()
        .filter(|p| !p.is_empty() && !p.chars().any(char::is_control))
        .ok_or_else(invalid)?;
    let lane = parts.next().ok_or_else(invalid)?;
    let (index, count) = lane.split_once('/').ok_or_else(invalid)?;
    let spec = LaneSpec::new(
        index.parse().map_err(|_| invalid())?,
        count.parse().map_err(|_| invalid())?,
    );
    if !spec.is_valid() || lane != format!("{}/{}", spec.index, spec.count) {
        return Err(invalid());
    }
    let mut frame = None;
    let mut round_slot = None;
    let scope = match kind {
        "pull" => {
            let slot = parts.next().ok_or_else(invalid)?;
            round_slot = match slot.as_bytes().first() {
                Some(b'a') => Some(0),
                Some(b'b') => Some(1),
                _ => None,
            };
            if matches!(slot, "a" | "b") {
                // Historical slots have no per-round nonce.
            } else if slot.len() == 17
                && matches!(slot.as_bytes()[0], b'a' | b'b' | b'c')
                && hex(&slot[1..]).is_ok_and(|n| n > 0)
            {
                frame = Some(hex(&slot[1..])?);
            } else {
                return Err(invalid());
            }
            &base[..base.len() - slot.len() - 1]
        }
        "push" => match parts.next() {
            None => base,
            Some(slot)
                if slot.len() == 17
                    && slot.starts_with('c')
                    && hex(&slot[1..]).is_ok_and(|n| n > 0) =>
            {
                frame = Some(hex(&slot[1..])?);
                &base[..base.len() - slot.len() - 1]
            }
            _ => return Err(invalid()),
        },
        _ => return Err(invalid()),
    };
    if parts.next().is_some() {
        return Err(invalid());
    }
    Ok(Owned {
        scope,
        family: &base[..kind.len() + 1 + peer.len()],
        peer,
        lane: spec,
        frame,
        round_slot,
        generation,
        layout,
    })
}
