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

#[derive(Debug)]
pub struct Owned<'a> {
    /// Pull slots a/b share a fence, while retaining distinct lease IDs.
    pub scope: &'a str,
    pub peer: &'a str,
    pub lane: LaneSpec,
    pub frame: Option<u64>,
    pub generation: Generation,
}

fn invalid() -> ErrorWire {
    ErrorWire::coded("INVALID_REQUEST", "malformed local lease generation")
}

pub fn qualify(base: &str, generation: Generation) -> Result<String, ErrorWire> {
    let session = format!(
        "{base}{MARKER}{:016x}.{:016x}",
        generation.incarnation, generation.run
    );
    parse(&session)?.ok_or_else(invalid)?;
    Ok(session)
}

pub fn parse(session: &str) -> Result<Option<Owned<'_>>, ErrorWire> {
    let Some((base, suffix)) = session.split_once(MARKER) else {
        return Ok(None);
    };
    if session.len() > 128 || suffix.len() != 33 || suffix.as_bytes()[16] != b'.' {
        return Err(invalid());
    }
    let hex = |part: &str| -> Result<u64, ErrorWire> {
        if !part
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid());
        }
        u64::from_str_radix(part, 16).map_err(|_| invalid())
    };
    let generation = Generation {
        incarnation: hex(&suffix[..16])?,
        run: hex(&suffix[17..])?,
    };
    if generation.run == 0 {
        return Err(invalid());
    }
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
    let scope = match kind {
        "pull" => {
            let slot = parts.next().ok_or_else(invalid)?;
            if !(matches!(slot, "a" | "b")
                || (slot.len() == 17
                    && slot.starts_with('c')
                    && hex(&slot[1..]).is_ok_and(|n| n > 0)))
            {
                return Err(invalid());
            }
            &base[..base.len() - slot.len() - 1]
        }
        "push" => base,
        _ => return Err(invalid()),
    };
    if parts.next().is_some() {
        return Err(invalid());
    }
    Ok(Some(Owned {
        scope,
        peer,
        lane: spec,
        frame: if kind == "pull" {
            base.rsplit(':')
                .next()
                .filter(|s| s.starts_with('c'))
                .map(|s| hex(&s[1..]))
                .transpose()?
        } else {
            None
        },
        generation,
    }))
}
