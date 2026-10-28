//! Clock sanity checks and NTP-style offset estimation between peers.
use serde::{Deserialize, Serialize};

/// 2020-01-01T00:00:00Z in Unix milliseconds. Earlier readings mean the
/// system clock was never set (or reset), and fiscal timestamps would be wrong.
pub const FLOOR_MS: u64 = 1_577_836_800_000;

/// Allowed skew, in milliseconds, before a peer's clock is reported as off.
pub const WARN_OFFSET_MS: i64 = 5_000;

/// Fails when the local system clock is before [`FLOOR_MS`].
pub fn check() -> anyhow::Result<()> {
    check_ms(crate::util::time::now_ms())
}

/// Fails when `now_ms` is before [`FLOOR_MS`].
pub fn check_ms(now_ms: u64) -> anyhow::Result<()> {
    if now_ms < FLOOR_MS {
        anyhow::bail!(
            "CLOCK_INVALID: system clock reads {now_ms} ms (before 2020-01-01T00:00:00Z); fix the clock (NTP) before running tkfs"
        );
    }
    Ok(())
}

/// One request/response exchange: `t1` client send, `t2` server receive,
/// `t3` server send, `t4` client receive. All in Unix milliseconds.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Sample {
    pub t1: u64,
    pub t2: u64,
    pub t3: u64,
    pub t4: u64,
}

/// Estimated clock offset of the peer (positive when the peer is ahead) and
/// the round-trip time with the server's processing time removed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Offset {
    pub offset_ms: i64,
    pub rtt_ms: u64,
}

impl Sample {
    pub fn offset(&self) -> Offset {
        let (t1, t2, t3, t4) = (
            i128::from(self.t1),
            i128::from(self.t2),
            i128::from(self.t3),
            i128::from(self.t4),
        );
        let offset = ((t2 - t1) + (t3 - t4)) / 2;
        let rtt = ((t4 - t1) - (t3 - t2)).max(0);
        Offset {
            offset_ms: offset.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
            rtt_ms: rtt.min(i128::from(u64::MAX)) as u64,
        }
    }
}

/// Exponential smoothing of offsets: the first sample is taken as-is, then
/// each sample moves the estimate a quarter of the way toward it.
pub fn smooth(previous: Option<i64>, sample: i64) -> i64 {
    match previous {
        None => sample,
        Some(p) => p.saturating_add(sample.saturating_sub(p) / 4),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_is_2020_01_01_utc() {
        assert_eq!(FLOOR_MS, 1_577_836_800_000);
    }

    #[test]
    fn clock_before_floor_is_invalid() {
        let error = check_ms(FLOOR_MS - 1).unwrap_err().to_string();
        assert!(error.starts_with("CLOCK_INVALID"), "{error}");
        assert!(check_ms(FLOOR_MS).is_ok());
        assert!(check_ms(1_790_000_000_000).is_ok());
    }

    #[test]
    fn ntp_offset_and_rtt() {
        let s = Sample {
            t1: 1000,
            t2: 1600,
            t3: 1610,
            t4: 1020,
        };
        assert_eq!(
            s.offset(),
            Offset {
                offset_ms: 595,
                rtt_ms: 10
            }
        );
    }

    #[test]
    fn smoothing_takes_first_sample_then_quarter_steps() {
        assert_eq!(smooth(None, 400), 400);
        assert_eq!(smooth(Some(400), 800), 500);
        assert_eq!(smooth(Some(0), -400), -100);
    }
}
