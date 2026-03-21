use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use regex::Regex;
use std::sync::LazyLock;

/// Canonical datetime length: YYYY-MM-DDTHH:MM:SS.fffffffffZ = 30 chars.
const MONOTONIC_DATETIME_LENGTH: usize = 30;

static MONOTONIC_DATETIME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$",
    )
    .unwrap()
});

/// Epoch anchor: wall-clock nanoseconds at process start.
static EPOCH_ANCHOR_NS: LazyLock<i64> = LazyLock::new(|| {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
});

/// Monotonic anchor: monotonic clock nanoseconds at process start.
static MONOTONIC_ANCHOR: LazyLock<Instant> = LazyLock::new(|| Instant::now());

/// Last emitted timestamp (nanoseconds since epoch) for monotonic guarantee.
static LAST_MONOTONIC_NS: AtomicI64 = AtomicI64::new(0);

/// Initialize anchors (call once at startup to ensure LazyLock is populated).
fn ensure_anchors() {
    let _ = *EPOCH_ANCHOR_NS;
    let _ = *MONOTONIC_ANCHOR;
}

/// Format epoch nanoseconds to canonical ISO 8601 string with 9 fractional digits.
pub fn format_epoch_ns_to_iso(epoch_ns: i64) -> Result<String, String> {
    let seconds = epoch_ns / 1_000_000_000;
    let fractional_ns = (epoch_ns % 1_000_000_000) as u64;

    let dt = chrono::DateTime::from_timestamp(seconds, fractional_ns as u32)
        .ok_or_else(|| format!("Invalid epoch nanoseconds: {}", epoch_ns))?;

    let year = dt.format("%Y").to_string().parse::<i32>().unwrap_or(0);
    if year <= 1990 || year >= 2500 {
        return Err("Datetime year must be >1990 and <2500".to_string());
    }

    let formatted = format!(
        "{}.{:09}Z",
        dt.format("%Y-%m-%dT%H:%M:%S"),
        fractional_ns
    );
    assert_eq!(
        formatted.len(),
        MONOTONIC_DATETIME_LENGTH,
        "Expected canonical datetime length {}, got {}: {:?}",
        MONOTONIC_DATETIME_LENGTH,
        formatted.len(),
        formatted
    );
    Ok(formatted)
}

/// Generate a monotonic datetime string, or normalize a provided ISO string.
///
/// Without arguments: returns a strictly monotonic UTC timestamp with nanosecond
/// precision, guaranteed to never go backwards even under clock skew.
///
/// With an ISO string: parses and normalizes to canonical format with 9 fractional digits.
pub fn monotonic_datetime(isostring: Option<&str>) -> Result<String, String> {
    if let Some(iso) = isostring {
        return normalize_iso_datetime(iso);
    }

    ensure_anchors();

    let elapsed_ns = MONOTONIC_ANCHOR.elapsed().as_nanos() as i64;
    let mut epoch_ns = *EPOCH_ANCHOR_NS + elapsed_ns;

    // Lock-free monotonic guarantee using CAS loop.
    loop {
        let last = LAST_MONOTONIC_NS.load(Ordering::Acquire);
        if epoch_ns <= last {
            epoch_ns = last + 1;
        }
        match LAST_MONOTONIC_NS.compare_exchange_weak(last, epoch_ns, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => {
                // Another thread updated; recalculate.
                if epoch_ns <= actual {
                    epoch_ns = actual + 1;
                }
            }
        }
    }

    format_epoch_ns_to_iso(epoch_ns)
}

/// Parse and normalize an ISO 8601 datetime string to canonical format.
fn normalize_iso_datetime(isostring: &str) -> Result<String, String> {
    let captures = MONOTONIC_DATETIME_REGEX
        .captures(isostring)
        .ok_or_else(|| format!("Invalid ISO datetime: {:?}", isostring))?;

    // Parse with chrono — replace Z with +00:00 for chrono compatibility.
    let chrono_str = isostring.replace('Z', "+00:00");
    let parsed = chrono::DateTime::parse_from_rfc3339(&chrono_str)
        .or_else(|_| chrono::DateTime::parse_from_str(&chrono_str, "%Y-%m-%dT%H:%M:%S%.f%:z"))
        .map_err(|e| format!("Invalid ISO datetime: {:?}: {}", isostring, e))?;

    let utc = parsed.with_timezone(&chrono::Utc);
    let year = utc.format("%Y").to_string().parse::<i32>().unwrap_or(0);
    if year <= 1990 || year >= 2500 {
        return Err(format!("Datetime year must be >1990 and <2500: {:?}", isostring));
    }

    // Extract fractional digits from the regex match, pad to 9 digits.
    let fractional = captures
        .get(7)
        .map(|m| m.as_str())
        .unwrap_or("");
    let padded_fractional = format!("{:0<9}", fractional);

    let normalized = format!(
        "{}.{}Z",
        utc.format("%Y-%m-%dT%H:%M:%S"),
        &padded_fractional[..9]
    );

    assert_eq!(
        normalized.len(),
        MONOTONIC_DATETIME_LENGTH,
        "Expected canonical datetime length {}, got {}: {:?}",
        MONOTONIC_DATETIME_LENGTH,
        normalized.len(),
        normalized
    );

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_datetime_no_arg() {
        let dt1 = monotonic_datetime(None).unwrap();
        let dt2 = monotonic_datetime(None).unwrap();
        assert_eq!(dt1.len(), 30);
        assert!(dt1.ends_with('Z'));
        assert!(dt2 > dt1, "Must be strictly monotonic");
    }

    #[test]
    fn test_monotonic_datetime_normalize() {
        let result = monotonic_datetime(Some("2025-01-02T03:04:05.678901234Z")).unwrap();
        assert_eq!(result, "2025-01-02T03:04:05.678901234Z");
    }

    #[test]
    fn test_monotonic_datetime_pad_fractional() {
        let result = monotonic_datetime(Some("2025-01-02T03:04:05.6Z")).unwrap();
        assert_eq!(result, "2025-01-02T03:04:05.600000000Z");
    }

    #[test]
    fn test_monotonic_datetime_no_fractional() {
        let result = monotonic_datetime(Some("2025-01-02T03:04:05Z")).unwrap();
        assert_eq!(result, "2025-01-02T03:04:05.000000000Z");
    }

    #[test]
    fn test_monotonic_datetime_with_offset() {
        let result = monotonic_datetime(Some("2025-01-02T03:04:05+00:00")).unwrap();
        assert_eq!(result, "2025-01-02T03:04:05.000000000Z");
    }

    #[test]
    fn test_invalid_iso_string() {
        assert!(monotonic_datetime(Some("not-a-date")).is_err());
    }

    #[test]
    fn test_monotonic_across_threads() {
        use std::thread;
        let handles: Vec<_> = (0..8)
            .map(|_| {
                thread::spawn(|| {
                    (0..1000)
                        .map(|_| monotonic_datetime(None).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        let mut all: Vec<String> = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }
        // All timestamps must be unique.
        let count = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), count, "All timestamps must be unique");
    }
}
