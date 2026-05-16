use crate::types::Timestamp;

pub fn timestamp_from_datetime(datetime: chrono::DateTime<chrono::Utc>) -> Timestamp {
    datetime.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

pub fn now_timestamp() -> Timestamp {
    timestamp_from_datetime(chrono::Utc::now())
}

pub fn timestamp_after_seconds(seconds: f64) -> Option<Timestamp> {
    timestamp_plus_seconds(&now_timestamp(), seconds)
}

pub fn timestamp_plus_seconds(start: &str, seconds: f64) -> Option<Timestamp> {
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    let Ok(start) = chrono::DateTime::parse_from_rfc3339(start) else {
        return None;
    };
    timestamp_plus_seconds_from_datetime(start.with_timezone(&chrono::Utc), seconds)
}

pub fn timestamp_plus_seconds_from_datetime(
    start: chrono::DateTime<chrono::Utc>,
    seconds: f64,
) -> Option<Timestamp> {
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    let nanos = (seconds * 1_000_000_000.0).round();
    if nanos > i64::MAX as f64 {
        return None;
    }
    let deadline = start + chrono::Duration::nanoseconds(nanos as i64);
    Some(timestamp_from_datetime(deadline))
}

pub fn timestamp_is_past(deadline: &str) -> bool {
    let Ok(deadline) = chrono::DateTime::parse_from_rfc3339(deadline) else {
        return false;
    };
    chrono::Utc::now() > deadline.with_timezone(&chrono::Utc)
}
