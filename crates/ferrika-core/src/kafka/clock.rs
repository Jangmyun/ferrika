//! Wall-clock helpers shared by the CLI, the GUI bridge, and audit logging.

/// Current Unix time in milliseconds, or 0 if the system clock is before 1970.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_ms_is_after_2020_and_monotone_enough() {
        let a = now_ms();
        let b = now_ms();
        // 2020-01-01T00:00:00Z
        assert!(a > 1_577_836_800_000);
        assert!(b >= a);
    }
}
