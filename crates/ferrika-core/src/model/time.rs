//! Time input shared by the CLI and the GUI.

/// `30m` / `2h` / `1d` / `45s` ago, epoch milliseconds, or
/// `YYYY-MM-DD[ HH:MM[:SS]]` in a zone `offset_ms` ahead of UTC (the CLI
/// passes 0, the GUI its display time zone).
pub fn parse_time_ms_in(text: &str, now_ms: i64, offset_ms: i64) -> Option<i64> {
    if let Some(unit) = text.chars().last().filter(|c| "smhd".contains(*c)) {
        let amount: i64 = text[..text.len() - 1]
            .trim()
            .parse()
            .ok()
            .filter(|n| *n > 0)?;
        let unit_ms = match unit {
            's' => 1_000,
            'm' => 60_000,
            'h' => 3_600_000,
            _ => 86_400_000,
        };
        return now_ms.checked_sub(amount.checked_mul(unit_ms)?);
    }
    if let Ok(ms) = text.parse::<i64>() {
        return (ms > 0).then_some(ms);
    }
    let (date, time) = text.split_once([' ', 'T']).unwrap_or((text, "00:00"));
    let mut d = date.split('-').map(|p| p.parse::<i64>().ok());
    let (year, month, day) = (d.next()??, d.next()??, d.next()??);
    if d.next().is_some()
        || !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
    {
        return None;
    }
    let mut t = time
        .trim_end_matches('Z')
        .split(':')
        .map(|p| p.parse::<i64>().ok());
    let (hour, minute) = (t.next()??, t.next()??);
    let second = t.next().unwrap_or(Some(0))?;
    if !(0..24).contains(&hour) || !(0..60).contains(&minute) || !(0..60).contains(&second) {
        return None;
    }
    // Days-from-civil (Howard Hinnant).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400) + hour * 3_600 + minute * 60 + second) * 1_000 - offset_ms)
}

/// Days in `month` (1-12) of `year`, leap years included.
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_time_ms_in;

    #[test]
    fn local_times_shift_by_the_zone_offset() {
        let kst = 9 * 3_600_000;
        let utc = parse_time_ms_in("2026-10-12 08:00", 0, 0).unwrap();
        assert_eq!(
            parse_time_ms_in("2026-10-12 08:00", 0, kst).unwrap(),
            utc - kst
        );
        // Relative times do not depend on the zone.
        assert_eq!(parse_time_ms_in("30m", 3_600_000, kst), Some(1_800_000));
    }

    #[test]
    fn accepts_epoch_dates_and_iso_t() {
        assert_eq!(
            parse_time_ms_in("1730538000000", 0, 0),
            Some(1_730_538_000_000)
        );
        assert_eq!(
            parse_time_ms_in("2024-11-02T09:00:00Z", 0, 0),
            Some(1_730_538_000_000)
        );
        assert_eq!(
            parse_time_ms_in("2024-11-02", 0, 0),
            Some(1_730_505_600_000)
        );
        assert_eq!(parse_time_ms_in("2024-13-02", 0, 0), None);
        assert_eq!(parse_time_ms_in("0m", 0, 0), None);
        assert_eq!(parse_time_ms_in("garbage", 0, 0), None);
    }

    #[test]
    fn rejects_days_the_month_does_not_have() {
        for bad in [
            "2026-02-30",
            "2026-04-31",
            "2026-02-29",
            "1900-02-29",
            "2026-06-31",
        ] {
            assert_eq!(parse_time_ms_in(bad, 0, 0), None, "{bad}");
        }
        for ok in [
            "2024-02-29",
            "2000-02-29",
            "2026-02-28",
            "2026-04-30",
            "2026-12-31",
        ] {
            assert!(parse_time_ms_in(ok, 0, 0).is_some(), "{ok}");
        }
    }

    #[test]
    fn relative_amounts_that_overflow_are_rejected() {
        assert_eq!(parse_time_ms_in("9223372036854775807d", 0, 0), None);
        assert_eq!(parse_time_ms_in("9223372036854775807s", 0, 0), None);
        assert_eq!(parse_time_ms_in("106751991167301d", 0, 0), None);
        assert_eq!(
            parse_time_ms_in("30d", 86_400_000 * 31, 0),
            Some(86_400_000)
        );
    }
}
