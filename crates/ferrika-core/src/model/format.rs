//! Display formats shared by the CLI, the GUI and reports: byte sizes and
//! durations.

/// "0 B", "980 B", "1.5 KB", "12.0 MB": a byte count in 1024-based units, one
/// decimal above 1023 bytes.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["KB", "MB", "GB", "TB", "PB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// `bytes` in KiB (1024-based), for fixed-unit tables such as `{:.2} KB`.
pub fn kib(bytes: u64) -> f64 {
    bytes as f64 / 1024.0
}

/// `bytes` in MiB (1024-based), for fixed-unit tables such as `{:.2} MB`.
pub fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// `45s`, `3m 12s`, `2h 5m`, `4d 2h`: how long ago, roughly. Negative input
/// reads as 0.
pub fn format_age(ms: i64) -> String {
    let s = ms.max(0) / 1000;
    match s {
        0..=59 => format!("{s}s"),
        60..=3599 => format!("{}m {}s", s / 60, s % 60),
        3600..=86_399 => format!("{}h {}m", s / 3600, (s % 3600) / 60),
        _ => format!("{}d {}h", s / 86_400, (s % 86_400) / 3600),
    }
}

/// The non-zero days, hours, minutes and seconds of `secs` joined by
/// `separator` (`1h 30m` with `" "`, `1h30m` with `""`); empty when `secs` is
/// zero or negative.
pub fn duration_parts(secs: i64, separator: &str) -> String {
    let parts = [
        (secs / 86_400, "d"),
        (secs % 86_400 / 3600, "h"),
        (secs % 3600 / 60, "m"),
        (secs % 60, "s"),
    ];
    parts
        .iter()
        .filter(|(n, _)| *n > 0)
        .map(|(n, unit)| format!("{n}{unit}"))
        .collect::<Vec<_>>()
        .join(separator)
}

/// `ms` as [`duration_parts`] of its whole seconds, or `500ms` when there are
/// none.
pub fn format_duration_ms(ms: i64, separator: &str) -> String {
    let out = duration_parts(ms / 1000, separator);
    if out.is_empty() {
        format!("{ms}ms")
    } else {
        out
    }
}

/// Lowercase hex, two digits per byte, no separators.
pub fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// A hexdump body: per line the offset (`{:08x}`), `width` hex columns (padded
/// on the last line) with an extra space after column `gap_after` when set,
/// and the printable ASCII between `|` bars. Every line ends with a newline;
/// empty input gives an empty string.
pub fn hexdump_lines(data: &[u8], width: usize, gap_after: Option<usize>) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(data.len() * 4 + 16);
    for (i, chunk) in data.chunks(width).enumerate() {
        let _ = write!(out, "{:08x}  ", i * width);
        for col in 0..width {
            match chunk.get(col) {
                Some(b) => {
                    let _ = write!(out, "{b:02x} ");
                }
                None => out.push_str("   "),
            }
            if gap_after == Some(col + 1) {
                out.push(' ');
            }
        }
        out.push_str(" |");
        out.extend(chunk.iter().map(|&b| {
            if (32..127).contains(&b) {
                b as char
            } else {
                '.'
            }
        }));
        out.push_str("|\n");
    }
    out
}

/// `url` without `user:password@`, for comparing and showing it.
pub fn redact_url(url: &str) -> String {
    let trimmed = url.trim().trim_end_matches('/');
    let (scheme, rest) = match trimmed.split_once("://") {
        Some((scheme, rest)) => (Some(scheme), rest),
        None => (None, trimmed),
    };
    let rest = match rest.split_once('/') {
        Some((authority, path)) => {
            let host = authority.rsplit('@').next().unwrap_or(authority);
            format!("{host}/{path}")
        }
        None => rest.rsplit('@').next().unwrap_or(rest).to_string(),
    };
    match scheme {
        Some(scheme) => format!("{}://{rest}", scheme.to_ascii_lowercase()),
        None => rest,
    }
}

/// Formats a Unix epoch timestamp in milliseconds as `YYYY-MM-DD HH:MM:SS.mmm` (UTC).
pub fn format_timestamp_ms(ms: i64) -> String {
    if ms <= 0 {
        return "-".to_string();
    }
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);

    // Civil-from-days (Howard Hinnant), valid for the proleptic Gregorian calendar.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);

    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{millis:03}",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_pick_a_readable_unit() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(12 * 1024 * 1024), "12.0 MB");
        assert_eq!(format_bytes(3 * 1024 * 1024 * 1024), "3.0 GB");
        assert_eq!(format_bytes(5 * 1024u64.pow(4)), "5.0 TB");
        assert_eq!(format_bytes(u64::MAX), "16384.0 PB");
    }

    #[test]
    fn fixed_units_match_the_tables() {
        assert_eq!(format!("{:.2} KB", kib(1536)), "1.50 KB");
        assert_eq!(format!("{:.2} MB", mib(1_048_576)), "1.00 MB");
        assert_eq!(mib(3 * 1_048_576), 3.0);
    }

    #[test]
    fn age_is_rough_and_never_negative() {
        assert_eq!(format_age(-5), "0s");
        assert_eq!(format_age(45_000), "45s");
        assert_eq!(format_age(192_000), "3m 12s");
        assert_eq!(format_age(7_500_000), "2h 5m");
        assert_eq!(format_age(352_800_000), "4d 2h");
    }

    #[test]
    fn durations_join_non_zero_parts() {
        assert_eq!(format_duration_ms(1_800_000, ""), "30m");
        assert_eq!(format_duration_ms(90_000, ""), "1m30s");
        assert_eq!(format_duration_ms(90_000, " "), "1m 30s");
        assert_eq!(format_duration_ms(86_400_000, ""), "1d");
        assert_eq!(format_duration_ms(93_600_000, " "), "1d 2h");
        assert_eq!(format_duration_ms(500, " "), "500ms");
        assert_eq!(format_duration_ms(-5, " "), "-5ms");
        assert_eq!(duration_parts(0, " "), "");
        assert_eq!(duration_parts(93_600, " "), "1d 2h");
    }

    #[test]
    fn hex_is_lowercase_and_unseparated() {
        assert_eq!(to_hex(&[]), "");
        assert_eq!(to_hex(&[0x00, 0x0f, 0xab, 0xff]), "000fabff");
    }

    #[test]
    fn hexdump_narrow_without_gap() {
        assert_eq!(hexdump_lines(&[], 8, None), "");
        assert_eq!(
            hexdump_lines(b"Kafka\x00\xff", 8, None),
            "00000000  4b 61 66 6b 61 00 ff     |Kafka..|\n"
        );
        assert_eq!(
            hexdump_lines(b"0123456789", 8, None),
            "00000000  30 31 32 33 34 35 36 37  |01234567|\n\
             00000008  38 39                    |89|\n"
        );
    }

    #[test]
    fn hexdump_wide_with_gap_after_eight_columns() {
        let dump = hexdump_lines(b"Hello, Ferrika Kafka!", 16, Some(8));
        let mut lines = dump.lines();
        assert_eq!(
            lines.next().unwrap(),
            "00000000  48 65 6c 6c 6f 2c 20 46  65 72 72 69 6b 61 20 4b  |Hello, Ferrika K|"
        );
        let second = format!("00000010  61 66 6b 61 21 {}|afka!|", " ".repeat(35));
        assert_eq!(lines.next().unwrap(), second);
        assert!(lines.next().is_none());
    }

    #[test]
    fn urls_lose_their_credentials() {
        assert_eq!(
            redact_url("https://user:pa@ss@sr.example:8081/prefix/"),
            "https://sr.example:8081/prefix"
        );
        assert_eq!(redact_url("HTTP://sr:8081"), "http://sr:8081");
        assert_eq!(redact_url("sr:8081"), "sr:8081");
    }
}
