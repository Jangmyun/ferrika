//! Text, regular-expression or expression matching of records.

use crate::model::cluster::SampleMessage;
use crate::model::expr::{expression_text, is_expression, Expr, RecordView};

/// Record filter over key and payload: case-insensitive text or regex, or an
/// expression such as `$.amount > 100` (see [`crate::model::expr`]).
pub enum RecordFilter {
    All,
    Text(String),
    Pattern(regex::Regex),
    Expression(Expr),
}

impl RecordFilter {
    pub fn parse(query: &str, regex: bool) -> Result<Self, String> {
        if query.is_empty() {
            return Ok(Self::All);
        }
        // With the regex toggle on, `$` is a regex anchor.
        if !regex && is_expression(query) {
            return Expr::parse(expression_text(query)).map(Self::Expression);
        }
        if regex {
            regex::RegexBuilder::new(query)
                .case_insensitive(true)
                .build()
                .map(Self::Pattern)
                .map_err(|e| {
                    // regex errors span several lines (pattern, caret, reason);
                    // keep only the reason so the toolbar stays one line tall.
                    let text = e.to_string();
                    let reason = text
                        .lines()
                        .last()
                        .unwrap_or(&text)
                        .trim()
                        .trim_start_matches("error: ");
                    format!("Invalid pattern: {reason}")
                })
        } else {
            Ok(Self::Text(query.to_lowercase()))
        }
    }

    /// Matches the key, the payload, or any header name or value.
    pub fn matches(&self, message: &SampleMessage) -> bool {
        let fields = || {
            std::iter::once(message.payload.as_str())
                .chain(message.key.as_deref())
                .chain(
                    message
                        .headers
                        .iter()
                        .flat_map(|(k, v)| [k.as_str(), v.as_str()]),
                )
        };
        match self {
            Self::All => true,
            Self::Text(q) => fields().any(|f| f.to_lowercase().contains(q)),
            Self::Pattern(re) => fields().any(|f| re.is_match(f)),
            Self::Expression(expr) => expr.matches(&RecordView {
                key: message.key.as_deref(),
                value: Some(&message.payload),
                headers: &message.headers,
                partition: message.partition,
                offset: message.offset,
                timestamp_ms: message.timestamp_ms,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::cluster::ClusterSnapshot;

    fn message(key: Option<&str>, payload: &str, headers: &[(&str, &str)]) -> SampleMessage {
        let mut m = ClusterSnapshot::demo().sample_messages[0].clone();
        m.key = key.map(str::to_string);
        m.payload = payload.to_string();
        m.headers = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        m
    }

    #[test]
    fn empty_query_matches_everything() {
        let f = RecordFilter::parse("", true).unwrap();
        assert!(f.matches(&message(None, "", &[])));
    }

    #[test]
    fn text_is_case_insensitive_and_covers_key_payload_and_headers() {
        let f = RecordFilter::parse("ORDER", false).unwrap();
        assert!(f.matches(&message(None, "new Order", &[])));
        assert!(f.matches(&message(Some("order-1"), "x", &[])));
        assert!(f.matches(&message(None, "x", &[("trace", "my-order")])));
        assert!(f.matches(&message(None, "x", &[("Order-Id", "1")])));
        assert!(!f.matches(&message(None, "x", &[])));
    }

    #[test]
    fn non_ascii_text_matches_case_insensitively() {
        let f = RecordFilter::parse("ÄPFEL", false).unwrap();
        assert!(f.matches(&message(None, "zwei äpfel", &[])));
        let f = RecordFilter::parse("주문", false).unwrap();
        assert!(f.matches(&message(None, "{\"memo\":\"주문 접수\"}", &[])));
    }

    #[test]
    fn dollar_amounts_are_text_but_regex_mode_keeps_anchors() {
        let f = RecordFilter::parse("$100", false).unwrap();
        assert!(matches!(f, RecordFilter::Text(_)));
        assert!(f.matches(&message(None, "cost $100", &[])));
        let f = RecordFilter::parse("^abc$", true).unwrap();
        assert!(f.matches(&message(None, "ABC", &[])));
        assert!(!f.matches(&message(None, "abcd", &[])));
    }

    #[test]
    fn invalid_regex_reports_a_single_line() {
        let Err(err) = RecordFilter::parse("(unclosed", true) else {
            panic!("expected an error");
        };
        assert!(err.starts_with("Invalid pattern: "), "{err}");
        assert!(!err.contains('\n'), "{err}");
    }

    #[test]
    fn expressions_are_evaluated_and_bad_ones_rejected() {
        let f = RecordFilter::parse("$.amount > 100", false).unwrap();
        assert!(f.matches(&message(None, "{\"amount\": 150}", &[])));
        assert!(!f.matches(&message(None, "{\"amount\": 50}", &[])));
        assert!(!f.matches(&message(None, "not json", &[])));
        assert!(RecordFilter::parse("? $.amount >", false).is_err());
        let too_long = format!("? $.a == \"{}\"", "x".repeat(100_000));
        assert!(RecordFilter::parse(&too_long, false).is_err());
    }
}
