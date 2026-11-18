use serde::{Deserialize, Serialize};

/// Distributed trace propagation standard
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceStandard {
    /// W3C Trace Context (traceparent / tracestate)
    W3c,
    /// Zipkin / B3 Multi-header (X-B3-TraceId, X-B3-SpanId)
    B3Multi,
    /// B3 Single-header (b3)
    B3Single,
    /// Uber Jaeger (uber-trace-id)
    Jaeger,
    /// No recognized trace header present
    Untraced,
}

impl TraceStandard {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::W3c => "W3C TraceContext",
            Self::B3Multi => "B3 (Multi-Header)",
            Self::B3Single => "B3 (Single-Header)",
            Self::Jaeger => "Jaeger",
            Self::Untraced => "Untraced (Dark Traffic)",
        }
    }
}

/// A random 64-bit value for trace and span ids (not for cryptography).
///
/// Each `RandomState` is seeded from the OS, and a process-wide counter keeps
/// ids generated in the same instant distinct.
fn random_u64() -> u64 {
    use std::hash::BuildHasher;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    std::collections::hash_map::RandomState::new().hash_one(COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// Parsed W3C TraceContext `traceparent` header (format: `00-<trace_id>-<parent_id>-<flags>`)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct W3cTraceparent {
    pub version: String,
    pub trace_id: String,
    pub parent_id: String,
    pub flags: String,
}

impl W3cTraceparent {
    /// Parses a standard W3C `traceparent` string
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.trim().split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        let (version, trace_id, parent_id, flags) = (parts[0], parts[1], parts[2], parts[3]);

        // Version: 2 hex characters, not ff
        if version.len() != 2 || version == "ff" || !is_valid_hex(version) {
            return None;
        }

        // Trace ID: 32 hex characters, cannot be all zeros
        if trace_id.len() != 32 || !is_valid_hex(trace_id) || trace_id.chars().all(|c| c == '0') {
            return None;
        }

        // Parent / Span ID: 16 hex characters, cannot be all zeros
        if parent_id.len() != 16 || !is_valid_hex(parent_id) || parent_id.chars().all(|c| c == '0')
        {
            return None;
        }

        // Flags: 2 hex characters
        if flags.len() != 2 || !is_valid_hex(flags) {
            return None;
        }

        Some(Self {
            version: version.to_string(),
            trace_id: trace_id.to_string(),
            parent_id: parent_id.to_string(),
            flags: flags.to_string(),
        })
    }

    /// Generates a valid random W3C `traceparent` header
    pub fn generate(sampled: bool) -> Self {
        let trace_val = (u128::from(random_u64()) << 64) | u128::from(random_u64());
        // All-zero ids are invalid; the chance is negligible but rule it out.
        let trace_val = trace_val.max(1);
        let parent_val = random_u64().max(1);

        let trace_id = format!("{trace_val:032x}");
        let parent_id = format!("{parent_val:016x}");
        let flags = if sampled {
            "01".to_string()
        } else {
            "00".to_string()
        };

        Self {
            version: "00".to_string(),
            trace_id,
            parent_id,
            flags,
        }
    }

    /// Formats as standard W3C `traceparent` header value
    pub fn to_header_value(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.version, self.trace_id, self.parent_id, self.flags
        )
    }

    /// Checks if the recorded trace is sampled (flag bit 0 set)
    pub fn is_sampled(&self) -> bool {
        u8::from_str_radix(&self.flags, 16)
            .map(|b| (b & 0x01) != 0)
            .unwrap_or(false)
    }
}

/// Extracted trace context details from a Kafka record's headers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedTrace {
    pub standard: TraceStandard,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub sampled: bool,
    pub raw_header_name: Option<String>,
    pub raw_header_value: Option<String>,
}

impl ExtractedTrace {
    /// Extracts trace context by scanning message headers for known tracing protocols
    pub fn from_headers(headers: &[(String, Vec<u8>)]) -> Self {
        // 1. Try W3C Trace Context ('traceparent')
        for (k, v) in headers {
            if k.eq_ignore_ascii_case("traceparent") {
                let s = String::from_utf8_lossy(v).trim().to_string();
                if let Some(tp) = W3cTraceparent::parse(&s) {
                    return Self {
                        standard: TraceStandard::W3c,
                        trace_id: Some(tp.trace_id.clone()),
                        span_id: Some(tp.parent_id.clone()),
                        sampled: tp.is_sampled(),
                        raw_header_name: Some(k.clone()),
                        raw_header_value: Some(s),
                    };
                }
            }
        }

        // 2. Try B3 Multi-header ('X-B3-TraceId', 'X-B3-SpanId', 'X-B3-Sampled')
        let mut b3_trace_id = None;
        let mut b3_span_id = None;
        let mut b3_sampled = false;

        for (k, v) in headers {
            let k_lower = k.to_lowercase();
            if k_lower == "x-b3-traceid" {
                b3_trace_id = Some(String::from_utf8_lossy(v).trim().to_string());
            } else if k_lower == "x-b3-spanid" {
                b3_span_id = Some(String::from_utf8_lossy(v).trim().to_string());
            } else if k_lower == "x-b3-sampled" {
                let val_str = String::from_utf8_lossy(v).trim().to_string();
                b3_sampled = val_str == "1" || val_str.eq_ignore_ascii_case("true");
            }
        }

        if let Some(tid) = b3_trace_id {
            return Self {
                standard: TraceStandard::B3Multi,
                trace_id: Some(tid),
                span_id: b3_span_id,
                sampled: b3_sampled,
                raw_header_name: Some("X-B3-TraceId".to_string()),
                raw_header_value: None,
            };
        }

        // 3. Try B3 Single-header ('b3')
        for (k, v) in headers {
            if k.eq_ignore_ascii_case("b3") {
                let s = String::from_utf8_lossy(v).trim().to_string();
                let parts: Vec<&str> = s.split('-').collect();
                if !parts.is_empty() && is_valid_hex(parts[0]) {
                    let tid = parts[0].to_string();
                    let sid = parts.get(1).map(|p| p.to_string());
                    let sampled = parts.get(2).is_some_and(|p| *p == "1" || *p == "d");
                    return Self {
                        standard: TraceStandard::B3Single,
                        trace_id: Some(tid),
                        span_id: sid,
                        sampled,
                        raw_header_name: Some(k.clone()),
                        raw_header_value: Some(s),
                    };
                }
            }
        }

        // 4. Try Uber Jaeger ('uber-trace-id')
        for (k, v) in headers {
            if k.eq_ignore_ascii_case("uber-trace-id") {
                let s = String::from_utf8_lossy(v).trim().to_string();
                let parts: Vec<&str> = s.split(':').collect();
                if parts.len() >= 3 && is_valid_hex(parts[0]) {
                    let tid = parts[0].to_string();
                    let sid = parts.get(1).map(|p| p.to_string());
                    // Jaeger flags are hex ("1", "01", "1f"), not decimal.
                    let flags = parts
                        .get(3)
                        .and_then(|f| u8::from_str_radix(f, 16).ok())
                        .unwrap_or(0);
                    return Self {
                        standard: TraceStandard::Jaeger,
                        trace_id: Some(tid),
                        span_id: sid,
                        sampled: (flags & 0x01) != 0,
                        raw_header_name: Some(k.clone()),
                        raw_header_value: Some(s),
                    };
                }
            }
        }

        Self {
            standard: TraceStandard::Untraced,
            trace_id: None,
            span_id: None,
            sampled: false,
            raw_header_name: None,
            raw_header_value: None,
        }
    }
}

impl ExtractedTrace {
    /// A copy for display while the company policy enforces masking. Trace
    /// headers are checked loosely (B3 IDs and the rest of a `b3` or
    /// `uber-trace-id` value are taken as they are), so whatever text they
    /// carry is masked too.
    pub fn masked(&self) -> Self {
        let mask = |text: &str| {
            ferrika_core::codec::pii::mask_payload(
                text,
                ferrika_core::codec::pii::MaskStrategy::Partial,
            )
        };
        Self {
            trace_id: self.trace_id.as_deref().map(mask),
            span_id: self.span_id.as_deref().map(mask),
            raw_header_value: self.raw_header_value.as_deref().map(mask),
            ..self.clone()
        }
    }
}

/// Helper function to check if a string contains valid hex characters
fn is_valid_hex(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Trace coverage and observability metrics across a batch of Kafka messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceAuditSummary {
    pub total_records: usize,
    pub traced_records: usize,
    pub untraced_records: usize,
    pub coverage_pct: f64,
    pub w3c_count: usize,
    pub b3_count: usize,
    pub jaeger_count: usize,
    pub sampled_count: usize,
    pub sampled_pct: f64,
}

impl TraceAuditSummary {
    /// Computes summary statistics over extracted message traces
    pub fn audit(traces: &[ExtractedTrace]) -> Self {
        let total = traces.len();
        let mut traced = 0;
        let mut untraced = 0;
        let mut w3c = 0;
        let mut b3 = 0;
        let mut jaeger = 0;
        let mut sampled = 0;

        for t in traces {
            match t.standard {
                TraceStandard::W3c => {
                    traced += 1;
                    w3c += 1;
                }
                TraceStandard::B3Multi | TraceStandard::B3Single => {
                    traced += 1;
                    b3 += 1;
                }
                TraceStandard::Jaeger => {
                    traced += 1;
                    jaeger += 1;
                }
                TraceStandard::Untraced => {
                    untraced += 1;
                }
            }

            if t.sampled {
                sampled += 1;
            }
        }

        let coverage_pct = if total > 0 {
            (traced as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        let sampled_pct = if traced > 0 {
            (sampled as f64 / traced as f64) * 100.0
        } else {
            0.0
        };

        Self {
            total_records: total,
            traced_records: traced,
            untraced_records: untraced,
            coverage_pct,
            w3c_count: w3c,
            b3_count: b3,
            jaeger_count: jaeger,
            sampled_count: sampled,
            sampled_pct,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jaeger(value: &str) -> ExtractedTrace {
        ExtractedTrace::from_headers(&[("uber-trace-id".to_string(), value.as_bytes().to_vec())])
    }

    #[test]
    fn jaeger_flags_are_hex() {
        assert!(jaeger("abc:def:0:1").sampled);
        assert!(jaeger("abc:def:0:01").sampled);
        // 0x1f has bit 0 set; read as decimal it failed to parse.
        assert!(jaeger("abc:def:0:1f").sampled);
        assert!(!jaeger("abc:def:0:0").sampled);
        // 0x0a = 0b1010, bit 0 clear.
        assert!(!jaeger("abc:def:0:0a").sampled);
        // Missing, empty or garbage flags are not sampled.
        assert!(!jaeger("abc:def:0").sampled);
        assert!(!jaeger("abc:def:0:").sampled);
        assert!(!jaeger("abc:def:0:zz").sampled);
        assert_eq!(jaeger("abc:def:0:1").standard, TraceStandard::Jaeger);
    }

    #[test]
    fn untraced_and_invalid_jaeger_headers() {
        assert_eq!(
            ExtractedTrace::from_headers(&[]).standard,
            TraceStandard::Untraced
        );
        assert_eq!(jaeger("not-hex!:a:b:1").standard, TraceStandard::Untraced);
        assert_eq!(jaeger("abc:def").standard, TraceStandard::Untraced);
    }

    #[test]
    fn test_generated_ids_are_unique_and_valid() {
        let ids: Vec<W3cTraceparent> = (0..200).map(|_| W3cTraceparent::generate(true)).collect();
        let traces: std::collections::HashSet<_> = ids.iter().map(|t| &t.trace_id).collect();
        let spans: std::collections::HashSet<_> = ids.iter().map(|t| &t.parent_id).collect();
        assert_eq!(traces.len(), ids.len());
        assert_eq!(spans.len(), ids.len());
        for id in &ids {
            assert!(W3cTraceparent::parse(&id.to_header_value()).is_some());
        }
    }

    #[test]
    fn test_w3c_traceparent_valid_parse() {
        let raw = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let parsed = W3cTraceparent::parse(raw).expect("Valid traceparent failed to parse");
        assert_eq!(parsed.version, "00");
        assert_eq!(parsed.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(parsed.parent_id, "00f067aa0ba902b7");
        assert_eq!(parsed.flags, "01");
        assert!(parsed.is_sampled());
        assert_eq!(parsed.to_header_value(), raw);
    }

    #[test]
    fn test_w3c_traceparent_invalid_parse() {
        // Invalid length
        assert!(W3cTraceparent::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-01").is_none());
        // All zero trace ID (prohibited by W3C spec)
        assert!(
            W3cTraceparent::parse("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
                .is_none()
        );
        // Non-hex
        assert!(
            W3cTraceparent::parse("00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn test_w3c_traceparent_generate() {
        let generated = W3cTraceparent::generate(true);
        assert_eq!(generated.version, "00");
        assert_eq!(generated.trace_id.len(), 32);
        assert_eq!(generated.parent_id.len(), 16);
        assert_eq!(generated.flags, "01");
        assert!(generated.is_sampled());

        let parsed = W3cTraceparent::parse(&generated.to_header_value())
            .expect("Generated header should be valid");
        assert_eq!(parsed.trace_id, generated.trace_id);
    }

    #[test]
    fn test_extracted_trace_b3_multi() {
        let headers = vec![
            (
                "X-B3-TraceId".to_string(),
                b"463ac35c9f6413ad48485a3953bb6124".to_vec(),
            ),
            ("X-B3-SpanId".to_string(), b"a2c4a886ba890421".to_vec()),
            ("X-B3-Sampled".to_string(), b"1".to_vec()),
        ];

        let extracted = ExtractedTrace::from_headers(&headers);
        assert_eq!(extracted.standard, TraceStandard::B3Multi);
        assert_eq!(
            extracted.trace_id.as_deref(),
            Some("463ac35c9f6413ad48485a3953bb6124")
        );
        assert_eq!(extracted.span_id.as_deref(), Some("a2c4a886ba890421"));
        assert!(extracted.sampled);
    }

    #[test]
    fn test_trace_audit_summary() {
        let t1 = ExtractedTrace {
            standard: TraceStandard::W3c,
            trace_id: Some("1".into()),
            span_id: Some("2".into()),
            sampled: true,
            raw_header_name: None,
            raw_header_value: None,
        };
        let t2 = ExtractedTrace {
            standard: TraceStandard::B3Multi,
            trace_id: Some("3".into()),
            span_id: Some("4".into()),
            sampled: false,
            raw_header_name: None,
            raw_header_value: None,
        };
        let t3 = ExtractedTrace {
            standard: TraceStandard::Untraced,
            trace_id: None,
            span_id: None,
            sampled: false,
            raw_header_name: None,
            raw_header_value: None,
        };

        let audit = TraceAuditSummary::audit(&[t1, t2, t3]);
        assert_eq!(audit.total_records, 3);
        assert_eq!(audit.traced_records, 2);
        assert_eq!(audit.untraced_records, 1);
        assert_eq!(audit.w3c_count, 1);
        assert_eq!(audit.b3_count, 1);
        assert_eq!(audit.sampled_count, 1);
        assert!((audit.coverage_pct - 66.66).abs() < 0.1);
        assert_eq!(audit.sampled_pct, 50.0);
    }
}
