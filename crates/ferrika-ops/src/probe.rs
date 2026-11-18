use serde::{Deserialize, Serialize};

/// Lightweight canary probe payload transmitted over Kafka
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProbePayload {
    pub probe_id: String,
    pub seq: u64,
    pub send_ts_us: u64,
}

impl ProbePayload {
    /// Creates a new canary probe message with unique ID and microsecond timestamp
    pub fn new(probe_id: impl Into<String>, seq: u64) -> Self {
        let send_ts_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        Self {
            probe_id: probe_id.into(),
            seq,
            send_ts_us,
        }
    }

    /// Serializes payload to compact JSON bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserializes a probe payload from JSON bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// Computes round-trip time in milliseconds against current system clock
    pub fn calculate_rtt_ms(&self) -> f64 {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        if now_us >= self.send_ts_us {
            (now_us - self.send_ts_us) as f64 / 1000.0
        } else {
            0.0
        }
    }
}

/// Measurement result of a single canary heartbeat round-trip
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeSample {
    pub seq: u64,
    pub rtt_ms: f64,
    pub partition: i32,
    pub offset: i64,
}

/// Statistical aggregates of end-to-end Kafka latency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeStats {
    pub sent: usize,
    pub received: usize,
    pub lost: usize,
    pub loss_pct: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub jitter_ms: f64,
}

impl ProbeStats {
    /// Computes latency distribution metrics and packet loss from recorded samples
    pub fn compute(samples: &[ProbeSample], total_sent: usize) -> Self {
        let received = samples.len();
        let lost = total_sent.saturating_sub(received);
        let loss_pct = if total_sent > 0 {
            (lost as f64 / total_sent as f64) * 100.0
        } else {
            0.0
        };

        if samples.is_empty() {
            return Self {
                sent: total_sent,
                received: 0,
                lost,
                loss_pct,
                min_ms: 0.0,
                max_ms: 0.0,
                mean_ms: 0.0,
                p50_ms: 0.0,
                p95_ms: 0.0,
                p99_ms: 0.0,
                jitter_ms: 0.0,
            };
        }

        let mut sorted_rtt: Vec<f64> = samples.iter().map(|s| s.rtt_ms).collect();
        sorted_rtt.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let min_ms = sorted_rtt.first().copied().unwrap_or(0.0);
        let max_ms = sorted_rtt.last().copied().unwrap_or(0.0);
        let sum: f64 = sorted_rtt.iter().sum();
        let mean_ms = sum / (received as f64);

        let p50_ms = quantile(&sorted_rtt, 0.50);
        let p95_ms = quantile(&sorted_rtt, 0.95);
        let p99_ms = quantile(&sorted_rtt, 0.99);

        // Jitter: Mean variation between consecutive received samples
        let mut jitter_sum = 0.0;
        let mut jitter_count = 0;
        for i in 1..samples.len() {
            jitter_sum += (samples[i].rtt_ms - samples[i - 1].rtt_ms).abs();
            jitter_count += 1;
        }
        let jitter_ms = if jitter_count > 0 {
            jitter_sum / (jitter_count as f64)
        } else {
            0.0
        };

        Self {
            sent: total_sent,
            received,
            lost,
            loss_pct,
            min_ms,
            max_ms,
            mean_ms,
            p50_ms,
            p95_ms,
            p99_ms,
            jitter_ms,
        }
    }
}

/// Helper function to calculate arbitrary quantiles (p in [0.0, 1.0]) on sorted slices
fn quantile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 || p <= 0.0 {
        return sorted[0];
    }
    if p >= 1.0 {
        return sorted[sorted.len() - 1];
    }

    let idx = (sorted.len() as f64 - 1.0) * p;
    let low = idx.floor() as usize;
    let high = idx.ceil() as usize;

    if low == high {
        sorted[low]
    } else {
        let weight = idx - (low as f64);
        sorted[low] * (1.0 - weight) + sorted[high] * weight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_probe_payload_serialization_roundtrip() {
        let probe = ProbePayload::new("probe-test-xyz", 42);
        let bytes = probe.to_bytes().expect("Serialization failed");
        let decoded = ProbePayload::from_bytes(&bytes).expect("Deserialization failed");
        assert_eq!(probe, decoded);
        assert_eq!(decoded.probe_id, "probe-test-xyz");
        assert_eq!(decoded.seq, 42);
    }

    #[test]
    fn test_probe_stats_computation() {
        let samples = vec![
            ProbeSample {
                seq: 1,
                rtt_ms: 10.0,
                partition: 0,
                offset: 100,
            },
            ProbeSample {
                seq: 2,
                rtt_ms: 12.0,
                partition: 0,
                offset: 101,
            },
            ProbeSample {
                seq: 3,
                rtt_ms: 15.0,
                partition: 0,
                offset: 102,
            },
            ProbeSample {
                seq: 4,
                rtt_ms: 20.0,
                partition: 0,
                offset: 103,
            },
            ProbeSample {
                seq: 5,
                rtt_ms: 25.0,
                partition: 0,
                offset: 104,
            },
        ];

        let stats = ProbeStats::compute(&samples, 5);
        assert_eq!(stats.sent, 5);
        assert_eq!(stats.received, 5);
        assert_eq!(stats.lost, 0);
        assert_eq!(stats.loss_pct, 0.0);
        assert_eq!(stats.min_ms, 10.0);
        assert_eq!(stats.max_ms, 25.0);
        assert_eq!(stats.mean_ms, 16.4);
        assert_eq!(stats.p50_ms, 15.0);
        assert!((stats.jitter_ms - 3.75).abs() < 0.01);
    }

    #[test]
    fn test_probe_stats_with_loss() {
        let samples = vec![
            ProbeSample {
                seq: 1,
                rtt_ms: 10.0,
                partition: 0,
                offset: 100,
            },
            ProbeSample {
                seq: 2,
                rtt_ms: 15.0,
                partition: 0,
                offset: 101,
            },
        ];

        let stats = ProbeStats::compute(&samples, 4); // 2 of 4 received -> 50% loss
        assert_eq!(stats.sent, 4);
        assert_eq!(stats.received, 2);
        assert_eq!(stats.lost, 2);
        assert_eq!(stats.loss_pct, 50.0);
    }
}
