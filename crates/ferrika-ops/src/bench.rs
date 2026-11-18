use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for a message producing performance benchmark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkProduceConfig {
    /// Target topic to publish synthetic records to.
    pub topic: String,
    /// Total number of records to produce.
    pub num_records: usize,
    /// Size of each record payload in bytes.
    pub record_size_bytes: usize,
    /// Target records-per-second limit (None for maximum throughput).
    pub target_throughput: Option<u32>,
    /// Compression codec (gzip, snappy, lz4, zstd, or none).
    pub compression: Option<String>,
}

impl Default for BenchmarkProduceConfig {
    fn default() -> Self {
        Self {
            topic: "bench-test".to_string(),
            num_records: 10_000,
            record_size_bytes: 512,
            target_throughput: None,
            compression: None,
        }
    }
}

/// Configuration for a message consuming performance benchmark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConsumeConfig {
    /// Target topic to consume benchmark records from.
    pub topic: String,
    /// Maximum number of messages to consume.
    pub max_messages: usize,
    /// Maximum idle wait time before completing.
    pub timeout_secs: u64,
}

impl Default for BenchmarkConsumeConfig {
    fn default() -> Self {
        Self {
            topic: "bench-test".to_string(),
            max_messages: 10_000,
            timeout_secs: 30,
        }
    }
}

/// Statistics and metrics collected during a benchmark run.
#[derive(Debug, Clone, Default)]
pub struct BenchmarkStats {
    /// Total records successfully processed.
    pub total_records: usize,
    /// Total payload bytes processed.
    pub total_bytes: usize,
    /// Total elapsed time.
    pub elapsed: Duration,
    /// Collected roundtrip / delivery latencies in milliseconds.
    pub latencies_ms: Vec<f64>,
}

impl BenchmarkStats {
    /// Records a single transaction with its elapsed latency in milliseconds and byte count.
    pub fn record(&mut self, latency_ms: f64, bytes: usize) {
        self.total_records += 1;
        self.total_bytes += bytes;
        self.latencies_ms.push(latency_ms);
    }

    /// Records a transaction that has no meaningful latency (e.g. a consumed message).
    pub fn record_bytes(&mut self, bytes: usize) {
        self.total_records += 1;
        self.total_bytes += bytes;
    }

    /// Records a batch of transactions.
    pub fn record_batch(&mut self, count: usize, bytes: usize, latency_ms: f64) {
        self.total_records += count;
        self.total_bytes += bytes;
        self.latencies_ms.push(latency_ms);
    }

    /// Calculates throughput in records per second.
    pub fn records_per_sec(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs > 0.0 {
            self.total_records as f64 / secs
        } else {
            0.0
        }
    }

    /// Calculates data transfer rate in megabytes per second.
    pub fn mb_per_sec(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs > 0.0 {
            ferrika_core::model::format::mib(self.total_bytes as u64) / secs
        } else {
            0.0
        }
    }

    /// Computes the average latency in milliseconds.
    pub fn avg_latency_ms(&self) -> f64 {
        if self.latencies_ms.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.latencies_ms.iter().sum();
        sum / self.latencies_ms.len() as f64
    }

    /// Computes the minimum latency in milliseconds.
    pub fn min_latency_ms(&self) -> f64 {
        if self.latencies_ms.is_empty() {
            return 0.0;
        }
        self.latencies_ms
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min)
    }

    /// Computes the maximum latency in milliseconds.
    pub fn max_latency_ms(&self) -> f64 {
        self.latencies_ms.iter().copied().fold(0.0, f64::max)
    }

    /// Computes a specified percentile (0.0 to 100.0) latency in milliseconds.
    pub fn percentile_latency_ms(&self, percentile: f64) -> f64 {
        if self.latencies_ms.is_empty() {
            return 0.0;
        }

        let mut sorted = self.latencies_ms.clone();
        sorted.sort_by(f64::total_cmp);
        percentile_of_sorted(&sorted, percentile)
    }

    /// Produces a complete summary report suitable for display or serialization.
    pub fn summarize(&self) -> BenchmarkSummary {
        let mut sorted = self.latencies_ms.clone();
        sorted.sort_by(f64::total_cmp);
        BenchmarkSummary {
            total_records: self.total_records,
            total_bytes: self.total_bytes,
            elapsed_secs: self.elapsed.as_secs_f64(),
            records_per_sec: self.records_per_sec(),
            mb_per_sec: self.mb_per_sec(),
            avg_latency_ms: self.avg_latency_ms(),
            min_latency_ms: self.min_latency_ms(),
            p50_latency_ms: percentile_of_sorted(&sorted, 50.0),
            p95_latency_ms: percentile_of_sorted(&sorted, 95.0),
            p99_latency_ms: percentile_of_sorted(&sorted, 99.0),
            max_latency_ms: self.max_latency_ms(),
            latency_samples: sorted.len(),
        }
    }
}

/// Nearest-rank percentile of an ascending-sorted slice; 0.0 when empty.
fn percentile_of_sorted(sorted: &[f64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((percentile / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

/// Complete performance benchmark report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    pub total_records: usize,
    pub total_bytes: usize,
    pub elapsed_secs: f64,
    pub records_per_sec: f64,
    pub mb_per_sec: f64,
    pub avg_latency_ms: f64,
    pub min_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub max_latency_ms: f64,
    /// Number of latency samples; 0 when the benchmark does not measure latency.
    #[serde(default)]
    pub latency_samples: usize,
}

/// Generates a synthetic byte payload of specified length.
pub fn generate_synthetic_payload(size: usize, seed: u64) -> Vec<u8> {
    const PATTERN: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut payload = Vec::with_capacity(size);

    let offset = (seed as usize) % PATTERN.len();
    for i in 0..size {
        payload.push(PATTERN[(offset + i) % PATTERN.len()]);
    }
    payload
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synthetic_payload_generation() {
        let p1 = generate_synthetic_payload(64, 0);
        assert_eq!(p1.len(), 64);
        assert_eq!(&p1[..4], b"abcd");

        let p2 = generate_synthetic_payload(128, 1);
        assert_eq!(p2.len(), 128);
        assert_eq!(&p2[..4], b"bcde");
    }

    #[test]
    fn test_benchmark_stats_computations() {
        let mut stats = BenchmarkStats {
            elapsed: Duration::from_secs(2),
            ..Default::default()
        };

        // Add 5 samples: 1.0, 2.0, 3.0, 4.0, 5.0 ms
        for i in 1..=5 {
            stats.record(i as f64, 1024);
        }

        assert_eq!(stats.total_records, 5);
        assert_eq!(stats.total_bytes, 5 * 1024);
        assert_eq!(stats.records_per_sec(), 2.5);
        assert_eq!(stats.avg_latency_ms(), 3.0);
        assert_eq!(stats.min_latency_ms(), 1.0);
        assert_eq!(stats.max_latency_ms(), 5.0);
        assert_eq!(stats.percentile_latency_ms(50.0), 3.0);
        assert_eq!(stats.percentile_latency_ms(99.0), 5.0);

        let summary = stats.summarize();
        assert_eq!(summary.total_records, 5);
        assert_eq!(summary.p50_latency_ms, 3.0);
    }
}
