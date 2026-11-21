use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::json;

use ferrika_core::codec::schema::encode_confluent_wire_format;

/// Minimal, zero-dependency fast pseudo-random number generator (xorshift64star).
#[derive(Debug, Clone)]
pub(crate) struct FastRng {
    state: u64,
}

impl FastRng {
    /// Creates a new PRNG seeded with the current system time or a provided non-zero seed.
    pub fn new(seed: Option<u64>) -> Self {
        let initial = seed.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x853c49e6748fea9b)
        });
        Self {
            state: if initial == 0 {
                0x853c49e6748fea9b
            } else {
                initial
            },
        }
    }

    /// Generates the next pseudo-random 64-bit unsigned integer.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545f4914f6cdd1d)
    }

    /// Generates a random integer within `[min, max]`.
    pub fn gen_range(&mut self, min: u64, max: u64) -> u64 {
        if min >= max {
            return min;
        }
        min + (self.next_u64() % (max - min + 1))
    }

    /// Generates a random float within `[min, max]`.
    pub fn gen_float(&mut self, min: f64, max: f64) -> f64 {
        let fraction = (self.next_u64() % 10_000) as f64 / 10_000.0;
        min + (max - min) * fraction
    }

    /// Picks a random item from a slice of string literals.
    pub fn choose<'a>(&mut self, slice: &'a [&'a str]) -> &'a str {
        if slice.is_empty() {
            return "";
        }
        let idx = (self.next_u64() as usize) % slice.len();
        slice[idx]
    }
}

/// Supported event generation templates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EventTemplate {
    /// E-commerce orders, items, amounts, and payments
    #[default]
    Ecommerce,
    /// IoT telemetry, sensors, battery, and alerts
    Iot,
    /// Web access logs, HTTP methods, paths, and status codes
    Web,
    /// Financial transactions, asset transfers, and fees
    Financial,
    /// Custom template string with replacement tokens (e.g. {{uuid}}, {{index}})
    Custom(String),
}

impl std::str::FromStr for EventTemplate {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ecommerce" | "ecom" | "order" => Ok(Self::Ecommerce),
            "iot" | "sensor" | "telemetry" => Ok(Self::Iot),
            "web" | "http" | "log" | "access" => Ok(Self::Web),
            "financial" | "fin" | "bank" | "tx" => Ok(Self::Financial),
            "custom" => Err("The custom template needs --custom '<JSON template>'".to_string()),
            other => Err(format!(
                "Unknown template '{other}'. Expected ecommerce, iot, web, financial, or --custom"
            )),
        }
    }
}

/// Configurable synthetic event generator.
#[derive(Debug, Clone)]
pub struct EventGenerator {
    template: EventTemplate,
    schema_id: Option<u32>,
    key_prefix: Option<String>,
    rng: FastRng,
    sequence: u64,
}

impl EventGenerator {
    /// Creates a new generator with the specified template.
    pub fn new(template: EventTemplate) -> Self {
        Self {
            template,
            schema_id: None,
            key_prefix: None,
            rng: FastRng::new(None),
            sequence: 0,
        }
    }

    /// Sets the random seed for reproducible sequence generation.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.rng = FastRng::new(Some(seed));
        self
    }

    /// Wraps generated payloads in Confluent Schema Registry wire-format with the given schema ID.
    pub fn with_schema_id(mut self, schema_id: u32) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    /// Sets an explicit key prefix (e.g. "order-") to prepend to generated sequence numbers.
    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = Some(prefix.into());
        self
    }

    /// Returns the current sequence number of generated messages.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Generates the next synthetic message `(key, payload_bytes)`.
    pub fn generate_next(&mut self) -> (Option<String>, Vec<u8>) {
        self.sequence += 1;
        let seq = self.sequence;

        let custom_tpl = match &self.template {
            EventTemplate::Custom(s) => Some(s.clone()),
            _ => None,
        };

        let (default_key, raw_json) = match &self.template {
            EventTemplate::Ecommerce => self.generate_ecommerce(seq),
            EventTemplate::Iot => self.generate_iot(seq),
            EventTemplate::Web => self.generate_web(seq),
            EventTemplate::Financial => self.generate_financial(seq),
            EventTemplate::Custom(_) => {
                let s = custom_tpl.unwrap_or_default();
                self.generate_custom(&s, seq)
            }
        };

        // Determine final partition key
        let key = if let Some(ref prefix) = self.key_prefix {
            Some(format!("{}{}", prefix, seq))
        } else {
            default_key
        };

        let payload_bytes = raw_json.as_bytes();

        // Encode with Confluent wire format if schema_id was provided
        let final_payload = if let Some(id) = self.schema_id {
            encode_confluent_wire_format(id, payload_bytes)
        } else {
            payload_bytes.to_vec()
        };

        (key, final_payload)
    }

    fn generate_ecommerce(&mut self, seq: u64) -> (Option<String>, String) {
        let order_id = format!("ORD-{:06}", 10000 + (self.rng.next_u64() % 90000));
        let cust_id = format!("CUST-{:04}", self.rng.gen_range(100, 999));
        let items_pool = [
            "Mechanical Keyboard",
            "4K Ultra-Wide Monitor",
            "Ergonomic Standing Desk",
            "Rust in Action (Book)",
            "USB-C Docking Station",
            "Noise-Cancelling Headphones",
            "Wireless Mouse",
            "High-Speed NVMe SSD",
        ];

        let num_items = self.rng.gen_range(1, 3) as usize;
        let mut items = Vec::new();
        let mut total_amount = 0.0;

        for _ in 0..num_items {
            let name = self.rng.choose(&items_pool);
            let qty = self.rng.gen_range(1, 4);
            let price = (self.rng.gen_float(19.99, 499.99) * 100.0).round() / 100.0;
            total_amount += price * (qty as f64);
            items.push(json!({
                "item": name,
                "quantity": qty,
                "unit_price": price,
            }));
        }

        total_amount = (total_amount * 100.0).round() / 100.0;
        let status = self
            .rng
            .choose(&["CREATED", "PAID", "PROCESSING", "SHIPPED", "DELIVERED"]);
        let currency = self.rng.choose(&["USD", "EUR", "KRW", "GBP", "CAD"]);

        let now_ms = ferrika_core::kafka::clock::now_ms();

        let body = json!({
            "order_id": order_id,
            "customer_id": cust_id,
            "sequence": seq,
            "status": status,
            "currency": currency,
            "total_amount": total_amount,
            "items": items,
            "timestamp": now_ms,
        });

        (Some(order_id), body.to_string())
    }

    fn generate_iot(&mut self, seq: u64) -> (Option<String>, String) {
        let device_id = format!("DEV-{:03}", self.rng.gen_range(1, 40));
        let sensor_type = self
            .rng
            .choose(&["temperature", "pressure", "humidity", "vibration"]);
        let location =
            self.rng
                .choose(&["Warehouse-A", "Building-4", "Plant-Floor", "DataCenter-01"]);

        let value = match sensor_type {
            "temperature" => (self.rng.gen_float(18.0, 85.0) * 10.0).round() / 10.0,
            "pressure" => (self.rng.gen_float(980.0, 1050.0) * 10.0).round() / 10.0,
            "humidity" => (self.rng.gen_float(25.0, 95.0) * 10.0).round() / 10.0,
            _ => (self.rng.gen_float(0.1, 15.0) * 100.0).round() / 100.0,
        };

        let battery_pct = self.rng.gen_range(15, 100);
        let alert = value > 75.0 || battery_pct < 20;

        let now_ms = ferrika_core::kafka::clock::now_ms();

        let body = json!({
            "device_id": device_id,
            "sensor": sensor_type,
            "reading": value,
            "battery_level": battery_pct,
            "location": location,
            "alert": alert,
            "sequence": seq,
            "timestamp": now_ms,
        });

        (Some(device_id), body.to_string())
    }

    fn generate_web(&mut self, seq: u64) -> (Option<String>, String) {
        let ip = format!(
            "{}.{}.{}.{}",
            self.rng.gen_range(10, 192),
            self.rng.gen_range(1, 254),
            self.rng.gen_range(1, 254),
            self.rng.gen_range(1, 254)
        );

        let method = self.rng.choose(&["GET", "POST", "PUT", "DELETE", "PATCH"]);
        let path = self.rng.choose(&[
            "/api/v1/orders",
            "/api/v1/users/login",
            "/catalog/products",
            "/healthz",
            "/cart/checkout",
            "/metrics",
        ]);

        let status_codes = [200, 200, 200, 201, 204, 400, 401, 404, 500];
        let status_code = status_codes[(self.rng.next_u64() as usize) % status_codes.len()];
        let latency_ms = self.rng.gen_range(5, 650);
        let session_id = format!("sess-{:08x}", self.rng.next_u64() % 0xffffffff);

        let now_ms = ferrika_core::kafka::clock::now_ms();

        let body = json!({
            "client_ip": ip,
            "method": method,
            "path": path,
            "status_code": status_code,
            "response_time_ms": latency_ms,
            "session_id": session_id,
            "sequence": seq,
            "timestamp": now_ms,
        });

        (Some(session_id), body.to_string())
    }

    fn generate_financial(&mut self, seq: u64) -> (Option<String>, String) {
        let tx_id = format!("TX-{:012x}", self.rng.next_u64() % 0xffffffffffff);
        let sender = format!("ACCT-{:05}", self.rng.gen_range(1000, 9999));
        let receiver = format!("ACCT-{:05}", self.rng.gen_range(1000, 9999));
        let asset = self.rng.choose(&["USD", "EUR", "BTC", "ETH", "USDT"]);
        let amount = (self.rng.gen_float(10.0, 25000.0) * 100.0).round() / 100.0;
        let fee = (self.rng.gen_float(0.15, 8.50) * 100.0).round() / 100.0;
        let status = self.rng.choose(&["SETTLED", "PENDING", "APPROVED"]);

        let now_ms = ferrika_core::kafka::clock::now_ms();

        let body = json!({
            "transaction_id": tx_id,
            "sender": sender,
            "receiver": receiver,
            "asset": asset,
            "amount": amount,
            "fee": fee,
            "status": status,
            "sequence": seq,
            "timestamp": now_ms,
        });

        (Some(sender), body.to_string())
    }

    fn generate_custom(&mut self, template: &str, seq: u64) -> (Option<String>, String) {
        let now_ms = ferrika_core::kafka::clock::now_ms();

        let uuid = format!("{:016x}{:016x}", self.rng.next_u64(), self.rng.next_u64());
        let rand_int = self.rng.gen_range(1, 10000);
        let rand_float = (self.rng.gen_float(1.0, 100.0) * 100.0).round() / 100.0;
        let status = self.rng.choose(&["SUCCESS", "PENDING", "FAILED"]);

        let replaced = template
            .replace("{{uuid}}", &uuid)
            .replace("{{id}}", &uuid[..8])
            .replace("{{index}}", &seq.to_string())
            .replace("{{seq}}", &seq.to_string())
            .replace("{{timestamp}}", &now_ms.to_string())
            .replace("{{random_int}}", &rand_int.to_string())
            .replace("{{random_float}}", &rand_float.to_string())
            .replace("{{status}}", status);

        (Some(format!("key-{}", seq)), replaced)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrika_core::codec::schema::parse_confluent_wire_format;

    #[test]
    fn test_fast_rng_range_and_choice() {
        let mut rng = FastRng::new(Some(12345));
        for _ in 0..100 {
            let val = rng.gen_range(10, 20);
            assert!((10..=20).contains(&val));
        }

        let slice = ["apple", "banana", "cherry"];
        let choice = rng.choose(&slice);
        assert!(slice.contains(&choice));
    }

    #[test]
    fn test_generate_ecommerce_event() {
        let mut gen = EventGenerator::new(EventTemplate::Ecommerce).with_seed(42);
        let (key, payload) = gen.generate_next();

        assert!(key.is_some());
        let json_val: serde_json::Value =
            serde_json::from_slice(&payload).expect("Valid JSON expected");
        assert!(json_val.get("order_id").is_some());
        assert!(json_val.get("total_amount").is_some());
        assert_eq!(json_val["sequence"], 1);
    }

    #[test]
    fn test_generate_iot_event() {
        let mut gen = EventGenerator::new(EventTemplate::Iot).with_seed(100);
        let (key, payload) = gen.generate_next();

        assert!(key.is_some());
        let json_val: serde_json::Value =
            serde_json::from_slice(&payload).expect("Valid JSON expected");
        assert!(json_val.get("device_id").is_some());
        assert!(json_val.get("battery_level").is_some());
    }

    #[test]
    fn test_generate_web_event() {
        let mut gen = EventGenerator::new(EventTemplate::Web).with_seed(200);
        let (key, payload) = gen.generate_next();

        assert!(key.is_some());
        let json_val: serde_json::Value =
            serde_json::from_slice(&payload).expect("Valid JSON expected");
        assert!(json_val.get("client_ip").is_some());
        assert!(json_val.get("status_code").is_some());
    }

    #[test]
    fn test_generate_financial_event() {
        let mut gen = EventGenerator::new(EventTemplate::Financial).with_seed(300);
        let (key, payload) = gen.generate_next();

        assert!(key.is_some());
        let json_val: serde_json::Value =
            serde_json::from_slice(&payload).expect("Valid JSON expected");
        assert!(json_val.get("transaction_id").is_some());
        assert!(json_val.get("asset").is_some());
    }

    #[test]
    fn test_generate_custom_template() {
        let template = r#"{"msg_id":"{{uuid}}","count":{{index}},"status":"{{status}}"}"#;
        let mut gen =
            EventGenerator::new(EventTemplate::Custom(template.to_string())).with_seed(400);
        let (_, payload) = gen.generate_next();

        let json_val: serde_json::Value =
            serde_json::from_slice(&payload).expect("Valid JSON expected");
        assert_eq!(json_val["count"], 1);
        assert!(json_val.get("msg_id").is_some());
    }

    #[test]
    fn test_generate_with_schema_id_wire_format() {
        let mut gen = EventGenerator::new(EventTemplate::Ecommerce)
            .with_schema_id(999)
            .with_seed(500);
        let (_, payload) = gen.generate_next();

        let wire = parse_confluent_wire_format(&payload).expect("Should parse as wire format");
        assert_eq!(wire.schema_id, 999);
        assert!(wire.is_json);
    }
}
