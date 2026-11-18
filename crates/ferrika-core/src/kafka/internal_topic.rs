//! The one rule for "is this an internal topic", shared by every caller.

/// Whether `name` is a Kafka-internal topic: anything starting with `_`
/// (`__consumer_offsets`, `__transaction_state`, `_schemas`, `_confluent-*`…).
pub fn is_internal_topic(name: &str) -> bool {
    name.starts_with('_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn underscore_prefix_is_internal() {
        for name in [
            "__consumer_offsets",
            "__transaction_state",
            "_schemas",
            "_x",
        ] {
            assert!(is_internal_topic(name), "{name}");
        }
        for name in ["orders", "a_b", "", "my__topic"] {
            assert!(!is_internal_topic(name), "{name}");
        }
    }
}
