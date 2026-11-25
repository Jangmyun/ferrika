//! MQTT topic names and topic filters: the checks the CLI makes before it
//! connects, and filter matching (MQTT 3.1.1 section 4.7, MQTT 5 section 4.7).
//!
//! ```
//! use ferrika_mqtt::topic::{matches, validate_filter, validate_topic};
//!
//! assert!(validate_filter("sensors/+/temp").is_ok());
//! assert!(validate_filter("sensors/#/temp").is_err());
//! assert!(validate_topic("sensors/1/temp").is_ok());
//! assert!(validate_topic("sensors/+").is_err());
//! assert!(matches("sensors/#", "sensors/1/temp"));
//! assert!(!matches("#", "$SYS/broker/uptime"));
//! ```

use crate::error::{MqttError, MqttResult};

/// The longest topic MQTT can encode (a two-byte length prefix).
pub const MAX_TOPIC_BYTES: usize = 65_535;

/// Prefixes of shared subscriptions: `$share/` (MQTT 5, also EMQX and
/// HiveMQ on 3.1.1) and `$queue/` (EMQX). A shared subscription takes
/// messages away from the other members of its group, so the CLI refuses
/// them (see `docs/plans/mqtt.md`).
pub const SHARED_PREFIXES: [&str; 2] = ["$share/", "$queue/"];

fn common_checks(text: &str, what: &str) -> MqttResult<()> {
    if text.is_empty() {
        return Err(MqttError::config(format!("The {what} must not be empty")));
    }
    if text.len() > MAX_TOPIC_BYTES {
        return Err(MqttError::config(format!(
            "The {what} is longer than {MAX_TOPIC_BYTES} bytes"
        )));
    }
    if text.contains('\0') {
        return Err(MqttError::config(format!(
            "The {what} must not contain a NUL character"
        )));
    }
    Ok(())
}

/// Checks a topic name to publish to: not empty, no wildcards.
pub fn validate_topic(topic: &str) -> MqttResult<()> {
    common_checks(topic, "topic")?;
    if topic.contains(['+', '#']) {
        return Err(MqttError::config(format!(
            "'{topic}' contains a wildcard (+ or #); give one topic name"
        )));
    }
    Ok(())
}

/// Checks a topic filter to subscribe to: `+` fills a whole level, `#` is
/// a whole last level.
pub fn validate_filter(filter: &str) -> MqttResult<()> {
    common_checks(filter, "topic filter")?;
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        let last = i + 1 == levels.len();
        if level.contains('#') && (*level != "#" || !last) {
            return Err(MqttError::config(format!(
                "Invalid topic filter '{filter}': # must be the whole last level"
            )));
        }
        if level.contains('+') && *level != "+" {
            return Err(MqttError::config(format!(
                "Invalid topic filter '{filter}': + must be a whole level"
            )));
        }
    }
    Ok(())
}

/// Whether `filter` names a shared subscription (`$share/...`,
/// `$queue/...`).
pub fn is_shared(filter: &str) -> bool {
    SHARED_PREFIXES.iter().any(|p| filter.starts_with(p))
}

/// Checks a filter for a read: a valid filter and not a shared
/// subscription, which would take messages from the group's consumers.
pub fn validate_read_filter(filter: &str) -> MqttResult<()> {
    validate_filter(filter)?;
    if is_shared(filter) {
        return Err(MqttError::config(format!(
            "'{filter}' is a shared subscription: the broker would hand this client messages \
             meant for the group's consumers, which would then not get them. Subscribe to the \
             topic filter without the $share/GROUP/ (or $queue/) prefix instead"
        )));
    }
    Ok(())
}

/// Whether `topic` matches `filter`. Wildcards at the first level do not
/// match topics starting with `$` (such as `$SYS/...`).
pub fn matches(filter: &str, topic: &str) -> bool {
    let mut filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/');
    let mut first = true;
    loop {
        match (filter_levels.next(), topic_levels.next()) {
            (Some("#"), Some(level)) => return !(first && level.starts_with('$')),
            // `a/#` matches `a` too (the parent level).
            (Some("#"), None) => return true,
            (Some("+"), Some(level)) => {
                if first && level.starts_with('$') {
                    return false;
                }
            }
            (Some(f), Some(t)) => {
                if f != t {
                    return false;
                }
            }
            (None, None) => return true,
            (Some(_), None) | (None, Some(_)) => return false,
        }
        first = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_names_have_no_wildcards() {
        assert!(validate_topic("a/b/c").is_ok());
        assert!(validate_topic("/leading/slash").is_ok());
        for bad in ["", "a/+/c", "a/#", "a\0b"] {
            assert!(validate_topic(bad).is_err(), "{bad:?}");
        }
        assert!(validate_topic(&"x".repeat(MAX_TOPIC_BYTES + 1)).is_err());
    }

    #[test]
    fn filters_follow_the_wildcard_rules() {
        for good in ["#", "+", "a/#", "a/+/c", "+/+", "a//b", "$SYS/#"] {
            assert!(validate_filter(good).is_ok(), "{good}");
        }
        for bad in ["", "a#", "a/#/c", "a/b+", "+a/b", "#/a"] {
            assert!(validate_filter(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn shared_subscriptions_are_refused_for_reads() {
        assert!(is_shared("$share/g/a/b"));
        assert!(is_shared("$queue/a"));
        assert!(!is_shared("share/a"));
        let err = validate_read_filter("$share/workers/jobs/#").unwrap_err();
        assert!(err.to_string().contains("shared subscription"), "{err}");
        assert!(validate_read_filter("jobs/#").is_ok());
    }

    #[test]
    fn filters_match_topics() {
        assert!(matches("a/b", "a/b"));
        assert!(!matches("a/b", "a/c"));
        assert!(matches("a/+/c", "a/x/c"));
        assert!(!matches("a/+/c", "a/x/y/c"));
        assert!(matches("a/#", "a"));
        assert!(matches("a/#", "a/b/c"));
        assert!(matches("#", "a/b"));
        assert!(!matches("#", "$SYS/uptime"));
        assert!(!matches("+/uptime", "$SYS/uptime"));
        assert!(matches("$SYS/#", "$SYS/uptime"));
        assert!(matches("+", ""));
        assert!(!matches("a", "a/b"));
        assert!(matches("a/+", "a/"));
    }
}
