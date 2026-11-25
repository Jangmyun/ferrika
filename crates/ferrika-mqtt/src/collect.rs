//! The bounded loops on top of a connection: waiting for a SUBACK, reading
//! messages up to a count or a time limit, collecting retained messages,
//! and waiting for a publish to be acknowledged. They read [`Event`]s
//! through [`EventSource`], so they are tested with a scripted event stream.
//!
//! Nothing here runs without a limit: every loop stops at a count or a
//! deadline.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::Instant;

use crate::client::{Event, EventSource, MqttMessage, PublishAck, SubscribeOutcome};
use crate::error::{MqttError, MqttResult};

/// The most messages `sub` reads.
pub const MAX_COUNT: u64 = 100_000;
/// The longest `sub` runs.
pub const MAX_DURATION: Duration = Duration::from_secs(24 * 60 * 60);
/// The most retained messages a listing collects.
pub const MAX_RETAINED: usize = 10_000;

/// Waits for the SUBACK of one topic filter, within `timeout`. Messages
/// that arrive first are returned so they are not lost; a refused filter
/// is an error naming the broker's reason.
pub async fn await_subscription<S: EventSource>(
    source: &mut S,
    filter: &str,
    timeout: Duration,
) -> MqttResult<(u8, Vec<MqttMessage>)> {
    let deadline = Instant::now() + timeout;
    let mut early = Vec::new();
    loop {
        let event = tokio::time::timeout_at(deadline, source.next_event())
            .await
            .map_err(|_| MqttError::Timeout {
                seconds: timeout.as_secs(),
                waiting_for: format!("the broker to confirm the subscription to '{filter}'"),
            })??;
        match event {
            Event::Subscribed(outcomes) => {
                return match outcomes.into_iter().next() {
                    Some(SubscribeOutcome::Granted(qos)) => Ok((qos, early)),
                    Some(SubscribeOutcome::Refused(reason)) => Err(MqttError::Rejected {
                        what: format!("the subscription to '{filter}'"),
                        reason,
                    }),
                    None => Err(MqttError::Connection(
                        "the broker answered the subscription with no result".to_string(),
                    )),
                };
            }
            Event::Message(message) => early.push(message),
            Event::PublishSent
            | Event::PublishReceived(_)
            | Event::PublishAcked(_)
            | Event::Other => {}
        }
    }
}

/// When a subscription stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The most messages to read.
    pub count: u64,
    /// The longest to wait, from the start.
    pub duration: Duration,
}

impl Limits {
    /// Checks both limits: at least one message and one millisecond, at
    /// most [`MAX_COUNT`] and [`MAX_DURATION`].
    pub fn validate(&self) -> MqttResult<()> {
        if self.count == 0 || self.count > MAX_COUNT {
            return Err(MqttError::config(format!(
                "--count must be between 1 and {MAX_COUNT}"
            )));
        }
        if self.duration.is_zero() || self.duration > MAX_DURATION {
            return Err(MqttError::config(format!(
                "--duration must be between 1 and {} seconds",
                MAX_DURATION.as_secs()
            )));
        }
        Ok(())
    }
}

/// Why a subscription stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// The count was reached.
    Count,
    /// The time ran out.
    Duration,
}

/// Hands each message to `on_message` until `limits` is reached; `early`
/// (messages that came before the SUBACK) go first. Returns how many were
/// read and why it stopped.
pub async fn read_messages<S: EventSource>(
    source: &mut S,
    early: Vec<MqttMessage>,
    limits: Limits,
    mut on_message: impl FnMut(&MqttMessage),
) -> MqttResult<(u64, StopReason)> {
    let deadline = Instant::now() + limits.duration;
    let mut received = 0u64;
    for message in early {
        if received >= limits.count {
            return Ok((received, StopReason::Count));
        }
        on_message(&message);
        received += 1;
    }
    loop {
        if received >= limits.count {
            return Ok((received, StopReason::Count));
        }
        match tokio::time::timeout_at(deadline, source.next_event()).await {
            Err(_) => return Ok((received, StopReason::Duration)),
            Ok(Err(e)) => return Err(e),
            Ok(Ok(Event::Message(message))) => {
                on_message(&message);
                received += 1;
            }
            Ok(Ok(
                Event::Subscribed(_)
                | Event::PublishSent
                | Event::PublishReceived(_)
                | Event::PublishAcked(_)
                | Event::Other,
            )) => {}
        }
    }
}

/// How long a retained-message listing waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedWindow {
    /// The longest to wait in all.
    pub wait: Duration,
    /// Stop when no retained message came for this long (brokers send the
    /// stored messages right after the SUBACK).
    pub quiet: Duration,
    /// The most retained messages to collect.
    pub max: usize,
}

impl RetainedWindow {
    /// The window for `wait` and `max`: quiet after one second without a
    /// retained message (or `wait`, when shorter).
    pub fn new(wait: Duration, max: usize) -> Self {
        Self {
            wait,
            quiet: Duration::from_secs(1).min(wait),
            max,
        }
    }

    pub fn validate(&self) -> MqttResult<()> {
        if self.wait.is_zero() || self.wait > Duration::from_secs(600) {
            return Err(MqttError::config(
                "--wait must be between 1 and 600 seconds",
            ));
        }
        if self.max == 0 || self.max > MAX_RETAINED {
            return Err(MqttError::config(format!(
                "--count must be between 1 and {MAX_RETAINED}"
            )));
        }
        Ok(())
    }
}

/// Why a retained-message listing stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetainedStop {
    /// No retained message came for the quiet time: the usual end.
    Quiet,
    /// The whole wait ran out while messages still came.
    Wait,
    /// The most messages were collected; there may be more.
    Max,
}

/// The retained messages under a filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedReport {
    /// One per topic (the latest), sorted by topic.
    pub messages: Vec<MqttMessage>,
    /// Live (not retained) messages seen and left out.
    pub live_skipped: u64,
    pub stopped: RetainedStop,
}

/// Collects the retained messages the broker sends after a subscription
/// to `filter`: messages with the retain flag, one per topic, until the
/// window closes. Live messages (no retain flag) and topics outside
/// `filter` are skipped.
pub async fn collect_retained<S: EventSource>(
    source: &mut S,
    early: Vec<MqttMessage>,
    filter: &str,
    window: RetainedWindow,
) -> MqttResult<RetainedReport> {
    let start = Instant::now();
    let end = start + window.wait;
    let mut last = start;
    let mut by_topic: BTreeMap<String, MqttMessage> = BTreeMap::new();
    let mut live_skipped = 0u64;
    let take = |message: MqttMessage,
                by_topic: &mut BTreeMap<String, MqttMessage>,
                live_skipped: &mut u64|
     -> bool {
        if message.retain && crate::topic::matches(filter, &message.topic) {
            by_topic.insert(message.topic.clone(), message);
            true
        } else {
            *live_skipped += 1;
            false
        }
    };
    for message in early {
        take(message, &mut by_topic, &mut live_skipped);
    }
    let stopped = loop {
        if by_topic.len() >= window.max {
            break RetainedStop::Max;
        }
        let quiet_at = last + window.quiet;
        let deadline = quiet_at.min(end);
        match tokio::time::timeout_at(deadline, source.next_event()).await {
            Err(_) if quiet_at <= end => break RetainedStop::Quiet,
            Err(_) => break RetainedStop::Wait,
            Ok(Err(e)) => return Err(e),
            Ok(Ok(Event::Message(message))) => {
                if take(message, &mut by_topic, &mut live_skipped) {
                    last = Instant::now();
                }
            }
            Ok(Ok(
                Event::Subscribed(_)
                | Event::PublishSent
                | Event::PublishReceived(_)
                | Event::PublishAcked(_)
                | Event::Other,
            )) => {}
        }
    };
    Ok(RetainedReport {
        messages: by_topic.into_values().collect(),
        live_skipped,
        stopped,
    })
}

/// How the broker took a publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct PublishOutcome {
    /// The broker acknowledged it (QoS 1 and 2); a QoS 0 publish gets no
    /// answer.
    pub acknowledged: bool,
    /// MQTT 5: the broker said no subscriber got it.
    pub no_subscribers: bool,
}

/// Waits until a publish of quality `qos` is done: handed to the network
/// (QoS 0), PUBACK (QoS 1) or PUBCOMP (QoS 2), within `timeout`. A refusal
/// in an MQTT 5 acknowledgement is an error.
pub async fn await_publish<S: EventSource>(
    source: &mut S,
    topic: &str,
    qos: u8,
    timeout: Duration,
) -> MqttResult<PublishOutcome> {
    let deadline = Instant::now() + timeout;
    let refused = |ack: &PublishAck| {
        ack.refusal.clone().map(|reason| MqttError::Rejected {
            what: format!("the message to '{topic}'"),
            reason,
        })
    };
    let mut no_subscribers = false;
    loop {
        let event = tokio::time::timeout_at(deadline, source.next_event())
            .await
            .map_err(|_| MqttError::Timeout {
                seconds: timeout.as_secs(),
                waiting_for: format!("the broker to acknowledge the message to '{topic}'"),
            })??;
        match event {
            Event::PublishSent if qos == 0 => {
                return Ok(PublishOutcome {
                    acknowledged: false,
                    no_subscribers: false,
                })
            }
            Event::PublishReceived(ack) => {
                if let Some(error) = refused(&ack) {
                    return Err(error);
                }
                no_subscribers |= ack.no_subscribers;
            }
            Event::PublishAcked(ack) => {
                if let Some(error) = refused(&ack) {
                    return Err(error);
                }
                return Ok(PublishOutcome {
                    acknowledged: true,
                    no_subscribers: no_subscribers || ack.no_subscribers,
                });
            }
            Event::PublishSent | Event::Subscribed(_) | Event::Message(_) | Event::Other => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// Scripted events: each after a delay from the previous one; then
    /// silence (or the error, when one is scripted last).
    struct Script {
        steps: VecDeque<(Duration, MqttResult<Event>)>,
    }

    impl Script {
        fn new(steps: Vec<(u64, MqttResult<Event>)>) -> Self {
            Self {
                steps: steps
                    .into_iter()
                    .map(|(ms, e)| (Duration::from_millis(ms), e))
                    .collect(),
            }
        }
    }

    impl EventSource for Script {
        async fn next_event(&mut self) -> MqttResult<Event> {
            match self.steps.pop_front() {
                Some((delay, event)) => {
                    tokio::time::sleep(delay).await;
                    event
                }
                None => std::future::pending().await,
            }
        }
    }

    fn message(topic: &str, retain: bool) -> Event {
        Event::Message(MqttMessage {
            topic: topic.to_string(),
            payload: topic.as_bytes().to_vec(),
            qos: 0,
            retain,
            user_properties: Vec::new(),
            content_type: None,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_subscription_waits_for_its_suback_and_keeps_early_messages() {
        let mut script = Script::new(vec![
            (5, Ok(Event::Other)),
            (5, Ok(message("a/early", false))),
            (5, Ok(Event::Subscribed(vec![SubscribeOutcome::Granted(1)]))),
        ]);
        let (qos, early) = await_subscription(&mut script, "a/#", Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(qos, 1);
        assert_eq!(early.len(), 1);

        let mut refused = Script::new(vec![(
            1,
            Ok(Event::Subscribed(vec![SubscribeOutcome::Refused(
                "NotAuthorized".to_string(),
            )])),
        )]);
        let err = await_subscription(&mut refused, "secret/#", Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("NotAuthorized"), "{err}");

        let mut silent = Script::new(vec![]);
        let err = await_subscription(&mut silent, "a", Duration::from_secs(3))
            .await
            .unwrap_err();
        assert!(
            matches!(err, MqttError::Timeout { seconds: 3, .. }),
            "{err}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reading_stops_at_the_count() {
        let mut script = Script::new(vec![
            (10, Ok(message("a/1", false))),
            (10, Ok(Event::Other)),
            (10, Ok(message("a/2", false))),
            (10, Ok(message("a/3", false))),
        ]);
        let mut seen = Vec::new();
        let limits = Limits {
            count: 2,
            duration: Duration::from_secs(60),
        };
        let (n, why) = read_messages(&mut script, vec![], limits, |m| seen.push(m.topic.clone()))
            .await
            .unwrap();
        assert_eq!((n, why), (2, StopReason::Count));
        assert_eq!(seen, ["a/1", "a/2"]);
    }

    #[tokio::test(start_paused = true)]
    async fn reading_stops_at_the_deadline_and_counts_early_messages() {
        let mut script = Script::new(vec![
            (100, Ok(message("a/1", false))),
            (5_000, Ok(message("a/late", false))),
        ]);
        let early = match message("a/0", false) {
            Event::Message(m) => vec![m],
            _ => unreachable!(),
        };
        let limits = Limits {
            count: 10,
            duration: Duration::from_secs(2),
        };
        let start = Instant::now();
        let mut seen = Vec::new();
        let (n, why) = read_messages(&mut script, early, limits, |m| seen.push(m.topic.clone()))
            .await
            .unwrap();
        assert_eq!((n, why), (2, StopReason::Duration));
        assert_eq!(seen, ["a/0", "a/1"]);
        assert_eq!(Instant::now() - start, Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_connection_ends_reading_with_an_error() {
        let mut script = Script::new(vec![
            (1, Ok(message("a/1", false))),
            (1, Err(MqttError::Connection("reset".to_string()))),
        ]);
        let limits = Limits {
            count: 10,
            duration: Duration::from_secs(5),
        };
        assert!(read_messages(&mut script, vec![], limits, |_| {})
            .await
            .is_err());
    }

    #[test]
    fn limits_are_bounded() {
        let ok = Limits {
            count: 1,
            duration: Duration::from_secs(1),
        };
        assert!(ok.validate().is_ok());
        assert!(Limits { count: 0, ..ok }.validate().is_err());
        assert!(Limits {
            count: MAX_COUNT + 1,
            ..ok
        }
        .validate()
        .is_err());
        assert!(Limits {
            duration: Duration::ZERO,
            ..ok
        }
        .validate()
        .is_err());
        assert!(RetainedWindow::new(Duration::ZERO, 10).validate().is_err());
        assert!(RetainedWindow::new(Duration::from_secs(1), 0)
            .validate()
            .is_err());
        assert_eq!(
            RetainedWindow::new(Duration::from_millis(300), 1).quiet,
            Duration::from_millis(300)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retained_messages_are_collected_until_the_broker_goes_quiet() {
        let mut script = Script::new(vec![
            (10, Ok(message("dev/1/state", true))),
            // A live message on a retained topic: left out.
            (10, Ok(message("dev/9/state", false))),
            (10, Ok(message("dev/2/state", true))),
            // A newer retained message for the same topic replaces the first.
            (10, Ok(message("dev/1/state", true))),
            // Outside the filter (a broker would not send it): left out.
            (10, Ok(message("other/x", true))),
            // After the quiet second: not waited for.
            (3_000, Ok(message("dev/3/state", true))),
        ]);
        let window = RetainedWindow::new(Duration::from_secs(10), 100);
        let start = Instant::now();
        let report = collect_retained(&mut script, vec![], "dev/+/state", window)
            .await
            .unwrap();
        let topics: Vec<&str> = report.messages.iter().map(|m| m.topic.as_str()).collect();
        assert_eq!(topics, ["dev/1/state", "dev/2/state"]);
        assert_eq!(report.live_skipped, 2);
        assert_eq!(report.stopped, RetainedStop::Quiet);
        // The last retained message came at 40 ms; quiet one second later.
        assert_eq!(Instant::now() - start, Duration::from_millis(1_040));
    }

    #[tokio::test(start_paused = true)]
    async fn a_retained_listing_stops_at_the_wait_and_at_the_maximum() {
        // A message every 500 ms keeps the broker from going quiet.
        let steps: Vec<(u64, MqttResult<Event>)> = (0..20)
            .map(|i| (500, Ok(message(&format!("t/{i:02}"), true))))
            .collect();
        let mut script = Script::new(steps.clone());
        let window = RetainedWindow::new(Duration::from_millis(2_200), 100);
        let report = collect_retained(&mut script, vec![], "t/#", window)
            .await
            .unwrap();
        assert_eq!(report.stopped, RetainedStop::Wait);
        assert_eq!(report.messages.len(), 4);

        let mut script = Script::new(steps);
        let window = RetainedWindow::new(Duration::from_secs(60), 4);
        let report = collect_retained(&mut script, vec![], "t/#", window)
            .await
            .unwrap();
        assert_eq!(report.stopped, RetainedStop::Max);
        assert_eq!(report.messages.len(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn no_retained_message_ends_after_the_quiet_time() {
        let mut script = Script::new(vec![]);
        let window = RetainedWindow::new(Duration::from_secs(5), 10);
        let start = Instant::now();
        let report = collect_retained(&mut script, vec![], "#", window)
            .await
            .unwrap();
        assert!(report.messages.is_empty());
        assert_eq!(report.stopped, RetainedStop::Quiet);
        assert_eq!(Instant::now() - start, Duration::from_secs(1));
    }

    fn ack(refusal: Option<&str>, no_subscribers: bool) -> PublishAck {
        PublishAck {
            refusal: refusal.map(str::to_string),
            no_subscribers,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn publishes_wait_for_their_acknowledgement() {
        let mut qos0 = Script::new(vec![(1, Ok(Event::PublishSent))]);
        let outcome = await_publish(&mut qos0, "t", 0, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(!outcome.acknowledged);

        let mut qos1 = Script::new(vec![
            (1, Ok(Event::PublishSent)),
            (1, Ok(Event::PublishAcked(ack(None, true)))),
        ]);
        let outcome = await_publish(&mut qos1, "t", 1, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            PublishOutcome {
                acknowledged: true,
                no_subscribers: true
            }
        );

        let mut qos2 = Script::new(vec![
            (1, Ok(Event::PublishSent)),
            (1, Ok(Event::PublishReceived(ack(None, true)))),
            (1, Ok(Event::PublishAcked(ack(None, false)))),
        ]);
        let outcome = await_publish(&mut qos2, "t", 2, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(outcome.acknowledged && outcome.no_subscribers);

        let mut refused = Script::new(vec![
            (1, Ok(Event::PublishSent)),
            (
                1,
                Ok(Event::PublishAcked(ack(Some("NotAuthorized"), false))),
            ),
        ]);
        let err = await_publish(&mut refused, "t", 1, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(err, MqttError::Rejected { .. }), "{err}");

        let mut silent = Script::new(vec![(1, Ok(Event::PublishSent))]);
        let err = await_publish(&mut silent, "t", 1, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(err, MqttError::Timeout { .. }), "{err}");
    }
}
