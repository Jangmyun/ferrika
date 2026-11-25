use std::time::Instant;

use serde::{Deserialize, Serialize};

use ferrika_core::codec::base64;
use ferrika_core::error::KafkaResult;
use ferrika_core::kafka::consumer::ConsumedMessage;

/// The dump format version [`DumpRecord::from_message`] writes in
/// `format_version`. A record without the field is version 1.
///
/// - 1: headers with a null value are left out; a [`DumpHeader`] with neither
///   `value` nor `value_base64` (never written) reads as an empty value.
/// - 2: headers with a null value are kept, as a [`DumpHeader`] with neither
///   `value` nor `value_base64` in `header_list`, and restore as null. Older
///   versions ignore the field and restore them as empty values.
pub const DUMP_FORMAT_VERSION: u32 = 2;

/// Represents a single backed-up Kafka record in portable JSON Lines format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DumpRecord {
    /// Format version of this record ([`DUMP_FORMAT_VERSION`]); `None`
    /// (absent) is version 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_version: Option<u32>,
    /// Topic where this record originated
    pub topic: String,
    /// Partition index
    pub partition: i32,
    /// Log offset
    pub offset: i64,
    /// Millisecond timestamp if available
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
    /// Human-readable key text if UTF-8. Also read as `key`, the field name
    /// `consume --save` writes, so its files can be restored.
    #[serde(default, alias = "key", skip_serializing_if = "Option::is_none")]
    pub key_text: Option<String>,
    /// Base64 representation of key if binary
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_base64: Option<String>,
    /// Human-readable payload text if UTF-8 (also read as `payload`).
    #[serde(default, alias = "payload", skip_serializing_if = "Option::is_none")]
    pub payload_text: Option<String>,
    /// Base64 representation of payload if binary
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,
    /// Message headers with UTF-8 values, an object in header order.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "ordered_object"
    )]
    pub headers: Vec<(String, String)>,
    /// Message headers whose values are not valid UTF-8, Base64 encoded
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "ordered_object"
    )]
    pub headers_base64: Vec<(String, String)>,
    /// Every header in record order, written only when `headers` and
    /// `headers_base64` cannot carry them exactly (a repeated key, a binary
    /// header before a text one, or a null value). When present it wins on
    /// read; the two objects beside it are the last-value-per-key view of the
    /// non-null headers that older versions read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_list: Option<Vec<DumpHeader>>,
}

/// One entry of [`DumpRecord::header_list`]: `value` for UTF-8 text,
/// `value_base64` for anything else, neither for a null value (format
/// version 2; see [`DUMP_FORMAT_VERSION`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DumpHeader {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_base64: Option<String>,
}

/// The JSON fields that carry a record's headers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderFields {
    pub headers: Vec<(String, String)>,
    pub headers_base64: Vec<(String, String)>,
    pub header_list: Option<Vec<DumpHeader>>,
}

impl HeaderFields {
    /// Splits headers into the text object, the Base64 object and, only when
    /// those two lose something, the ordered list. Records whose keys are
    /// all distinct (and whose text headers all come before the binary ones)
    /// serialize exactly as in the object-only format.
    /// `sorted_objects` is for writers whose JSON objects reorder their keys.
    pub fn from_headers(headers: &[(String, Vec<u8>)], sorted_objects: bool) -> Self {
        let all: Vec<(&str, Option<&[u8]>)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), Some(v.as_slice())))
            .collect();
        Self::from_all_headers(&all, sorted_objects)
    }

    /// Like [`HeaderFields::from_headers`], for headers that may have a null
    /// value (`None`). A null value is kept only in the ordered list, as an
    /// entry with neither `value` nor `value_base64`, so the record needs
    /// [`DUMP_FORMAT_VERSION`] 2 to be read back as null.
    pub fn from_all_headers(headers: &[(&str, Option<&[u8]>)], sorted_objects: bool) -> Self {
        let mut fields = Self::default();
        let mut seen_binary = false;
        let mut lossy = false;
        let mut keys = std::collections::HashSet::new();
        for &(k, v) in headers {
            if !keys.insert(k) {
                lossy = true;
            }
            let Some(v) = v else {
                lossy = true;
                continue;
            };
            match std::str::from_utf8(v) {
                Ok(text) => {
                    lossy |= seen_binary;
                    fields.headers.push((k.to_string(), text.to_string()));
                }
                Err(_) => {
                    seen_binary = true;
                    fields
                        .headers_base64
                        .push((k.to_string(), base64::encode(v)));
                }
            }
        }
        if sorted_objects {
            let sorted = |list: &[(String, String)]| list.windows(2).all(|w| w[0].0 <= w[1].0);
            lossy |= !sorted(&fields.headers) || !sorted(&fields.headers_base64);
        }
        if lossy {
            fields.header_list = Some(
                headers
                    .iter()
                    .map(|&(k, v)| {
                        let key = k.to_string();
                        match v.map(|v| (v, std::str::from_utf8(v))) {
                            None => DumpHeader {
                                key,
                                value: None,
                                value_base64: None,
                            },
                            Some((_, Ok(text))) => DumpHeader {
                                key,
                                value: Some(text.to_string()),
                                value_base64: None,
                            },
                            Some((v, Err(_))) => DumpHeader {
                                key,
                                value: None,
                                value_base64: Some(base64::encode(v)),
                            },
                        }
                    })
                    .collect(),
            );
            // The objects are the last-value-per-key view for older readers.
            for list in [&mut fields.headers, &mut fields.headers_base64] {
                let mut last: Vec<(String, String)> = Vec::new();
                for (k, v) in list.drain(..) {
                    match last.iter_mut().find(|(lk, _)| *lk == k) {
                        Some(slot) => slot.1 = v,
                        None => last.push((k, v)),
                    }
                }
                *list = last;
            }
        }
        fields
    }
}

/// A string-to-string JSON object kept as an ordered list of pairs.
mod ordered_object {
    use serde::de::{MapAccess, Visitor};
    use serde::ser::SerializeMap;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        pairs: &[(String, String)],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(pairs.len()))?;
        for (k, v) in pairs {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<(String, String)>, D::Error> {
        struct Pairs;
        impl<'de> Visitor<'de> for Pairs {
            type Value = Vec<(String, String)>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object of strings")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(pair) = map.next_entry::<String, String>()? {
                    out.push(pair);
                }
                Ok(out)
            }
        }
        deserializer.deserialize_map(Pairs)
    }
}

impl DumpRecord {
    /// Converts a consumed Kafka message into a serialized DumpRecord.
    pub fn from_message(msg: &ConsumedMessage) -> Self {
        let (key_text, key_base64) = if let Some(ref k) = msg.key {
            match std::str::from_utf8(k) {
                Ok(s) => (Some(s.to_string()), None),
                Err(_) => (None, Some(base64::encode(k))),
            }
        } else {
            (None, None)
        };

        let (payload_text, payload_base64) = if let Some(ref p) = msg.payload {
            match std::str::from_utf8(p) {
                Ok(s) => (Some(s.to_string()), None),
                Err(_) => (None, Some(base64::encode(p))),
            }
        } else {
            (None, None)
        };

        let HeaderFields {
            headers,
            headers_base64,
            header_list,
        } = HeaderFields::from_all_headers(&msg.all_headers(), false);

        Self {
            format_version: Some(DUMP_FORMAT_VERSION),
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            timestamp_ms: msg.timestamp_ms,
            key_text,
            key_base64,
            payload_text,
            payload_base64,
            headers,
            headers_base64,
            header_list,
        }
    }

    /// Extracts raw key bytes from either text or Base64 encoding (`None` for a null key).
    pub fn extract_key_bytes(&self) -> KafkaResult<Option<Vec<u8>>> {
        extract_bytes(self.key_text.as_deref(), self.key_base64.as_deref())
    }

    /// Extracts raw payload bytes from either text or Base64 encoding.
    /// Returns `None` for a null payload (a tombstone on compacted topics).
    pub fn extract_payload_bytes(&self) -> KafkaResult<Option<Vec<u8>>> {
        extract_bytes(self.payload_text.as_deref(), self.payload_base64.as_deref())
    }

    /// Extracts all headers in record order, decoding Base64-encoded binary
    /// values; `None` is a null value. Files without `header_list` list the
    /// text headers, then the binary ones. A `header_list` entry with neither
    /// value is null from format version 2 on and empty before it (as older
    /// versions read it).
    pub fn extract_all_headers(&self) -> KafkaResult<Vec<(String, Option<Vec<u8>>)>> {
        if let Some(list) = &self.header_list {
            let keeps_nulls = self.format_version.unwrap_or(1) >= 2;
            return list
                .iter()
                .map(|h| {
                    let value = match (&h.value, &h.value_base64) {
                        (Some(text), _) => Some(text.as_bytes().to_vec()),
                        (None, Some(b64)) => Some(base64::decode(b64)?),
                        (None, None) if keeps_nulls => None,
                        (None, None) => Some(Vec::new()),
                    };
                    Ok((h.key.clone(), value))
                })
                .collect();
        }
        let mut headers: Vec<(String, Option<Vec<u8>>)> = self
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.as_bytes().to_vec())))
            .collect();
        for (k, v) in &self.headers_base64 {
            headers.push((k.clone(), Some(base64::decode(v)?)));
        }
        Ok(headers)
    }

    /// [`DumpRecord::extract_all_headers`] with null values read as empty
    /// ones, as versions before format version 2 restore them.
    pub fn extract_headers(&self) -> KafkaResult<Vec<(String, Vec<u8>)>> {
        Ok(self
            .extract_all_headers()?
            .into_iter()
            .map(|(k, v)| (k, v.unwrap_or_default()))
            .collect())
    }
}

/// Builds one JSON Lines record as `consume --save` and the GUI export write
/// it. Non-UTF-8 keys, payloads and header values are stored Base64-encoded in
/// `*_base64` fields instead of being dropped; `DumpRecord` reads the result
/// back (`key` and `payload` are aliases of its text fields), so `restore` can
/// replay a saved file.
pub fn jsonl_record(msg: &ConsumedMessage) -> serde_json::Value {
    // serde_json objects sort their keys, so records whose header order that
    // would change also get the ordered `header_list`.
    let fields = HeaderFields::from_headers(&msg.headers, true);
    let to_map = |pairs: Vec<(String, String)>| {
        pairs
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::from(v)))
            .collect::<serde_json::Map<_, _>>()
    };
    let headers = to_map(fields.headers);
    let headers_base64 = to_map(fields.headers_base64);

    let mut record = serde_json::json!({
        "topic": msg.topic,
        "partition": msg.partition,
        "offset": msg.offset,
        "timestamp_ms": msg.timestamp_ms,
        "key": msg.key_str(),
        "payload": msg.payload_str(),
        "headers": headers,
    });
    if let Some(obj) = record.as_object_mut() {
        if let Some(k) = msg
            .key
            .as_deref()
            .filter(|k| std::str::from_utf8(k).is_err())
        {
            obj.insert("key_base64".into(), base64::encode(k).into());
        }
        if let Some(p) = msg
            .payload
            .as_deref()
            .filter(|p| std::str::from_utf8(p).is_err())
        {
            obj.insert("payload_base64".into(), base64::encode(p).into());
        }
        if !headers_base64.is_empty() {
            obj.insert("headers_base64".into(), headers_base64.into());
        }
        if let Some(list) = fields.header_list {
            obj.insert(
                "header_list".into(),
                serde_json::to_value(list).unwrap_or_default(),
            );
        }
    }
    record
}

fn extract_bytes(text: Option<&str>, base64: Option<&str>) -> KafkaResult<Option<Vec<u8>>> {
    match (text, base64) {
        (Some(text), _) => Ok(Some(text.as_bytes().to_vec())),
        (None, Some(b64)) => Ok(Some(base64::decode(b64)?)),
        (None, None) => Ok(None),
    }
}

/// Records to produce from a text file: lines in the `dump` JSONL format
/// keep their key and payload (text or Base64), any other non-empty line
/// is one payload.
pub fn records_from_lines(text: &str) -> Vec<(Option<Vec<u8>>, Vec<u8>)> {
    text.lines()
        .map(|line| line.trim_end_matches('\r'))
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<DumpRecord>(line)
                .ok()
                .and_then(|r| {
                    let payload = r.extract_payload_bytes().ok()??;
                    let key = r.extract_key_bytes().ok()?;
                    Some((key, payload))
                })
                .unwrap_or_else(|| (None, line.as_bytes().to_vec()))
        })
        .collect()
}

/// Statistics for topic backup (dump) operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpStats {
    /// Number of records dumped
    pub records_dumped: u64,
    /// Total payload data dumped in bytes
    pub bytes_dumped: u64,
    /// Elapsed time in seconds
    pub elapsed_secs: f64,
    /// Average throughput (records per second)
    pub average_eps: f64,
}

impl DumpStats {
    /// Creates a new timer for dump operation.
    pub fn new() -> (Self, Instant) {
        (
            Self {
                records_dumped: 0,
                bytes_dumped: 0,
                elapsed_secs: 0.0,
                average_eps: 0.0,
            },
            Instant::now(),
        )
    }
}

/// Statistics for topic restore operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreStats {
    /// Number of records restored
    pub records_restored: u64,
    /// Total bytes restored
    pub bytes_restored: u64,
    /// Elapsed time in seconds
    pub elapsed_secs: f64,
    /// Average throughput (records per second)
    pub average_eps: f64,
}

impl RestoreStats {
    /// Creates a new timer for restore operation.
    pub fn new() -> (Self, Instant) {
        (
            Self {
                records_restored: 0,
                bytes_restored: 0,
                elapsed_secs: 0.0,
                average_eps: 0.0,
            },
            Instant::now(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reads_consume_save_records() {
        // The shape `ferrika consume --save` writes.
        let line = r#"{"topic":"t","partition":1,"offset":7,"timestamp_ms":5,"key":"k1","payload":"hello","headers":{"h":"v"}}"#;
        let record: DumpRecord = serde_json::from_str(line).unwrap();
        assert_eq!(record.extract_key_bytes().unwrap(), Some(b"k1".to_vec()));
        assert_eq!(
            record.extract_payload_bytes().unwrap(),
            Some(b"hello".to_vec())
        );
        assert_eq!(record.headers, [("h".to_string(), "v".to_string())]);

        let binary = r#"{"topic":"t","partition":0,"offset":1,"key":null,"payload":null,"payload_base64":"AAE="}"#;
        let record: DumpRecord = serde_json::from_str(binary).unwrap();
        assert_eq!(record.extract_payload_bytes().unwrap(), Some(vec![0, 1]));
    }

    #[test]
    fn records_from_lines_reads_dump_lines_and_plain_lines() {
        let text = "{\"topic\":\"t\",\"partition\":0,\"offset\":1,\"key\":\"k\",\"payload\":\"v\"}\r\n\nplain line\n{\"other\":1}\n";
        let records = records_from_lines(text);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], (Some(b"k".to_vec()), b"v".to_vec()));
        assert_eq!(records[1], (None, b"plain line".to_vec()));
        assert_eq!(records[2], (None, b"{\"other\":1}".to_vec()));
    }

    #[test]
    fn test_dump_record_from_consumed_message_utf8() {
        let msg = ConsumedMessage {
            topic: "orders".into(),
            partition: 1,
            offset: 42,
            key: Some(b"order-101".to_vec()),
            payload: Some(b"{\"item\":\"book\",\"price\":29.99}".to_vec()),
            timestamp_ms: Some(1726900000000),
            headers: Vec::new(),
            null_headers: Vec::new(),
        };

        let dump = DumpRecord::from_message(&msg);
        assert_eq!(dump.topic, "orders");
        assert_eq!(dump.partition, 1);
        assert_eq!(dump.offset, 42);
        assert_eq!(dump.key_text.as_deref(), Some("order-101"));
        assert!(dump.key_base64.is_none());
        assert_eq!(
            dump.payload_text.as_deref(),
            Some("{\"item\":\"book\",\"price\":29.99}")
        );

        assert_eq!(
            dump.extract_key_bytes().unwrap(),
            Some(b"order-101".to_vec())
        );
        assert_eq!(
            dump.extract_payload_bytes().unwrap(),
            Some(b"{\"item\":\"book\",\"price\":29.99}".to_vec())
        );
    }

    #[test]
    fn test_dump_record_from_consumed_message_binary() {
        let binary_payload = vec![0x00, 0x01, 0x02, 0xfe, 0xff];
        let msg = ConsumedMessage {
            topic: "telemetry".into(),
            partition: 0,
            offset: 99,
            key: None,
            payload: Some(binary_payload.clone()),
            timestamp_ms: None,
            headers: Vec::new(),
            null_headers: Vec::new(),
        };

        let dump = DumpRecord::from_message(&msg);
        assert!(dump.payload_text.is_none());
        assert!(dump.payload_base64.is_some());

        let extracted = dump.extract_payload_bytes().unwrap();
        assert_eq!(extracted, Some(binary_payload));
    }

    #[test]
    fn test_dump_record_preserves_tombstone_and_binary_header() {
        let headers = vec![
            ("trace".to_string(), b"abc".to_vec()),
            ("bin".to_string(), vec![0xff, 0x00, 0xfe]),
        ];
        let msg = ConsumedMessage {
            topic: "compacted".into(),
            partition: 0,
            offset: 7,
            key: Some(b"user-1".to_vec()),
            payload: None,
            timestamp_ms: Some(1000),
            headers: headers.clone(),
            null_headers: Vec::new(),
        };

        let dump = DumpRecord::from_message(&msg);
        let line = serde_json::to_string(&dump).unwrap();
        let restored: DumpRecord = serde_json::from_str(&line).unwrap();

        assert_eq!(restored.extract_payload_bytes().unwrap(), None);
        assert_eq!(restored.extract_headers().unwrap(), headers);
    }

    fn msg_with(headers: Vec<(String, Vec<u8>)>) -> ConsumedMessage {
        ConsumedMessage {
            topic: "t".into(),
            partition: 1,
            offset: 5,
            key: Some(b"k".to_vec()),
            payload: Some(b"v".to_vec()),
            timestamp_ms: Some(9),
            headers,
            null_headers: Vec::new(),
        }
    }

    fn h(k: &str, v: &[u8]) -> (String, Vec<u8>) {
        (k.to_string(), v.to_vec())
    }

    fn roundtrip(headers: Vec<(String, Vec<u8>)>) -> (String, Vec<(String, Vec<u8>)>) {
        let line = serde_json::to_string(&DumpRecord::from_message(&msg_with(headers))).unwrap();
        let back: DumpRecord = serde_json::from_str(&line).unwrap();
        (line, back.extract_headers().unwrap())
    }

    #[test]
    fn dump_keeps_repeated_header_keys_in_order() {
        let headers = vec![h("x", b"1"), h("x", b"2"), h("y", b"z")];
        let (line, back) = roundtrip(headers.clone());
        assert_eq!(back, headers, "{line}");
        // Older versions still read the last value of each key.
        let legacy: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(legacy["headers"]["x"], "2");
        assert_eq!(legacy["headers"]["y"], "z");
    }

    #[test]
    fn dump_keeps_the_order_of_interleaved_text_and_binary_headers() {
        let headers = vec![
            h("a", &[0xff, 0x00]),
            h("b", b"text"),
            h("a", b""),
            h("c", &[0xfe]),
        ];
        let (line, back) = roundtrip(headers.clone());
        assert_eq!(back, headers, "{line}");
    }

    #[test]
    fn dump_of_distinct_keys_keeps_the_object_format_byte_for_byte() {
        let (line, back) = roundtrip(vec![h("trace", b"abc"), h("empty", b""), h("bin", &[0xff])]);
        assert_eq!(
            line,
            r#"{"format_version":2,"topic":"t","partition":1,"offset":5,"timestamp_ms":9,"key_text":"k","payload_text":"v","headers":{"trace":"abc","empty":""},"headers_base64":{"bin":"/w=="}}"#
        );
        assert_eq!(
            back,
            vec![h("trace", b"abc"), h("empty", b""), h("bin", &[0xff])]
        );
        let (line, _) = roundtrip(vec![h("a", b"1")]);
        assert!(!line.contains("header_list"), "{line}");
    }

    #[test]
    fn restore_reads_the_old_object_format() {
        let line = r#"{"topic":"t","partition":0,"offset":1,"key_text":"k","payload_text":"v","headers":{"b":"2","a":"1"},"headers_base64":{"bin":"/w=="}}"#;
        let record: DumpRecord = serde_json::from_str(line).unwrap();
        assert_eq!(
            record.extract_headers().unwrap(),
            vec![h("b", b"2"), h("a", b"1"), h("bin", &[0xff])]
        );
        let none: DumpRecord =
            serde_json::from_str(r#"{"topic":"t","partition":0,"offset":1}"#).unwrap();
        assert!(none.extract_headers().unwrap().is_empty());
    }

    #[test]
    fn header_fields_use_a_list_only_when_the_objects_would_lose_something() {
        let plain = HeaderFields::from_headers(&[h("a", b"1"), h("b", b"2")], false);
        assert_eq!(plain.header_list, None);
        let dup = HeaderFields::from_headers(&[h("a", b"1"), h("a", b"2")], false);
        assert_eq!(dup.header_list.as_ref().map(Vec::len), Some(2));
        assert_eq!(dup.headers, [("a".to_string(), "2".to_string())]);
        // An object that sorts its keys reorders these.
        let unsorted = [h("b", b"1"), h("a", b"2")];
        assert_eq!(
            HeaderFields::from_headers(&unsorted, false).header_list,
            None
        );
        assert!(HeaderFields::from_headers(&unsorted, true)
            .header_list
            .is_some());
    }

    fn msg_with_nulls() -> ConsumedMessage {
        let mut msg = msg_with(vec![h("a", b"1"), h("bin", &[0xff]), h("e", b"")]);
        msg.null_headers = vec![(0, "n0".to_string()), (2, "n2".to_string())];
        msg
    }

    fn all(headers: &[(&str, Option<&[u8]>)]) -> Vec<(String, Option<Vec<u8>>)> {
        headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.map(<[u8]>::to_vec)))
            .collect()
    }

    #[test]
    fn dump_keeps_null_headers_and_restores_them_as_null() {
        let msg = msg_with_nulls();
        let line = serde_json::to_string(&DumpRecord::from_message(&msg)).unwrap();
        let json: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(json["format_version"], 2, "{line}");
        assert_eq!(json["header_list"][0], serde_json::json!({"key": "n0"}));
        // The objects older versions read without `header_list` leave nulls out.
        assert_eq!(json["headers"], serde_json::json!({"a": "1", "e": ""}));
        assert_eq!(json["headers_base64"], serde_json::json!({"bin": "/w=="}));

        let back: DumpRecord = serde_json::from_str(&line).unwrap();
        assert_eq!(back.extract_all_headers().unwrap(), all(&msg.all_headers()));
        assert_eq!(
            back.extract_all_headers().unwrap(),
            vec![
                ("n0".to_string(), None),
                ("a".to_string(), Some(b"1".to_vec())),
                ("n2".to_string(), None),
                ("bin".to_string(), Some(vec![0xff])),
                ("e".to_string(), Some(Vec::new())),
            ]
        );
        // The byte view reads a null as empty, as older versions restore it.
        assert_eq!(
            back.extract_headers().unwrap(),
            vec![
                h("n0", b""),
                h("a", b"1"),
                h("n2", b""),
                h("bin", &[0xff]),
                h("e", b"")
            ]
        );
    }

    #[test]
    fn dump_without_null_headers_round_trips_in_format_2() {
        let msg = msg_with(vec![h("x", b"1"), h("x", &[0xfe]), h("y", b"")]);
        let dump = DumpRecord::from_message(&msg);
        assert_eq!(dump.format_version, Some(DUMP_FORMAT_VERSION));
        let back: DumpRecord =
            serde_json::from_str(&serde_json::to_string(&dump).unwrap()).unwrap();
        assert_eq!(back, dump);
        assert_eq!(back.extract_all_headers().unwrap(), all(&msg.all_headers()));
    }

    #[test]
    fn restore_reads_a_valueless_list_entry_of_an_old_dump_as_empty() {
        // A dump without `format_version` (version 1) restores exactly as
        // before: an entry with neither value is an empty value, not null.
        let line = r#"{"topic":"t","partition":0,"offset":1,"payload_text":"v","headers":{"a":"2"},"header_list":[{"key":"a","value":"1"},{"key":"n"},{"key":"a","value":"2"}]}"#;
        let record: DumpRecord = serde_json::from_str(line).unwrap();
        assert_eq!(record.format_version, None);
        assert_eq!(
            record.extract_all_headers().unwrap(),
            vec![
                ("a".to_string(), Some(b"1".to_vec())),
                ("n".to_string(), Some(Vec::new())),
                ("a".to_string(), Some(b"2".to_vec())),
            ]
        );
    }

    #[test]
    fn a_format_2_line_reads_like_an_old_dump_without_the_version() {
        // What an older version does with a new dump: it ignores
        // `format_version` (and any other unknown field) and restores the
        // null headers as empty values, keeping every other header.
        let line = serde_json::to_string(&DumpRecord::from_message(&msg_with_nulls())).unwrap();
        let mut json: serde_json::Value = serde_json::from_str(&line).unwrap();
        let obj = json.as_object_mut().unwrap();
        obj.remove("format_version");
        obj.insert("some_future_field".into(), 1.into());
        let old: DumpRecord = serde_json::from_value(json).unwrap();
        assert_eq!(
            old.extract_all_headers().unwrap(),
            vec![
                ("n0".to_string(), Some(Vec::new())),
                ("a".to_string(), Some(b"1".to_vec())),
                ("n2".to_string(), Some(Vec::new())),
                ("bin".to_string(), Some(vec![0xff])),
                ("e".to_string(), Some(Vec::new())),
            ]
        );
    }

    #[test]
    fn consume_save_lines_still_leave_null_headers_out() {
        let json = jsonl_record(&msg_with_nulls());
        assert!(json.get("format_version").is_none());
        assert!(!json.to_string().contains("n0"), "{json}");
        assert_eq!(json["headers"], serde_json::json!({"a": "1", "e": ""}));
    }

    #[test]
    fn test_dump_record_invalid_base64_is_error() {
        let record = DumpRecord {
            format_version: None,
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp_ms: None,
            key_text: None,
            key_base64: None,
            payload_text: None,
            payload_base64: Some("not*base64".into()),
            headers: Vec::new(),
            headers_base64: Vec::new(),
            header_list: None,
        };
        assert!(record.extract_payload_bytes().is_err());
    }

    #[test]
    fn test_dump_record_json_serialization() {
        let record = DumpRecord {
            format_version: None,
            topic: "events".into(),
            partition: 2,
            offset: 10,
            timestamp_ms: Some(1000),
            key_text: Some("k1".into()),
            key_base64: None,
            payload_text: Some("hello".into()),
            payload_base64: None,
            headers: Vec::new(),
            headers_base64: Vec::new(),
            header_list: None,
        };

        let json_line = serde_json::to_string(&record).expect("Serialize to json line");
        let parsed: DumpRecord =
            serde_json::from_str(&json_line).expect("Deserialize from json line");
        assert_eq!(parsed, record);
    }
}
