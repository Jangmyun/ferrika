//! Schema-less Protobuf decoding, like `protoc --decode_raw`: shows a
//! message's fields by number and wire type. Length-delimited fields are
//! shown as nested messages when they parse as one, else as text or bytes.

use serde_json::{Map, Value};

use crate::error::{KafkaError, KafkaResult};

/// Nested messages deeper than this are shown as bytes.
const MAX_DEPTH: usize = 32;

fn err(msg: &str) -> KafkaError {
    KafkaError::Serialization(format!("Not a Protobuf message: {msg}"))
}

/// Decodes `bytes` as one Protobuf message into `{"<field>": value}`;
/// repeated fields become arrays.
pub fn decode_raw(bytes: &[u8]) -> KafkaResult<Value> {
    decode_message(bytes, 0)
}

/// Skips the Confluent Protobuf message-index list (a zig-zag varint count,
/// then that many indexes; a single 0 byte for the first message type).
pub fn skip_message_indexes(bytes: &[u8]) -> KafkaResult<&[u8]> {
    let mut pos = 0;
    let count = zigzag(read_varint(bytes, &mut pos)?);
    for _ in 0..count.max(0) {
        read_varint(bytes, &mut pos)?;
    }
    Ok(&bytes[pos..])
}

fn zigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ -((v & 1) as i64)
}

fn read_varint(bytes: &[u8], pos: &mut usize) -> KafkaResult<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*pos).ok_or_else(|| err("varint ends early"))?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(err("varint too long"))
}

fn decode_message(bytes: &[u8], depth: usize) -> KafkaResult<Value> {
    if depth > MAX_DEPTH {
        return Err(err("nested too deeply"));
    }
    let mut fields = Map::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let key = read_varint(bytes, &mut pos)?;
        let (number, wire) = (key >> 3, key & 7);
        if number == 0 {
            return Err(err("field number 0"));
        }
        let value = match wire {
            0 => Value::from(read_varint(bytes, &mut pos)?),
            1 => {
                let raw = bytes
                    .get(pos..pos + 8)
                    .ok_or_else(|| err("fixed64 ends early"))?;
                pos += 8;
                let mut a = [0u8; 8];
                a.copy_from_slice(raw);
                Value::from(u64::from_le_bytes(a))
            }
            2 => {
                let len = usize::try_from(read_varint(bytes, &mut pos)?)
                    .map_err(|_| err("length too large"))?;
                let raw = bytes
                    .get(pos..pos.checked_add(len).ok_or_else(|| err("length overflow"))?)
                    .ok_or_else(|| err("length-delimited field ends early"))?;
                pos += len;
                length_delimited(raw, depth)
            }
            5 => {
                let raw = bytes
                    .get(pos..pos + 4)
                    .ok_or_else(|| err("fixed32 ends early"))?;
                pos += 4;
                Value::from(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
            }
            other => return Err(err(&format!("unsupported wire type {other}"))),
        };
        let key = number.to_string();
        match fields.get_mut(&key) {
            Some(Value::Array(items)) => items.push(value),
            Some(existing) => {
                let first = existing.take();
                *existing = Value::Array(vec![first, value]);
            }
            None => {
                fields.insert(key, value);
            }
        }
    }
    Ok(Value::Object(fields))
}

/// Nested message if it parses as one (and is not plain text), else UTF-8
/// text, else hex bytes.
fn length_delimited(raw: &[u8], depth: usize) -> Value {
    let text = std::str::from_utf8(raw).ok();
    let printable = text.is_some_and(|t| t.chars().all(|c| !c.is_control() || c.is_whitespace()));
    if !raw.is_empty() && !printable {
        if let Ok(nested) = decode_message(raw, depth + 1) {
            return nested;
        }
    }
    match text {
        Some(t) if printable => Value::String(t.to_string()),
        _ => Value::String(crate::model::format::to_hex(raw)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_varints_strings_nested_and_repeated_fields() {
        // message { id: 150 (1), name: "hi" (2), child: { flag: 1 (1) } (3), tags: 1, 2 (4) }
        let bytes = [
            0x08, 0x96, 0x01, // 1: 150
            0x12, 0x02, b'h', b'i', // 2: "hi"
            0x1a, 0x02, 0x08, 0x01, // 3: {1: 1}
            0x20, 0x01, 0x20, 0x02, // 4: 1, 4: 2
            0x2d, 0x01, 0x00, 0x00, 0x00, // 5: fixed32 1
        ];
        let v = decode_raw(&bytes).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"1": 150, "2": "hi", "3": {"1": 1}, "4": [1, 2], "5": 1})
        );
        assert!(decode_raw(&[0x12, 0x05, b'a']).is_err());
        assert_eq!(
            skip_message_indexes(&[0x00, 0x08, 0x01]).unwrap(),
            &[0x08, 0x01]
        );
    }
}
