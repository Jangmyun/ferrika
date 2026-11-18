//! Standard Base64 (RFC 4648 section 4) without an external dependency.

use std::fmt;

/// Input that is not valid padded Base64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeError(&'static str);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for DecodeError {}

const BASE64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encodes `data` as standard Base64 with padding.
pub fn encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    let mut i = 0;

    while i < data.len() {
        let b0 = data[i];
        let b1 = if i + 1 < data.len() { data[i + 1] } else { 0 };
        let b2 = if i + 2 < data.len() { data[i + 2] } else { 0 };

        let n = ((b0 as u32) << 16) | ((b1 as u32) << 8) | (b2 as u32);

        out.push(BASE64_CHARS[((n >> 18) & 0x3f) as usize] as char);
        out.push(BASE64_CHARS[((n >> 12) & 0x3f) as usize] as char);

        if i + 1 < data.len() {
            out.push(BASE64_CHARS[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }

        if i + 2 < data.len() {
            out.push(BASE64_CHARS[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }

        i += 3;
    }

    out
}

/// Decodes standard Base64. Whitespace is ignored; padding is required.
pub fn decode(input: &str) -> Result<Vec<u8>, DecodeError> {
    let mut decode_table = [255u8; 256];
    for (i, &c) in BASE64_CHARS.iter().enumerate() {
        decode_table[c as usize] = i as u8;
    }

    let cleaned: Vec<u8> = input.bytes().filter(|b| !b.is_ascii_whitespace()).collect();

    if cleaned.is_empty() {
        return Ok(Vec::new());
    }

    if !cleaned.len().is_multiple_of(4) {
        return Err(DecodeError("Base64 string length is not a multiple of 4"));
    }

    let mut out = Vec::with_capacity(cleaned.len() / 4 * 3);
    for chunk in cleaned.chunks(4) {
        let c0 = decode_table[chunk[0] as usize];
        let c1 = decode_table[chunk[1] as usize];
        if c0 == 255 || c1 == 255 {
            return Err(DecodeError("Invalid Base64 character"));
        }

        let n0 = (c0 as u32) << 18 | (c1 as u32) << 12;
        out.push(((n0 >> 16) & 0xff) as u8);

        if chunk[2] != b'=' {
            let c2 = decode_table[chunk[2] as usize];
            if c2 == 255 {
                return Err(DecodeError("Invalid Base64 character"));
            }
            let n1 = n0 | ((c2 as u32) << 6);
            out.push(((n1 >> 8) & 0xff) as u8);

            if chunk[3] != b'=' {
                let c3 = decode_table[chunk[3] as usize];
                if c3 == 255 {
                    return Err(DecodeError("Invalid Base64 character"));
                }
                let n2 = n1 | (c3 as u32);
                out.push((n2 & 0xff) as u8);
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let test_cases: Vec<&[u8]> = vec![
            b"",
            b"f",
            b"fo",
            b"foo",
            b"foob",
            b"fooba",
            b"foobar",
            b"Hello, Ferrika Kafka Dump & Restore!",
            &[0x00, 0xff, 0xde, 0xad, 0xbe, 0xef],
        ];

        for original in test_cases {
            let encoded = encode(original);
            let decoded = decode(&encoded).expect("Valid base64 decode");
            assert_eq!(decoded, original);
        }
    }
}
