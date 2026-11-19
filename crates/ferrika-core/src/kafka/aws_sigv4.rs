//! AWS Signature Version 4 query presigning, enough for the AWS MSK IAM
//! SASL token: the client presigns
//! `GET https://kafka.<region>.amazonaws.com/?Action=kafka-cluster%3AConnect`
//! and sends the URL (base64url, no padding) as an OAUTHBEARER token.
//!
//! Written against `ring` (already used for TLS and SCRAM) instead of pulling
//! in the AWS SDK. The format follows the SigV4 documentation and
//! `aws-msk-iam-sasl-signer`.

use std::collections::BTreeMap;

use jiff::Timestamp;
use ring::{digest, hmac};

/// AWS access keys. `Debug` hides the secret and the session token.
#[derive(Clone, PartialEq, Eq)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    /// When temporary credentials stop working, if known.
    pub expiration: Option<Timestamp>,
}

impl AwsCredentials {
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
            expiration: None,
        }
    }
}

impl std::fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("expiration", &self.expiration)
            .finish()
    }
}

/// The SigV4 service name MSK IAM signs for.
pub const MSK_SERVICE: &str = "kafka-cluster";

/// How long a presigned MSK token is valid, in seconds (the signer's default).
pub const MSK_TOKEN_EXPIRES_SECS: u64 = 900;

/// SHA-256 of the empty string: the payload hash of a presigned GET.
const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// A request to presign with query parameters (`X-Amz-*`), signing only the
/// `host` header.
#[derive(Debug, Clone)]
pub struct PresignRequest<'a> {
    pub method: &'a str,
    pub host: &'a str,
    /// Absolute path, already URI-encoded (`/` for the root).
    pub path: &'a str,
    /// Query parameters besides the `X-Amz-*` ones, unencoded.
    pub query: Vec<(String, String)>,
    pub region: &'a str,
    pub service: &'a str,
    pub expires_secs: u64,
    /// Hex SHA-256 of the payload, or `UNSIGNED-PAYLOAD` (S3).
    pub payload_hash: &'a str,
}

/// Percent-encodes `text` the way SigV4 wants: everything but the RFC 3986
/// unreserved characters, upper-case hex.
pub fn uri_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Lower-case hex.
pub fn hex(bytes: &[u8]) -> String {
    crate::model::format::to_hex(bytes)
}

fn sha256_hex(data: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, data).as_ref())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
        .as_ref()
        .to_vec()
}

/// The SigV4 signing key for one day, region and service.
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Sorted, encoded `k=v&k=v` query string.
fn canonical_query(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The string to sign for a canonical request.
pub fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    )
}

/// Presigns `request`, returning the query string with the signature
/// (`...&X-Amz-Signature=<hex>`), parameters sorted.
pub fn presign_query(
    request: &PresignRequest<'_>,
    credentials: &AwsCredentials,
    now: Timestamp,
) -> String {
    let amz_date = now.strftime("%Y%m%dT%H%M%SZ").to_string();
    let date = now.strftime("%Y%m%d").to_string();
    let scope = format!("{date}/{}/{}/aws4_request", request.region, request.service);

    // Encoded key -> encoded value; BTreeMap sorts by encoded key, which is
    // what SigV4 requires.
    let mut params = BTreeMap::new();
    for (k, v) in &request.query {
        params.insert(uri_encode(k), uri_encode(v));
    }
    let mut amz = |k: &str, v: &str| {
        params.insert(k.to_string(), uri_encode(v));
    };
    amz("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
    amz(
        "X-Amz-Credential",
        &format!("{}/{scope}", credentials.access_key_id),
    );
    amz("X-Amz-Date", &amz_date);
    amz("X-Amz-Expires", &request.expires_secs.to_string());
    if let Some(token) = &credentials.session_token {
        amz("X-Amz-Security-Token", token);
    }
    amz("X-Amz-SignedHeaders", "host");

    let query = canonical_query(&params);
    let canonical_request = format!(
        "{}\n{}\n{query}\nhost:{}\n\nhost\n{}",
        request.method, request.path, request.host, request.payload_hash
    );
    let key = signing_key(
        &credentials.secret_access_key,
        &date,
        request.region,
        request.service,
    );
    let to_sign = string_to_sign(&amz_date, &scope, &canonical_request);
    let signature = hex(&hmac_sha256(&key, to_sign.as_bytes()));
    format!("{query}&X-Amz-Signature={signature}")
}

/// The presigned MSK IAM URL for `region`, `User-Agent` parameter included.
pub fn msk_presigned_url(credentials: &AwsCredentials, region: &str, now: Timestamp) -> String {
    let host = format!("kafka.{region}.amazonaws.com");
    let request = PresignRequest {
        method: "GET",
        host: &host,
        path: "/",
        query: vec![("Action".to_string(), "kafka-cluster:Connect".to_string())],
        region,
        service: MSK_SERVICE,
        expires_secs: MSK_TOKEN_EXPIRES_SECS,
        payload_hash: EMPTY_PAYLOAD_SHA256,
    };
    let query = presign_query(&request, credentials, now);
    let agent = uri_encode(&format!("ferrika/{}", env!("CARGO_PKG_VERSION")));
    format!("https://{host}/?{query}&User-Agent={agent}")
}

/// Base64url without padding (RFC 4648 section 5).
pub fn base64url(data: &[u8]) -> String {
    let mut out = crate::codec::base64::encode(data);
    out.retain(|c| c != '=');
    out.chars()
        .map(|c| match c {
            '+' => '-',
            '/' => '_',
            c => c,
        })
        .collect()
}

/// The MSK IAM OAUTHBEARER token: the presigned URL, base64url-encoded
/// without padding. It expires [`MSK_TOKEN_EXPIRES_SECS`] after `now`.
pub fn msk_token(credentials: &AwsCredentials, region: &str, now: Timestamp) -> String {
    base64url(msk_presigned_url(credentials, region, now).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> Timestamp {
        jiff::civil::DateTime::strptime("%Y%m%dT%H%M%SZ", s)
            .unwrap()
            .to_zoned_datetime(jiff::tz::TimeZone::UTC)
            .unwrap()
            .timestamp()
    }

    /// AWS SigV4 test suite, `get-vanilla-query`: the signing key, canonical
    /// request hash and signature of a header-signed `GET /`.
    #[test]
    fn sigv4_suite_get_vanilla_query() {
        let canonical = "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\n\
                         host;x-amz-date\n"
            .to_string()
            + EMPTY_PAYLOAD_SHA256;
        let scope = "20150830/us-east-1/service/aws4_request";
        let to_sign = string_to_sign("20150830T123600Z", scope, &canonical);
        assert_eq!(
            to_sign,
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n\
             bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
        );
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "service",
        );
        assert_eq!(
            hex(&hmac_sha256(&key, to_sign.as_bytes())),
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    /// The presigned URL example of the S3 SigV4 documentation
    /// ("Authenticating Requests: Using Query Parameters").
    #[test]
    fn s3_presign_documentation_example() {
        let credentials = AwsCredentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        let request = PresignRequest {
            method: "GET",
            host: "examplebucket.s3.amazonaws.com",
            path: "/test.txt",
            query: Vec::new(),
            region: "us-east-1",
            service: "s3",
            expires_secs: 86400,
            payload_hash: "UNSIGNED-PAYLOAD",
        };
        let query = presign_query(&request, &credentials, at("20130524T000000Z"));
        assert_eq!(
            query,
            "X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\
             &X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    #[test]
    fn msk_url_has_the_signer_layout() {
        let mut credentials = AwsCredentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        let url = msk_presigned_url(&credentials, "us-west-2", at("20240101T000000Z"));
        let prefix = "https://kafka.us-west-2.amazonaws.com/?Action=kafka-cluster%3AConnect\
                      &X-Amz-Algorithm=AWS4-HMAC-SHA256\
                      &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240101%2Fus-west-2%2Fkafka-cluster%2Faws4_request\
                      &X-Amz-Date=20240101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host\
                      &X-Amz-Signature=";
        assert!(url.starts_with(prefix), "{url}");
        let rest = &url[prefix.len()..];
        let (signature, agent) = rest.split_once('&').unwrap();
        assert_eq!(signature.len(), 64);
        assert!(signature.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(
            agent,
            format!("User-Agent=ferrika%2F{}", env!("CARGO_PKG_VERSION"))
        );

        // A session token is signed in, sorted before X-Amz-SignedHeaders.
        credentials.session_token = Some("tok/en=".to_string());
        let url = msk_presigned_url(&credentials, "us-west-2", at("20240101T000000Z"));
        assert!(
            url.contains(
                "&X-Amz-Expires=900&X-Amz-Security-Token=tok%2Fen%3D&X-Amz-SignedHeaders=host"
            ),
            "{url}"
        );
        // Deterministic for the same inputs.
        assert_eq!(
            url,
            msk_presigned_url(&credentials, "us-west-2", at("20240101T000000Z"))
        );
    }

    #[test]
    fn msk_token_is_unpadded_base64url_of_the_url() {
        let credentials = AwsCredentials::new("AKID", "secret");
        let now = at("20240101T000000Z");
        let token = msk_token(&credentials, "eu-central-1", now);
        assert!(!token.contains('=') && !token.contains('+') && !token.contains('/'));
        assert!(token.starts_with(&base64url(b"https://kafka.eu-central-1")[..20]));
    }

    #[test]
    fn base64url_matches_rfc4648_vectors() {
        let cases: [(&[u8], &str); 7] = [
            (b"", ""),
            (b"f", "Zg"),
            (b"fo", "Zm8"),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg"),
            (b"fooba", "Zm9vYmE"),
            (b"foobar", "Zm9vYmFy"),
        ];
        for (input, expected) in cases {
            assert_eq!(base64url(input), expected);
        }
        assert_eq!(base64url(&[0xfb, 0xff]), "-_8");
    }

    #[test]
    fn uri_encode_keeps_only_unreserved() {
        assert_eq!(uri_encode("a-Z_0.~"), "a-Z_0.~");
        assert_eq!(uri_encode("a/b c:d=é"), "a%2Fb%20c%3Ad%3D%C3%A9");
    }

    #[test]
    fn debug_hides_secrets() {
        let mut c = AwsCredentials::new("AKIDPUBLIC", "hunter2-secret");
        c.session_token = Some("hunter2-token".into());
        let shown = format!("{c:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("AKIDPUBLIC"), "{shown}");
    }
}
