//! SASL SCRAM-SHA-256 / SCRAM-SHA-512 client exchange (RFC 5802, RFC 7677)
//! for the native admin requests.

use std::num::NonZeroU32;

use ring::{digest, hmac, pbkdf2, rand::SecureRandom};

use crate::codec::base64;
use crate::error::{KafkaError, KafkaResult};

/// The hash a SCRAM mechanism uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramHash {
    Sha256,
    Sha512,
}

impl ScramHash {
    /// The SASL mechanism name sent in the handshake.
    pub fn mechanism(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    fn hmac(self) -> hmac::Algorithm {
        match self {
            Self::Sha256 => hmac::HMAC_SHA256,
            Self::Sha512 => hmac::HMAC_SHA512,
        }
    }

    fn digest(self) -> &'static digest::Algorithm {
        match self {
            Self::Sha256 => &digest::SHA256,
            Self::Sha512 => &digest::SHA512,
        }
    }

    fn pbkdf2(self) -> pbkdf2::Algorithm {
        match self {
            Self::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            Self::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }

    fn len(self) -> usize {
        self.digest().output_len()
    }

    /// The mechanism id in the SCRAM credential APIs.
    pub fn id(self) -> i8 {
        match self {
            Self::Sha256 => 1,
            Self::Sha512 => 2,
        }
    }

    pub fn from_id(id: i8) -> Option<Self> {
        match id {
            1 => Some(Self::Sha256),
            2 => Some(Self::Sha512),
            _ => None,
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().replace('_', "-").as_str() {
            "SCRAM-SHA-256" | "SHA-256" | "SHA256" => Some(Self::Sha256),
            "SCRAM-SHA-512" | "SHA-512" | "SHA512" => Some(Self::Sha512),
            _ => None,
        }
    }

    /// A fresh random salt and the salted password a broker stores for
    /// `password` (PBKDF2 with this hash).
    pub fn salt_password(self, password: &str, iterations: u32) -> KafkaResult<(Vec<u8>, Vec<u8>)> {
        let mut salt = vec![0u8; 32];
        ring::rand::SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| err("no random source for the salt"))?;
        let iterations = NonZeroU32::new(iterations).ok_or_else(|| err("zero iteration count"))?;
        let mut salted = vec![0u8; self.len()];
        pbkdf2::derive(
            self.pbkdf2(),
            iterations,
            &salt,
            password.as_bytes(),
            &mut salted,
        );
        Ok((salt, salted))
    }
}

/// Lowest server iteration count accepted (the Java client's minimum).
const MIN_ITERATIONS: u32 = 4096;
/// Highest server iteration count accepted, so a hostile broker cannot
/// stall the client in PBKDF2.
const MAX_ITERATIONS: u32 = 1_000_000;

fn err(msg: impl Into<String>) -> KafkaError {
    KafkaError::acl(format!("SCRAM authentication failed: {}", msg.into()))
}

/// One SCRAM exchange: `client_first`, then `client_final` with the
/// server's first message, then `verify_server_final`.
pub struct ScramClient {
    hash: ScramHash,
    password: String,
    client_first_bare: String,
    nonce: String,
    /// `ServerSignature`, known after `client_final`.
    server_signature: Option<Vec<u8>>,
}

impl ScramClient {
    pub fn new(hash: ScramHash, user: &str, password: &str) -> KafkaResult<Self> {
        let mut raw = [0u8; 24];
        ring::rand::SystemRandom::new()
            .fill(&mut raw)
            .map_err(|_| err("no random source for the nonce"))?;
        Ok(Self::with_nonce(
            hash,
            user,
            password,
            &base64::encode(&raw),
        ))
    }

    fn with_nonce(hash: ScramHash, user: &str, password: &str, nonce: &str) -> Self {
        let user = user.replace('=', "=3D").replace(',', "=2C");
        Self {
            hash,
            password: password.to_string(),
            client_first_bare: format!("n={user},r={nonce}"),
            nonce: nonce.to_string(),
            server_signature: None,
        }
    }

    pub fn client_first(&self) -> String {
        format!("n,,{}", self.client_first_bare)
    }

    /// Answers the server's first message with the client proof.
    pub fn client_final(&mut self, server_first: &str) -> KafkaResult<String> {
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for part in server_first.split(',') {
            match part.split_once('=') {
                Some(("r", v)) => nonce = Some(v),
                Some(("s", v)) => salt = Some(v),
                Some(("i", v)) => iterations = v.parse::<u32>().ok(),
                Some(("e", v)) => return Err(err(format!("the broker refused: {v}"))),
                // RFC 5802 5.1: an unknown mandatory extension must fail
                // the exchange.
                Some(("m", _)) => return Err(err("the server requires an unsupported extension")),
                _ => {}
            }
        }
        let (Some(nonce), Some(salt), Some(iterations)) = (nonce, salt, iterations) else {
            return Err(err(format!("unexpected server message '{server_first}'")));
        };
        if nonce.len() <= self.nonce.len() || !nonce.starts_with(&self.nonce) {
            return Err(err("the server nonce does not extend the client nonce"));
        }
        let salt = base64::decode(salt).map_err(|_| err("the salt is not base64"))?;
        if !(MIN_ITERATIONS..=MAX_ITERATIONS).contains(&iterations) {
            return Err(err(format!(
                "iteration count {iterations} outside {MIN_ITERATIONS}..={MAX_ITERATIONS}"
            )));
        }
        let iterations = NonZeroU32::new(iterations).ok_or_else(|| err("zero iteration count"))?;

        let mut salted = vec![0u8; self.hash.len()];
        pbkdf2::derive(
            self.hash.pbkdf2(),
            iterations,
            &salt,
            self.password.as_bytes(),
            &mut salted,
        );
        let salted_key = hmac::Key::new(self.hash.hmac(), &salted);
        let client_key = hmac::sign(&salted_key, b"Client Key");
        let stored_key = digest::digest(self.hash.digest(), client_key.as_ref());
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{},{server_first},{without_proof}", self.client_first_bare);
        let client_signature = hmac::sign(
            &hmac::Key::new(self.hash.hmac(), stored_key.as_ref()),
            auth_message.as_bytes(),
        );
        let proof: Vec<u8> = client_key
            .as_ref()
            .iter()
            .zip(client_signature.as_ref())
            .map(|(k, s)| k ^ s)
            .collect();
        let server_key = hmac::sign(&salted_key, b"Server Key");
        self.server_signature = Some(
            hmac::sign(
                &hmac::Key::new(self.hash.hmac(), server_key.as_ref()),
                auth_message.as_bytes(),
            )
            .as_ref()
            .to_vec(),
        );
        Ok(format!("{without_proof},p={}", base64::encode(&proof)))
    }

    /// Checks that the server proved it knows the password too.
    pub fn verify_server_final(&self, server_final: &str) -> KafkaResult<()> {
        if let Some(e) = server_final.strip_prefix("e=") {
            return Err(err(format!("the broker refused: {e}")));
        }
        let signature = server_final
            .split(',')
            .find_map(|p| p.strip_prefix("v="))
            .ok_or_else(|| err(format!("unexpected server message '{server_final}'")))?;
        let signature = base64::decode(signature).map_err(|_| err("bad server signature"))?;
        match &self.server_signature {
            Some(expected) if *expected == signature => Ok(()),
            _ => Err(err("the broker's signature does not match")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc7677_sha256_test_vector() {
        let mut client =
            ScramClient::with_nonce(ScramHash::Sha256, "user", "pencil", "rOprNGfwEbeRWgbNEkqO");
        assert_eq!(client.client_first(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                            s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        assert_eq!(
            client.client_final(server_first).unwrap(),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        client
            .verify_server_final("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
        assert!(client.verify_server_final("v=AAAA").is_err());
        assert!(client.verify_server_final("e=invalid-proof").is_err());
    }

    #[test]
    fn rejects_foreign_nonces_and_escapes_names() {
        let mut client = ScramClient::with_nonce(ScramHash::Sha512, "a=b,c", "p", "abc");
        assert_eq!(client.client_first(), "n,,n=a=3Db=2Cc,r=abc");
        assert!(client.client_final("r=xyz,s=QUJD,i=4096").is_err());
        assert!(client.client_final("e=unknown-user").is_err());
    }

    #[test]
    fn rejects_bad_iteration_counts() {
        let mut client = ScramClient::with_nonce(ScramHash::Sha256, "u", "p", "abc");
        assert!(client.client_final("r=abcdef,s=QUJD,i=4095").is_err());
        assert!(client.client_final("r=abcdef,s=QUJD,i=1000001").is_err());
        assert!(client.client_final("r=abcdef,s=QUJD,i=0").is_err());
        assert!(client.client_final("r=abcdef,s=QUJD,i=4096").is_ok());
    }

    #[test]
    fn rejects_unextended_nonce_and_mandatory_extensions() {
        let mut client = ScramClient::with_nonce(ScramHash::Sha256, "u", "p", "abc");
        assert!(client.client_final("r=abc,s=QUJD,i=4096").is_err());
        assert!(client.client_final("m=ext,r=abcdef,s=QUJD,i=4096").is_err());
    }
}
