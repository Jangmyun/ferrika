use std::sync::LazyLock;

use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Classification of detected PII or sensitive secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PiiType {
    /// Credit Card Number verified via Luhn algorithm.
    CreditCard,
    /// Email address (`user@domain.com`).
    EmailAddress,
    /// US Social Security Number (`###-##-####`).
    SocialSecurityNumber,
    /// JSON Web Token or Bearer Authorization token.
    BearerToken,
    /// AWS Access Key ID (`AKIA...`).
    AwsAccessKey,
    /// Sensitive credential field identified by JSON property key name.
    SensitiveKey,
    /// 주민등록번호 / 외국인등록번호 (`YYMMDD-GXXXXXX`).
    KoreanResidentNumber,
    /// 휴대전화 번호 (`010-1234-5678`).
    KoreanMobile,
    /// 사업자등록번호 (`XXX-XX-XXXXX`, checksum verified).
    KoreanBusinessNumber,
    /// 계좌번호 (under an account key, or a hyphenated bank format).
    BankAccount,
    /// A person's name, recognised by its JSON key (`name`, `customer`, `이름`…).
    PersonName,
}

impl PiiType {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::CreditCard => "Credit Card",
            Self::EmailAddress => "Email Address",
            Self::SocialSecurityNumber => "Social Security Number (SSN)",
            Self::BearerToken => "JWT / Bearer Token",
            Self::AwsAccessKey => "AWS Access Key",
            Self::SensitiveKey => "Sensitive Field Key",
            Self::KoreanResidentNumber => "Korean Resident Registration No.",
            Self::KoreanMobile => "Korean Mobile Phone",
            Self::KoreanBusinessNumber => "Korean Business Registration No.",
            Self::BankAccount => "Bank Account",
            Self::PersonName => "Person Name",
        }
    }
}

/// Strategy for obfuscating detected sensitive content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskStrategy {
    /// Completely redact with `"[REDACTED]"`.
    #[default]
    Redact,
    /// Retain leading/trailing characters (e.g. `4111-XXXX-XXXX-1111`, `j***@corp.com`).
    Partial,
}

/// Individual PII violation detected in a record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PiiFinding {
    /// Partition ID if scanning a Kafka message.
    pub partition: Option<i32>,
    /// Message offset if scanning a Kafka message.
    pub offset: Option<i64>,
    /// JSON path or locator (e.g. `$.user.billing.card_number`).
    pub field_path: String,
    /// Classification of the violation.
    pub pii_type: PiiType,
    /// Safe masked preview of the leaked value.
    pub masked_sample: String,
}

/// Aggregated report of a topic PII audit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PiiAuditReport {
    /// Name of the topic audited.
    pub topic: String,
    /// Total messages sampled and analyzed.
    pub messages_scanned: usize,
    /// Total PII/secret violations detected.
    pub total_violations: usize,
    /// Number of credit cards detected.
    pub credit_cards_found: usize,
    /// Number of emails detected.
    pub emails_found: usize,
    /// Number of SSNs detected.
    pub ssns_found: usize,
    /// Number of tokens/API keys detected.
    pub tokens_found: usize,
    /// Number of sensitive field keys detected.
    pub sensitive_keys_found: usize,
    /// Number of Korean resident/business numbers, mobile numbers and bank accounts.
    #[serde(default)]
    pub korean_pii_found: usize,
    /// Number of person names (values under name keys).
    #[serde(default)]
    pub names_found: usize,
    /// Clean compliance rate percentage (100.0% = completely clean).
    pub compliance_score_pct: f64,
    /// Detailed violation records.
    pub findings: Vec<PiiFinding>,
}

/// Checks whether a sequence of digits passes the Luhn checksum.
pub fn is_luhn_valid(number_str: &str) -> bool {
    let digits: Vec<u32> = number_str
        .chars()
        .filter(|c| c.is_ascii_digit())
        .filter_map(|c| c.to_digit(10))
        .collect();

    if digits.len() < 13 || digits.len() > 19 {
        return false;
    }

    let mut sum = 0;
    let mut double = false;

    for &digit in digits.iter().rev() {
        if double {
            let doubled = digit * 2;
            sum += if doubled > 9 { doubled - 9 } else { doubled };
        } else {
            sum += digit;
        }
        double = !double;
    }

    sum % 10 == 0
}

/// Checks if a string looks like a credit card number (with or without dashes/spaces).
pub fn detect_credit_card(input: &str) -> Option<String> {
    let cleaned: String = input
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '-' || *c == ' ')
        .collect();

    // Split words/tokens
    for part in input.split(|c: char| c.is_whitespace() || c == ',' || c == '"') {
        let digits_only: String = part.chars().filter(|c| c.is_ascii_digit()).collect();
        if (digits_only.len() >= 13 && digits_only.len() <= 19) && is_luhn_valid(&digits_only) {
            // Check known issuer prefixes (Visa: 4, MC: 51-55 or 22-27, Amex: 34/37, Disc: 6011/65)
            let starts_valid = digits_only.starts_with('4')
                || digits_only.starts_with("51")
                || digits_only.starts_with("52")
                || digits_only.starts_with("53")
                || digits_only.starts_with("54")
                || digits_only.starts_with("55")
                || digits_only.starts_with("34")
                || digits_only.starts_with("37")
                || digits_only.starts_with("6011")
                || digits_only.starts_with("65");

            if starts_valid {
                return Some(part.to_string());
            }
        }
    }

    let digits_only: String = cleaned.chars().filter(|c| c.is_ascii_digit()).collect();
    if (digits_only.len() >= 13 && digits_only.len() <= 19) && is_luhn_valid(&digits_only) {
        return Some(cleaned);
    }

    None
}

/// Checks if a string contains a standard email address format.
pub fn detect_email(input: &str) -> Option<String> {
    for word in input.split(|c: char| c.is_whitespace() || c == '"' || c == '<' || c == '>') {
        if let Some(at_idx) = word.find('@') {
            if at_idx > 0 && at_idx < word.len() - 1 {
                let user = &word[..at_idx];
                let domain = &word[at_idx + 1..];
                if domain.contains('.') && domain.len() >= 3 && !user.is_empty() {
                    return Some(word.to_string());
                }
            }
        }
    }
    None
}

/// Checks if a string contains an SSN in `###-##-####` format.
pub fn detect_ssn(input: &str) -> Option<String> {
    for word in input.split(|c: char| c.is_whitespace() || c == '"') {
        let parts: Vec<&str> = word.split('-').collect();
        if parts.len() == 3
            && parts[0].len() == 3
            && parts[0].chars().all(|c| c.is_ascii_digit())
            && parts[1].len() == 2
            && parts[1].chars().all(|c| c.is_ascii_digit())
            && parts[2].len() == 4
            && parts[2].chars().all(|c| c.is_ascii_digit())
        {
            // Avoid placeholder 000-00-0000
            if parts[0] != "000" && parts[1] != "00" && parts[2] != "0000" {
                return Some(word.to_string());
            }
        }
    }
    None
}

/// Checks if a string contains an authorization token or AWS key.
pub fn detect_secret_token(input: &str) -> Option<(PiiType, String)> {
    if let Some(idx) = input.find("AKIA") {
        // `get` returns None when idx+20 is not a char boundary (e.g. Korean text after "AKIA").
        if let Some(key) = input.get(idx..idx + 20) {
            if key.chars().all(|c| c.is_ascii_alphanumeric()) {
                return Some((PiiType::AwsAccessKey, key.to_string()));
            }
        }
    }

    if input.starts_with("Bearer ") && input.len() > 15 {
        return Some((PiiType::BearerToken, input.to_string()));
    }

    if input.starts_with("eyJ") && input.contains('.') {
        let parts: Vec<&str> = input.split('.').collect();
        if parts.len() == 3 {
            return Some((PiiType::BearerToken, input.to_string()));
        }
    }

    None
}

/// Determines whether a JSON object key indicates a sensitive field.
///
/// Keys are compared lowercased with `-` and `.` read as `_`. Long, unambiguous
/// words match anywhere in the key; short ones (`pwd`, `pass`) only as a
/// whole `_`-separated token, so `passport` and `compass` stay visible.
pub fn is_sensitive_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase().replace(['-', '.'], "_");
    const TOKENS: &[&str] = &[
        "pwd",
        "pass",
        "passwd",
        "authorization",
        "credential",
        "credentials",
    ];
    if lower.split('_').any(|t| TOKENS.contains(&t)) {
        return true;
    }
    lower.contains("password")
        || lower.contains("passwd")
        || lower.contains("passphrase")
        || lower.contains("credential")
        || lower.contains("access_key")
        || lower.contains("accesskey")
        || lower.contains("secret_key")
        || lower.contains("secret")
        || lower.contains("token")
        || lower.contains("ssn")
        || lower.contains("social_security")
        || lower.contains("cvv")
        || lower.contains("credit_card")
        || lower.contains("card_number")
        || lower.contains("cardnumber")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("auth_key")
        || lower.contains("private_key")
        || lower.contains("jumin")
        || lower.contains("resident_registration")
        || lower == "rrn"
        || key.contains("주민")
}

/// Whether a JSON key names a bank account (`account_no`, `acct`, `계좌`…).
pub fn is_account_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower == "account"
        || lower.contains("account_no")
        || lower.contains("accountno")
        || lower.contains("account_num")
        || lower.contains("accountnum")
        || lower.contains("bank_account")
        || lower.contains("acct")
        || key.contains("계좌")
}

/// Keys whose string value is a person's name, compared after lowercasing
/// and dropping `_`, `-`, `.` and spaces (so `firstName`, `first_name` and
/// `First-Name` all match). Exact matches only: compound keys such as
/// `topic_name`, `connector_name`, `file_name` or `host_name` name things,
/// not people.
const NAME_KEYS: &[&str] = &[
    "name",
    "fullname",
    "firstname",
    "lastname",
    "middlename",
    "givenname",
    "familyname",
    "surname",
    "personname",
    "realname",
    "legalname",
    "customer",
    "customername",
    "clientname",
    "holder",
    "holdername",
    "accountholder",
    "accountholdername",
    "cardholder",
    "cardholdername",
    "recipient",
    "recipientname",
    "recipients",
    "receivername",
    "sendername",
    "payer",
    "payername",
    "payee",
    "payeename",
    "beneficiary",
    "beneficiaryname",
    "contactname",
    "employeename",
    "membername",
    "patientname",
    "이름",
    "성명",
    "성함",
    "고객",
    "고객명",
    "고객이름",
    "예금주",
    "예금주명",
    "수취인",
    "수취인명",
    "수령인",
    "수령인명",
    "받는사람",
    "보내는사람",
    "송금인",
    "환자명",
    "회원명",
    "담당자",
    "담당자명",
];

/// Whether a JSON key names a person (`name`, `full_name`, `customer`,
/// `holder_name`, `이름`, `성명`, `고객명`, `예금주`…).
pub fn is_name_key(key: &str) -> bool {
    let normalized: String = key
        .chars()
        .filter(|c| !matches!(c, '_' | '-' | '.' | ' '))
        .flat_map(char::to_lowercase)
        .collect();
    NAME_KEYS.contains(&normalized.as_str())
}

fn is_hangul_syllable(c: char) -> bool {
    ('\u{AC00}'..='\u{D7A3}').contains(&c)
}

/// Masks a person's name word by word, keeping whitespace: Hangul words of
/// three or more syllables keep the first and last (`김민준` → `김*준`),
/// everything else keeps only the first character (`김민` → `김*`,
/// `John Smith` → `J*** S****`). A one-character word becomes `*`.
pub fn mask_person_name(name: &str, strategy: MaskStrategy) -> String {
    if strategy == MaskStrategy::Redact {
        return "[REDACTED_NAME]".to_string();
    }
    fn push_word(word: &mut Vec<char>, out: &mut String) {
        let n = word.len();
        let keep_last = n >= 3 && word.iter().all(|c| is_hangul_syllable(*c));
        for (i, c) in word.iter().enumerate() {
            let keep = n > 1 && (i == 0 || (keep_last && i == n - 1));
            out.push(if keep { *c } else { '*' });
        }
        word.clear();
    }
    let mut out = String::with_capacity(name.len());
    let mut word: Vec<char> = Vec::new();
    for c in name.chars() {
        if c.is_whitespace() {
            push_word(&mut word, &mut out);
            out.push(c);
        } else {
            word.push(c);
        }
    }
    push_word(&mut word, &mut out);
    out
}

/// Masks the strings under a name key: a string, or the strings of an array
/// (`"recipients": ["김민준", "이서연"]`). Objects and other values are
/// masked as usual (`"customer": {"name": …}` reaches `name` on its own).
fn mask_name_value(value: &mut Value, strategy: MaskStrategy, depth: usize) {
    match value {
        Value::String(s) => *s = mask_person_name(s, strategy),
        Value::Array(items) => {
            for item in items {
                match item {
                    Value::String(s) => *s = mask_person_name(s, strategy),
                    other => mask_json_at(other, strategy, depth),
                }
            }
        }
        other => mask_json_at(other, strategy, depth),
    }
}

/// A value that looks like an account number: 8–20 digits with optional
/// hyphens or spaces and nothing else.
fn looks_like_account(text: &str) -> bool {
    let digits = text.chars().filter(char::is_ascii_digit).count();
    (8..=20).contains(&digits)
        && text
            .chars()
            .all(|c| c.is_ascii_digit() || c == '-' || c == ' ')
}

/// Replaces every digit but the last four with `*`, keeping separators.
fn mask_digits_keep_last4(text: &str) -> String {
    let total = text.chars().filter(char::is_ascii_digit).count();
    let mut seen = 0;
    text.chars()
        .map(|c| {
            if c.is_ascii_digit() {
                seen += 1;
                if seen + 4 > total {
                    c
                } else {
                    '*'
                }
            } else {
                c
            }
        })
        .collect()
}

/// Whether `yymmdd` is a real date in the century the gender digit implies
/// (1, 2, 5, 6: 1900s; 3, 4, 7, 8: 2000s).
fn valid_rrn_date(yymmdd: &str, gender: u32) -> bool {
    let num = |range: std::ops::Range<usize>| {
        yymmdd
            .get(range)
            .and_then(|d| d.parse::<u32>().ok())
            .unwrap_or(0)
    };
    let (yy, mm, dd) = (num(0..2), num(2..4), num(4..6));
    let year = match gender {
        1 | 2 | 5 | 6 => 1900 + yy,
        3 | 4 | 7 | 8 => 2000 + yy,
        _ => return false,
    };
    // Nobody is born in the future.
    if i64::from(year)
        > i64::from(
            jiff::Timestamp::now()
                .to_zoned_datetime(jiff::tz::TimeZone::UTC)
                .year(),
        )
    {
        return false;
    }
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days = match mm {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days).contains(&dd)
}

/// The check digit of numbers issued before October 2020.
fn rrn_checksum_ok(digits: &[u32]) -> bool {
    const WEIGHTS: [u32; 12] = [2, 3, 4, 5, 6, 7, 8, 9, 2, 3, 4, 5];
    if digits.len() != 13 {
        return false;
    }
    let sum: u32 = digits.iter().zip(WEIGHTS).map(|(d, w)| d * w).sum();
    (11 - sum % 11) % 10 == digits[12]
}

/// 주민등록번호 / 외국인등록번호. With a hyphen a valid birth date and gender
/// digit suffice (numbers issued since October 2020 have no check digit);
/// a bare 13-digit number must also pass the old checksum, or be born in
/// 2020 or later, to keep timestamps and ids from matching.
pub fn is_korean_rrn(text: &str) -> bool {
    let hyphenated = text.len() == 14 && text.as_bytes().get(6) == Some(&b'-');
    let digits: Vec<u32> = text.chars().filter_map(|c| c.to_digit(10)).collect();
    if digits.len() != 13 || !(hyphenated || text.len() == 13) {
        return false;
    }
    let birth: String = text.chars().take(6).collect();
    let gender = digits[6];
    if !valid_rrn_date(&birth, gender) {
        return false;
    }
    hyphenated || rrn_checksum_ok(&digits) || (matches!(gender, 3 | 4 | 7 | 8) && digits[0] >= 2)
}

/// 사업자등록번호 check digit.
pub fn is_korean_business_number(text: &str) -> bool {
    const WEIGHTS: [u32; 9] = [1, 3, 7, 1, 3, 7, 1, 3, 5];
    let digits: Vec<u32> = text.chars().filter_map(|c| c.to_digit(10)).collect();
    if digits.len() != 10 {
        return false;
    }
    let sum: u32 = digits.iter().zip(WEIGHTS).map(|(d, w)| d * w).sum::<u32>() + digits[8] * 5 / 10;
    (10 - sum % 10) % 10 == digits[9]
}

/// Korean PII found in free text, in order: resident numbers, business
/// numbers, mobile numbers, then hyphenated bank account formats.
pub fn detect_korean_pii(input: &str) -> Vec<(PiiType, String)> {
    let mut found = Vec::new();
    for m in RRN_RE.find_iter(input) {
        if is_korean_rrn(m.as_str()) {
            found.push((PiiType::KoreanResidentNumber, m.as_str().to_string()));
        }
    }
    for m in BIZ_RE.find_iter(input) {
        if is_korean_business_number(m.as_str()) {
            found.push((PiiType::KoreanBusinessNumber, m.as_str().to_string()));
        }
    }
    for m in MOBILE_RE.find_iter(input) {
        found.push((PiiType::KoreanMobile, m.as_str().to_string()));
    }
    let masked = mask_korean(input, MaskStrategy::Partial, false);
    for m in BANK_RE.find_iter(&masked) {
        if is_bank_format(m.as_str()) {
            found.push((PiiType::BankAccount, m.as_str().to_string()));
        }
    }
    found
}

/// Hyphenated bank account formats have 11–14 digits (e.g. `110-123-456789`);
/// phone-shaped numbers starting with 0 (`02-1234-5678`) are not accounts.
fn is_bank_format(text: &str) -> bool {
    let digits = text.chars().filter(char::is_ascii_digit).count();
    let groups: Vec<&str> = text.split('-').collect();
    let phone_like = text.starts_with('0')
        && groups.len() == 3
        && (2..=3).contains(&groups[0].len())
        && (3..=4).contains(&groups[1].len())
        && groups[2].len() == 4;
    (11..=14).contains(&digits) && !phone_like
}

/// Masks Korean resident/business/mobile numbers (and, with `accounts`,
/// hyphenated bank account formats) in free text.
fn mask_korean(input: &str, strategy: MaskStrategy, accounts: bool) -> String {
    let text = RRN_RE.replace_all(input, |c: &Captures| {
        let m = &c[0];
        if !is_korean_rrn(m) {
            return m.to_string();
        }
        match strategy {
            MaskStrategy::Redact => "[REDACTED_RRN]".to_string(),
            MaskStrategy::Partial => {
                let birth: String = m.chars().take(6).collect();
                let gender = m.chars().filter(char::is_ascii_digit).nth(6).unwrap_or('*');
                format!("{birth}-{gender}******")
            }
        }
    });
    let text = BIZ_RE.replace_all(&text, |c: &Captures| {
        let m = &c[0];
        if !is_korean_business_number(m) {
            return m.to_string();
        }
        match strategy {
            MaskStrategy::Redact => "[REDACTED_BIZ_NO]".to_string(),
            MaskStrategy::Partial => format!("{}-**-*****", &m[..3]),
        }
    });
    let text = MOBILE_RE.replace_all(&text, |c: &Captures| match strategy {
        MaskStrategy::Redact => "[REDACTED_PHONE]".to_string(),
        MaskStrategy::Partial => {
            let digits: String = c[0].chars().filter(char::is_ascii_digit).collect();
            format!("{}-****-{}", &digits[..3], &digits[digits.len() - 4..])
        }
    });
    if !accounts {
        return text.into_owned();
    }
    let text = BANK_RE.replace_all(&text, |c: &Captures| {
        let m = &c[0];
        if !is_bank_format(m) {
            return m.to_string();
        }
        match strategy {
            MaskStrategy::Redact => "[REDACTED_ACCOUNT]".to_string(),
            MaskStrategy::Partial => mask_digits_keep_last4(m),
        }
    });
    text.into_owned()
}

/// Masks an email according to the specified strategy.
pub fn mask_email(email: &str, strategy: MaskStrategy) -> String {
    match strategy {
        MaskStrategy::Redact => "[REDACTED_EMAIL]".to_string(),
        MaskStrategy::Partial => {
            if let Some(at_idx) = email.find('@') {
                let user = &email[..at_idx];
                let domain = &email[at_idx + 1..];
                if user.len() <= 2 {
                    format!("*@{domain}")
                } else {
                    let first = user.chars().next().unwrap_or('*');
                    let last = user.chars().last().unwrap_or('*');
                    format!("{first}***{last}@{domain}")
                }
            } else {
                "[REDACTED_EMAIL]".to_string()
            }
        }
    }
}

/// Masks a credit card number according to the specified strategy.
pub fn mask_credit_card(card: &str, strategy: MaskStrategy) -> String {
    match strategy {
        MaskStrategy::Redact => "[REDACTED_CARD]".to_string(),
        MaskStrategy::Partial => {
            let digits: String = card.chars().filter(|c| c.is_ascii_digit()).collect();
            if digits.len() >= 4 {
                let last4 = &digits[digits.len() - 4..];
                format!("****-****-****-{last4}")
            } else {
                "****-****-****-****".to_string()
            }
        }
    }
}

/// Masks an SSN according to the specified strategy.
pub fn mask_ssn(_ssn: &str, strategy: MaskStrategy) -> String {
    match strategy {
        MaskStrategy::Redact => "[REDACTED_SSN]".to_string(),
        MaskStrategy::Partial => "***-**-****".to_string(),
    }
}

/// Recursively traverses a JSON value and masks all sensitive keys and detected PII values.
pub fn mask_json_value(value: &mut Value, strategy: MaskStrategy) {
    mask_json_at(value, strategy, 0);
}

/// How many times a JSON document encoded inside a JSON string is unpacked
/// and masked field by field; deeper levels are masked as free text.
const MAX_EMBEDDED_JSON_DEPTH: usize = 4;

/// A string that is itself a JSON object or array (a double-encoded
/// payload such as `"data":"{\"password\":\"…\"}"`).
fn embedded_json(text: &str) -> Option<Value> {
    let trimmed = text.trim_start();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return None;
    }
    serde_json::from_str::<Value>(text)
        .ok()
        .filter(|v| v.is_object() || v.is_array())
}

/// `key` with PII masked; a masked key that collides with one already in
/// `map` gets a `#n` suffix so no field is dropped.
fn masked_key(key: &str, strategy: MaskStrategy, map: &serde_json::Map<String, Value>) -> String {
    let masked = mask_text(key, strategy);
    if masked == key || !map.contains_key(&masked) {
        return masked;
    }
    (2..)
        .map(|n| format!("{masked}#{n}"))
        .find(|candidate| !map.contains_key(candidate))
        .unwrap_or(masked)
}

fn mask_json_at(value: &mut Value, strategy: MaskStrategy, depth: usize) {
    match value {
        Value::Object(map) => {
            let entries = std::mem::take(map);
            for (key, mut val) in entries {
                mask_json_field(&key, &mut val, strategy, depth);
                // Keys can carry PII too (`{"jane@x.com": …}`).
                let key = masked_key(&key, strategy, map);
                map.insert(key, val);
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                mask_json_at(item, strategy, depth);
            }
        }
        Value::String(s) => match embedded_json(s) {
            Some(mut inner) if depth < MAX_EMBEDDED_JSON_DEPTH => {
                mask_json_at(&mut inner, strategy, depth + 1);
                *s = inner.to_string();
            }
            _ => *s = mask_text(s, strategy),
        },
        // Card and resident numbers are often sent as JSON numbers.
        Value::Number(n) => {
            let text = n.to_string();
            let masked = mask_text(&text, strategy);
            if masked != text {
                *value = Value::String(masked);
            }
        }
        _ => {}
    }
}

/// Masks the value of one object field, judged by its (unmasked) key.
fn mask_json_field(key: &str, val: &mut Value, strategy: MaskStrategy, depth: usize) {
    let account = is_account_key(key)
        && match &*val {
            Value::String(s) => looks_like_account(s),
            Value::Number(n) => looks_like_account(&n.to_string()),
            _ => false,
        };
    if account {
        let text = match &*val {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        *val = Value::String(match strategy {
            MaskStrategy::Redact => "[REDACTED_ACCOUNT]".to_string(),
            MaskStrategy::Partial => mask_digits_keep_last4(&text),
        });
    } else if is_sensitive_key(key) {
        // Credentials and identifiers under a sensitive key are hidden
        // whole: even a two-character prefix of a password or SSN leaks.
        *val = match strategy {
            MaskStrategy::Redact => Value::String("[REDACTED]".to_string()),
            MaskStrategy::Partial => Value::String("****".to_string()),
        };
    } else if is_name_key(key) {
        mask_name_value(val, strategy, depth);
    } else {
        mask_json_at(val, strategy, depth);
    }
}

static CARD_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:\d[ -]?){12,18}\d\b").expect("valid card pattern"));
static EMAIL_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Unicode letters/digits, Hangul included, are allowed in the user part and the
    // domain labels (`홍길동@회사.kr`, `user@한글.kr`). The user part also takes Hangul
    // stuck in front of it ("문의foo@bar.com" is masked whole): over-masking beats a leak.
    // The TLD stays ASCII so a trailing Korean particle ("a@b.com입니다") is not swallowed.
    Regex::new(r"[\p{L}\p{N}._%+-]+@[\p{L}\p{N}-]+(?:\.[\p{L}\p{N}-]+)*\.[A-Za-z]{2,}")
        .expect("valid email pattern")
});
static SSN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(\d{3})-(\d{2})-(\d{4})\b").expect("valid SSN pattern"));
// `(?-u:\b)`: an ASCII word boundary, so "전화010-1234-5678" still matches
// (Hangul counts as a word character for the Unicode `\b`).
static RRN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?-u:\b)\d{6}-?[1-8]\d{6}(?-u:\b)").expect("valid resident number pattern")
});
static BIZ_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?-u:\b)\d{3}-\d{2}-\d{5}(?-u:\b)").expect("valid business number pattern")
});
static MOBILE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?-u:\b)01[016789]-?\d{3,4}-?\d{4}(?-u:\b)").expect("valid mobile pattern")
});
static BANK_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?-u:\b)\d{2,6}(?:-\d{2,7}){2,3}(?-u:\b)").expect("valid bank account pattern")
});
static SECRET_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"AKIA[0-9A-Z]{16}|Bearer\s+[A-Za-z0-9._~+/=-]{8,}|eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+|\bgh[pousr]_[A-Za-z0-9]{20,}|\b[sr]k_(?:live|test)_[A-Za-z0-9]{8,}|\bxox[abposr]-[A-Za-z0-9-]{8,}",
    )
    .expect("valid secret pattern")
});

static PEM_BEGIN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"-----BEGIN [A-Z ]*PRIVATE KEY-----").expect("valid PEM begin pattern")
});
static PEM_END_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"-----END [A-Z ]*PRIVATE KEY-----").expect("valid PEM end pattern")
});

/// Replaces every PEM private key (`BEGIN` to `END`, or just the `BEGIN` line
/// when no `END` follows) with `[REDACTED_SECRET]`. Done by hand rather than
/// with one lazy `BEGIN[\s\S]*?END` regex: on text with many `BEGIN` lines
/// and no `END`, that regex rescans to the end of the text for each of them
/// (quadratic).
fn redact_private_keys(input: &str) -> std::borrow::Cow<'_, str> {
    let Some(last_end) = PEM_END_RE.find_iter(input).last().map(|m| m.start()) else {
        return PEM_BEGIN_RE.replace_all(input, "[REDACTED_SECRET]");
    };
    let mut out = String::with_capacity(input.len());
    let mut pos = 0;
    while let Some(begin) = PEM_BEGIN_RE.find_at(input, pos) {
        out.push_str(&input[pos..begin.start()]);
        // An END can follow this BEGIN only when the last one starts after it.
        let end = if last_end >= begin.end() {
            PEM_END_RE.find_at(input, begin.end()).map(|m| m.end())
        } else {
            None
        };
        out.push_str("[REDACTED_SECRET]");
        pos = end.unwrap_or(begin.end());
    }
    out.push_str(&input[pos..]);
    std::borrow::Cow::Owned(out)
}

// `Basic <token>`: only a token shaped like real credentials is a secret, so
// prose such as "Basic configuration" stays readable.
static BASIC_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\bBasic\s+([A-Za-z0-9+/]{4,}={0,2})").expect("valid basic auth pattern")
});

/// Whether `token` (after `Basic `) looks like base64 `user:password`: padded
/// to a multiple of 4, or decoding to text with a `:`.
fn looks_like_basic_credentials(token: &str) -> bool {
    if !token.len().is_multiple_of(4) {
        return false;
    }
    if token.ends_with('=') {
        return true;
    }
    crate::codec::base64::decode(token).is_ok_and(|raw| raw.contains(&b':'))
}

/// Masks every card number, email address, SSN and secret token in free
/// text, wherever it appears (plain-text payloads as well as JSON strings).
/// Unlike the `detect_*` helpers, this replaces all occurrences, not the first.
pub fn mask_text(input: &str, strategy: MaskStrategy) -> String {
    let text = SECRET_RE.replace_all(input, "[REDACTED_SECRET]");
    let text = redact_private_keys(&text);
    let text = BASIC_RE.replace_all(&text, |c: &Captures| {
        if looks_like_basic_credentials(&c[1]) {
            "[REDACTED_SECRET]".to_string()
        } else {
            c[0].to_string()
        }
    });
    // Korean numbers first: a hyphenated resident number is also 13 digits
    // the card pattern would look at. Bank formats are masked last.
    let text = mask_korean(&text, strategy, false);
    let text = CARD_RE.replace_all(&text, |c: &Captures| {
        let candidate = &c[0];
        let digits: String = candidate.chars().filter(char::is_ascii_digit).collect();
        if (13..=19).contains(&digits.len()) && is_luhn_valid(&digits) {
            mask_credit_card(candidate, strategy)
        } else {
            candidate.to_string()
        }
    });
    let text = EMAIL_RE.replace_all(&text, |c: &Captures| mask_email(&c[0], strategy));
    let text = SSN_RE.replace_all(&text, |c: &Captures| {
        // Placeholder numbers (000-xx-xxxx etc.) are not real SSNs.
        if &c[1] == "000" || &c[2] == "00" || &c[3] == "0000" {
            c[0].to_string()
        } else {
            mask_ssn(&c[0], strategy)
        }
    });
    mask_korean(&text, strategy, true)
}

/// Masks one payload: JSON is masked field by field (sensitive keys hidden
/// whole), anything else as free text. A leading byte order mark and
/// several JSON documents on separate lines still count as JSON: as free
/// text, a `"password"` field would only be masked if its value looked
/// like a known pattern.
pub fn mask_payload(payload: &str, strategy: MaskStrategy) -> String {
    let body = payload.strip_prefix('\u{FEFF}').unwrap_or(payload);
    if let Ok(mut json) = serde_json::from_str::<Value>(body) {
        mask_json_value(&mut json, strategy);
        return json.to_string();
    }
    let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
    if lines.len() > 1 {
        let parsed: Option<Vec<Value>> = lines
            .iter()
            .map(|line| serde_json::from_str::<Value>(line).ok())
            .collect();
        if let Some(mut docs) = parsed {
            for doc in &mut docs {
                mask_json_value(doc, strategy);
            }
            return docs
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n");
        }
    }
    mask_text(payload, strategy)
}

/// Masks one record field for display: UTF-8 text through [`mask_payload`],
/// anything else hidden whole, since it cannot be scanned. That includes
/// the Confluent wire format (magic byte 0, a schema id, then Avro,
/// Protobuf or JSON Schema bytes) when it was not decoded: short records
/// are often valid UTF-8, and scanned as text their names and passwords
/// would pass, since only field names say what they are.
pub fn mask_bytes(bytes: &[u8], strategy: MaskStrategy) -> String {
    if bytes.len() >= 5 && bytes[0] == 0 {
        return format!("[schema-encoded {} bytes hidden]", bytes.len());
    }
    match std::str::from_utf8(bytes) {
        Ok(text) => mask_payload(text, strategy),
        Err(_) => format!("[binary {} bytes hidden]", bytes.len()),
    }
}

/// Masks one record header value for display, judged by the header name the
/// way JSON fields are judged by their key: credentials (`x-api-key`,
/// `authorization`, `password`…) are hidden whole, names (`customer_name`)
/// and account numbers partially, anything else through [`mask_bytes`].
pub fn mask_header_value(name: &str, value: &[u8], strategy: MaskStrategy) -> String {
    let Ok(text) = std::str::from_utf8(value) else {
        return mask_bytes(value, strategy);
    };
    // Header names are usually kebab-case: `x-api-key` should match `api_key`.
    let snake = name.replace('-', "_");
    let lower = snake.to_ascii_lowercase();
    if is_sensitive_key(&snake)
        || matches!(
            lower.as_str(),
            "authorization" | "proxy_authorization" | "cookie" | "set_cookie"
        )
    {
        return match strategy {
            MaskStrategy::Redact => "[REDACTED]".to_string(),
            MaskStrategy::Partial => "****".to_string(),
        };
    }
    if is_account_key(&snake) && looks_like_account(text) {
        return match strategy {
            MaskStrategy::Redact => "[REDACTED_ACCOUNT]".to_string(),
            MaskStrategy::Partial => mask_digits_keep_last4(text),
        };
    }
    // Custom headers often carry an `x-` prefix (`X-Customer-Name`).
    if is_name_key(name) || lower.strip_prefix("x_").is_some_and(is_name_key) {
        return mask_person_name(text, strategy);
    }
    mask_payload(text, strategy)
}

/// One record with every field masked for display.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaskedRecord {
    pub key: Option<String>,
    pub payload: Option<String>,
    pub headers: Vec<(String, String)>,
}

/// Masks a whole record by one rule: key and payload through [`mask_bytes`]
/// (binary hidden whole), each header value through [`mask_header_value`]
/// (judged by its name). Callers that display a record should use this so the
/// CLI, the UI, the DLQ and MCP cannot drift apart.
pub fn mask_record<'a>(
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
    headers: impl IntoIterator<Item = (&'a str, &'a [u8])>,
    strategy: MaskStrategy,
) -> MaskedRecord {
    MaskedRecord {
        key: key.map(|b| mask_bytes(b, strategy)),
        payload: payload.map(|b| mask_bytes(b, strategy)),
        headers: headers
            .into_iter()
            .map(|(k, v)| (k.to_string(), mask_header_value(k, v, strategy)))
            .collect(),
    }
}

/// The first `max_chars` characters of `text`, with `...` when it was cut.
/// Cuts on character boundaries, so Korean text never splits a character.
pub fn ellipsize(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((at, _)) => format!("{}...", &text[..at]),
        None => text.to_string(),
    }
}

/// Scans a JSON value for PII violations and records detailed findings.
pub fn scan_json_for_pii(
    value: &Value,
    current_path: &str,
    partition: Option<i32>,
    offset: Option<i64>,
    findings: &mut Vec<PiiFinding>,
) {
    match value {
        Value::Object(map) => {
            for (key, val) in map {
                // A key can itself be PII (an email as a map key), and the
                // audit report prints the path: mask it.
                let shown_key = mask_text(key, MaskStrategy::Redact);
                let next_path = if current_path == "$" {
                    format!("$.{shown_key}")
                } else {
                    format!("{current_path}.{shown_key}")
                };

                if is_account_key(key)
                    && val
                        .as_str()
                        .map(looks_like_account)
                        .unwrap_or_else(|| val.is_number())
                {
                    findings.push(PiiFinding {
                        partition,
                        offset,
                        field_path: next_path.clone(),
                        pii_type: PiiType::BankAccount,
                        masked_sample: mask_digits_keep_last4(
                            &val.as_str().map_or_else(|| val.to_string(), str::to_string),
                        ),
                    });
                    continue;
                }
                if is_sensitive_key(key) {
                    // Hidden whole, as in `mask_json_field`: even the last
                    // four characters of a password or token leak.
                    let sample = match val {
                        Value::String(_) => "[REDACTED_SECRET]".to_string(),
                        _ => "[NON_STRING_SECRET]".to_string(),
                    };
                    findings.push(PiiFinding {
                        partition,
                        offset,
                        field_path: next_path.clone(),
                        pii_type: PiiType::SensitiveKey,
                        masked_sample: sample,
                    });
                }
                if is_name_key(key) {
                    let names: Vec<(String, &str)> = match val {
                        Value::String(s) => vec![(next_path.clone(), s.as_str())],
                        Value::Array(items) => items
                            .iter()
                            .enumerate()
                            .filter_map(|(i, v)| {
                                v.as_str().map(|s| (format!("{next_path}[{i}]"), s))
                            })
                            .collect(),
                        _ => Vec::new(),
                    };
                    for (field_path, name) in names {
                        if name.trim().is_empty() {
                            continue;
                        }
                        findings.push(PiiFinding {
                            partition,
                            offset,
                            field_path,
                            pii_type: PiiType::PersonName,
                            masked_sample: mask_person_name(name, MaskStrategy::Partial),
                        });
                    }
                }

                scan_json_for_pii(val, &next_path, partition, offset, findings);
            }
        }
        Value::Array(arr) => {
            for (idx, item) in arr.iter().enumerate() {
                let next_path = format!("{current_path}[{idx}]");
                scan_json_for_pii(item, &next_path, partition, offset, findings);
            }
        }
        Value::String(s) => {
            if let Some(card) = detect_credit_card(s) {
                findings.push(PiiFinding {
                    partition,
                    offset,
                    field_path: current_path.to_string(),
                    pii_type: PiiType::CreditCard,
                    masked_sample: mask_credit_card(&card, MaskStrategy::Partial),
                });
            }
            if let Some(email) = detect_email(s) {
                findings.push(PiiFinding {
                    partition,
                    offset,
                    field_path: current_path.to_string(),
                    pii_type: PiiType::EmailAddress,
                    masked_sample: mask_email(&email, MaskStrategy::Partial),
                });
            }
            if let Some(ssn) = detect_ssn(s) {
                findings.push(PiiFinding {
                    partition,
                    offset,
                    field_path: current_path.to_string(),
                    pii_type: PiiType::SocialSecurityNumber,
                    masked_sample: mask_ssn(&ssn, MaskStrategy::Partial),
                });
            }
            for (kind, found) in detect_korean_pii(s) {
                let masked_sample = match kind {
                    PiiType::BankAccount => mask_digits_keep_last4(&found),
                    _ => mask_korean(&found, MaskStrategy::Partial, true),
                };
                findings.push(PiiFinding {
                    partition,
                    offset,
                    field_path: current_path.to_string(),
                    pii_type: kind,
                    masked_sample,
                });
            }
            if let Some((kind, token)) = detect_secret_token(s) {
                let masked = match kind {
                    PiiType::AwsAccessKey => format!("{}****", &token[..4]),
                    _ => "Bearer eyJ...[MASKED]".to_string(),
                };
                findings.push(PiiFinding {
                    partition,
                    offset,
                    field_path: current_path.to_string(),
                    pii_type: kind,
                    masked_sample: masked,
                });
            }
        }
        _ => {}
    }
}

/// Builds an aggregated PII audit report from collected findings.
pub fn build_audit_report(
    topic: &str,
    messages_scanned: usize,
    findings: Vec<PiiFinding>,
) -> PiiAuditReport {
    let mut credit_cards_found = 0;
    let mut emails_found = 0;
    let mut ssns_found = 0;
    let mut tokens_found = 0;
    let mut sensitive_keys_found = 0;
    let mut korean_pii_found = 0;
    let mut names_found = 0;

    for f in &findings {
        match f.pii_type {
            PiiType::CreditCard => credit_cards_found += 1,
            PiiType::EmailAddress => emails_found += 1,
            PiiType::SocialSecurityNumber => ssns_found += 1,
            PiiType::BearerToken | PiiType::AwsAccessKey => tokens_found += 1,
            PiiType::SensitiveKey => sensitive_keys_found += 1,
            PiiType::KoreanResidentNumber
            | PiiType::KoreanMobile
            | PiiType::KoreanBusinessNumber
            | PiiType::BankAccount => korean_pii_found += 1,
            PiiType::PersonName => names_found += 1,
        }
    }

    let total_violations = findings.len();
    let compliance_score_pct = if messages_scanned == 0 {
        100.0
    } else {
        // Score by messages with at least one violation, not by violation count.
        let violating_messages = findings
            .iter()
            .map(|f| (f.partition, f.offset))
            .collect::<std::collections::HashSet<_>>()
            .len();
        let clean_messages = messages_scanned.saturating_sub(violating_messages);
        (clean_messages as f64 / messages_scanned as f64) * 100.0
    };

    PiiAuditReport {
        topic: topic.to_string(),
        messages_scanned,
        total_violations,
        credit_cards_found,
        emails_found,
        ssns_found,
        tokens_found,
        sensitive_keys_found,
        korean_pii_found,
        names_found,
        compliance_score_pct,
        findings,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn aws_key_detection_survives_multibyte_text() {
        // Byte idx+20 falls inside a Hangul character: must not panic.
        assert_eq!(detect_secret_token("AKIA가나다라마바사아자"), None);
        assert_eq!(detect_secret_token("메모 AKIA가나다라마바사아자차"), None);
        assert_eq!(detect_secret_token("AKIA"), None);
        let key = "AKIAABCDEFGHIJKLMNOP";
        assert_eq!(
            detect_secret_token(&format!("메모 {key}")),
            Some((PiiType::AwsAccessKey, key.to_string()))
        );
    }

    #[test]
    fn compliance_score_counts_violating_messages_not_violations() {
        let finding = |offset, path: &str| PiiFinding {
            partition: Some(0),
            offset: Some(offset),
            field_path: path.to_string(),
            pii_type: PiiType::EmailAddress,
            masked_sample: "x".to_string(),
        };
        // One message of five has four PII fields.
        let findings = vec![
            finding(4, "$.a"),
            finding(4, "$.b"),
            finding(4, "$.c"),
            finding(4, "$.d"),
        ];
        let report = build_audit_report("t", 5, findings);
        assert_eq!(report.total_violations, 4);
        assert!((report.compliance_score_pct - 80.0).abs() < f64::EPSILON);
        // Same offset on another partition is another message.
        let mut other = finding(4, "$.a");
        other.partition = Some(1);
        let report = build_audit_report("t", 5, vec![finding(4, "$.a"), other]);
        assert!((report.compliance_score_pct - 60.0).abs() < f64::EPSILON);
    }

    #[test]
    fn mask_text_masks_non_ascii_emails_but_not_plain_at_signs() {
        assert_eq!(
            mask_text("mail ünï@çödé.com now", MaskStrategy::Redact),
            "mail [REDACTED_EMAIL] now"
        );
        assert_eq!(
            mask_text("alice@example.com", MaskStrategy::Redact),
            "[REDACTED_EMAIL]"
        );
        // Hangul in the address is masked; a particle after the TLD is kept.
        for (input, want) in [
            ("홍길동@회사.kr", "[REDACTED_EMAIL]"),
            ("user@한글.kr", "[REDACTED_EMAIL]"),
            ("a@b.com입니다", "[REDACTED_EMAIL]입니다"),
            ("문의는 a@b.com 으로", "문의는 [REDACTED_EMAIL] 으로"),
            // Hangul stuck to the address: masked whole, the safe side.
            ("문의는a@b.com입니다", "[REDACTED_EMAIL]입니다"),
        ] {
            assert_eq!(mask_text(input, MaskStrategy::Redact), want, "{input}");
        }
        for plain in [
            "meet @ 5pm",
            "@handle only",
            "ratio 10@3 today",
            "user@localhost",
            "see v1.2@3.45",
            "가@나",
        ] {
            assert_eq!(mask_text(plain, MaskStrategy::Redact), plain, "{plain}");
        }
    }

    use super::*;

    #[test]
    fn basic_auth_masks_credentials_not_prose() {
        for v in [
            "Basic dXNlcjpwYXNzd29yZA==",
            "Basic dXNlcg==",
            "Basic dXNlcjpwYXNz",
        ] {
            assert_eq!(
                mask_text(v, MaskStrategy::Redact),
                "[REDACTED_SECRET]",
                "{v}"
            );
        }
        for plain in [
            "Basic configuration",
            "Basic authentication required",
            "see Basic configuration options",
        ] {
            assert_eq!(mask_text(plain, MaskStrategy::Redact), plain, "{plain}");
        }
    }

    #[test]
    fn dotted_keys_are_sensitive_like_snake_case() {
        for k in ["api.key", "ssl.key.password", "access.key", "private.key"] {
            assert!(is_sensitive_key(k), "{k}");
        }
        assert!(!is_sensitive_key("connector.class"));
    }

    #[test]
    fn schema_encoded_records_are_hidden_whole() {
        // Avro: magic 0, schema id 1, then "Kim Minjun" and "hunter2".
        let mut avro = vec![0u8, 0, 0, 0, 1, 0x14];
        avro.extend_from_slice(b"Kim Minjun");
        avro.push(0x0e);
        avro.extend_from_slice(b"hunter2");
        assert!(
            std::str::from_utf8(&avro).is_ok(),
            "the case is valid UTF-8"
        );
        let shown = mask_bytes(&avro, MaskStrategy::Redact);
        assert!(
            !shown.contains("hunter2") && !shown.contains("Minjun"),
            "{shown}"
        );
        assert!(shown.contains("schema-encoded"), "{shown}");
        // JSON Schema wire format too.
        let json_schema = b"\0\0\0\0\x01{\"password\":\"hunter2\"}";
        assert!(!mask_bytes(json_schema, MaskStrategy::Redact).contains("hunter2"));
    }

    #[test]
    fn json_with_a_bom_or_several_lines_is_masked_by_field() {
        let bom = "\u{FEFF}{\"password\":\"hunter2\"}";
        assert!(!mask_payload(bom, MaskStrategy::Redact).contains("hunter2"));
        let lines = "{\"password\":\"a1b2c3\"}\n{\"password\":\"d4e5f6\"}";
        let masked = mask_payload(lines, MaskStrategy::Redact);
        assert!(
            !masked.contains("a1b2c3") && !masked.contains("d4e5f6"),
            "{masked}"
        );
        assert_eq!(masked.lines().count(), 2);
    }

    #[test]
    fn set_cookie_headers_are_hidden() {
        let shown = mask_header_value("Set-Cookie", b"session=abc123", MaskStrategy::Redact);
        assert!(!shown.contains("abc123"), "{shown}");
    }

    #[test]
    fn mask_payload_keeps_the_key_order_of_the_record() {
        let payload = r#"{"zeta":1,"alpha":{"y":2,"b":3},"mid":"x"}"#;
        assert_eq!(mask_payload(payload, MaskStrategy::Partial), payload);
    }

    #[test]
    fn mask_record_judges_headers_by_name_and_hides_binary() {
        let r = mask_record(
            Some(&[0xff, 0xfe]),
            Some(b"hello"),
            [
                ("authorization", b"Bearer abc".as_slice()),
                ("x-api-key", b"secret".as_slice()),
                ("trace", b"t-1".as_slice()),
            ],
            MaskStrategy::Partial,
        );
        assert_eq!(r.key.as_deref(), Some("[binary 2 bytes hidden]"));
        assert_eq!(r.headers[0].1, "****");
        assert_eq!(r.headers[1].1, "****");
        assert_eq!(r.headers[2].1, "t-1");
    }

    #[test]
    fn korean_resident_numbers() {
        // Hyphenated: a valid date and gender digit are enough.
        assert!(is_korean_rrn("900101-1234567"));
        assert!(!is_korean_rrn("010229-3234567")); // 2001 is not a leap year
        assert!(is_korean_rrn("000229-3234567")); // 2000 is a leap year
        assert!(is_korean_rrn("850315-5123456")); // foreigner, 1900s
        assert!(!is_korean_rrn("901301-1234567")); // month 13
        assert!(!is_korean_rrn("900132-1234567")); // day 32
        assert!(!is_korean_rrn("900101-9234567")); // gender digit 9
                                                   // Bare: needs the old checksum, or a 2020+ birth year.
        assert!(is_korean_rrn("9001011234568"));
        assert!(!is_korean_rrn("9001011234567"));
        assert!(is_korean_rrn("2001014123456"));
        // A millisecond timestamp is not a resident number.
        assert!(!is_korean_rrn("1726921800120"));

        let masked = mask_text("주민번호:900101-1234567 끝", MaskStrategy::Partial);
        assert_eq!(masked, "주민번호:900101-1****** 끝");
        assert_eq!(
            mask_text("rrn 900101-1234567", MaskStrategy::Redact),
            "rrn [REDACTED_RRN]"
        );
        assert_eq!(
            mask_text("ts 1726921800120", MaskStrategy::Partial),
            "ts 1726921800120"
        );
    }

    #[test]
    fn korean_mobile_numbers() {
        assert_eq!(
            mask_text("연락처 010-1234-5678", MaskStrategy::Partial),
            "연락처 010-****-5678"
        );
        assert_eq!(
            mask_text("전화01198765432", MaskStrategy::Partial),
            "전화011-****-5432"
        );
        // Not a mobile prefix, and part of a longer number.
        assert_eq!(
            mask_text("020-1234-5678", MaskStrategy::Partial),
            "020-1234-5678"
        );
        assert_eq!(
            mask_text("id 9010123456789", MaskStrategy::Partial),
            "id 9010123456789"
        );
    }

    #[test]
    fn korean_business_numbers() {
        assert!(is_korean_business_number("123-45-67891"));
        assert!(!is_korean_business_number("123-45-67890"));
        assert_eq!(
            mask_text("사업자 123-45-67891", MaskStrategy::Partial),
            "사업자 123-**-*****"
        );
        // A failing checksum is left alone.
        assert_eq!(
            mask_text("ref 123-45-67890", MaskStrategy::Partial),
            "ref 123-45-67890"
        );
    }

    #[test]
    fn bank_accounts_only_under_account_keys_or_bank_formats() {
        let masked = mask_payload(
            r#"{"account_no":"1002123456789","계좌":"110-123-456789","account":"acme-corp","order_id":"1002123456789"}"#,
            MaskStrategy::Partial,
        );
        let v: Value = serde_json::from_str(&masked).unwrap();
        assert_eq!(v["account_no"], "*********6789");
        assert_eq!(v["계좌"], "***-***-**6789");
        assert_eq!(v["account"], "acme-corp");
        // A bare number under a non-account key is not an account.
        assert_eq!(v["order_id"], "1002123456789");
        // Hyphenated bank format in free text; dates are not.
        assert_eq!(
            mask_text("입금 110-123-456789 on 2026-11-02", MaskStrategy::Partial),
            "입금 ***-***-**6789 on 2026-11-02"
        );
    }

    #[test]
    fn scan_reports_korean_pii() {
        let v: Value = serde_json::from_str(
            r#"{"name":"김민수","phone":"010-1234-5678","rrn_text":"900101-1234567","acct":"110-123-456789"}"#,
        )
        .unwrap();
        let mut findings = Vec::new();
        scan_json_for_pii(&v, "$", None, None, &mut findings);
        let kinds: Vec<PiiType> = findings.iter().map(|f| f.pii_type).collect();
        assert!(kinds.contains(&PiiType::KoreanMobile));
        assert!(kinds.contains(&PiiType::KoreanResidentNumber));
        assert!(kinds.contains(&PiiType::BankAccount));
        assert!(findings
            .iter()
            .all(|f| !f.masked_sample.contains("1234567")));
        let report = build_audit_report("users", 1, findings);
        assert_eq!(report.korean_pii_found, 3);
    }

    #[test]
    fn test_mask_text_masks_every_kind_and_occurrence() {
        let text = "mail alice@example.com and bob@corp.io, card 4111 1111 1111 1111,                     ssn 123-45-6789, key AKIAABCDEFGHIJKLMNOP, auth Bearer abcdef123456";
        let masked = mask_text(text, MaskStrategy::Redact);
        for leaked in [
            "alice@example.com",
            "bob@corp.io",
            "4111 1111 1111 1111",
            "123-45-6789",
            "AKIAABCDEFGHIJKLMNOP",
            "abcdef123456",
        ] {
            assert!(!masked.contains(leaked), "{leaked} leaked: {masked}");
        }
        assert!(masked.starts_with("mail [REDACTED_EMAIL] and [REDACTED_EMAIL]"));
        // Not PII: order ids and placeholder SSNs stay.
        assert_eq!(
            mask_text("order 1234567890 ref 000-12-3456", MaskStrategy::Redact),
            "order 1234567890 ref 000-12-3456"
        );
    }

    #[test]
    fn test_mask_payload_hides_sensitive_keys_whole() {
        let masked = mask_payload(
            r#"{"password":"hunter2","ssn":"123-45-6789","note":"mail x@y.com"}"#,
            MaskStrategy::Partial,
        );
        assert!(!masked.contains("hu"), "{masked}");
        assert!(!masked.contains("12"), "{masked}");
        assert!(masked.contains(r#""password":"****""#), "{masked}");
        assert!(!masked.contains("x@y.com"), "{masked}");
        assert_eq!(
            mask_payload("plain alice@example.com", MaskStrategy::Redact),
            "plain [REDACTED_EMAIL]"
        );
    }
    use serde_json::json;

    #[test]
    fn test_luhn_credit_card_validation() {
        // Valid Visa test card
        assert!(is_luhn_valid("4532015112830366"));
        // Invalid check digit
        assert!(!is_luhn_valid("4532015112830367"));
        // Random short string
        assert!(!is_luhn_valid("12345"));
    }

    #[test]
    fn test_detect_and_mask_email() {
        let text = "Contact user at alice@ferrika.io for updates.";
        let found = detect_email(text);
        assert_eq!(found, Some("alice@ferrika.io".to_string()));

        let partial = mask_email("alice@ferrika.io", MaskStrategy::Partial);
        assert_eq!(partial, "a***e@ferrika.io");

        let redact = mask_email("alice@ferrika.io", MaskStrategy::Redact);
        assert_eq!(redact, "[REDACTED_EMAIL]");
    }

    #[test]
    fn test_detect_and_mask_ssn() {
        let text = "Customer SSN: 123-45-6789";
        let found = detect_ssn(text);
        assert_eq!(found, Some("123-45-6789".to_string()));

        // Placeholder 000-00-0000 ignored
        assert_eq!(detect_ssn("000-00-0000"), None);

        let masked = mask_ssn("123-45-6789", MaskStrategy::Partial);
        assert_eq!(masked, "***-**-****");
    }

    #[test]
    fn test_detect_aws_and_bearer_tokens() {
        let aws_str = "Credentials: AKIAIOSFODNN7EXAMPLE";
        let found_aws = detect_secret_token(aws_str);
        assert!(found_aws.is_some());
        assert_eq!(found_aws.unwrap().0, PiiType::AwsAccessKey);

        let bearer = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M";
        let found_jwt = detect_secret_token(bearer);
        assert!(found_jwt.is_some());
        assert_eq!(found_jwt.unwrap().0, PiiType::BearerToken);
    }

    #[test]
    fn test_recursive_json_masking() {
        let mut sample = json!({
            "order_id": "ORD-9921",
            "password": "supersecretpassword",
            "user": {
                "name": "Jane Doe",
                "email": "jane.doe@acme.com",
                "payment": {
                    "card_number": "4532015112830366",
                    "cvv": "123"
                }
            }
        });

        mask_json_value(&mut sample, MaskStrategy::Redact);

        assert_eq!(sample["order_id"], "ORD-9921");
        assert_eq!(sample["password"], "[REDACTED]");
        assert_eq!(sample["user"]["payment"]["card_number"], "[REDACTED]");
        assert_eq!(sample["user"]["payment"]["cvv"], "[REDACTED]");
        assert_eq!(sample["user"]["email"], "[REDACTED_EMAIL]");
    }

    #[test]
    fn test_scan_json_for_pii_and_audit_report() {
        let sample = json!({
            "order_id": "ORD-1234",
            "ssn": "123-45-6789",
            "user": {
                "email": "customer@ferrika.org"
            }
        });

        let mut findings = Vec::new();
        scan_json_for_pii(&sample, "$", Some(0), Some(105), &mut findings);

        // Expect 3 findings: 1 SensitiveKey ('ssn'), 1 SSN value ('123-45-6789'), 1 Email ('customer@ferrika.org')
        assert_eq!(findings.len(), 3);
        let report = build_audit_report("orders-v1", 10, findings);

        assert_eq!(report.topic, "orders-v1");
        assert_eq!(report.total_violations, 3);
        assert_eq!(report.sensitive_keys_found, 1);
        assert_eq!(report.ssns_found, 1);
        assert_eq!(report.emails_found, 1);
        // 3 findings, all in the one message at partition 0 offset 105: 9 of 10 are clean.
        assert!((report.compliance_score_pct - 90.0).abs() < f64::EPSILON);
    }

    #[test]
    fn names_under_name_keys_are_masked_partially() {
        let out = |payload: &str| -> Value {
            serde_json::from_str(&mask_payload(payload, MaskStrategy::Partial)).unwrap()
        };
        let v = out(
            r#"{"customer":"김민준","name":"이서연","holder_name":"박지","성명":"남궁민수","firstName":"John Smith","예금주":"최"}"#,
        );
        assert_eq!(v["customer"], "김*준");
        assert_eq!(v["name"], "이*연");
        assert_eq!(v["holder_name"], "박*");
        assert_eq!(v["성명"], "남**수");
        assert_eq!(v["firstName"], "J*** S****");
        assert_eq!(v["예금주"], "*");

        let v = out(
            r#"{"Full_Name":"Ada Lovelace","LAST-NAME":"Kim","이름":"정우성","고객명":"한가인","recipient_name":"Lee Min","sender_name":"Park"}"#,
        );
        assert_eq!(v["Full_Name"], "A** L*******");
        assert_eq!(v["LAST-NAME"], "K**");
        assert_eq!(v["이름"], "정*성");
        assert_eq!(v["고객명"], "한*인");
        assert_eq!(v["recipient_name"], "L** M**");
        assert_eq!(v["sender_name"], "P***");
        assert_eq!(
            mask_payload(r#"{"name":"김민준"}"#, MaskStrategy::Redact),
            r#"{"name":"[REDACTED_NAME]"}"#
        );
    }

    #[test]
    fn generic_name_keys_are_left_alone() {
        let payload = r#"{"topic_name":"orders","connector_name":"jdbc-sink","file_name":"a.csv","schema_name":"Order","host_name":"broker-1","display_name":"Orders","username":"svc-app","product_name":"Widget"}"#;
        let masked: Value =
            serde_json::from_str(&mask_payload(payload, MaskStrategy::Partial)).unwrap();
        let original: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(masked, original);
        for key in ["topic_name", "connector_name", "file_name", "host_name"] {
            assert!(!is_name_key(key), "{key}");
        }
    }

    #[test]
    fn names_in_nested_objects_and_arrays() {
        let payload = r#"{"order":{"customer":{"name":"김민준","id":7,"email":"kim@corp.com"},"items":[{"name":"이서연"},{"sku":"A1"}]},"recipients":["John Smith","박지민"],"customer":null,"name":42}"#;
        let v: Value = serde_json::from_str(&mask_payload(payload, MaskStrategy::Partial)).unwrap();
        assert_eq!(v["order"]["customer"]["name"], "김*준");
        assert_eq!(v["order"]["customer"]["id"], 7);
        assert_ne!(v["order"]["customer"]["email"], "kim@corp.com");
        assert_eq!(v["order"]["items"][0]["name"], "이*연");
        assert_eq!(v["order"]["items"][1]["sku"], "A1");
        assert_eq!(v["recipients"][0], "J*** S****");
        assert_eq!(v["recipients"][1], "박*민");
        assert_eq!(v["customer"], Value::Null);
        assert_eq!(v["name"], 42);
        // Non-JSON payloads have no keys, so names in free text stay as they are.
        assert_eq!(
            mask_payload("고객 김민준", MaskStrategy::Partial),
            "고객 김민준"
        );
    }

    #[test]
    fn scan_reports_person_names() {
        let sample =
            json!({"customer": "김민준", "topic_name": "orders", "recipients": ["Ann Lee", ""]});
        let mut findings = Vec::new();
        scan_json_for_pii(&sample, "$", None, None, &mut findings);
        let names: Vec<(&str, &str)> = findings
            .iter()
            .filter(|f| f.pii_type == PiiType::PersonName)
            .map(|f| (f.field_path.as_str(), f.masked_sample.as_str()))
            .collect();
        assert_eq!(
            names,
            vec![("$.customer", "김*준"), ("$.recipients[0]", "A** L**")]
        );
        let report = build_audit_report("t", 5, findings);
        assert_eq!(report.names_found, 2);
    }

    #[test]
    fn json_numbers_are_scanned_for_pii() {
        let masked = mask_payload(
            r#"{"card":4111111111111111,"id":9001011234568,"qty":3,"ts":1726921800120}"#,
            MaskStrategy::Partial,
        );
        let v: Value = serde_json::from_str(&masked).unwrap();
        assert_eq!(v["card"], "****-****-****-1111");
        assert_eq!(v["id"], "900101-1******");
        // Ordinary numbers stay numbers.
        assert_eq!(v["qty"], 3);
        assert_eq!(v["ts"], 1726921800120u64);
        // Numbers inside arrays, and under sensitive keys, are masked too.
        let v: Value = serde_json::from_str(&mask_payload(
            r#"{"cards":[4111111111111111],"cvv":123,"pin_token":42}"#,
            MaskStrategy::Redact,
        ))
        .unwrap();
        assert_eq!(v["cards"][0], "[REDACTED_CARD]");
        assert_eq!(v["cvv"], "[REDACTED]");
        assert_eq!(v["pin_token"], "[REDACTED]");
    }

    #[test]
    fn double_encoded_json_strings_are_masked_field_by_field() {
        let payload = r#"{"data":"{\"password\":\"hunter2\",\"name\":\"김민준\",\"inner\":\"{\\\"api_key\\\":\\\"abc\\\"}\"}"}"#;
        let masked = mask_payload(payload, MaskStrategy::Partial);
        assert!(
            !masked.contains("hunter2") && !masked.contains("abc"),
            "{masked}"
        );
        let outer: Value = serde_json::from_str(&masked).unwrap();
        let inner: Value = serde_json::from_str(outer["data"].as_str().unwrap()).unwrap();
        assert_eq!(inner["password"], "****");
        assert_eq!(inner["name"], "김*준");
        let innermost: Value = serde_json::from_str(inner["inner"].as_str().unwrap()).unwrap();
        assert_eq!(innermost["api_key"], "****");
        // A string that only looks like JSON is still masked as text.
        let v: Value = serde_json::from_str(&mask_payload(
            r#"{"note":"{not json} kim@example.com"}"#,
            MaskStrategy::Redact,
        ))
        .unwrap();
        assert_eq!(v["note"], "{not json} [REDACTED_EMAIL]");
    }

    #[test]
    fn deeply_nested_encoded_json_is_bounded() {
        let mut payload = json!({"password": "hunter2"}).to_string();
        for _ in 0..(MAX_EMBEDDED_JSON_DEPTH + 3) {
            payload = json!({ "data": payload }).to_string();
        }
        // Deeper levels fall back to free-text masking but never panic.
        let masked = mask_payload(&payload, MaskStrategy::Partial);
        assert!(serde_json::from_str::<Value>(&masked).is_ok());
    }

    #[test]
    fn json_keys_are_masked() {
        let v: Value = serde_json::from_str(&mask_payload(
            r#"{"jane@x.com":1,"kim@x.com":{"a":"b"},"plain":2}"#,
            MaskStrategy::Redact,
        ))
        .unwrap();
        let obj = v.as_object().unwrap();
        assert!(obj
            .keys()
            .all(|k| !k.contains("jane") && !k.contains("kim")));
        assert_eq!(obj.len(), 3, "colliding masked keys must not drop fields");
        assert_eq!(v["plain"], 2);
    }

    #[test]
    fn header_values_are_judged_by_header_name() {
        let p = MaskStrategy::Partial;
        assert_eq!(mask_header_value("x-api-key", b"abc", p), "****");
        assert_eq!(
            mask_header_value("Authorization", b"Basic dXNlcg==", p),
            "****"
        );
        assert_eq!(mask_header_value("password", b"hunter2", p), "****");
        assert_eq!(
            mask_header_value("customer_name", "김민준".as_bytes(), p),
            "김*준"
        );
        assert_eq!(
            mask_header_value("X-Customer-Name", b"John Smith", p),
            "J*** S****"
        );
        assert_eq!(
            mask_header_value("account_no", b"110123456789", p),
            "********6789"
        );
        assert_eq!(
            mask_header_value("reply-to", b"kim@example.com", MaskStrategy::Redact),
            "[REDACTED_EMAIL]"
        );
        assert_eq!(mask_header_value("trace", b"ok", p), "ok");
        assert_eq!(
            mask_header_value("bin", &[0xff, 0xfe], p),
            "[binary 2 bytes hidden]"
        );
    }

    #[test]
    fn sensitive_keys_cover_hyphen_and_short_forms() {
        for k in [
            "api-key",
            "API-Key",
            "pwd",
            "passwd",
            "user_pass",
            "authorization",
            "Authorization",
            "credential",
            "credentials",
            "access_key",
            "access-key",
            "accessKey",
            "client-secret",
            "secret_key",
            "db-password",
        ] {
            assert!(is_sensitive_key(k), "{k} should be sensitive");
        }
        for k in [
            "passport",
            "compass",
            "author",
            "bypass",
            "authority",
            "passenger",
            "name",
        ] {
            assert!(!is_sensitive_key(k), "{k} should stay visible");
        }
    }

    #[test]
    fn nested_hyphen_keys_are_masked_in_payload() {
        let out = mask_payload(
            r#"{"nested":{"api-key":"AbCdEf123456","pwd":"p4ss","passport":"M1234567"}}"#,
            MaskStrategy::Partial,
        );
        assert!(!out.contains("AbCdEf123456"), "{out}");
        assert!(!out.contains("p4ss"), "{out}");
        assert!(out.contains("M1234567"), "{out}");
    }

    #[test]
    fn secret_value_patterns() {
        for v in [
            "ghp_abcdefghijklmnopqrstuvwxyz0123456789",
            "sk_live_abcdefgh12345678",
            "xoxb-123456789012-abcdefghij",
            "Basic dXNlcjpwYXNzd29yZA==",
            "-----BEGIN RSA PRIVATE KEY-----\nMIIEabc\n-----END RSA PRIVATE KEY-----",
            "-----BEGIN PRIVATE KEY-----",
        ] {
            let out = mask_text(&format!("x {v} y"), MaskStrategy::Redact);
            assert!(out.contains("[REDACTED_SECRET]"), "{v} -> {out}");
            assert!(
                !out.contains("MIIEabc") && !out.contains("dXNlcjpw"),
                "{out}"
            );
        }
        let plain = "basic usage, the task_live is fine";
        assert_eq!(mask_text(plain, MaskStrategy::Redact), plain);
    }

    #[test]
    fn many_private_key_headers_without_an_end_are_masked_in_linear_time() {
        let text = "-----BEGIN PRIVATE KEY-----".repeat(20_000);
        let started = std::time::Instant::now();
        let out = mask_text(&text, MaskStrategy::Redact);
        assert!(!out.contains("BEGIN"));
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
        // Two keys, the second without an END: the first spans to its END only.
        let two = "a -----BEGIN PRIVATE KEY-----\nK1\n-----END PRIVATE KEY----- b -----BEGIN EC PRIVATE KEY-----\nK2 c";
        assert_eq!(
            mask_text(two, MaskStrategy::Redact),
            "a [REDACTED_SECRET] b [REDACTED_SECRET]\nK2 c"
        );
    }

    #[test]
    fn audit_paths_do_not_carry_pii_keys() {
        let value: Value =
            serde_json::from_str(r#"{"kim@example.com":{"password":"x"},"ok":{"card":"1"}}"#)
                .unwrap();
        let mut findings = Vec::new();
        scan_json_for_pii(&value, "$", None, None, &mut findings);
        assert!(!findings.is_empty());
        for f in &findings {
            assert!(
                !f.field_path.contains("kim@example.com"),
                "{}",
                f.field_path
            );
        }
    }

    #[test]
    fn ellipsize_cuts_on_character_boundaries() {
        assert_eq!(ellipsize("", 0), "");
        assert_eq!(ellipsize("abc", 3), "abc");
        assert_eq!(ellipsize("abcd", 3), "abc...");
        assert_eq!(ellipsize("abc", 0), "...");
        assert_eq!(ellipsize("한글입니다", 2), "한글...");
        assert_eq!(ellipsize("한글", 5), "한글");
        assert_eq!(ellipsize("a\u{1F600}b", 2), "a\u{1F600}...");
    }
}
