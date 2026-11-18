//! Record filter expressions, e.g. `$.amount > 100 && $.status == "PAID"`.
//!
//! A filter that starts with `$` is an expression rather than search text.
//!
//! - `$.a.b`, `$.items[0]`, `$["odd key"]`: a field of the JSON value (`$`
//!   alone is the whole value, as JSON when it parses, else as text)
//! - `key`, `partition`, `offset`, `timestamp` (ms), `headers.<name>`
//! - `== != > >= < <=`, `=~` (regex, case-insensitive), `contains`
//!   (text or array element, case-insensitive), `exists`, `&&`/`and`, `||`/`or`, `!`/`not`
//! - strings in `"…"` or `'…'`, numbers, `true`, `false`, `null`
//!
//! A bare operand is true when it exists and is not `null`, `false`, `0` or
//! `""`. Comparing a number with a numeric string compares numbers.

use std::cmp::Ordering;
use std::ops::Range;

use serde_json::Value;

/// The record fields an expression can read.
pub struct RecordView<'a> {
    pub key: Option<&'a str>,
    pub value: Option<&'a str>,
    pub headers: &'a [(String, String)],
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
}

/// The longest expression text accepted, in bytes.
const MAX_EXPR_LEN: usize = 16 * 1024;
/// How deeply `(` and `!` may nest. Parsing and evaluation recurse once per
/// level, so this bounds their stack use.
const MAX_DEPTH: usize = 64;
/// The most comparisons/operands one expression may hold.
const MAX_TERMS: usize = 1024;

#[derive(Debug, Clone)]
pub struct Expr(Node);

#[derive(Debug, Clone)]
enum Node {
    // `a || b || c` is one flat list, so long chains recurse neither in
    // `eval` nor in `Drop`.
    Or(Vec<Node>),
    And(Vec<Node>),
    Not(Box<Node>),
    Compare(Operand, Op, Operand),
    Exists(Operand),
    Truthy(Operand),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Op {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Matches,
    Contains,
}

#[derive(Debug, Clone)]
enum Operand {
    Literal(Value),
    Regex(regex::Regex),
    Path(Vec<Step>),
    Key,
    Partition,
    Offset,
    Timestamp,
    Header(String),
}

#[derive(Debug, Clone)]
enum Step {
    Field(String),
    Index(usize),
}

/// A token of the expression language. [`LexMode::Lenient`] adds
/// `PartialStr` and `Unknown` so text still being typed tokenizes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token {
    Dollar,
    Dot,
    LBracket,
    RBracket,
    LParen,
    RParen,
    Ident(String),
    Str(String),
    Num(serde_json::Number),
    Op(&'static str),
    /// A string without its closing quote, up to the end of the text (lenient only).
    PartialStr {
        quote: char,
        text: String,
    },
    /// Text that is not a token, such as `#` or `1e999` (lenient only).
    Unknown(String),
}

/// A token and the byte range of the text it came from.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Spanned {
    pub token: Token,
    pub span: Range<usize>,
}

/// How [`lex`] treats text that is not a token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LexMode {
    /// Stop at the first problem and report it (what [`Expr::parse`] uses).
    Strict,
    /// Keep going: an unclosed string becomes `PartialStr`, anything else `Unknown`.
    Lenient,
}

/// Two-character operators.
pub(crate) const OPERATORS: [&str; 7] = ["==", "!=", ">=", "<=", "=~", "&&", "||"];
/// One-character operators (a single `=` reads as `==`).
const SHORT_OPERATORS: [&str; 4] = ["<", ">", "!", "="];

/// Whether `c` may appear in a field name after `.` (`$.a-b_2`).
pub fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-'
}

/// Whether `query` is meant as an expression rather than search text: a
/// leading `?` (as `--where` accepts), `$` followed by `.`, `[`, a space or
/// nothing, or a record field (`key`, `partition`, `offset`, `timestamp`,
/// `headers.<name>`) followed by an operator. `$100`, `$USD` or `key rotation`
/// stay text.
pub fn is_expression(query: &str) -> bool {
    let query = query.trim_start();
    if query.starts_with('?') {
        return true;
    }
    let mut chars = query.chars();
    if chars.next() == Some('$') {
        return chars
            .next()
            .is_none_or(|c| c == '.' || c == '[' || c.is_whitespace());
    }
    let field_len = query
        .find(|c: char| c.is_whitespace() || "=!<>".contains(c))
        .unwrap_or(query.len());
    let (field, rest) = query.split_at(field_len);
    let is_field = matches!(field, "key" | "partition" | "offset" | "timestamp")
        || field
            .strip_prefix("headers.")
            .is_some_and(|name| !name.is_empty());
    let rest = rest.trim_start();
    is_field
        && (["==", "!=", ">", "<", "=~"]
            .iter()
            .any(|op| rest.starts_with(op))
            || ["contains", "exists"].iter().any(|word| {
                rest.strip_prefix(word)
                    .is_some_and(|after| after.chars().next().is_none_or(|c| !c.is_alphanumeric()))
            }))
}

/// The expression text without the optional leading `?`.
pub fn expression_text(query: &str) -> &str {
    query.trim_start().trim_start_matches('?')
}

impl Expr {
    pub fn parse(text: &str) -> Result<Self, String> {
        if text.len() > MAX_EXPR_LEN {
            return Err(format!(
                "The expression is too long (max {MAX_EXPR_LEN} bytes)"
            ));
        }
        let tokens = tokenize(text)?;
        let mut parser = Parser {
            tokens,
            pos: 0,
            depth: 0,
            terms: 0,
        };
        let node = parser.or()?;
        match parser.peek() {
            None => Ok(Expr(node)),
            Some(t) => Err(format!("Unexpected {} in the expression", describe(t))),
        }
    }

    pub fn matches(&self, record: &RecordView) -> bool {
        // Parse the value once per record, only if a path needs it.
        let json = std::cell::OnceCell::new();
        let ctx = Ctx { record, json };
        eval(&self.0, &ctx)
    }
}

fn describe(token: &Token) -> String {
    match token {
        Token::Ident(s) => format!("'{s}'"),
        Token::Str(s) => format!("\"{s}\""),
        Token::Num(n) => n.to_string(),
        Token::Op(o) => format!("'{o}'"),
        Token::Dollar => "'$'".into(),
        Token::Dot => "'.'".into(),
        Token::LBracket => "'['".into(),
        Token::RBracket => "']'".into(),
        Token::LParen => "'('".into(),
        Token::RParen => "')'".into(),
        Token::PartialStr { quote, text } => format!("{quote}{text}"),
        Token::Unknown(s) => format!("'{s}'"),
    }
}

fn tokenize(text: &str) -> Result<Vec<Token>, String> {
    match lex(text, LexMode::Strict) {
        (_, Some(e)) => Err(e),
        (tokens, None) => Ok(tokens.into_iter().map(|t| t.token).collect()),
    }
}

/// Splits `text` into tokens with their byte ranges. In strict mode the
/// first problem stops lexing and is returned with the tokens before it; in
/// lenient mode there is never an error.
pub(crate) fn lex(text: &str, mode: LexMode) -> (Vec<Spanned>, Option<String>) {
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    // Byte offset of the `i`th character (the text's length past the end).
    let at = |i: usize| chars.get(i).map_or(text.len(), |&(b, _)| b);
    let ch = |i: usize| chars.get(i).map(|&(_, c)| c);
    let lenient = mode == LexMode::Lenient;
    let mut tokens: Vec<Spanned> = Vec::new();
    let mut i = 0;
    while let Some(c) = ch(i) {
        let start = i;
        let next = ch(i + 1);
        let last = tokens.last().map(|t| &t.token);
        let token = match c {
            c if c.is_whitespace() => {
                i += 1;
                continue;
            }
            '$' => {
                i += 1;
                Token::Dollar
            }
            // In a path (`$.a.0`) a dot is a separator even before a digit.
            '.' if !next.is_some_and(|n| n.is_ascii_digit())
                || matches!(
                    last,
                    Some(Token::Dollar | Token::Ident(_) | Token::RBracket)
                ) =>
            {
                i += 1;
                Token::Dot
            }
            // A field name after a dot may start with a digit (`$.2fa`).
            c if last == Some(&Token::Dot) && is_name_char(c) => {
                while ch(i).is_some_and(is_name_char) {
                    i += 1;
                }
                Token::Ident(text[at(start)..at(i)].to_string())
            }
            '[' => {
                i += 1;
                Token::LBracket
            }
            ']' => {
                i += 1;
                Token::RBracket
            }
            '(' => {
                i += 1;
                Token::LParen
            }
            ')' => {
                i += 1;
                Token::RParen
            }
            '"' | '\'' => {
                let quote = c;
                let mut s = String::new();
                i += 1;
                let mut closed = false;
                while let Some(c) = ch(i) {
                    match c {
                        // Only quotes and backslashes are escaped, so regex
                        // classes such as \d keep their backslash.
                        '\\' => {
                            match ch(i + 1) {
                                Some(e) if e == quote || e == '\\' => s.push(e),
                                Some(e) => {
                                    s.push('\\');
                                    s.push(e);
                                }
                                None => s.push('\\'),
                            }
                            i += 2;
                        }
                        q if q == quote => {
                            i += 1;
                            closed = true;
                            break;
                        }
                        c => {
                            s.push(c);
                            i += 1;
                        }
                    }
                }
                i = i.min(chars.len());
                match (closed, lenient) {
                    (true, _) => Token::Str(s),
                    (false, true) => Token::PartialStr { quote, text: s },
                    (false, false) => {
                        return (tokens, Some("Unclosed string in the expression".into()))
                    }
                }
            }
            c if c.is_ascii_digit()
                || ((c == '-' || c == '.') && next.is_some_and(|n| n.is_ascii_digit())) =>
            {
                i += 1;
                while let Some(c) = ch(i) {
                    let exponent_sign =
                        matches!(c, '+' | '-') && matches!(ch(i - 1), Some('e' | 'E'));
                    if !(c.is_ascii_digit() || matches!(c, '.' | 'e' | 'E') || exponent_sign) {
                        break;
                    }
                    i += 1;
                }
                let s = &text[at(start)..at(i)];
                // Integers stay exact so large ids compare without f64 rounding.
                let n = if let Ok(n) = s.parse::<i64>() {
                    Some(serde_json::Number::from(n))
                } else if let Ok(n) = s.parse::<u64>() {
                    Some(serde_json::Number::from(n))
                } else {
                    s.parse::<f64>().ok().and_then(serde_json::Number::from_f64)
                };
                match (n, lenient) {
                    (Some(n), _) => Token::Num(n),
                    (None, true) => Token::Unknown(s.to_string()),
                    (None, false) => return (tokens, Some(format!("'{s}' is not a number"))),
                }
            }
            c if c.is_alphabetic() || c == '_' => {
                while ch(i).is_some_and(is_name_char) {
                    i += 1;
                }
                Token::Ident(text[at(start)..at(i)].to_string())
            }
            _ => {
                let two = &text[at(i)..at((i + 2).min(chars.len()))];
                let op = OPERATORS
                    .into_iter()
                    .find(|o| *o == two)
                    .or_else(|| SHORT_OPERATORS.into_iter().find(|o| o.starts_with(c)));
                match op {
                    // A single '=' reads as '=='.
                    Some("=") => {
                        i += 1;
                        Token::Op("==")
                    }
                    Some(o) => {
                        i += o.len();
                        Token::Op(o)
                    }
                    None if lenient => {
                        i += 1;
                        Token::Unknown(c.to_string())
                    }
                    None => return (tokens, Some(format!("Unexpected '{c}' in the expression"))),
                }
            }
        };
        tokens.push(Spanned {
            token,
            span: at(start)..at(i),
        });
    }
    (tokens, None)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// Current `(`/`!` nesting.
    depth: usize,
    /// Operands parsed so far.
    terms: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    fn word(&self, w: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case(w))
    }

    /// Enters one `(` or `!` level, refusing nesting beyond [`MAX_DEPTH`].
    fn descend(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(format!(
                "The expression nests too deeply (max {MAX_DEPTH} levels of '(' or '!')"
            ));
        }
        Ok(())
    }

    fn or(&mut self) -> Result<Node, String> {
        let mut items = vec![self.and()?];
        while self.peek() == Some(&Token::Op("||")) || self.word("or") {
            self.pos += 1;
            items.push(self.and()?);
        }
        Ok(match items.len() {
            1 => items.remove(0),
            _ => Node::Or(items),
        })
    }

    fn and(&mut self) -> Result<Node, String> {
        let mut items = vec![self.not()?];
        while self.peek() == Some(&Token::Op("&&")) || self.word("and") {
            self.pos += 1;
            items.push(self.not()?);
        }
        Ok(match items.len() {
            1 => items.remove(0),
            _ => Node::And(items),
        })
    }

    fn not(&mut self) -> Result<Node, String> {
        if self.peek() == Some(&Token::Op("!")) || self.word("not") {
            self.pos += 1;
            self.descend()?;
            let inner = self.not()?;
            self.depth -= 1;
            return Ok(Node::Not(Box::new(inner)));
        }
        self.comparison()
    }

    fn comparison(&mut self) -> Result<Node, String> {
        if self.peek() == Some(&Token::LParen) {
            self.pos += 1;
            self.descend()?;
            let inner = self.or()?;
            self.depth -= 1;
            if self.next() != Some(Token::RParen) {
                return Err("Missing ')' in the expression".into());
            }
            return Ok(inner);
        }
        self.terms += 1;
        if self.terms > MAX_TERMS {
            return Err(format!(
                "The expression has too many terms (max {MAX_TERMS})"
            ));
        }
        let left = self.operand()?;
        if self.word("exists") {
            self.pos += 1;
            return Ok(Node::Exists(left));
        }
        let op = match self.peek() {
            Some(Token::Op("==")) => Op::Eq,
            Some(Token::Op("!=")) => Op::Ne,
            Some(Token::Op(">")) => Op::Gt,
            Some(Token::Op(">=")) => Op::Ge,
            Some(Token::Op("<")) => Op::Lt,
            Some(Token::Op("<=")) => Op::Le,
            Some(Token::Op("=~")) => Op::Matches,
            _ if self.word("contains") => Op::Contains,
            _ => return Ok(Node::Truthy(left)),
        };
        self.pos += 1;
        let right = self.operand()?;
        let right = match (op, right) {
            (Op::Matches, Operand::Literal(Value::String(p))) => Operand::Regex(
                regex::RegexBuilder::new(&p)
                    .case_insensitive(true)
                    .build()
                    .map_err(|e| {
                        let text = e.to_string();
                        format!(
                            "Invalid pattern: {}",
                            text.lines().last().unwrap_or(&text).trim()
                        )
                    })?,
            ),
            (Op::Matches, _) => return Err("'=~' needs a quoted pattern".into()),
            (_, r) => r,
        };
        Ok(Node::Compare(left, op, right))
    }

    fn operand(&mut self) -> Result<Operand, String> {
        match self.next() {
            Some(Token::Dollar) => {
                let mut steps = Vec::new();
                loop {
                    match self.peek() {
                        Some(Token::Dot) => {
                            self.pos += 1;
                            match self.next() {
                                Some(Token::Ident(f)) => steps.push(Step::Field(f)),
                                Some(Token::Str(f)) => steps.push(Step::Field(f)),
                                Some(Token::Num(n)) => steps.push(Step::Field(n.to_string())),
                                _ => return Err("Expected a field name after '.'".into()),
                            }
                        }
                        Some(Token::LBracket) => {
                            self.pos += 1;
                            match self.next() {
                                Some(Token::Num(n)) if index_of(&n).is_some() => {
                                    steps.push(Step::Index(index_of(&n).unwrap_or_default()))
                                }
                                Some(Token::Str(f)) => steps.push(Step::Field(f)),
                                _ => return Err("Expected an index or quoted name in '[]'".into()),
                            }
                            if self.next() != Some(Token::RBracket) {
                                return Err("Missing ']' in the expression".into());
                            }
                        }
                        _ => break,
                    }
                }
                Ok(Operand::Path(steps))
            }
            Some(Token::Str(s)) => Ok(Operand::Literal(Value::String(s))),
            Some(Token::Num(n)) => Ok(Operand::Literal(Value::Number(n))),
            Some(Token::Ident(word)) => match word.to_ascii_lowercase().as_str() {
                "true" => Ok(Operand::Literal(Value::Bool(true))),
                "false" => Ok(Operand::Literal(Value::Bool(false))),
                "null" => Ok(Operand::Literal(Value::Null)),
                "key" => Ok(Operand::Key),
                "partition" => Ok(Operand::Partition),
                "offset" => Ok(Operand::Offset),
                "timestamp" => Ok(Operand::Timestamp),
                "headers" => {
                    if self.next() != Some(Token::Dot) {
                        return Err("Use headers.<name>".into());
                    }
                    match self.next() {
                        Some(Token::Ident(h)) | Some(Token::Str(h)) => Ok(Operand::Header(h)),
                        _ => Err("Expected a header name after 'headers.'".into()),
                    }
                }
                _ => Err(format!(
                    "Unknown name '{word}' (use $.field, key, partition, offset, timestamp or headers.<name>)"
                )),
            },
            Some(t) => Err(format!("Unexpected {} in the expression", describe(&t))),
            None => Err("The expression ends too early".into()),
        }
    }
}

/// A whole, non-negative number as an array index (`[2]` or `[2.0]`).
fn index_of(n: &serde_json::Number) -> Option<usize> {
    n.as_u64()
        .or_else(|| {
            n.as_f64()
                .filter(|f| *f >= 0.0 && f.fract() == 0.0 && *f < u64::MAX as f64)
                .map(|f| f as u64)
        })
        .and_then(|i| usize::try_from(i).ok())
}

struct Ctx<'a, 'r> {
    record: &'a RecordView<'r>,
    json: std::cell::OnceCell<Option<Value>>,
}

impl Ctx<'_, '_> {
    fn value_json(&self) -> Option<&Value> {
        self.json
            .get_or_init(|| self.record.value.and_then(|v| serde_json::from_str(v).ok()))
            .as_ref()
    }

    /// The operand's value; `None` when it does not exist.
    fn resolve(&self, operand: &Operand) -> Option<Value> {
        let r = self.record;
        match operand {
            Operand::Literal(v) => Some(v.clone()),
            Operand::Regex(re) => Some(Value::String(re.as_str().to_string())),
            Operand::Key => r.key.map(|k| Value::String(k.to_string())),
            Operand::Partition => Some(Value::from(r.partition)),
            Operand::Offset => Some(Value::from(r.offset)),
            Operand::Timestamp => Some(Value::from(r.timestamp_ms)),
            Operand::Header(name) => r
                .headers
                .iter()
                .rev()
                .find(|(k, _)| k == name)
                .map(|(_, v)| Value::String(v.clone())),
            Operand::Path(steps) => {
                let Some(root) = self.value_json() else {
                    // Not JSON: only `$` itself means something (the text).
                    return steps
                        .is_empty()
                        .then(|| r.value.map(|v| Value::String(v.to_string())))
                        .flatten();
                };
                let mut current = root;
                for step in steps {
                    current = match step {
                        Step::Field(f) => current.get(f)?,
                        Step::Index(i) => current.get(*i)?,
                    };
                }
                Some(current.clone())
            }
        }
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

fn as_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// The value as an exact integer when it is one (a JSON integer or an
/// integer string), so large ids compare without f64 rounding.
fn as_integer(value: &Value) -> Option<i128> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from)),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Orders two values: numerically when either is a number and both read as
/// numbers, else as text.
fn order(a: &Value, b: &Value) -> Option<Ordering> {
    if a.is_number() || b.is_number() {
        if let (Some(x), Some(y)) = (as_integer(a), as_integer(b)) {
            return Some(x.cmp(&y));
        }
        if let (Some(x), Some(y)) = (as_number(a), as_number(b)) {
            return x.partial_cmp(&y);
        }
    }
    match (a, b) {
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn equal(a: &Value, b: &Value) -> bool {
    match order(a, b) {
        Some(o) => o == Ordering::Equal,
        None => a == b,
    }
}

fn eval(node: &Node, ctx: &Ctx) -> bool {
    match node {
        Node::Or(items) => items.iter().any(|n| eval(n, ctx)),
        Node::And(items) => items.iter().all(|n| eval(n, ctx)),
        Node::Not(a) => !eval(a, ctx),
        Node::Exists(a) => ctx.resolve(a).is_some(),
        Node::Truthy(a) => ctx.resolve(a).is_some_and(|v| truthy(&v)),
        Node::Compare(left, op, right) => {
            let l = ctx.resolve(left);
            if let (Op::Matches, Operand::Regex(re)) = (op, right) {
                return l.is_some_and(|v| re.is_match(&text(&v)));
            }
            let r = ctx.resolve(right);
            match (l, r, op) {
                (None, None, Op::Eq) => true,
                (None, Some(r), Op::Eq) => r.is_null(),
                (Some(l), None, Op::Eq) => l.is_null(),
                (None, None, Op::Ne) => false,
                (None, Some(r), Op::Ne) => !r.is_null(),
                (Some(l), None, Op::Ne) => !l.is_null(),
                (Some(l), Some(r), Op::Eq) => equal(&l, &r),
                (Some(l), Some(r), Op::Ne) => !equal(&l, &r),
                (Some(l), Some(r), Op::Contains) => match &l {
                    Value::Array(items) => items.iter().any(|i| match (i, &r) {
                        (Value::String(x), Value::String(y)) => {
                            x.to_lowercase() == y.to_lowercase()
                        }
                        _ => equal(i, &r),
                    }),
                    _ => text(&l).to_lowercase().contains(&text(&r).to_lowercase()),
                },
                (Some(l), Some(r), cmp) => order(&l, &r).is_some_and(|o| match cmp {
                    Op::Gt => o == Ordering::Greater,
                    Op::Ge => o != Ordering::Less,
                    Op::Lt => o == Ordering::Less,
                    Op::Le => o != Ordering::Greater,
                    _ => false,
                }),
                _ => false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(value: &str) -> (String, Vec<(String, String)>) {
        (
            value.to_string(),
            vec![("trace".to_string(), "abc-1".to_string())],
        )
    }

    fn check(expr: &str, value: &str) -> bool {
        let (value, headers) = rec(value);
        let view = RecordView {
            key: Some("ord-17"),
            value: Some(&value),
            headers: &headers,
            partition: 2,
            offset: 40,
            timestamp_ms: 1_700_000_000_000,
        };
        Expr::parse(expr).unwrap().matches(&view)
    }

    const ORDER: &str = r#"{"id":"o1","amount":120.5,"status":"PAID","tags":["vip","eu"],"customer":{"tier":"gold","age":"42"},"note":null}"#;

    #[test]
    fn compares_fields_numbers_and_text() {
        assert!(check(r#"$.amount > 100 && $.status == "PAID""#, ORDER));
        assert!(!check(r#"$.amount > 200 || $.status = 'NEW'"#, ORDER));
        assert!(check("$.customer.age >= 40", ORDER)); // numeric string
        assert!(check(r#"$.customer["tier"] == "gold""#, ORDER));
        assert!(check(
            r#"$.tags[1] == "eu" and $.tags contains "VIP" or false"#,
            ORDER
        ));
        assert!(check(r#"$.tags contains "vip""#, ORDER));
        assert!(check(r#"$.id =~ "^O\d""#, ORDER));
        assert!(check("!($.amount < 100)", ORDER));
        assert!(check("not $.missing", ORDER));
    }

    #[test]
    fn numbers_and_numeric_field_names() {
        let doc = r#"{"0":"zero","2fa":true,"rate":0.001,"tags":["Été"]}"#;
        assert!(check(r#"$.0 == "zero""#, doc));
        assert!(check("$.2fa", doc));
        assert!(check("$.rate == 1e-3 && $.rate < 1E+0", doc));
        assert!(check(r#"$.tags contains "éTÉ""#, doc));
    }

    #[test]
    fn existence_null_and_truthiness() {
        assert!(check("$.note exists", ORDER));
        assert!(check("$.note == null", ORDER));
        assert!(check("$.missing == null", ORDER));
        assert!(!check("$.missing exists", ORDER));
        assert!(!check("$.note", ORDER));
        assert!(check("$.amount", ORDER));
    }

    #[test]
    fn repeated_header_resolves_to_the_last_value() {
        // Same rule as `--header-filter`: of a repeated key the last one counts.
        let headers = vec![
            ("x".to_string(), "first".to_string()),
            ("x".to_string(), "last".to_string()),
        ];
        let view = RecordView {
            key: None,
            value: Some("{}"),
            headers: &headers,
            partition: 0,
            offset: 0,
            timestamp_ms: 0,
        };
        assert!(Expr::parse(r#"headers.x == "last""#)
            .unwrap()
            .matches(&view));
        assert!(!Expr::parse(r#"headers.x == "first""#)
            .unwrap()
            .matches(&view));
    }

    #[test]
    fn record_fields_and_plain_text_values() {
        assert!(check(
            r#"key =~ "^ord-" && partition == 2 && offset >= 40"#,
            ORDER
        ));
        assert!(check("timestamp > 1600000000000", ORDER));
        assert!(check(r#"headers.trace == "abc-1""#, ORDER));
        assert!(!check("headers.missing exists", ORDER));
        assert!(check(r#"$ contains "error""#, "disk error on node 3"));
        assert!(!check("$.level", "plain text"));
    }

    #[test]
    fn reports_bad_expressions() {
        for bad in [
            "$.a >",
            "$.a == \"x",
            "$.a =~ 3",
            "colour == 1",
            "$.a [",
            "($.a",
        ] {
            assert!(Expr::parse(bad).is_err(), "{bad}");
        }
        assert!(is_expression("  $.a"));
        assert!(is_expression("$ contains \"x\""));
        assert!(!is_expression("error"));
        assert!(!is_expression("$100"));
        assert!(!is_expression("$USD"));
        assert!(is_expression("?$.a > 1"));
        assert!(is_expression(r#"key =~ "^eu-""#));
        assert!(is_expression("partition==2"));
        assert!(is_expression("headers.trace-id exists"));
        assert!(is_expression(r#"key contains "x""#));
        assert!(!is_expression("key rotation failed"));
        assert!(!is_expression("partition"));
        assert!(!is_expression("headers. exists"));
        assert!(!is_expression("offset existsx"));
        assert_eq!(expression_text("  ?key == \"a\""), "key == \"a\"");
    }

    #[test]
    fn lenient_lexing_matches_strict_on_valid_text_and_keeps_spans() {
        for text in [
            r#"$.amount > 100 && $.status == "PAID""#,
            r#"$.customer["tier"] == 'gold' || !(key =~ "^o\d")"#,
            "$.a.0 >= -1.5e3 and headers.x-trace exists",
            "$.주문.금액 > 1",
        ] {
            let (strict, err) = lex(text, LexMode::Strict);
            assert!(err.is_none(), "{text}");
            assert_eq!(
                lex(text, LexMode::Lenient),
                (strict.clone(), None),
                "{text}"
            );
            for t in &strict {
                assert!(text.get(t.span.clone()).is_some(), "{text}: {:?}", t.span);
            }
        }
        // Spans are byte offsets, also after multi-byte names.
        let text = "$.주문 == 'x";
        let (tokens, err) = lex(text, LexMode::Lenient);
        assert!(err.is_none());
        assert_eq!(tokens[2].token, Token::Ident("주문".into()));
        assert_eq!(&text[tokens[2].span.clone()], "주문");
        assert_eq!(
            tokens[4].token,
            Token::PartialStr {
                quote: '\'',
                text: "x".into()
            }
        );
        assert_eq!(tokens[4].span, 12..text.len());
        assert!(lex(text, LexMode::Strict).1.is_some());
        let (tokens, _) = lex("$.a # 1e999 == 1", LexMode::Lenient);
        assert_eq!(tokens[3].token, Token::Unknown("#".into()));
        assert_eq!(tokens[4].token, Token::Unknown("1e999".into()));
        assert_eq!(tokens[5].token, Token::Op("=="));
        let (tokens, err) = lex("$.a # 1", LexMode::Strict);
        assert_eq!(tokens.len(), 3);
        assert!(err.is_some_and(|e| e.contains("'#'")));
    }

    // These run on the normal test thread on purpose: before the limits,
    // each input overflowed the stack (a crash under panic = "abort").
    #[test]
    fn refuses_deep_nesting_without_overflowing() {
        let deep_parens = format!("{}$.a{}", "(".repeat(1000), ")".repeat(1000));
        let err = Expr::parse(&deep_parens).unwrap_err();
        assert!(err.contains("nests too deeply"), "{err}");
        let deep_nots = format!("{}$.a", "!".repeat(1000));
        assert!(Expr::parse(&deep_nots).unwrap_err().contains("too deeply"));
        let deep_words = format!("{}$.a", "not ".repeat(1000));
        assert!(Expr::parse(&deep_words).is_err());

        let ok = format!("{}$.a{}", "(".repeat(MAX_DEPTH), ")".repeat(MAX_DEPTH));
        assert!(check(&ok, r#"{"a":1}"#));
        assert!(check(
            &format!("{}$.a", "!".repeat(MAX_DEPTH)),
            r#"{"a":1}"#
        ));
    }

    #[test]
    fn long_chains_evaluate_iteratively_and_are_capped() {
        let chain = format!("$.a{}", " || $.a".repeat(999));
        assert!(!check(&chain, r#"{"a":0}"#));
        assert!(check(&format!("{chain} || $.b"), r#"{"a":0,"b":1}"#));
        let chain = format!("$.a{}", " && $.a".repeat(999));
        assert!(check(&chain, r#"{"a":1}"#));

        let too_long = format!("$.a{}", " || $.a".repeat(5000));
        assert!(Expr::parse(&too_long).unwrap_err().contains("too long"));
        let too_many = format!("$.a{}", "||$.a".repeat(MAX_TERMS));
        assert!(Expr::parse(&too_many)
            .unwrap_err()
            .contains("too many terms"));
    }

    #[test]
    fn large_integers_compare_exactly() {
        let doc = r#"{"id":9007199254740993,"big":18446744073709551615,"neg":-9007199254740993,"s":"9007199254740993"}"#;
        assert!(!check("$.id == 9007199254740992", doc));
        assert!(check("$.id == 9007199254740993", doc));
        assert!(check("$.id > 9007199254740992", doc));
        assert!(check("$.id != 9007199254740992", doc));
        assert!(check("$.big == 18446744073709551615", doc));
        assert!(check("$.big > 18446744073709551614", doc));
        assert!(check("$.neg < -9007199254740992", doc));
        assert!(check("$.s == 9007199254740993", doc));
        assert!(!check("$.s == 9007199254740992", doc));
        // Mixed integer/float still compares numerically.
        assert!(check("$.id > 1.5", doc));
        assert!(check("$.x == 2.0", r#"{"x":2}"#));
        assert!(check("$.items[1.0] == 5", r#"{"items":[4,5]}"#));
        assert!(Expr::parse("$.a == 1e999").is_err());
    }
}
