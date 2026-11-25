//! Splitting ksqlDB scripts into statements and classifying them for the
//! company policy, without a full ksqlDB parser.
//!
//! A small lexer drops `--` and `/* */` comments, keeps `'string'`
//! literals (with `''` escapes) and `` `quoted` `` / `"quoted"` identifiers
//! intact, and splits on `;` outside of them. Each statement is then
//! classified by its leading keywords:
//!
//! - **reads**: `SELECT` (pull, or push with `EMIT CHANGES` / `EMIT FINAL`),
//!   `PRINT`, `SHOW` / `LIST`, `DESCRIBE`, `EXPLAIN`, `ASSERT`;
//! - **session** statements the client keeps itself: `SET`, `UNSET`,
//!   `DEFINE`, `UNDEFINE`;
//! - **writes**, each with the policy categories it needs
//!   ([`Statement::policy_actions`]): `CREATE` (streams, tables, types;
//!   `AS SELECT` or a `PARTITIONS` property also creates a Kafka topic),
//!   `CREATE SOURCE|SINK CONNECTOR` and `DROP CONNECTOR` (Kafka Connect),
//!   `DROP` (with `DELETE TOPIC` also deleting the topic), `INSERT`,
//!   `TERMINATE`, `PAUSE`, `RESUME`, `ALTER`;
//! - anything else is [`StatementKind::Unknown`]: a write that needs every
//!   category a ksqlDB statement can need, so a policy that forbids any of
//!   them refuses it. A statement that uses a `${variable}` (other than the
//!   session statements) is unknown too: its text is only known once the
//!   server substitutes it.

use ferrika_core::model::policy::PolicyAction;

use crate::error::{KsqlError, KsqlResult};

/// A lexical token. Comments and whitespace are dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// An unquoted identifier, keyword or number, as written.
    Word(String),
    /// A `` `quoted` `` or `"quoted"` identifier, without the quotes.
    Quoted(String),
    /// A `'string'` literal, unescaped.
    Str(String),
    /// Any other character: punctuation and operators.
    Sym(char),
}

/// What a statement is made of: its tokens and where it sits in the script.
struct Lexed {
    tokens: Vec<Token>,
    start: usize,
    end: usize,
}

/// Which kind of ksqlDB object a `CREATE`, `DROP` or `ALTER` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Stream,
    Table,
    Type,
}

impl ObjectKind {
    fn keyword(self) -> &'static str {
        match self {
            Self::Stream => "STREAM",
            Self::Table => "TABLE",
            Self::Type => "TYPE",
        }
    }

    fn parse(word: &str) -> Option<Self> {
        match word {
            "STREAM" => Some(Self::Stream),
            "TABLE" => Some(Self::Table),
            "TYPE" => Some(Self::Type),
            _ => None,
        }
    }
}

/// What a statement does, from its leading keywords.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementKind {
    /// A pull query, or a push query (`EMIT CHANGES` / `EMIT FINAL`).
    Select {
        push: bool,
    },
    /// `PRINT topic`: raw records of a Kafka topic.
    Print,
    /// `SHOW ...` or `LIST ...`.
    Show,
    Describe,
    Explain,
    /// `ASSERT TOPIC|SCHEMA ...`: checks, changes nothing.
    Assert,
    /// `SET 'property' = 'value'`: a property for the statements after it.
    Set {
        property: String,
        value: String,
    },
    /// `UNSET 'property'`.
    Unset {
        property: String,
    },
    /// `DEFINE variable = 'value'`: a variable for `${variable}`.
    Define {
        variable: String,
        value: String,
    },
    /// `UNDEFINE variable`.
    Undefine {
        variable: String,
    },
    /// `CREATE [OR REPLACE] [SOURCE] STREAM|TABLE|TYPE ...`.
    Create {
        object: ObjectKind,
        /// `CREATE ... AS SELECT`: a persistent query writing to a new topic.
        as_select: bool,
        /// The server creates a Kafka topic (`AS SELECT`, or a `PARTITIONS`
        /// property for a topic that does not exist yet).
        creates_topic: bool,
    },
    /// `CREATE SOURCE|SINK CONNECTOR`: a Kafka Connect connector.
    CreateConnector,
    /// `DROP STREAM|TABLE|TYPE ... [DELETE TOPIC]`.
    Drop {
        object: ObjectKind,
        /// `DELETE TOPIC`: the Kafka topic is deleted too.
        delete_topic: bool,
    },
    /// `DROP CONNECTOR`.
    DropConnector,
    /// `INSERT INTO ... VALUES` (`values`) or `INSERT INTO ... SELECT`.
    Insert {
        values: bool,
    },
    Terminate,
    Pause,
    Resume,
    /// `ALTER STREAM|TABLE ... ADD COLUMN`.
    Alter,
    /// Not recognised: treated as a write that needs every category.
    Unknown,
}

/// Whether a statement reads, only sets session state, or changes the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    Session,
    Write,
}

/// One statement of a script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    /// The statement as written (comments inside it kept), ending in `;`.
    pub text: String,
    pub kind: StatementKind,
    /// The stream, table, type, connector, query id or topic the statement
    /// names: unquoted identifiers in upper case (as ksqlDB stores them),
    /// quoted ones as written.
    pub object: Option<String>,
    /// The `FROM` and `JOIN` sources of a query (also of `CREATE ... AS
    /// SELECT` and `INSERT ... SELECT`).
    pub sources: Vec<String>,
}

/// Every category a classified statement can need; an unknown statement
/// needs all of them and `config.change`, the broadest existing category.
const UNKNOWN_ACTIONS: [PolicyAction; 9] = [
    PolicyAction::ConfigChange,
    PolicyAction::KsqlCreate,
    PolicyAction::KsqlDrop,
    PolicyAction::KsqlInsert,
    PolicyAction::KsqlTerminate,
    PolicyAction::KsqlAlter,
    PolicyAction::TopicsCreate,
    PolicyAction::TopicsDelete,
    PolicyAction::ConnectChange,
];

impl Statement {
    /// Classifies one statement (see [`split`] for scripts). Fails when it
    /// holds no statement or more than one.
    pub fn parse(sql: &str) -> KsqlResult<Self> {
        let mut statements = split(sql)?;
        match statements.len() {
            1 => Ok(statements.remove(0)),
            n => Err(KsqlError::Statement(format!(
                "expected one statement, found {n}"
            ))),
        }
    }

    /// Whether it reads, sets session state, or writes.
    pub fn access(&self) -> Access {
        match self.kind {
            StatementKind::Select { .. }
            | StatementKind::Print
            | StatementKind::Show
            | StatementKind::Describe
            | StatementKind::Explain
            | StatementKind::Assert => Access::Read,
            StatementKind::Set { .. }
            | StatementKind::Unset { .. }
            | StatementKind::Define { .. }
            | StatementKind::Undefine { .. } => Access::Session,
            StatementKind::Create { .. }
            | StatementKind::CreateConnector
            | StatementKind::Drop { .. }
            | StatementKind::DropConnector
            | StatementKind::Insert { .. }
            | StatementKind::Terminate
            | StatementKind::Pause
            | StatementKind::Resume
            | StatementKind::Alter
            | StatementKind::Unknown => Access::Write,
        }
    }

    pub fn is_write(&self) -> bool {
        self.access() == Access::Write
    }

    /// A query that returns rows and runs on `/query-stream` (`SELECT`,
    /// `PRINT`), not on `/ksql`.
    pub fn is_query(&self) -> bool {
        matches!(
            self.kind,
            StatementKind::Select { .. } | StatementKind::Print
        )
    }

    /// The company-policy categories the statement needs allowed, the main
    /// one first, including what each also requires
    /// ([`PolicyAction::also_requires`]). Empty for reads and session
    /// statements.
    pub fn policy_actions(&self) -> Vec<PolicyAction> {
        use PolicyAction as A;
        let direct: &[PolicyAction] = match &self.kind {
            StatementKind::Create {
                object: ObjectKind::Type,
                ..
            } => &[A::KsqlCreate],
            StatementKind::Create {
                creates_topic: true,
                ..
            } => &[A::KsqlCreate, A::TopicsCreate],
            StatementKind::Create { .. } => &[A::KsqlCreate],
            StatementKind::CreateConnector | StatementKind::DropConnector => &[A::ConnectChange],
            StatementKind::Drop {
                delete_topic: true, ..
            } => &[A::KsqlDrop, A::TopicsDelete],
            StatementKind::Drop { .. } => &[A::KsqlDrop],
            StatementKind::Insert { .. } => &[A::KsqlInsert],
            StatementKind::Terminate | StatementKind::Pause | StatementKind::Resume => {
                &[A::KsqlTerminate]
            }
            StatementKind::Alter => &[A::KsqlAlter],
            StatementKind::Unknown => &UNKNOWN_ACTIONS,
            StatementKind::Select { .. }
            | StatementKind::Print
            | StatementKind::Show
            | StatementKind::Describe
            | StatementKind::Explain
            | StatementKind::Assert
            | StatementKind::Set { .. }
            | StatementKind::Unset { .. }
            | StatementKind::Define { .. }
            | StatementKind::Undefine { .. } => &[],
        };
        let mut actions: Vec<PolicyAction> = Vec::new();
        for action in direct {
            for a in std::iter::once(action).chain(action.also_requires()) {
                if !actions.contains(a) {
                    actions.push(*a);
                }
            }
        }
        actions
    }

    /// What the statement does in a few words, for previews and audit
    /// targets: keywords and identifiers only, never literals (an `INSERT`'s
    /// values or a `SET` value can be personal data or a secret).
    pub fn summary(&self) -> String {
        let name = self.object.as_deref().unwrap_or("?");
        match &self.kind {
            StatementKind::Select { push } => {
                let kind = if *push { "push query" } else { "pull query" };
                match self.sources.is_empty() {
                    true => format!("SELECT ({kind})"),
                    false => format!("SELECT FROM {} ({kind})", self.sources.join(", ")),
                }
            }
            StatementKind::Print => format!("PRINT {name}"),
            StatementKind::Show => format!("SHOW {}", self.leading_words(1, 3)),
            StatementKind::Describe => format!("DESCRIBE {}", self.leading_words(1, 3)),
            StatementKind::Explain => format!("EXPLAIN {name}"),
            StatementKind::Assert => format!("ASSERT {}", self.leading_words(1, 2)),
            StatementKind::Set { property, .. } => format!("SET '{property}'"),
            StatementKind::Unset { property } => format!("UNSET '{property}'"),
            StatementKind::Define { variable, .. } => format!("DEFINE {variable}"),
            StatementKind::Undefine { variable } => format!("UNDEFINE {variable}"),
            StatementKind::Create {
                object, as_select, ..
            } => {
                let tail = if *as_select { " AS SELECT" } else { "" };
                format!("CREATE {} {name}{tail}", object.keyword())
            }
            StatementKind::CreateConnector => format!("CREATE CONNECTOR {name}"),
            StatementKind::Drop {
                object,
                delete_topic,
            } => {
                let tail = if *delete_topic { " DELETE TOPIC" } else { "" };
                format!("DROP {} {name}{tail}", object.keyword())
            }
            StatementKind::DropConnector => format!("DROP CONNECTOR {name}"),
            StatementKind::Insert { values: true } => format!("INSERT INTO {name} VALUES"),
            StatementKind::Insert { values: false } => format!("INSERT INTO {name} SELECT"),
            StatementKind::Terminate => format!("TERMINATE {name}"),
            StatementKind::Pause => format!("PAUSE {name}"),
            StatementKind::Resume => format!("RESUME {name}"),
            StatementKind::Alter => format!("ALTER {}", self.leading_words(1, 2)),
            StatementKind::Unknown => match self.first_word() {
                Some(word) => format!("unrecognized statement ({word} ...)"),
                None => "unrecognized statement".to_string(),
            },
        }
    }

    /// The statement's first word in upper case, when it starts with one.
    fn first_word(&self) -> Option<String> {
        lex(&self.text).ok().and_then(|statements| {
            statements.first().and_then(|s| match s.tokens.first() {
                Some(Token::Word(w)) => Some(shorten(&w.to_ascii_uppercase())),
                _ => None,
            })
        })
    }

    /// Up to `count` identifier tokens from position `from`, for summaries.
    fn leading_words(&self, from: usize, count: usize) -> String {
        let tokens = lex(&self.text)
            .ok()
            .and_then(|mut s| (!s.is_empty()).then(|| s.remove(0).tokens))
            .unwrap_or_default();
        tokens
            .iter()
            .skip(from)
            .take(count)
            .map_while(|t| match t {
                Token::Word(w) => Some(shorten(&w.to_ascii_uppercase())),
                Token::Quoted(q) => Some(format!("`{}`", shorten(q))),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn shorten(text: &str) -> String {
    const MAX: usize = 64;
    match text.char_indices().nth(MAX) {
        Some((i, _)) => format!("{}...", &text[..i]),
        None => text.to_string(),
    }
}

/// Splits `sql` into statements (on `;` outside literals, quoted
/// identifiers and comments) and classifies each. Empty statements and
/// comments are dropped; the last statement may omit its `;`. Fails on an
/// unterminated literal, quoted identifier or comment, or when there is no
/// statement at all.
pub fn split(sql: &str) -> KsqlResult<Vec<Statement>> {
    let lexed = lex(sql)?;
    if lexed.is_empty() {
        return Err(KsqlError::Statement("no statement given".to_string()));
    }
    Ok(lexed
        .into_iter()
        .map(|l| {
            let text = format!("{};", sql[l.start..l.end].trim_end());
            let (kind, object, sources) = classify(&l.tokens);
            Statement {
                text,
                kind,
                object,
                sources,
            }
        })
        .collect())
}

/// The statements of `sql` as tokens with their spans (without the `;`).
fn lex(sql: &str) -> KsqlResult<Vec<Lexed>> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut tokens = Vec::new();
    let mut start = None;
    let mut end = 0;
    let mut i = 0;
    let unterminated = |what: &str, at: usize| {
        let line = sql[..at].matches('\n').count() + 1;
        KsqlError::Statement(format!("unterminated {what} starting on line {line}"))
    };
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = sql[i..].find('\n').map_or(bytes.len(), |n| i + n + 1);
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let close = sql[i + 2..]
                    .find("*/")
                    .ok_or_else(|| unterminated("comment", i))?;
                i = i + 2 + close + 2;
            }
            b';' => {
                if let Some(s) = start.take() {
                    out.push(Lexed {
                        tokens: std::mem::take(&mut tokens),
                        start: s,
                        end,
                    });
                }
                i += 1;
            }
            b'\'' | b'`' | b'"' => {
                let (text, next) = quoted(sql, i, c).ok_or_else(|| {
                    unterminated(
                        if c == b'\'' {
                            "string literal"
                        } else {
                            "quoted identifier"
                        },
                        i,
                    )
                })?;
                start.get_or_insert(i);
                tokens.push(if c == b'\'' {
                    Token::Str(text)
                } else {
                    Token::Quoted(text)
                });
                i = next;
                end = next;
            }
            _ => {
                let ch = sql[i..].chars().next().unwrap_or('\0');
                start.get_or_insert(i);
                if ch.is_alphanumeric() || ch == '_' {
                    let len = sql[i..]
                        .char_indices()
                        .find(|(_, c)| !(c.is_alphanumeric() || *c == '_'))
                        .map_or(sql.len() - i, |(n, _)| n);
                    tokens.push(Token::Word(sql[i..i + len].to_string()));
                    i += len;
                } else {
                    tokens.push(Token::Sym(ch));
                    i += ch.len_utf8();
                }
                end = i;
            }
        }
    }
    if let Some(s) = start {
        out.push(Lexed {
            tokens,
            start: s,
            end,
        });
    }
    Ok(out)
}

/// The text of the literal or quoted identifier opening at `at` with
/// `quote` (a doubled quote inside stands for one), and the index after it.
fn quoted(sql: &str, at: usize, quote: u8) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let mut text = Vec::new();
    let mut i = at + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                text.push(quote);
                i += 2;
                continue;
            }
            return Some((String::from_utf8_lossy(&text).into_owned(), i + 1));
        }
        text.push(bytes[i]);
        i += 1;
    }
    None
}

/// Classifies one statement's tokens: kind, the object it names, sources.
fn classify(tokens: &[Token]) -> (StatementKind, Option<String>, Vec<String>) {
    let word = |i: usize| match tokens.get(i) {
        Some(Token::Word(w)) => w.to_ascii_uppercase(),
        _ => String::new(),
    };
    let name = |i: usize| match tokens.get(i) {
        Some(Token::Word(w)) => Some(w.to_ascii_uppercase()),
        Some(Token::Quoted(q)) => Some(q.clone()),
        _ => None,
    };
    let has_word = |w: &str| (0..tokens.len()).any(|i| word(i) == w);
    let followed_by = |first: &str, seconds: &[&str]| {
        (0..tokens.len()).any(|i| word(i) == first && seconds.contains(&word(i + 1).as_str()))
    };
    // `IF [NOT] EXISTS` after position `i`: where the name starts.
    let skip_if_exists = |i: usize| {
        if word(i) == "IF" && word(i + 1) == "NOT" && word(i + 2) == "EXISTS" {
            i + 3
        } else if word(i) == "IF" && word(i + 1) == "EXISTS" {
            i + 2
        } else {
            i
        }
    };
    let sources = || {
        let mut out: Vec<String> = Vec::new();
        for i in 0..tokens.len() {
            if matches!(word(i).as_str(), "FROM" | "JOIN") {
                if let Some(n) = name(i + 1) {
                    if !out.contains(&n) {
                        out.push(n);
                    }
                }
            }
        }
        out
    };
    let string = |i: usize| match tokens.get(i) {
        Some(Token::Str(s)) => Some(s.clone()),
        _ => None,
    };
    let is_sym = |i: usize, c: char| tokens.get(i) == Some(&Token::Sym(c));
    let unknown = (StatementKind::Unknown, None, Vec::new());

    let lead = word(0);
    let session = matches!(lead.as_str(), "SET" | "UNSET" | "DEFINE" | "UNDEFINE");
    let uses_variables = (0..tokens.len()).any(|i| is_sym(i, '$') && is_sym(i + 1, '{'));
    if uses_variables && !session {
        return unknown;
    }
    match lead.as_str() {
        "SELECT" => (
            StatementKind::Select {
                push: followed_by("EMIT", &["CHANGES", "FINAL"]),
            },
            None,
            sources(),
        ),
        "PRINT" => (StatementKind::Print, name(1), Vec::new()),
        "SHOW" | "LIST" => (StatementKind::Show, None, Vec::new()),
        "DESCRIBE" => {
            let object = match word(1).as_str() {
                "STREAMS" | "TABLES" => None,
                "FUNCTION" | "CONNECTOR" => name(2),
                _ => name(1),
            };
            (StatementKind::Describe, object, Vec::new())
        }
        "EXPLAIN" => (StatementKind::Explain, name(1), Vec::new()),
        "ASSERT" => (StatementKind::Assert, None, Vec::new()),
        "SET" if tokens.len() == 4 && is_sym(2, '=') => match (string(1), string(3)) {
            (Some(property), Some(value)) => {
                (StatementKind::Set { property, value }, None, Vec::new())
            }
            _ => unknown,
        },
        "UNSET" if tokens.len() == 2 => match string(1) {
            Some(property) => (StatementKind::Unset { property }, None, Vec::new()),
            None => unknown,
        },
        "DEFINE" if tokens.len() == 4 && is_sym(2, '=') => match (tokens.get(1), string(3)) {
            (Some(Token::Word(variable)), Some(value)) => (
                StatementKind::Define {
                    variable: variable.clone(),
                    value,
                },
                None,
                Vec::new(),
            ),
            _ => unknown,
        },
        "UNDEFINE" if tokens.len() == 2 => match tokens.get(1) {
            Some(Token::Word(variable)) => (
                StatementKind::Undefine {
                    variable: variable.clone(),
                },
                None,
                Vec::new(),
            ),
            _ => unknown,
        },
        "CREATE" => {
            let mut i = 1;
            if word(i) == "OR" && word(i + 1) == "REPLACE" {
                i += 2;
            }
            if matches!(word(i).as_str(), "SOURCE" | "SINK") && word(i + 1) == "CONNECTOR" {
                let at = skip_if_exists(i + 2);
                return (StatementKind::CreateConnector, name(at), Vec::new());
            }
            if word(i) == "SOURCE" {
                i += 1;
            }
            let Some(object) = ObjectKind::parse(&word(i)) else {
                return unknown;
            };
            let at = skip_if_exists(i + 1);
            let as_select = object != ObjectKind::Type && followed_by("AS", &["SELECT"]);
            let creates_topic = object != ObjectKind::Type && (as_select || has_word("PARTITIONS"));
            (
                StatementKind::Create {
                    object,
                    as_select,
                    creates_topic,
                },
                name(at),
                if as_select { sources() } else { Vec::new() },
            )
        }
        "DROP" => {
            if word(1) == "CONNECTOR" {
                return (
                    StatementKind::DropConnector,
                    name(skip_if_exists(2)),
                    Vec::new(),
                );
            }
            let Some(object) = ObjectKind::parse(&word(1)) else {
                return unknown;
            };
            let n = tokens.len();
            let delete_topic = n >= 2 && word(n - 2) == "DELETE" && word(n - 1) == "TOPIC";
            (
                StatementKind::Drop {
                    object,
                    delete_topic,
                },
                name(skip_if_exists(2)),
                Vec::new(),
            )
        }
        "INSERT" if word(1) == "INTO" => {
            let values = !has_word("SELECT");
            (
                StatementKind::Insert { values },
                name(2),
                if values { Vec::new() } else { sources() },
            )
        }
        "TERMINATE" => (StatementKind::Terminate, name(1), Vec::new()),
        "PAUSE" => (StatementKind::Pause, name(1), Vec::new()),
        "RESUME" => (StatementKind::Resume, name(1), Vec::new()),
        "ALTER" if matches!(word(1).as_str(), "STREAM" | "TABLE") => {
            (StatementKind::Alter, name(2), Vec::new())
        }
        _ => unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use PolicyAction as A;

    fn one(sql: &str) -> Statement {
        Statement::parse(sql).unwrap()
    }

    fn kind(sql: &str) -> StatementKind {
        one(sql).kind
    }

    #[test]
    fn reads_are_classified_by_their_first_keyword() {
        assert_eq!(
            kind("SELECT * FROM orders;"),
            StatementKind::Select { push: false }
        );
        assert_eq!(
            kind("select * from orders emit changes"),
            StatementKind::Select { push: true }
        );
        assert_eq!(
            kind("SELECT id, COUNT(*) FROM o WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY id EMIT FINAL;"),
            StatementKind::Select { push: true }
        );
        for sql in [
            "SHOW STREAMS;",
            "list tables",
            "SHOW QUERIES EXTENDED;",
            "show properties",
        ] {
            assert_eq!(kind(sql), StatementKind::Show, "{sql}");
        }
        assert_eq!(kind("DESCRIBE orders EXTENDED;"), StatementKind::Describe);
        assert_eq!(kind("EXPLAIN CTAS_X_1;"), StatementKind::Explain);
        assert_eq!(kind("PRINT 'orders' FROM BEGINNING;"), StatementKind::Print);
        assert_eq!(
            kind("ASSERT TOPIC orders TIMEOUT 3 SECONDS;"),
            StatementKind::Assert
        );
        for sql in ["SELECT 1 FROM t", "PRINT t", "SHOW TOPICS", "DESCRIBE t"] {
            let s = one(sql);
            assert_eq!(s.access(), Access::Read, "{sql}");
            assert!(s.policy_actions().is_empty(), "{sql}");
        }
        assert!(one("SELECT 1 FROM t").is_query());
        assert!(one("PRINT t").is_query());
        assert!(!one("SHOW STREAMS").is_query());
    }

    #[test]
    fn create_statements_need_ksql_create_and_maybe_topics_create() {
        let s = one("CREATE STREAM kq_orders (id INT KEY, v STRING) WITH (KAFKA_TOPIC='kq_orders', VALUE_FORMAT='JSON');");
        assert_eq!(
            s.kind,
            StatementKind::Create {
                object: ObjectKind::Stream,
                as_select: false,
                creates_topic: false
            }
        );
        assert_eq!(s.object.as_deref(), Some("KQ_ORDERS"));
        assert_eq!(s.policy_actions(), [A::KsqlCreate]);
        assert_eq!(s.summary(), "CREATE STREAM KQ_ORDERS");

        // PARTITIONS makes the server create the topic when it is missing.
        let s = one(
            "create stream x (id int) with (kafka_topic='x', partitions=3, value_format='json')",
        );
        assert_eq!(s.policy_actions(), [A::KsqlCreate, A::TopicsCreate]);

        let s =
            one("CREATE STREAM kq_big AS SELECT * FROM kq_orders WHERE amount > 10 EMIT CHANGES;");
        assert_eq!(
            s.kind,
            StatementKind::Create {
                object: ObjectKind::Stream,
                as_select: true,
                creates_topic: true
            }
        );
        assert_eq!(s.object.as_deref(), Some("KQ_BIG"));
        assert_eq!(s.sources, ["KQ_ORDERS"]);
        assert_eq!(s.policy_actions(), [A::KsqlCreate, A::TopicsCreate]);
        assert_eq!(s.summary(), "CREATE STREAM KQ_BIG AS SELECT");

        let s = one("CREATE OR REPLACE TABLE IF NOT EXISTS `Per User` AS SELECT u, COUNT(*) FROM a JOIN b WITHIN 1 HOUR ON a.id = b.id GROUP BY u;");
        assert_eq!(s.object.as_deref(), Some("Per User"));
        assert_eq!(s.sources, ["A", "B"]);
        assert!(matches!(
            s.kind,
            StatementKind::Create {
                object: ObjectKind::Table,
                as_select: true,
                ..
            }
        ));

        let s = one("CREATE SOURCE TABLE t (id INT PRIMARY KEY) WITH (KAFKA_TOPIC='t', VALUE_FORMAT='JSON');");
        assert_eq!(s.object.as_deref(), Some("T"));
        assert_eq!(s.policy_actions(), [A::KsqlCreate]);

        // A type: no topic, and its `AS STRUCT<...>` is not a query.
        let s = one("CREATE TYPE address AS STRUCT<street STRING, zip INT>;");
        assert_eq!(
            s.kind,
            StatementKind::Create {
                object: ObjectKind::Type,
                as_select: false,
                creates_topic: false
            }
        );
        assert_eq!(s.policy_actions(), [A::KsqlCreate]);
    }

    #[test]
    fn connectors_are_connect_changes() {
        let s = one("CREATE SOURCE CONNECTOR IF NOT EXISTS jdbc WITH ('connector.class'='x');");
        assert_eq!(s.kind, StatementKind::CreateConnector);
        assert_eq!(s.object.as_deref(), Some("JDBC"));
        assert_eq!(s.policy_actions(), [A::ConnectChange]);
        let s = one("DROP CONNECTOR IF EXISTS `jdbc`;");
        assert_eq!(s.kind, StatementKind::DropConnector);
        assert_eq!(s.object.as_deref(), Some("jdbc"));
        assert_eq!(s.policy_actions(), [A::ConnectChange]);
    }

    #[test]
    fn drop_needs_ksql_drop_and_with_delete_topic_topics_delete() {
        let s = one("DROP STREAM IF EXISTS kq_orders;");
        assert_eq!(
            s.kind,
            StatementKind::Drop {
                object: ObjectKind::Stream,
                delete_topic: false
            }
        );
        assert_eq!(s.object.as_deref(), Some("KQ_ORDERS"));
        assert_eq!(s.policy_actions(), [A::KsqlDrop]);
        let s = one("drop table kq_t delete topic");
        assert_eq!(s.policy_actions(), [A::KsqlDrop, A::TopicsDelete]);
        assert_eq!(s.summary(), "DROP TABLE KQ_T DELETE TOPIC");
        assert_eq!(one("DROP TYPE address;").policy_actions(), [A::KsqlDrop]);
    }

    #[test]
    fn insert_needs_ksql_insert_and_produce() {
        let s = one("INSERT INTO kq_orders (id, email) VALUES (1, 'jane@example.com');");
        assert_eq!(s.kind, StatementKind::Insert { values: true });
        assert_eq!(s.policy_actions(), [A::KsqlInsert, A::Produce]);
        // The summary never carries the values.
        assert_eq!(s.summary(), "INSERT INTO KQ_ORDERS VALUES");
        assert!(!s.summary().contains("jane"));
        let s = one("INSERT INTO all_orders SELECT * FROM eu_orders EMIT CHANGES;");
        assert_eq!(s.kind, StatementKind::Insert { values: false });
        assert_eq!(s.sources, ["EU_ORDERS"]);
        assert_eq!(s.summary(), "INSERT INTO ALL_ORDERS SELECT");
    }

    #[test]
    fn query_control_and_alter() {
        for (sql, k) in [
            ("TERMINATE CTAS_X_1;", StatementKind::Terminate),
            ("terminate all", StatementKind::Terminate),
            ("PAUSE CSAS_Y_3;", StatementKind::Pause),
            ("RESUME ALL;", StatementKind::Resume),
        ] {
            let s = one(sql);
            assert_eq!(s.kind, k, "{sql}");
            assert_eq!(s.policy_actions(), [A::KsqlTerminate], "{sql}");
        }
        assert_eq!(
            one("TERMINATE CTAS_X_1").object.as_deref(),
            Some("CTAS_X_1")
        );
        let s = one("ALTER STREAM kq_orders ADD COLUMN note STRING;");
        assert_eq!(s.kind, StatementKind::Alter);
        assert_eq!(s.object.as_deref(), Some("KQ_ORDERS"));
        assert_eq!(s.policy_actions(), [A::KsqlAlter]);
        assert_eq!(s.summary(), "ALTER STREAM KQ_ORDERS");
    }

    #[test]
    fn unknown_statements_need_every_category() {
        for sql in [
            "RUN SCRIPT '/tmp/x.sql';",
            "ALTER SYSTEM 'ksql.x'='y';",
            "CREATE MATERIALIZED VIEW v AS SELECT 1;",
            "DROP SOMETHING x;",
            "(SELECT 1);",
            "EXPORT everything;",
            "INSERT orders VALUES (1);",
        ] {
            let s = one(sql);
            assert_eq!(s.kind, StatementKind::Unknown, "{sql}");
            assert_eq!(s.access(), Access::Write, "{sql}");
            let actions = s.policy_actions();
            for a in [
                A::ConfigChange,
                A::KsqlCreate,
                A::KsqlDrop,
                A::KsqlInsert,
                A::KsqlTerminate,
                A::KsqlAlter,
                A::TopicsCreate,
                A::TopicsDelete,
                A::ConnectChange,
                A::Produce,
            ] {
                assert!(actions.contains(&a), "{sql}: {a:?}");
            }
        }
        assert_eq!(
            one("RUN SCRIPT '/tmp/x.sql';").summary(),
            "unrecognized statement (RUN ...)"
        );
        assert_eq!(one("(SELECT 1)").summary(), "unrecognized statement");
    }

    #[test]
    fn variables_make_a_statement_unknown() {
        assert_eq!(kind("SELECT * FROM ${src};"), StatementKind::Unknown);
        assert_eq!(kind("${verb} STREAM s;"), StatementKind::Unknown);
        // Inside a literal it is only text.
        assert_eq!(
            kind("INSERT INTO s (v) VALUES ('${x}');"),
            StatementKind::Insert { values: true }
        );
        assert_eq!(
            kind("DEFINE src = 'orders';"),
            StatementKind::Define {
                variable: "src".into(),
                value: "orders".into()
            }
        );
    }

    #[test]
    fn session_statements() {
        assert_eq!(
            kind("SET 'auto.offset.reset' = 'earliest';"),
            StatementKind::Set {
                property: "auto.offset.reset".into(),
                value: "earliest".into()
            }
        );
        assert_eq!(
            kind("unset 'auto.offset.reset'"),
            StatementKind::Unset {
                property: "auto.offset.reset".into()
            }
        );
        assert_eq!(
            kind("UNDEFINE src;"),
            StatementKind::Undefine {
                variable: "src".into()
            }
        );
        let s = one("SET 'ssl.key.password'='hunter2';");
        assert_eq!(s.access(), Access::Session);
        assert!(s.policy_actions().is_empty());
        assert!(!s.summary().contains("hunter2"), "{}", s.summary());
        // Malformed session statements are unknown, so they are gated.
        assert_eq!(
            kind("SET auto.offset.reset = earliest;"),
            StatementKind::Unknown
        );
    }

    #[test]
    fn comments_strings_and_quoted_identifiers_do_not_split_or_classify() {
        let script = "-- leading comment; DROP STREAM nope;\n\
            /* block; DROP TABLE nope2; */ CREATE STREAM `semi;colon` (v STRING) \
            WITH (KAFKA_TOPIC='a;b', VALUE_FORMAT='JSON');\n\
            INSERT INTO `semi;colon` (v) VALUES ('it''s; -- not a comment');\n\
            -- trailing comment only\n";
        let statements = split(script).unwrap();
        assert_eq!(statements.len(), 2, "{statements:#?}");
        assert!(matches!(
            statements[0].kind,
            StatementKind::Create {
                object: ObjectKind::Stream,
                ..
            }
        ));
        assert_eq!(statements[0].object.as_deref(), Some("semi;colon"));
        assert!(statements[0].text.starts_with("CREATE STREAM"));
        assert!(statements[0].text.ends_with("VALUE_FORMAT='JSON');"));
        assert_eq!(statements[1].kind, StatementKind::Insert { values: true });
        assert!(statements[1]
            .text
            .ends_with("VALUES ('it''s; -- not a comment');"));
        // A comment inside a statement stays in its text.
        let s = one("SELECT /* x; */ * FROM t -- y;\n EMIT CHANGES");
        assert_eq!(s.kind, StatementKind::Select { push: true });
        assert_eq!(s.text, "SELECT /* x; */ * FROM t -- y;\n EMIT CHANGES;");
        // Keywords inside literals or quoted names do not count.
        assert_eq!(
            kind("SELECT 'EMIT CHANGES' FROM t;"),
            StatementKind::Select { push: false }
        );
        assert_eq!(
            kind("DROP STREAM `x DELETE TOPIC`;"),
            StatementKind::Drop {
                object: ObjectKind::Stream,
                delete_topic: false
            }
        );
        assert_eq!(
            kind("CREATE STREAM s WITH (KAFKA_TOPIC='PARTITIONS');"),
            StatementKind::Create {
                object: ObjectKind::Stream,
                as_select: false,
                creates_topic: false
            }
        );
    }

    #[test]
    fn multiple_statements_keep_their_order_and_categories() {
        let statements = split(
            "create stream a (v string) with (kafka_topic='a', value_format='json');\
             insert into a (v) values ('x');;  \
             SHOW STREAMS;\
             terminate CSAS_B_1;\
             drop stream a delete topic",
        )
        .unwrap();
        let kinds: Vec<Access> = statements.iter().map(Statement::access).collect();
        assert_eq!(
            kinds,
            [
                Access::Write,
                Access::Write,
                Access::Read,
                Access::Write,
                Access::Write
            ]
        );
        assert_eq!(statements[4].text, "drop stream a delete topic;");
        assert_eq!(
            statements[4].policy_actions(),
            [A::KsqlDrop, A::TopicsDelete]
        );
    }

    #[test]
    fn broken_scripts_are_errors() {
        for (sql, what) in [
            ("SELECT 'open FROM t;", "string literal"),
            ("SELECT * FROM `open;", "quoted identifier"),
            ("SHOW STREAMS; /* never closed", "comment"),
        ] {
            let err = split(sql).unwrap_err().to_string();
            assert!(err.contains(what), "{sql}: {err}");
        }
        assert!(split("  -- nothing\n ;; ").is_err());
        assert!(Statement::parse("SHOW STREAMS; SHOW TABLES;").is_err());
    }

    #[test]
    fn unicode_and_long_names_are_handled() {
        let s = one("DESCRIBE 주문;");
        assert_eq!(s.object.as_deref(), Some("주문"));
        let long = "X".repeat(200);
        let s = one(&format!("FROB {long};"));
        assert!(s.summary().len() < 120, "{}", s.summary());
    }
}
