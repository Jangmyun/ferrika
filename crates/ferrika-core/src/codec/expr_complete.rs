//! Autocomplete for filter expressions: what may come at the cursor (a
//! field path, header name, operator, value or `&&`), ranked against the
//! fields a topic's schema and records give. No IO; runs on every key press,
//! so it stays within the expression language's own limits.

use std::ops::Range;

use crate::codec::field_infer::SampleCatalog;
use crate::codec::schema_fields::{
    quote, render_path, render_step, FieldCatalog, FieldKind, PathStep,
};
use crate::model::expr::{lex, LexMode, Spanned, Token};

/// Text longer than this (the expression limit) gets no suggestions.
const MAX_TEXT: usize = 16 * 1024;
/// Suggestions made at most.
const MAX_SUGGESTIONS: usize = 50;

/// Where a merged field came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Schema,
    Sample,
    Both,
}

impl Source {
    pub fn label(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::Sample => "sample",
            Self::Both => "schema+sample",
        }
    }
}

/// One field of a topic: from its schema(s), its records, or both.
#[derive(Debug, Clone, PartialEq)]
pub struct MergedEntry {
    pub steps: Vec<PathStep>,
    pub kind: FieldKind,
    pub optional: bool,
    /// From the schema: always shown.
    pub enum_values: Vec<String>,
    /// From records: only present when the masking conditions allowed them.
    pub sample_values: Vec<String>,
    pub recursive: bool,
    pub group: Option<String>,
    pub source: Source,
    /// Share of records the path was present in, when records were read.
    pub seen_ratio: Option<f32>,
}

/// The fields filter autocomplete offers for one topic.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MergedCatalog {
    pub entries: Vec<MergedEntry>,
    /// Header names from records, most seen first.
    pub headers: Vec<String>,
    pub notes: Vec<String>,
}

impl MergedCatalog {
    /// Schema fields, plus record fields where the schema leaves room: no
    /// schema at all, a truncated catalog, or below an unknown, mixed, map
    /// or recursive schema field.
    pub fn merge(schemas: &[&FieldCatalog], sample: Option<&SampleCatalog>) -> Self {
        let mut out = MergedCatalog::default();
        let mut truncated = false;
        for catalog in schemas {
            truncated |= catalog.truncated;
            for note in &catalog.notes {
                if !out.notes.contains(note) {
                    out.notes.push(note.clone());
                }
            }
            for e in &catalog.entries {
                match out.entries.iter_mut().find(|m| m.steps == e.steps) {
                    Some(m) => {
                        if m.kind != e.kind {
                            m.kind =
                                FieldKind::combine(vec![m.kind.clone(), e.kind.clone()], false);
                        }
                        m.optional |= e.optional;
                        m.recursive |= e.recursive;
                        for v in &e.enum_values {
                            if !m.enum_values.contains(v) {
                                m.enum_values.push(v.clone());
                            }
                        }
                    }
                    None => out.entries.push(MergedEntry {
                        steps: e.steps.clone(),
                        kind: e.kind.clone(),
                        optional: e.optional,
                        enum_values: e.enum_values.clone(),
                        sample_values: Vec::new(),
                        recursive: e.recursive,
                        group: e.group.clone(),
                        source: Source::Schema,
                        seen_ratio: None,
                    }),
                }
            }
        }
        let schema_count = out.entries.len();
        if let Some(sample) = sample {
            let records = sample.records.max(1) as f32;
            for s in &sample.entries {
                let ratio = s.seen as f32 / records;
                let values = s.values.clone().unwrap_or_default();
                if let Some(m) = out.entries[..schema_count]
                    .iter_mut()
                    .find(|m| m.steps == s.steps)
                {
                    m.source = Source::Both;
                    m.seen_ratio = Some(ratio);
                    m.sample_values = values;
                    continue;
                }
                let open = schema_count == 0 || truncated || {
                    // The nearest schema field above this path.
                    let ancestor = (0..s.steps.len()).rev().find_map(|len| {
                        out.entries[..schema_count]
                            .iter()
                            .find(|m| m.steps == s.steps[..len])
                    });
                    ancestor.is_some_and(|a| {
                        a.recursive
                            || matches!(
                                a.kind,
                                FieldKind::Unknown | FieldKind::Mixed | FieldKind::Map(_)
                            )
                    })
                };
                if open {
                    out.entries.push(MergedEntry {
                        steps: s.steps.clone(),
                        kind: s.kind.clone(),
                        optional: false,
                        enum_values: Vec::new(),
                        sample_values: values,
                        recursive: false,
                        group: None,
                        source: Source::Sample,
                        seen_ratio: Some(ratio),
                    });
                }
            }
            out.headers = sample.headers.iter().map(|(h, _)| h.clone()).collect();
        }
        out
    }

    fn get(&self, steps: &[PathStep]) -> Option<&MergedEntry> {
        self.entries.iter().find(|e| e.steps == steps)
    }

    fn children<'a>(
        &'a self,
        parent: &'a [PathStep],
    ) -> impl Iterator<Item = &'a MergedEntry> + 'a {
        self.entries
            .iter()
            .filter(move |e| e.steps.len() == parent.len() + 1 && e.steps.starts_with(parent))
    }

    fn has_children(&self, parent: &[PathStep]) -> bool {
        self.children(parent).next().is_some()
    }
}

/// What a suggestion inserts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuggestKind {
    /// A field path (or record field) of the payload.
    Field,
    /// `key`, `partition`, `offset`, `timestamp`, `headers.`.
    RecordField,
    Header,
    Operator,
    Value,
    /// `(`, `!`, `&&`, `||`, `)`.
    Keyword,
}

impl SuggestKind {
    /// The short tag shown before the label.
    pub fn tag(self) -> &'static str {
        match self {
            Self::Field => "field",
            Self::RecordField => "record",
            Self::Header => "header",
            Self::Operator => "op",
            Self::Value => "value",
            Self::Keyword => "kw",
        }
    }
}

/// One candidate: accepting it replaces `replace` with `insert`.
#[derive(Debug, Clone, PartialEq)]
pub struct Suggestion {
    pub label: String,
    pub insert: String,
    /// Byte range of the text to replace.
    pub replace: Range<usize>,
    pub kind: SuggestKind,
    pub detail: Option<String>,
    pub score: i32,
    /// Show suggestions again after accepting (a record path, an operator).
    pub reopen: bool,
}

impl Suggestion {
    /// The text after accepting, and the cursor (a byte offset) in it.
    pub fn apply(&self, text: &str) -> (String, usize) {
        let start = self.replace.start.min(text.len());
        let end = self.replace.end.clamp(start, text.len());
        let mut out = String::with_capacity(text.len() + self.insert.len());
        out.push_str(&text[..start]);
        out.push_str(&self.insert);
        out.push_str(&text[end..]);
        (out, start + self.insert.len())
    }
}

/// Context the caller knows beyond the text.
#[derive(Debug, Clone, Default)]
pub struct CompleteOptions {
    /// Paths (`$.status`) and header names used in saved filters and in
    /// filters applied this session; they rank higher.
    pub history: Vec<String>,
}

/// Paths and header names an expression mentions, for [`CompleteOptions::history`].
pub fn mentioned_paths(text: &str) -> Vec<String> {
    let (tokens, _) = lex(text, LexMode::Lenient);
    let mut out = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        match &tokens[i].token {
            Token::Dollar => {
                let mut steps = Vec::new();
                let mut j = i + 1;
                while j + 1 < tokens.len() {
                    match (&tokens[j].token, &tokens[j + 1].token) {
                        (Token::Dot, Token::Ident(n) | Token::Str(n)) => {
                            steps.push(PathStep::Field(n.clone()));
                            j += 2;
                        }
                        (Token::LBracket, Token::Str(n)) => {
                            steps.push(PathStep::Field(n.clone()));
                            j += 3;
                        }
                        (Token::LBracket, Token::Num(_)) => {
                            steps.push(PathStep::AnyIndex);
                            j += 3;
                        }
                        _ => break,
                    }
                }
                if !steps.is_empty() {
                    out.push(render_path(&steps));
                }
                i = j;
            }
            Token::Ident(word) if word.eq_ignore_ascii_case("headers") => {
                if let (Some(Token::Dot), Some(Token::Ident(h) | Token::Str(h))) = (
                    tokens.get(i + 1).map(|t| &t.token),
                    tokens.get(i + 2).map(|t| &t.token),
                ) {
                    out.push(format!("headers.{h}"));
                }
                i += 3;
            }
            _ => i += 1,
        }
    }
    out
}

/// How good a match `candidate` is for what was typed; `None` for no match.
/// Prefix beats word-boundary prefix beats substring beats in-order letters.
pub fn match_score(candidate: &str, typed: &str) -> Option<i32> {
    if typed.is_empty() {
        return Some(0);
    }
    let c = candidate.to_lowercase();
    let t = typed.to_lowercase();
    if c.starts_with(&t) {
        return Some(1000 - (c.len() as i32 - t.len() as i32).min(100));
    }
    // Word starts: after `_`, `-`, `.`, `[`, `"` or a lower-to-upper case change.
    let chars: Vec<char> = candidate.chars().collect();
    for (i, ch) in candidate.char_indices() {
        let ix = candidate[..i].chars().count();
        let boundary = ix > 0 && {
            let prev = chars[ix - 1];
            matches!(prev, '_' | '-' | '.' | '[' | '"' | ' ')
                || (prev.is_lowercase() && ch.is_uppercase())
        };
        if boundary && candidate[i..].to_lowercase().starts_with(&t) {
            return Some(700);
        }
    }
    if c.contains(&t) {
        return Some(400);
    }
    let mut rest = c.chars();
    t.chars().all(|tc| rest.any(|cc| cc == tc)).then_some(100)
}

/// A field path as typed (indexes stay numbers).
#[derive(Debug, Clone, PartialEq)]
enum Typed {
    Field(String),
    Index,
}

/// What the expression holds just before the cursor.
#[derive(Debug, Clone, PartialEq)]
enum State {
    /// An operand may start here.
    Operand,
    /// Words that are not a known name (`cust.na`), matched against whole
    /// paths; the byte offset where they start.
    Loose(usize),
    /// After `$` or a path step: more steps, an operator, ...
    Path(Vec<Typed>),
    /// After `<path>.`
    PathDot(Vec<Typed>),
    /// After `<path>[`
    PathBracket(Vec<Typed>),
    /// After `<path>[0` or `<path>["x"`, waiting for `]`.
    PathClose(Vec<Typed>, Typed),
    Headers,
    HeadersDot,
    /// After a complete operand other than a path.
    Operand2(Left),
    /// After an operator, waiting for its right side.
    Value(Left, &'static str),
    /// After a whole comparison.
    Done,
    /// Text that does not parse; nothing to suggest.
    Lost,
}

/// The left side of a comparison.
#[derive(Debug, Clone, PartialEq)]
enum Left {
    Path(Vec<Typed>),
    Builtin(&'static str),
    Header,
    Literal,
}

fn builtin(word: &str) -> Option<&'static str> {
    ["key", "partition", "offset", "timestamp"]
        .into_iter()
        .find(|b| word.eq_ignore_ascii_case(b))
}

fn comparison(token: &Token) -> Option<&'static str> {
    match token {
        Token::Op(op) if !matches!(*op, "&&" | "||" | "!") => Some(op),
        Token::Ident(w) if w.eq_ignore_ascii_case("contains") => Some("contains"),
        _ => None,
    }
}

fn is_word(token: &Token, word: &str) -> bool {
    matches!(token, Token::Ident(w) if w.eq_ignore_ascii_case(word))
}

fn joins(token: &Token) -> bool {
    matches!(token, Token::Op("&&" | "||")) || is_word(token, "and") || is_word(token, "or")
}

/// Runs the tokens before the cursor through the grammar; returns the state
/// and how many `(` are open.
fn read_state(tokens: &[Spanned]) -> (State, usize) {
    let mut state = State::Operand;
    let mut depth = 0usize;
    for t in tokens {
        let tok = &t.token;
        state = match state {
            State::Lost => return (State::Lost, depth),
            State::Operand => match tok {
                Token::LParen => {
                    depth += 1;
                    State::Operand
                }
                Token::Op("!") => State::Operand,
                _ if is_word(tok, "not") => State::Operand,
                Token::Dollar => State::Path(Vec::new()),
                Token::Ident(w) if w.eq_ignore_ascii_case("headers") => State::Headers,
                Token::Ident(w) => match builtin(w) {
                    Some(b) => State::Operand2(Left::Builtin(b)),
                    None if ["true", "false", "null"]
                        .iter()
                        .any(|l| w.eq_ignore_ascii_case(l)) =>
                    {
                        State::Operand2(Left::Literal)
                    }
                    None => State::Loose(t.span.start),
                },
                Token::Str(_) | Token::Num(_) => State::Operand2(Left::Literal),
                _ => State::Lost,
            },
            State::Loose(start) => match tok {
                Token::Dot | Token::Ident(_) | Token::Num(_) => State::Loose(start),
                _ => State::Lost,
            },
            State::Path(steps) => match tok {
                Token::Dot => State::PathDot(steps),
                Token::LBracket => State::PathBracket(steps),
                _ => after_operand(Left::Path(steps), tok, &mut depth),
            },
            State::PathDot(mut steps) => match tok {
                Token::Ident(n) | Token::Str(n) => {
                    steps.push(Typed::Field(n.clone()));
                    State::Path(steps)
                }
                Token::Num(n) => {
                    steps.push(Typed::Field(n.to_string()));
                    State::Path(steps)
                }
                _ => State::Lost,
            },
            State::PathBracket(steps) => match tok {
                Token::Num(_) => State::PathClose(steps, Typed::Index),
                Token::Str(n) => State::PathClose(steps, Typed::Field(n.clone())),
                _ => State::Lost,
            },
            State::PathClose(mut steps, step) => match tok {
                Token::RBracket => {
                    steps.push(step);
                    State::Path(steps)
                }
                _ => State::Lost,
            },
            State::Headers => match tok {
                Token::Dot => State::HeadersDot,
                _ => State::Lost,
            },
            State::HeadersDot => match tok {
                Token::Ident(_) | Token::Str(_) => State::Operand2(Left::Header),
                _ => State::Lost,
            },
            State::Operand2(left) => after_operand(left, tok, &mut depth),
            State::Value(_, _) => match tok {
                Token::Str(_) | Token::Num(_) => State::Done,
                Token::Ident(w)
                    if ["true", "false", "null"]
                        .iter()
                        .any(|l| w.eq_ignore_ascii_case(l))
                        || builtin(w).is_some() =>
                {
                    State::Done
                }
                // A path on the right side: read it, then it ends the comparison.
                Token::Dollar => State::Path(Vec::new()),
                _ => State::Lost,
            },
            State::Done => match tok {
                _ if joins(tok) => State::Operand,
                Token::RParen if depth > 0 => {
                    depth -= 1;
                    State::Done
                }
                _ => State::Lost,
            },
        };
    }
    (state, depth)
}

fn after_operand(left: Left, tok: &Token, depth: &mut usize) -> State {
    if let Some(op) = comparison(tok) {
        return State::Value(left, op);
    }
    if is_word(tok, "exists") {
        return State::Done;
    }
    if joins(tok) {
        return State::Operand;
    }
    match tok {
        Token::RParen if *depth > 0 => {
            *depth -= 1;
            State::Done
        }
        _ => State::Lost,
    }
}

/// The catalog path a typed path points at: names as they are (a map's
/// keys as "any key"), indexes as "any element".
fn resolve(catalog: &MergedCatalog, typed: &[Typed]) -> Option<Vec<PathStep>> {
    let mut steps: Vec<PathStep> = Vec::new();
    for t in typed {
        let next = match t {
            Typed::Index => PathStep::AnyIndex,
            Typed::Field(name) => {
                let mut exact = steps.clone();
                exact.push(PathStep::Field(name.clone()));
                if catalog.get(&exact).is_some() {
                    PathStep::Field(name.clone())
                } else {
                    PathStep::AnyKey
                }
            }
        };
        steps.push(next);
        catalog.get(&steps)?;
    }
    Some(steps)
}

/// Operators in the order that suits a left side of `kind`.
fn operators_for(kind: Option<&FieldKind>) -> &'static [&'static str] {
    const NUMERIC: &[&str] = &[">", ">=", "<", "<=", "==", "!=", "exists"];
    const TEXT: &[&str] = &["==", "!=", "=~", "contains", "exists"];
    const EXACT: &[&str] = &["==", "!=", "exists"];
    const ARRAY: &[&str] = &["contains", "exists"];
    const OBJECT: &[&str] = &["exists"];
    const ALL: &[&str] = &["==", "!=", ">", ">=", "<", "<=", "=~", "contains", "exists"];
    match kind {
        Some(FieldKind::Integer | FieldKind::Number) => NUMERIC,
        Some(FieldKind::String | FieldKind::Bytes) => TEXT,
        Some(FieldKind::Enum | FieldKind::Boolean) => EXACT,
        Some(FieldKind::Array(_)) => ARRAY,
        Some(FieldKind::Record | FieldKind::Map(_)) => OBJECT,
        _ => ALL,
    }
}

fn detail(entry: &MergedEntry) -> String {
    let mut parts = vec![entry.kind.label()];
    if !entry.enum_values.is_empty() {
        parts[0] = format!("{} · {} values", parts[0], entry.enum_values.len());
    }
    if entry.optional {
        parts.push("optional".into());
    }
    if entry.recursive {
        parts.push("recursive".into());
    }
    if let Some(group) = &entry.group {
        parts.push(format!("oneof {group}"));
    }
    parts.push(entry.source.label().into());
    parts.join(" · ")
}

/// Ranking extras for a field: its source, how often records have it, use
/// in past filters, and depth.
fn field_bonus(entry: &MergedEntry, opts: &CompleteOptions) -> i32 {
    let mut score = match entry.source {
        Source::Both => 60,
        Source::Schema => 40,
        Source::Sample => 20,
    };
    if entry.source == Source::Sample && entry.seen_ratio.is_some_and(|r| r < 0.1) {
        score -= 100;
    }
    if opts.history.contains(&render_path(&entry.steps)) {
        score += 80;
    }
    score - 5 * entry.steps.len() as i32
}

/// A value literal for an expression: enum symbols and record strings
/// quoted, numbers and booleans as they are for non-text fields.
fn value_literal(value: &str, kind: &FieldKind) -> String {
    let bare = matches!(
        kind,
        FieldKind::Integer
            | FieldKind::Number
            | FieldKind::Boolean
            | FieldKind::Mixed
            | FieldKind::Unknown
    ) && serde_json::from_str::<serde_json::Value>(value)
        .is_ok_and(|v| v.is_number() || v.is_boolean() || v.is_null());
    if bare {
        value.to_string()
    } else {
        quote(value)
    }
}

/// Suggestions for the cursor at byte `cursor` of `text`, best first.
pub fn complete(
    text: &str,
    cursor: usize,
    catalog: &MergedCatalog,
    opts: &CompleteOptions,
) -> Vec<Suggestion> {
    if text.len() > MAX_TEXT || !text.is_char_boundary(cursor.min(text.len())) {
        return Vec::new();
    }
    let cursor = cursor.min(text.len());
    let (mut tokens, _) = lex(text, LexMode::Lenient);
    // A leading `?` only marks the text as an expression.
    if tokens
        .first()
        .is_some_and(|t| t.token == Token::Unknown("?".into()))
    {
        tokens.remove(0);
    }
    // The token being typed: a word, number or string the cursor is in or
    // right after.
    let current = tokens.iter().position(|t| {
        let typing = matches!(
            t.token,
            Token::Ident(_) | Token::Num(_) | Token::PartialStr { .. }
        ) && t.span.start < cursor
            && cursor <= t.span.end;
        let in_string =
            matches!(t.token, Token::Str(_)) && t.span.start < cursor && cursor < t.span.end;
        typing || in_string
    });
    let before_end =
        current.unwrap_or_else(|| tokens.iter().take_while(|t| t.span.end <= cursor).count());
    let (state, depth) = read_state(&tokens[..before_end]);
    let current_token = current.map(|i| &tokens[i]);
    let replace = current_token.map_or(cursor..cursor, |t| t.span.clone());
    let typed: String = current_token.map_or(String::new(), |t| match &t.token {
        Token::PartialStr { text, .. } => text.clone(),
        Token::Str(_) => text[t.span.start + 1..cursor].to_string(),
        _ => text[t.span.start..cursor].to_string(),
    });
    // Whether the cursor follows the previous token with no space between.
    let glued = before_end > 0 && current.is_none() && tokens[before_end - 1].span.end == cursor;
    let space = if glued { " " } else { "" };

    let mut out: Vec<Suggestion> = Vec::new();
    let mut push = |label: String,
                    insert: String,
                    replace: Range<usize>,
                    kind: SuggestKind,
                    detail: Option<String>,
                    score: i32,
                    reopen: bool| {
        out.push(Suggestion {
            label,
            insert,
            replace,
            kind,
            detail,
            score,
            reopen,
        });
    };

    match state {
        State::Lost => {}
        State::Operand | State::Loose(_) => {
            // Words that are not a name yet match whole paths (`cust.na`).
            let (typed, replace) = match state {
                State::Loose(start) => (
                    text[start..cursor].to_string(),
                    start..replace.end.max(cursor),
                ),
                _ => (typed, replace),
            };
            let whole_paths = !typed.is_empty();
            for e in &catalog.entries {
                if e.steps.contains(&PathStep::AnyKey) {
                    continue;
                }
                let top = e.steps.len() == 1;
                if !top && !whole_paths {
                    continue;
                }
                let path = render_path(&e.steps);
                let name_score = match e.steps.last() {
                    Some(PathStep::Field(n)) if top => match_score(n, &typed),
                    _ => None,
                };
                let path_score = match_score(&path[2.min(path.len())..], &typed).map(|s| s - 50);
                let Some(score) = name_score.max(path_score) else {
                    continue;
                };
                for (label, insert, reopen) in field_inserts(catalog, e) {
                    push(
                        label,
                        insert,
                        replace.clone(),
                        SuggestKind::Field,
                        Some(detail(e)),
                        score + field_bonus(e, opts),
                        reopen,
                    );
                }
            }
            let mut fixed: Vec<(&str, &str, SuggestKind, bool)> = vec![
                ("$.", "$.", SuggestKind::Field, true),
                ("key", "key ", SuggestKind::RecordField, true),
                ("partition", "partition ", SuggestKind::RecordField, true),
                ("offset", "offset ", SuggestKind::RecordField, true),
                ("timestamp", "timestamp ", SuggestKind::RecordField, true),
                ("headers.", "headers.", SuggestKind::RecordField, true),
            ];
            if typed.is_empty() {
                fixed.push(("(", "(", SuggestKind::Keyword, true));
                fixed.push(("!", "!", SuggestKind::Keyword, true));
            }
            for (label, insert, kind, reopen) in fixed {
                if let Some(score) = match_score(label, &typed) {
                    push(
                        label.into(),
                        insert.into(),
                        replace.clone(),
                        kind,
                        None,
                        score - 20,
                        reopen,
                    );
                }
            }
        }
        State::Path(typed_steps) if glued => {
            // Right after a path: more steps, or an operator after a space.
            if let Some(steps) = resolve(catalog, &typed_steps) {
                for child in catalog.children(&steps) {
                    let step = child.steps.last().cloned().unwrap_or(PathStep::AnyIndex);
                    if step == PathStep::AnyKey {
                        continue;
                    }
                    let insert = render_step(&step);
                    push(
                        insert.clone(),
                        insert,
                        cursor..cursor,
                        SuggestKind::Field,
                        Some(detail(child)),
                        field_bonus(child, opts),
                        catalog.has_children(&child.steps),
                    );
                }
                let kind = catalog.get(&steps).map(|e| &e.kind);
                if !steps.is_empty() {
                    operator_suggestions(&mut push, kind, "", cursor..cursor, " ");
                }
            } else if !typed_steps.is_empty() {
                operator_suggestions(&mut push, None, "", cursor..cursor, " ");
            }
        }
        State::Path(typed_steps) => {
            let steps = resolve(catalog, &typed_steps);
            let kind = steps
                .as_deref()
                .and_then(|s| catalog.get(s))
                .map(|e| &e.kind);
            operator_suggestions(&mut push, kind, &typed, replace, space);
        }
        State::PathDot(typed_steps) => {
            let Some(steps) = resolve(catalog, &typed_steps) else {
                return Vec::new();
            };
            // The dot is replaced too, so names that need quoting fit.
            let dot_start = tokens[before_end - 1].span.start;
            for child in catalog.children(&steps) {
                let step = child.steps.last().cloned().unwrap_or(PathStep::AnyIndex);
                let PathStep::Field(name) = &step else {
                    continue;
                };
                let Some(score) = match_score(name, &typed) else {
                    continue;
                };
                for (label, insert, reopen) in field_inserts(catalog, child) {
                    // The inserts are whole paths; keep the part after the parent.
                    let parent = render_path(&steps);
                    let tail = insert.strip_prefix(&parent).unwrap_or(&insert).to_string();
                    let shown = label.strip_prefix(&parent).unwrap_or(&label).to_string();
                    push(
                        shown,
                        tail,
                        dot_start..replace.end.max(cursor),
                        SuggestKind::Field,
                        Some(detail(child)),
                        score + field_bonus(child, opts),
                        reopen,
                    );
                }
            }
        }
        State::PathBracket(typed_steps) => {
            let Some(steps) = resolve(catalog, &typed_steps) else {
                return Vec::new();
            };
            // A `]` already after the cursor is replaced, not doubled.
            let end = closing_after(text, replace.end.max(cursor), ']');
            let quoting = current_token
                .is_some_and(|t| matches!(t.token, Token::PartialStr { .. } | Token::Str(_)));
            if matches!(
                catalog.get(&steps).map(|e| &e.kind),
                Some(FieldKind::Array(_))
            ) && !quoting
            {
                let has = catalog.has_children(&[steps.clone(), vec![PathStep::AnyIndex]].concat());
                push(
                    "0]".into(),
                    "0]".into(),
                    replace.start..end,
                    SuggestKind::Field,
                    None,
                    500,
                    has,
                );
            }
            for child in catalog.children(&steps) {
                let PathStep::Field(name) =
                    child.steps.last().cloned().unwrap_or(PathStep::AnyIndex)
                else {
                    continue;
                };
                let Some(score) = match_score(&name, &typed) else {
                    continue;
                };
                let insert = format!("{}]", quote(&name));
                push(
                    insert.clone(),
                    insert,
                    replace.start..end,
                    SuggestKind::Field,
                    Some(detail(child)),
                    score + field_bonus(child, opts),
                    catalog.has_children(&child.steps),
                );
            }
        }
        State::PathClose(_, _) => {
            push(
                "]".into(),
                "]".into(),
                cursor..cursor,
                SuggestKind::Keyword,
                None,
                0,
                false,
            );
        }
        State::Headers => {
            push(
                "headers.".into(),
                ".".into(),
                cursor..cursor,
                SuggestKind::RecordField,
                None,
                0,
                true,
            );
        }
        State::HeadersDot => {
            for (i, name) in catalog.headers.iter().enumerate() {
                let Some(score) = match_score(name, &typed) else {
                    continue;
                };
                let written =
                    if !name.is_empty() && name.chars().all(crate::model::expr::is_name_char) {
                        name.clone()
                    } else {
                        quote(name)
                    };
                let used = opts.history.contains(&format!("headers.{name}"));
                push(
                    name.clone(),
                    format!("{written} "),
                    replace.clone(),
                    SuggestKind::Header,
                    Some("header".into()),
                    score - i as i32 + if used { 80 } else { 0 },
                    true,
                );
            }
        }
        State::Operand2(left) => {
            let kind = match left {
                Left::Builtin("key") | Left::Header => Some(FieldKind::String),
                Left::Builtin(_) => Some(FieldKind::Integer),
                Left::Literal | Left::Path(_) => None,
            };
            operator_suggestions(&mut push, kind.as_ref(), &typed, replace, space);
        }
        State::Value(left, op) => {
            let entry = match &left {
                Left::Path(typed_steps) => {
                    resolve(catalog, typed_steps).and_then(|s| catalog.get(&s).cloned())
                }
                _ => None,
            };
            let in_string = current_token
                .is_some_and(|t| matches!(t.token, Token::PartialStr { .. } | Token::Str(_)));
            // A closing quote already there is replaced, not doubled.
            let replace = match current_token {
                Some(t) if matches!(t.token, Token::Str(_)) => t.span.clone(),
                _ => replace,
            };
            if op == "=~" {
                if !in_string || typed.is_empty() {
                    push(
                        "\"^…\"".into(),
                        format!("{space}\"^"),
                        replace,
                        SuggestKind::Value,
                        Some("regex, case-insensitive".into()),
                        0,
                        false,
                    );
                }
                return finish(out);
            }
            let mut values: Vec<(String, &str)> = Vec::new();
            let kind = entry
                .as_ref()
                .map_or(FieldKind::Unknown, |e| e.kind.clone());
            if let Some(e) = &entry {
                values.extend(e.enum_values.iter().map(|v| (v.clone(), "enum")));
                values.extend(e.sample_values.iter().map(|v| (v.clone(), "sample")));
            }
            if kind == FieldKind::Boolean || matches!(left, Left::Literal) {
                values.push(("true".into(), "boolean"));
                values.push(("false".into(), "boolean"));
            }
            if entry.as_ref().is_some_and(|e| e.optional) || matches!(kind, FieldKind::Null) {
                values.push(("null".into(), "null"));
            }
            for (rank, (value, origin)) in values.into_iter().enumerate() {
                let literal = match origin {
                    "null" | "boolean" => value.clone(),
                    _ => value_literal(&value, &kind),
                };
                let quoted = literal.starts_with('"');
                if in_string && !quoted {
                    continue;
                }
                let Some(score) = match_score(&value, &typed) else {
                    continue;
                };
                push(
                    literal.clone(),
                    format!("{space}{literal} "),
                    replace.clone(),
                    SuggestKind::Value,
                    Some(origin.into()),
                    score - rank as i32,
                    true,
                );
            }
        }
        State::Done => {
            let mut words = vec![("&&", "&& ", 2), ("||", "|| ", 1)];
            if depth > 0 {
                words.push((")", ") ", 0));
            }
            for (label, insert, score) in words {
                if typed.is_empty() || match_score(label, &typed).is_some() {
                    push(
                        label.into(),
                        format!("{space}{insert}"),
                        replace.clone(),
                        SuggestKind::Keyword,
                        None,
                        score,
                        label != ")",
                    );
                }
            }
        }
    }
    finish(out)
}

/// The end of the replaced range: past `close` when it directly follows.
fn closing_after(text: &str, at: usize, close: char) -> usize {
    match text[at..].chars().next() {
        Some(c) if c == close => at + c.len_utf8(),
        _ => at,
    }
}

/// What accepting a field path inserts: a record or map path ends in `.`
/// and shows its fields next; an array offers itself and `[0].`; a leaf
/// path ends in a space and shows operators next.
fn field_inserts(catalog: &MergedCatalog, entry: &MergedEntry) -> Vec<(String, String, bool)> {
    let path = render_path(&entry.steps);
    match &entry.kind {
        FieldKind::Record if catalog.has_children(&entry.steps) => {
            vec![(path.clone(), format!("{path}."), true)]
        }
        FieldKind::Array(_) => {
            let mut out = vec![(path.clone(), format!("{path} "), true)];
            let element = [entry.steps.clone(), vec![PathStep::AnyIndex]].concat();
            if catalog.has_children(&element) {
                out.push((format!("{path}[0]."), format!("{path}[0]."), true));
            }
            out
        }
        _ => vec![(path.clone(), format!("{path} "), true)],
    }
}

type Push<'a> =
    dyn FnMut(String, String, Range<usize>, SuggestKind, Option<String>, i32, bool) + 'a;

fn operator_suggestions(
    push: &mut Push<'_>,
    kind: Option<&FieldKind>,
    typed: &str,
    replace: Range<usize>,
    space: &str,
) {
    for (rank, op) in operators_for(kind).iter().enumerate() {
        let Some(score) = match_score(op, typed) else {
            continue;
        };
        // `exists` ends the comparison; others wait for a value.
        push(
            (*op).into(),
            format!("{space}{op} "),
            replace.clone(),
            SuggestKind::Operator,
            None,
            score - rank as i32,
            true,
        );
    }
}

/// Best first, ties by label; at most [`MAX_SUGGESTIONS`], no duplicates.
fn finish(mut out: Vec<Suggestion>) -> Vec<Suggestion> {
    out.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| a.label.to_lowercase().cmp(&b.label.to_lowercase()))
    });
    let mut seen = std::collections::HashSet::new();
    out.retain(|s| seen.insert((s.label.clone(), s.insert.clone())));
    out.truncate(MAX_SUGGESTIONS);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::schema_fields::{avro_catalog, Limits};
    use crate::model::expr::Expr;

    const ORDER: &str = r#"{
        "type": "record", "name": "Order",
        "fields": [
            {"name": "amount", "type": "double"},
            {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["PENDING", "PAID", "REFUNDED"]}},
            {"name": "customer", "type": {"type": "record", "name": "Customer", "fields": [
                {"name": "id", "type": "long"},
                {"name": "email", "type": ["null", "string"]},
                {"name": "tier", "type": "string"},
                {"name": "odd key", "type": "string"}
            ]}},
            {"name": "items", "type": {"type": "array", "items": {"type": "record", "name": "Line", "fields": [
                {"name": "sku", "type": "string"}, {"name": "qty", "type": "int"}
            ]}}},
            {"name": "paid", "type": "boolean"},
            {"name": "주문", "type": {"type": "record", "name": "K", "fields": [{"name": "금액", "type": "long"}]}},
            {"name": "attrs", "type": {"type": "map", "values": "string"}}
        ]
    }"#;

    fn catalog() -> MergedCatalog {
        let schema = crate::codec::avro::Schema::parse(ORDER).unwrap();
        let fields = avro_catalog(&schema, &[], &Limits::default());
        let mut merged = MergedCatalog::merge(&[&fields], None);
        merged.headers = vec!["x-trace-id".into(), "content type".into()];
        merged
    }

    /// Suggestions with the cursor at `|`.
    fn at(text_with_cursor: &str) -> (String, Vec<Suggestion>) {
        // The last `|`, so `||` before the cursor stays an operator.
        let cursor = text_with_cursor.rfind('|').expect("cursor mark");
        let text = format!(
            "{}{}",
            &text_with_cursor[..cursor],
            &text_with_cursor[cursor + 1..]
        );
        let found = complete(&text, cursor, &catalog(), &CompleteOptions::default());
        (text, found)
    }

    fn labels(found: &[Suggestion]) -> Vec<&str> {
        found.iter().map(|s| s.label.as_str()).collect()
    }

    /// The text after accepting the first suggestion.
    fn accept_first(text_with_cursor: &str) -> String {
        let (text, found) = at(text_with_cursor);
        let first = found
            .first()
            .unwrap_or_else(|| panic!("nothing for {text_with_cursor}"));
        first.apply(&text).0
    }

    #[test]
    fn operand_start_offers_fields_and_record_fields() {
        let (_, found) = at("|");
        let l = labels(&found);
        for want in [
            "$.amount",
            "$.status",
            "$.customer",
            "$.",
            "key",
            "headers.",
            "(",
            "!",
        ] {
            assert!(l.contains(&want), "{want} in {l:?}");
        }
        assert!(
            !l.iter().any(|x| x.contains("customer.id")),
            "only top level: {l:?}"
        );
        // Record paths end in `.` and reopen; leaves end in a space.
        let customer = found.iter().find(|s| s.label == "$.customer").unwrap();
        assert_eq!(
            (customer.insert.as_str(), customer.reopen),
            ("$.customer.", true)
        );
        let amount = found.iter().find(|s| s.label == "$.amount").unwrap();
        assert_eq!(amount.insert, "$.amount ");
        let items: Vec<&str> = found
            .iter()
            .filter(|s| s.label.starts_with("$.items"))
            .map(|s| s.insert.as_str())
            .collect();
        assert_eq!(items, ["$.items ", "$.items[0]."]);
        // Typed words match whole paths too.
        let (_, found) = at("cust.em|");
        assert!(!found.is_empty());
        assert!(found.iter().all(|s| s.replace == (0..7)), "{found:?}");
        let (_, found) = at("tier|");
        assert_eq!(found[0].label, "$.customer.tier");
        assert_eq!(found[0].replace, 0..4);
        let (_, found) = at("($.amount > 1 || ke|");
        assert_eq!(found[0].label, "key");
        assert_eq!(found[0].replace, 17..19);
        let (_, found) = at("!|");
        assert!(labels(&found).contains(&"$.amount"));
        let (_, found) = at("$.amount > 1 && |");
        assert!(labels(&found).contains(&"$.status"));
    }

    #[test]
    fn path_steps_offer_children() {
        let (_, found) = at("$.|");
        assert!(labels(&found).contains(&".amount"), "{:?}", labels(&found));
        let (text, found) = at("$.cu|");
        assert_eq!(found[0].label, ".customer");
        assert_eq!(found[0].apply(&text), ("$.customer.".to_string(), 11));
        assert!(found[0].reopen);
        let (_, found) = at("$.customer.|");
        let l = labels(&found);
        assert!(l.contains(&".id") && l.contains(&".tier"), "{l:?}");
        assert!(l.contains(&"[\"odd key\"]"), "{l:?}");
        let odd = accept_first("$.customer.od|");
        assert_eq!(odd, "$.customer[\"odd key\"] ");
        assert!(Expr::parse(&format!("{odd}exists")).is_ok());
        let (_, found) = at("$.items[0].|");
        assert_eq!(labels(&found)[..2], [".qty", ".sku"]);
        let (_, found) = at("!$.|");
        assert!(labels(&found).contains(&".status"));
        let (text, found) = at("$.customer[\"od|");
        assert_eq!(found[0].label, "\"odd key\"]");
        assert_eq!(found[0].apply(&text).0, "$.customer[\"odd key\"]");
        let (_, found) = at("$.items[|");
        assert_eq!(found[0].label, "0]");
        // Right after a path: more steps and operators.
        let (_, found) = at("$.items[0]|");
        let l = labels(&found);
        assert!(l.contains(&".sku") && l.contains(&"exists"), "{l:?}");
        let (_, found) = at("$.주|");
        assert_eq!(found[0].label, ".주문");
        let (text, found) = at("$.주문.|");
        assert_eq!(found[0].label, ".금액");
        assert_eq!(found[0].replace, 8..9);
        assert_eq!(found[0].apply(&text).0, "$.주문.금액 ");
    }

    #[test]
    fn operators_follow_the_left_side() {
        let (_, found) = at("$.amount |");
        assert_eq!(labels(&found)[..4], [">", ">=", "<", "<="]);
        let (_, found) = at("$.customer.tier |");
        assert_eq!(labels(&found)[..3], ["==", "!=", "=~"]);
        let (_, found) = at("$.status |");
        assert_eq!(labels(&found), ["==", "!=", "exists"]);
        let (_, found) = at("$.items |");
        assert_eq!(labels(&found), ["contains", "exists"]);
        let (_, found) = at("$.customer |");
        assert_eq!(labels(&found), ["exists"]);
        let (_, found) = at("key |");
        assert_eq!(labels(&found)[0], "==");
        assert!(labels(&found).contains(&"=~"));
        let (_, found) = at("partition |");
        assert_eq!(labels(&found)[0], ">");
        let (_, found) = at("$.customer.tier con|");
        assert_eq!(found[0].label, "contains");
        assert_eq!(found[0].insert, "contains ");
        let (_, found) = at("$.unknown.path |");
        assert!(labels(&found).contains(&"=="));
        let (_, found) = at("$.amount|");
        assert_eq!(found[0].label, ".amount");
    }

    #[test]
    fn values_come_from_the_schema() {
        let (_, found) = at("$.status == |");
        assert_eq!(labels(&found), ["\"PENDING\"", "\"PAID\"", "\"REFUNDED\""]);
        assert_eq!(found[0].insert, "\"PENDING\" ");
        let (text, found) = at("$.status == \"PA|");
        assert_eq!(found[0].label, "\"PAID\"");
        assert_eq!(found[0].apply(&text).0, "$.status == \"PAID\" ");
        // A closing quote already there is not doubled.
        let (text, found) = at("$.status == \"PA|\"");
        assert_eq!(found[0].apply(&text).0, "$.status == \"PAID\" ");
        let (_, found) = at("$.paid == |");
        assert_eq!(labels(&found), ["true", "false"]);
        let (_, found) = at("$.customer.email == |");
        assert_eq!(labels(&found), ["null"]);
        let (_, found) = at("key =~ |");
        assert_eq!(found[0].insert, "\"^");
        let (_, found) = at("$.status ==|");
        assert_eq!(found[0].insert, " \"PENDING\" ");
    }

    #[test]
    fn after_a_comparison_offers_joins() {
        let (_, found) = at("$.status == \"PAID\" |");
        assert_eq!(labels(&found), ["&&", "||"]);
        let (_, found) = at("($.amount > 1 |");
        assert_eq!(labels(&found), ["&&", "||", ")"]);
        let (_, found) = at("$.status == \"PAID\"|");
        assert_eq!(found[0].insert, " && ");
        let (_, found) = at("$.a exists |");
        assert_eq!(labels(&found), ["&&", "||"]);
        let (_, found) = at("?$.amount > 1 |");
        assert_eq!(labels(&found), ["&&", "||"]);
    }

    #[test]
    fn headers_come_from_records() {
        let (_, found) = at("headers.|");
        assert_eq!(labels(&found), ["x-trace-id", "content type"]);
        assert_eq!(found[1].insert, "\"content type\" ");
        let (text, found) = at("headers.x-tr|");
        assert_eq!(found[0].apply(&text).0, "headers.x-trace-id ");
        let (_, found) = at("headers.x-trace-id |");
        assert_eq!(labels(&found)[0], "==");
    }

    #[test]
    fn broken_text_and_limits_give_nothing() {
        assert!(at("$.a == == |").1.is_empty());
        assert!(at("$.a # |").1.is_empty());
        assert!(at("$.status == \"unclosed |").1.is_empty());
        let long = format!("{}|", "x".repeat(17 * 1024));
        assert!(at(&long).1.is_empty());
        // A cursor inside a multi-byte character is not a position.
        assert!(complete("$.주문", 3, &catalog(), &CompleteOptions::default()).is_empty());
    }

    #[test]
    fn accepting_first_suggestions_builds_a_valid_expression() {
        let catalog = catalog();
        for start in ["$.st", "$.amount > 1 && $.pa", "$.cu"] {
            let mut text = start.to_string();
            for _ in 0..12 {
                let found = complete(&text, text.len(), &catalog, &CompleteOptions::default());
                // Skip keywords so the walk builds a comparison.
                let Some(pick) = found
                    .iter()
                    .find(|s| !matches!(s.kind, SuggestKind::Keyword))
                    .or(found.first())
                else {
                    break;
                };
                text = pick.apply(&text).0;
                if Expr::parse(text.trim()).is_ok() && text.ends_with(' ') {
                    break;
                }
            }
            assert!(Expr::parse(text.trim()).is_ok(), "{start}: {text}");
        }
    }

    #[test]
    fn ranking_prefers_prefix_source_and_history() {
        assert!(match_score("status", "st") > match_score("customer_status", "st"));
        assert!(match_score("customer_status", "st") > match_score("cost", "st"));
        assert!(match_score("cost", "st") > match_score("sxt", "st"));
        assert_eq!(match_score("abc", "x"), None);
        assert_eq!(match_score("orderId", "id"), Some(700));
        let sample = SampleCatalog {
            entries: vec![crate::codec::field_infer::SampleEntry {
                steps: vec![PathStep::Field("amount".into())],
                kind: FieldKind::Number,
                seen: 10,
                values: None,
            }],
            headers: Vec::new(),
            records: 10,
        };
        let schema = crate::codec::avro::Schema::parse(
            r#"{"type":"record","name":"R","fields":[{"name":"amortized","type":"double"},{"name":"amount","type":"double"}]}"#,
        )
        .unwrap();
        let fields = avro_catalog(&schema, &[], &Limits::default());
        let merged = MergedCatalog::merge(&[&fields], Some(&sample));
        assert_eq!(merged.entries[1].source, Source::Both);
        let found = complete("am", 2, &merged, &CompleteOptions::default());
        assert_eq!(found[0].label, "$.amount");
        let history = CompleteOptions {
            history: mentioned_paths("$.amortized > 1 && headers.trace exists"),
        };
        assert_eq!(history.history, ["$.amortized", "headers.trace"]);
        let found = complete("am", 2, &merged, &history);
        assert_eq!(found[0].label, "$.amortized");
    }

    #[test]
    fn merge_adds_record_fields_only_where_the_schema_is_open() {
        let schema = crate::codec::avro::Schema::parse(
            r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"m","type":{"type":"map","values":"string"}}]}"#,
        )
        .unwrap();
        let fields = avro_catalog(&schema, &[], &Limits::default());
        let entry = |names: &[&str], kind| crate::codec::field_infer::SampleEntry {
            steps: names
                .iter()
                .map(|n| PathStep::Field(n.to_string()))
                .collect(),
            kind,
            seen: 1,
            values: Some(vec!["v".into()]),
        };
        let sample = SampleCatalog {
            entries: vec![
                entry(&["a"], FieldKind::Integer),
                entry(&["stray"], FieldKind::String),
                entry(&["m", "color"], FieldKind::String),
            ],
            headers: vec![("h".into(), 1)],
            records: 1,
        };
        let merged = MergedCatalog::merge(&[&fields], Some(&sample));
        let paths: Vec<String> = merged
            .entries
            .iter()
            .map(|e| render_path(&e.steps))
            .collect();
        assert_eq!(paths, ["$.a", "$.m", "$.m[\"…\"]", "$.m.color"]);
        assert_eq!(merged.entries[0].sample_values, ["v"]);
        assert_eq!(merged.headers, ["h"]);
        let only_sample = MergedCatalog::merge(&[], Some(&sample));
        assert_eq!(only_sample.entries.len(), 3);
        assert!(only_sample
            .entries
            .iter()
            .all(|e| e.source == Source::Sample));
    }

    #[test]
    fn completes_two_thousand_fields_quickly() {
        let entries = (0..2000)
            .map(|i| MergedEntry {
                steps: vec![
                    PathStep::Field(format!("group{}", i / 50)),
                    PathStep::Field(format!("field_{i}")),
                ],
                kind: FieldKind::String,
                optional: false,
                enum_values: Vec::new(),
                sample_values: Vec::new(),
                recursive: false,
                group: None,
                source: Source::Schema,
                seen_ratio: None,
            })
            .collect();
        let catalog = MergedCatalog {
            entries,
            headers: Vec::new(),
            notes: Vec::new(),
        };
        let started = std::time::Instant::now();
        for _ in 0..20 {
            let found = complete("field_19", 8, &catalog, &CompleteOptions::default());
            assert!(!found.is_empty());
        }
        // Generous for debug builds and busy machines.
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }
}
