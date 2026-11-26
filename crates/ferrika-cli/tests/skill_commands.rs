//! Every `ferrika ...` command line in the Agent Skills under `skills/` must
//! parse against the current command line, so the skills do not drift from
//! the CLI. Only parsing is checked (subcommands, flags, value types); nothing
//! is run.
//!
//! A command line is a line in a fenced `bash` or `sh` block whose command
//! starts with `ferrika` (after `$ ` and `NAME=value` prefixes, also after
//! `|`, `&&`, `||` and `;`). Lines ending in `\` continue on the next line.
//! The command ends at a shell operator, a redirection or a `#` comment.

use std::path::{Path, PathBuf};

use clap::Parser;
use ferrika_cli::cli::app::Cli;

/// `skills/` at the repository root; `None` outside the repository (for
/// example in the crates.io package, which does not ship the skills).
fn skills_dir() -> Option<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills");
    dir.is_dir().then_some(dir)
}

fn markdown_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            markdown_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "md") {
            out.push(path);
        }
    }
}

/// The logical lines (continuations joined) of the shell code blocks, with
/// the line number each starts on.
fn shell_lines(markdown: &str) -> Vec<(usize, String)> {
    let mut lines = Vec::new();
    let mut in_shell = false;
    let mut in_other = false;
    let mut pending: Option<(usize, String)> = None;
    for (index, raw) in markdown.lines().enumerate() {
        let trimmed = raw.trim();
        if let Some(info) = trimmed.strip_prefix("```") {
            if in_shell || in_other {
                in_shell = false;
                in_other = false;
                if let Some(done) = pending.take() {
                    lines.push(done);
                }
            } else {
                let lang = info.trim();
                in_shell = matches!(lang, "bash" | "sh" | "shell");
                in_other = !in_shell;
            }
            continue;
        }
        if !in_shell {
            continue;
        }
        let (start, mut text) = match pending.take() {
            Some((start, text)) => (start, text + " "),
            None => (index + 1, String::new()),
        };
        match trimmed.strip_suffix('\\') {
            Some(head) => {
                text.push_str(head.trim_end());
                pending = Some((start, text));
            }
            None => {
                text.push_str(trimmed);
                lines.push((start, text));
            }
        }
    }
    lines
}

#[derive(Debug, PartialEq)]
enum Token {
    Word(String),
    /// `|`, `||`, `&&`, `;`, `&`: a new command starts after it.
    Separator,
    /// `>`, `>>`, `<`, `2>`, `2>&1`, ...: the command's arguments end here.
    Redirect,
    /// `#` at the start of a word: the rest of the line is a comment.
    Comment,
}

/// Splits a line like a POSIX shell for the cases the skills use: single and
/// double quotes, backslash escapes, operators and comments. Variables and
/// command substitutions are kept as text.
fn tokenize(line: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_word = true;
                for q in chars.by_ref() {
                    if q == '\'' {
                        break;
                    }
                    word.push(q);
                }
            }
            '"' => {
                in_word = true;
                while let Some(q) = chars.next() {
                    match q {
                        '"' => break,
                        '\\' if matches!(chars.peek(), Some('"' | '\\' | '$' | '`')) => {
                            if let Some(next) = chars.next() {
                                word.push(next);
                            }
                        }
                        _ => word.push(q),
                    }
                }
            }
            '\\' => {
                in_word = true;
                if let Some(next) = chars.next() {
                    word.push(next);
                }
            }
            '#' if !in_word => {
                tokens.push(Token::Comment);
                return tokens;
            }
            ' ' | '\t' => {
                if in_word {
                    tokens.push(Token::Word(std::mem::take(&mut word)));
                    in_word = false;
                }
            }
            '|' | '&' | ';' => {
                // `2>&1` is a redirection, handled below; here `&` is an operator.
                if in_word {
                    tokens.push(Token::Word(std::mem::take(&mut word)));
                    in_word = false;
                }
                if matches!(chars.peek(), Some('|' | '&')) {
                    chars.next();
                }
                tokens.push(Token::Separator);
            }
            '>' | '<' => {
                // A word of digits right before it is a file descriptor (`2>`).
                if in_word && !word.chars().all(|d| d.is_ascii_digit()) {
                    tokens.push(Token::Word(std::mem::take(&mut word)));
                }
                word.clear();
                in_word = false;
                tokens.push(Token::Redirect);
                // Skip the rest of the operator (`>>`, `>&1`) and its target.
                while matches!(chars.peek(), Some('>' | '&')) {
                    chars.next();
                }
                while matches!(chars.peek(), Some(' ')) {
                    chars.next();
                }
                while matches!(chars.peek(), Some(c) if !c.is_whitespace()) {
                    chars.next();
                }
            }
            _ => {
                in_word = true;
                word.push(c);
            }
        }
    }
    if in_word {
        tokens.push(Token::Word(word));
    }
    tokens
}

/// The argument lists of the `ferrika` commands on one line.
fn ferrika_commands(line: &str) -> Vec<Vec<String>> {
    let line = line.strip_prefix("$ ").unwrap_or(line);
    let mut commands = Vec::new();
    let mut current: Option<Vec<String>> = None;
    let mut at_start = true;
    let mut stopped = false;
    for token in tokenize(line) {
        match token {
            Token::Separator => {
                commands.extend(current.take());
                at_start = true;
                stopped = false;
            }
            Token::Redirect => stopped = true,
            Token::Comment => break,
            Token::Word(word) => {
                if stopped {
                    continue;
                }
                if let Some(args) = current.as_mut() {
                    args.push(word);
                } else if at_start {
                    let is_assignment = word.split_once('=').is_some_and(|(name, _)| {
                        !name.is_empty()
                            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    });
                    if is_assignment {
                        continue;
                    }
                    at_start = false;
                    if word == "ferrika" {
                        current = Some(vec![word]);
                    }
                }
            }
        }
    }
    commands.extend(current);
    commands
}

#[test]
fn every_command_in_the_skills_parses() {
    let Some(dir) = skills_dir() else {
        eprintln!("skills/ not found; nothing to check");
        return;
    };
    let mut files = Vec::new();
    markdown_files(&dir, &mut files);
    let mut checked = 0;
    let mut failures = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).unwrap();
        for (line_no, line) in shell_lines(&text) {
            for args in ferrika_commands(&line) {
                if !cfg!(feature = "mqtt") && args.iter().any(|a| a == "mqtt") {
                    continue;
                }
                checked += 1;
                if let Err(error) = Cli::try_parse_from(&args) {
                    use clap::error::ErrorKind;
                    if matches!(
                        error.kind(),
                        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
                    ) {
                        continue;
                    }
                    let first = error.to_string();
                    let first = first.lines().next().unwrap_or_default().to_string();
                    failures.push(format!(
                        "{}:{line_no}: `{}`: {first}",
                        file.strip_prefix(&dir).unwrap_or(file).display(),
                        args.join(" ")
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {checked} skill command(s) do not parse:\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert!(
        checked >= 20,
        "only {checked} commands found in {}; is the extraction broken?",
        dir.display()
    );
}

#[test]
fn shell_lines_join_continuations_and_skip_other_blocks() {
    let md = "text\n```bash\nferrika a \\\n  --b c\n# note\n```\n```json\nferrika x\n```\n";
    assert_eq!(
        shell_lines(md),
        vec![
            (3, "ferrika a --b c".to_string()),
            (5, "# note".to_string())
        ]
    );
}

#[test]
fn commands_are_split_like_a_shell() {
    let words = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert_eq!(
        ferrika_commands(r#"ferrika consume t -W '$.a > 1 && $.b == "x"' -n 5"#),
        vec![words(&[
            "ferrika",
            "consume",
            "t",
            "-W",
            r#"$.a > 1 && $.b == "x""#,
            "-n",
            "5"
        ])]
    );
    assert_eq!(
        ferrika_commands("ferrika -f json health 2>&1 | jq . > out.json # c"),
        vec![words(&["ferrika", "-f", "json", "health"])]
    );
    assert_eq!(
        ferrika_commands("echo x | FOO=1 ferrika produce t && ferrika undo --dry-run"),
        vec![
            words(&["ferrika", "produce", "t"]),
            words(&["ferrika", "undo", "--dry-run"])
        ]
    );
    assert_eq!(
        ferrika_commands("jq '.[] | .lag' ferrika"),
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        ferrika_commands("ferrika report -o incident.md"),
        vec![words(&["ferrika", "report", "-o", "incident.md"])]
    );
}

#[test]
fn a_wrong_flag_is_caught() {
    let args = &ferrika_commands("ferrika offset reset g -t t --to-yesterday")[0];
    assert!(Cli::try_parse_from(args).is_err());
}
