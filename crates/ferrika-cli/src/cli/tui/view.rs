//! Drawing the dashboard. Everything shown comes from [`App`]; nothing here
//! talks to the cluster.

use std::time::Instant;

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Clear, Paragraph, Row, Table, TableState, Tabs, Wrap};
use ratatui::Frame;

use super::state::{
    format_age, format_count, format_rate, App, Column, GroupColumn, Sort, TopicColumn, View,
};
use crate::model::cluster::ConsumerState;
use crate::ops::health::Severity;

/// Findings listed on the Overview.
const OVERVIEW_FINDINGS: usize = 8;

pub fn draw(frame: &mut Frame, app: &App, now: Instant) {
    let [tabs, body, status] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_tabs(frame, app, tabs);
    if app.data.is_none() {
        draw_waiting(frame, app, body);
    } else {
        match app.view {
            View::Overview => draw_overview(frame, app, body),
            View::Topics => draw_topics(frame, app, body),
            View::Groups => draw_groups(frame, app, body),
            View::Findings => draw_findings(frame, app, body),
        }
    }
    draw_status(frame, app, now, status);
    if app.show_help {
        draw_help(frame, frame.area());
    }
}

fn bold() -> Style {
    Style::new().add_modifier(Modifier::BOLD)
}

fn draw_tabs(frame: &mut Frame, app: &App, area: Rect) {
    let [title, tabs] =
        Layout::horizontal([Constraint::Length(14), Constraint::Min(1)]).areas(area);
    frame.render_widget(
        Paragraph::new(Span::styled(" ferrika top ", bold().fg(Color::Cyan))),
        title,
    );
    let titles = View::ALL
        .iter()
        .enumerate()
        .map(|(i, v)| format!("{} {}", i + 1, v.title()));
    frame.render_widget(
        Tabs::new(titles)
            .select(app.view.index())
            .highlight_style(bold().fg(Color::Yellow))
            .divider("|"),
        tabs,
    );
}

fn draw_waiting(frame: &mut Frame, app: &App, area: Rect) {
    let text = match &app.error {
        Some(e) => format!("Cannot read {}: {}", app.cluster, first_line(e)),
        None => format!("Reading {} ...", app.cluster),
    };
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .block(Block::bordered()),
        area,
    );
}

/// A label and value line of the Overview.
fn field<'a>(label: &'a str, value: String, style: Style) -> Line<'a> {
    Line::from(vec![
        Span::styled(format!("{label:<20}"), Style::new().fg(Color::Gray)),
        Span::styled(value, style),
    ])
}

/// Red when `bad`, else the default style.
fn alert(bad: bool, color: Color) -> Style {
    if bad {
        bold().fg(color)
    } else {
        Style::new()
    }
}

fn draw_overview(frame: &mut Frame, app: &App, area: Rect) {
    let Some(data) = &app.data else {
        return;
    };
    let o = &data.overview;
    let [cluster, findings] =
        Layout::vertical([Constraint::Length(11), Constraint::Min(3)]).areas(area);

    let brokers = if o.brokers_offline.is_empty() {
        format!("{} online", o.brokers_online)
    } else {
        let ids: Vec<String> = o.brokers_offline.iter().map(i32::to_string).collect();
        format!(
            "{} online, {} offline (node {})",
            o.brokers_online,
            o.brokers_offline.len(),
            ids.join(", ")
        )
    };
    let lines = vec![
        field(
            "Cluster id",
            if o.cluster_id.is_empty() {
                "unknown".to_string()
            } else {
                o.cluster_id.clone()
            },
            Style::new(),
        ),
        field(
            "Brokers",
            brokers,
            alert(!o.brokers_offline.is_empty(), Color::Red),
        ),
        field(
            "Controller",
            o.controller
                .map_or("unknown".to_string(), |id| format!("node {id}")),
            alert(o.controller.is_none(), Color::Yellow),
        ),
        field(
            "Topics",
            format!("{} (+{} internal)", o.topics, o.internal_topics),
            Style::new(),
        ),
        field(
            "Partitions",
            format_count(o.partitions as i64),
            Style::new(),
        ),
        field(
            "Under-replicated",
            format_count(o.under_replicated as i64),
            alert(o.under_replicated > 0, Color::Yellow),
        ),
        field(
            "Offline partitions",
            format_count(o.offline_partitions as i64),
            alert(o.offline_partitions > 0, Color::Red),
        ),
        field(
            "Consumer groups",
            format!("{}, total lag {}", o.groups, format_count(o.total_lag)),
            Style::new(),
        ),
        field("Produce rate", format_rate(o.produce_rate), Style::new()),
    ];
    frame.render_widget(
        Paragraph::new(lines).block(Block::bordered().title(" Cluster ")),
        cluster,
    );

    let count = |s: Severity| data.findings.iter().filter(|f| f.severity == s).count();
    let title = format!(
        " Findings: {} critical, {} warning, {} info ",
        count(Severity::Critical),
        count(Severity::Warning),
        count(Severity::Info)
    );
    let mut lines: Vec<Line> = data
        .findings
        .iter()
        .take(OVERVIEW_FINDINGS)
        .map(|f| {
            Line::from(vec![
                Span::styled(
                    format!("{:<9}", severity_label(f.severity)),
                    severity_style(f.severity),
                ),
                Span::raw(f.message.clone()),
            ])
        })
        .collect();
    if lines.is_empty() {
        lines.push(Line::styled("No findings.", Style::new().fg(Color::Green)));
    } else if data.findings.len() > OVERVIEW_FINDINGS {
        lines.push(Line::styled(
            format!(
                "... {} more on the Findings tab (4)",
                data.findings.len() - OVERVIEW_FINDINGS
            ),
            Style::new().fg(Color::Gray),
        ));
    }
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: true })
            .block(Block::bordered().title(title)),
        findings,
    );
}

fn severity_label(severity: Severity) -> &'static str {
    match severity {
        Severity::Critical => "CRITICAL",
        Severity::Warning => "WARNING",
        Severity::Info => "INFO",
    }
}

fn severity_style(severity: Severity) -> Style {
    match severity {
        Severity::Critical => bold().fg(Color::Red),
        Severity::Warning => Style::new().fg(Color::Yellow),
        Severity::Info => Style::new().fg(Color::Cyan),
    }
}

/// Header cells, the sorted column marked with its direction.
fn header<C: Column>(columns: &[C], sort: Sort<C>, extra: &[&'static str]) -> Row<'static> {
    let mut cells: Vec<Cell> = columns
        .iter()
        .map(|c| {
            if *c == sort.column {
                let arrow = if sort.descending { "v" } else { "^" };
                Cell::from(format!("{} {arrow}", c.label())).style(bold().fg(Color::Yellow))
            } else {
                Cell::from(c.label())
            }
        })
        .collect();
    cells.extend(extra.iter().map(|e| Cell::from(*e)));
    Row::new(cells).style(bold())
}

fn list_title(name: &str, shown: usize, total: usize, filter: &str) -> String {
    if filter.is_empty() {
        format!(" {name} ({total}) ")
    } else {
        format!(" {name} ({shown} of {total}, filter \"{filter}\") ")
    }
}

fn selection(app: &App, rows: usize) -> TableState {
    TableState::default().with_selected((rows > 0).then(|| app.selected_row()))
}

fn draw_topics(frame: &mut Frame, app: &App, area: Rect) {
    let total = app.data.as_ref().map_or(0, |d| d.topics.len());
    let topics = app.visible_topics();
    let rows: Vec<Row> = topics
        .iter()
        .map(|t| {
            let style = if t.offline > 0 {
                Style::new().fg(Color::Red)
            } else if t.under_replicated > 0 {
                Style::new().fg(Color::Yellow)
            } else {
                Style::new()
            };
            Row::new(vec![
                Cell::from(t.name.clone()),
                Cell::from(t.partitions.to_string()),
                Cell::from(t.replication_factor.to_string()),
                Cell::from(t.under_replicated.to_string()),
                Cell::from(t.records.map_or("-".to_string(), format_count)),
                Cell::from(format_rate(t.rate)),
                Cell::from(t.offline.to_string()),
            ])
            .style(style)
        })
        .collect();
    let widths = [
        Constraint::Min(20),
        Constraint::Length(8),
        Constraint::Length(5),
        Constraint::Length(11),
        Constraint::Length(15),
        Constraint::Length(10),
        Constraint::Length(8),
    ];
    let table = Table::new(rows, widths)
        .header(header(TopicColumn::ALL, app.topic_sort, &["Offline"]))
        .block(Block::bordered().title(list_title("Topics", topics.len(), total, &app.filter)))
        .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED))
        .highlight_symbol("> ");
    frame.render_stateful_widget(table, area, &mut selection(app, topics.len()));
}

fn state_style(state: &ConsumerState) -> Style {
    match state {
        ConsumerState::Stable => Style::new().fg(Color::Green),
        ConsumerState::Rebalancing => Style::new().fg(Color::Yellow),
        ConsumerState::Empty => Style::new().fg(Color::Gray),
        ConsumerState::Dead => Style::new().fg(Color::Red),
    }
}

fn draw_groups(frame: &mut Frame, app: &App, area: Rect) {
    let total = app.data.as_ref().map_or(0, |d| d.groups.len());
    let groups = app.visible_groups();
    let rows: Vec<Row> = groups
        .iter()
        .map(|g| {
            let lag_style = if g.lag > 0 {
                Style::new().fg(Color::Yellow)
            } else {
                Style::new()
            };
            Row::new(vec![
                Cell::from(g.group.clone()),
                Cell::from(g.state.as_str()).style(state_style(&g.state)),
                Cell::from(g.members.to_string()),
                Cell::from(format_count(g.lag)).style(lag_style),
                Cell::from(g.trend.arrow()),
                Cell::from(g.topics.join(", ")),
            ])
        })
        .collect();
    let widths = [
        Constraint::Min(20),
        Constraint::Length(12),
        Constraint::Length(9),
        Constraint::Length(15),
        Constraint::Length(5),
        Constraint::Min(10),
    ];
    let table = Table::new(rows, widths)
        .header(header(
            GroupColumn::ALL,
            app.group_sort,
            &["Trend", "Topics"],
        ))
        .block(Block::bordered().title(list_title(
            "Consumer groups",
            groups.len(),
            total,
            &app.filter,
        )))
        .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED))
        .highlight_symbol("> ");
    frame.render_stateful_widget(table, area, &mut selection(app, groups.len()));
}

fn draw_findings(frame: &mut Frame, app: &App, area: Rect) {
    let total = app.data.as_ref().map_or(0, |d| d.findings.len());
    let findings = app.visible_findings();
    let rows: Vec<Row> = findings
        .iter()
        .map(|f| {
            Row::new(vec![
                Cell::from(severity_label(f.severity)).style(severity_style(f.severity)),
                Cell::from(f.message.clone()),
            ])
        })
        .collect();
    let table = Table::new(rows, [Constraint::Length(9), Constraint::Min(20)])
        .header(Row::new(vec!["Severity", "Finding"]).style(bold()))
        .block(Block::bordered().title(list_title(
            "Health findings",
            findings.len(),
            total,
            &app.filter,
        )))
        .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED))
        .highlight_symbol("> ");
    frame.render_stateful_widget(table, area, &mut selection(app, findings.len()));
}

fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or("")
}

fn draw_status(frame: &mut Frame, app: &App, now: Instant, area: Rect) {
    let mut spans = vec![Span::styled(format!(" {} ", app.cluster), bold())];
    let updated = match app.updated_at {
        Some(at) => format!("updated {}", format_age(now.saturating_duration_since(at))),
        None => "not updated yet".to_string(),
    };
    spans.push(Span::raw(format!(
        "| {updated}, every {}s ",
        app.interval.as_secs()
    )));
    if app.fetching {
        spans.push(Span::styled("| refreshing ", Style::new().fg(Color::Cyan)));
    }
    if app.editing_filter {
        spans.push(Span::styled(
            format!("| filter: {}_ ", app.filter),
            bold().fg(Color::Yellow),
        ));
    } else if !app.filter.is_empty() {
        spans.push(Span::raw(format!("| filter: {} ", app.filter)));
    }
    if let Some(e) = &app.error {
        spans.push(Span::styled(
            format!("| error: {} ", first_line(e)),
            bold().fg(Color::Red),
        ));
    }
    spans.push(Span::styled(
        "| ? help  q quit",
        Style::new().fg(Color::Gray),
    ));
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

const HELP: &[(&str, &str)] = &[
    ("Tab / Right / l", "next view"),
    ("Shift+Tab / Left / h", "previous view"),
    ("1-4", "go to a view"),
    ("Up Down / j k", "move the selection"),
    ("PgUp PgDn Home End", "move by a page, to the ends"),
    ("/", "filter by name (Enter keeps it, Esc clears it)"),
    ("s / S", "sort by the next column / reverse the order"),
    ("r", "refresh now"),
    ("?", "show or hide this help"),
    ("q / Esc / Ctrl+C", "quit (Esc clears a filter first)"),
];

/// `width` x `height` in the middle of `area`, clipped to it.
fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    )
}

fn draw_help(frame: &mut Frame, area: Rect) {
    let mut lines: Vec<Line> = HELP
        .iter()
        .map(|(keys, what)| {
            Line::from(vec![
                Span::styled(format!("{keys:<22}"), bold().fg(Color::Yellow)),
                Span::raw(*what),
            ])
        })
        .collect();
    lines.push(Line::raw(""));
    lines.push(Line::styled(
        "Read-only: the dashboard never changes the cluster.",
        Style::new().fg(Color::Gray),
    ));
    let height = lines.len() as u16 + 2;
    let popup = centered(area, 72, height);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines).block(Block::bordered().title(" Keys ")),
        popup,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::tui::state::tests::sample_app;
    use crate::cli::tui::state::Update;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    /// Renders `app` on a 100x30 screen and returns its text, one line per row.
    fn render(app: &App) -> String {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal.draw(|f| draw(f, app, Instant::now())).unwrap();
        let buffer = terminal.backend().buffer();
        let mut text = String::new();
        for y in 0..buffer.area.height {
            for x in 0..buffer.area.width {
                text.push_str(buffer[(x, y)].symbol());
            }
            text.push('\n');
        }
        text
    }

    #[test]
    fn overview_shows_cluster_counts_and_findings() {
        let app = sample_app();
        let screen = render(&app);
        assert!(screen.contains("ferrika top"), "{screen}");
        assert!(screen.contains("abc123"), "{screen}");
        assert!(screen.contains("2 online, 1 offline (node 3)"), "{screen}");
        assert!(screen.contains("node 1"), "{screen}");
        assert!(screen.contains("2 (+1 internal)"), "{screen}");
        assert!(screen.contains("total lag 500"), "{screen}");
        assert!(screen.contains("CRITICAL"), "{screen}");
        assert!(screen.contains("updated 0s ago"), "{screen}");
    }

    #[test]
    fn topics_view_lists_filtered_rows_and_the_sort_column() {
        let mut app = sample_app();
        app.view = View::Topics;
        let screen = render(&app);
        assert!(screen.contains("Topics (2)"), "{screen}");
        assert!(screen.contains("Topic ^"), "{screen}");
        assert!(screen.contains("orders"), "{screen}");
        assert!(screen.contains("payments"), "{screen}");
        assert!(!screen.contains("__consumer_offsets"), "{screen}");

        app.filter = "pay".into();
        let screen = render(&app);
        assert!(
            screen.contains("Topics (1 of 2, filter \"pay\")"),
            "{screen}"
        );
        assert!(!screen.contains("orders"), "{screen}");
    }

    #[test]
    fn waiting_screen_and_status_line_show_the_error() {
        let mut app = App::new("dev".into(), std::time::Duration::from_secs(5));
        app.apply(Update::Failed("Broker transport failure\nmore".into()));
        let screen = render(&app);
        assert!(screen.contains("Cannot read dev"), "{screen}");
        assert!(
            screen.contains("error: Broker transport failure"),
            "{screen}"
        );
        assert!(screen.contains("not updated yet"), "{screen}");
    }

    #[test]
    fn help_popup_lists_the_keys() {
        let mut app = sample_app();
        app.show_help = true;
        let screen = render(&app);
        assert!(screen.contains("refresh now"), "{screen}");
        assert!(screen.contains("Read-only"), "{screen}");
    }
}
