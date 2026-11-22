//! Dashboard state: the rows each view shows, built from cluster snapshots,
//! and what each key does. No I/O here, so all of it is unit tested.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::kafka::internal_topic::is_internal_topic;
use crate::model::cluster::{ClusterSnapshot, ConsumerState};
use crate::ops::health::{health_checks, HealthIssue};

/// Low and high watermark of each `(topic, partition)` read in a refresh.
pub type Watermarks = HashMap<(String, i32), (i64, i64)>;

/// What the fetch worker reports to the screen.
#[derive(Debug)]
pub enum Update {
    /// A refresh started.
    Started,
    /// A refresh finished.
    Fetched {
        snapshot: Box<ClusterSnapshot>,
        watermarks: Watermarks,
        /// When the snapshot was read.
        at: Instant,
        /// Something that was left out of the snapshot (e.g. watermarks).
        warning: Option<String>,
    },
    /// A refresh failed; the previous data stays on screen.
    Failed(String),
}

/// The dashboard's views, in tab order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    Overview,
    Topics,
    Groups,
    Findings,
}

impl View {
    pub const ALL: [View; 4] = [View::Overview, View::Topics, View::Groups, View::Findings];

    pub fn title(self) -> &'static str {
        match self {
            View::Overview => "Overview",
            View::Topics => "Topics",
            View::Groups => "Consumer groups",
            View::Findings => "Findings",
        }
    }

    pub fn index(self) -> usize {
        match self {
            View::Overview => 0,
            View::Topics => 1,
            View::Groups => 2,
            View::Findings => 3,
        }
    }

    fn next(self) -> Self {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    fn prev(self) -> Self {
        Self::ALL[(self.index() + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

/// How a group's lag moved since the previous refresh.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trend {
    /// First sample of the group.
    Unknown,
    Rising,
    Falling,
    Steady,
}

impl Trend {
    pub fn between(prev: Option<i64>, current: i64) -> Self {
        match prev {
            None => Trend::Unknown,
            Some(p) => match current.cmp(&p) {
                Ordering::Greater => Trend::Rising,
                Ordering::Less => Trend::Falling,
                Ordering::Equal => Trend::Steady,
            },
        }
    }

    pub fn arrow(self) -> &'static str {
        match self {
            Trend::Unknown => "·",
            Trend::Rising => "↑",
            Trend::Falling => "↓",
            Trend::Steady => "→",
        }
    }
}

/// Cluster-wide numbers for the Overview.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Overview {
    pub cluster_id: String,
    pub brokers_online: usize,
    /// Node ids of brokers that host replicas but are not in the metadata.
    pub brokers_offline: Vec<i32>,
    pub controller: Option<i32>,
    /// Topics other than internal ones.
    pub topics: usize,
    pub internal_topics: usize,
    /// Partitions of all topics, internal ones included.
    pub partitions: usize,
    pub under_replicated: usize,
    /// Partitions without a leader.
    pub offline_partitions: usize,
    pub groups: usize,
    pub total_lag: i64,
    /// Records produced per second over all shown topics; `None` until two
    /// refreshes with watermarks have been seen.
    pub produce_rate: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicRow {
    pub name: String,
    pub partitions: usize,
    pub replication_factor: usize,
    pub under_replicated: usize,
    /// Partitions without a leader.
    pub offline: usize,
    /// Records retained (high minus low watermarks); `None` when not every
    /// partition's watermarks could be read.
    pub records: Option<i64>,
    /// Sum of the partitions' high watermarks, the base of `rate`.
    pub end_offsets: Option<i64>,
    /// Records produced per second since the previous refresh.
    pub rate: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupRow {
    pub group: String,
    pub state: ConsumerState,
    pub members: usize,
    pub lag: i64,
    pub trend: Trend,
    pub topics: Vec<String>,
}

/// Everything the views show, from one refresh.
#[derive(Debug, Clone, PartialEq)]
pub struct Dashboard {
    pub overview: Overview,
    /// Topics other than internal ones.
    pub topics: Vec<TopicRow>,
    pub groups: Vec<GroupRow>,
    /// Health findings, most severe first.
    pub findings: Vec<HealthIssue>,
}

/// Records per second between two summed high watermarks `elapsed` apart.
/// `None` when no time passed or the offsets went back (topic recreated).
pub fn rate_per_sec(prev: i64, current: i64, elapsed: Duration) -> Option<f64> {
    let secs = elapsed.as_secs_f64();
    if secs < 0.001 || current < prev {
        return None;
    }
    Some((current - prev) as f64 / secs)
}

impl Dashboard {
    /// Builds the views from `snapshot` and the `watermarks` read with it.
    /// `prev` is the previous refresh and the time since it, for produce
    /// rates and lag trends.
    pub fn build(
        snapshot: &ClusterSnapshot,
        watermarks: &Watermarks,
        prev: Option<(&Dashboard, Duration)>,
    ) -> Self {
        let mut topics = Vec::new();
        let mut overview = Overview {
            cluster_id: snapshot.cluster_id.clone(),
            brokers_online: snapshot.brokers.iter().filter(|b| b.is_online).count(),
            brokers_offline: snapshot
                .brokers
                .iter()
                .filter(|b| !b.is_online)
                .map(|b| b.id)
                .collect(),
            controller: snapshot
                .brokers
                .iter()
                .find(|b| b.is_controller)
                .map(|b| b.id),
            ..Overview::default()
        };

        for t in &snapshot.topics {
            let under_replicated = t
                .partitions
                .iter()
                .filter(|p| p.isr.len() < p.replicas.len())
                .count();
            let offline = t.partitions.iter().filter(|p| p.leader < 0).count();
            overview.partitions += t.partition_count;
            overview.under_replicated += under_replicated;
            overview.offline_partitions += offline;
            if t.is_internal || is_internal_topic(&t.name) {
                overview.internal_topics += 1;
                continue;
            }

            let marks: Option<Vec<(i64, i64)>> = t
                .partitions
                .iter()
                .map(|p| watermarks.get(&(t.name.clone(), p.partition_id)).copied())
                .collect();
            let records: Option<i64> = marks
                .as_ref()
                .map(|m| m.iter().map(|(low, high)| (high - low).max(0)).sum());
            let end_offsets: Option<i64> = marks.map(|m| m.iter().map(|(_, high)| high).sum());
            let rate = match (end_offsets, prev) {
                (Some(current), Some((before, elapsed))) => before
                    .topics
                    .iter()
                    .find(|p| p.name == t.name)
                    .and_then(|p| p.end_offsets)
                    .and_then(|p| rate_per_sec(p, current, elapsed)),
                _ => None,
            };
            topics.push(TopicRow {
                name: t.name.clone(),
                partitions: t.partition_count,
                replication_factor: t.replication_factor,
                under_replicated,
                offline,
                records,
                end_offsets,
                rate,
            });
        }
        overview.topics = topics.len();
        overview.produce_rate = topics
            .iter()
            .filter_map(|t| t.rate)
            .fold(None, |sum, r| Some(sum.unwrap_or(0.0) + r));

        let groups: Vec<GroupRow> = snapshot
            .consumers
            .iter()
            .map(|g| {
                let before = prev.and_then(|(d, _)| {
                    d.groups
                        .iter()
                        .find(|p| p.group == g.group_id)
                        .map(|p| p.lag)
                });
                GroupRow {
                    group: g.group_id.clone(),
                    state: g.state.clone(),
                    members: g.active_members,
                    lag: g.total_lag,
                    trend: Trend::between(before, g.total_lag),
                    topics: g.target_topics.clone(),
                }
            })
            .collect();
        overview.groups = groups.len();
        overview.total_lag = groups.iter().map(|g| g.lag).sum();

        Dashboard {
            overview,
            topics,
            groups,
            findings: health_checks(snapshot),
        }
    }
}

/// A sortable column of a view.
pub trait Column: Copy + Eq + 'static {
    const ALL: &'static [Self];
    fn label(self) -> &'static str;
    /// Numbers sort largest first, names A to Z.
    fn descending_by_default(self) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicColumn {
    Name,
    Partitions,
    Replication,
    UnderReplicated,
    Records,
    Rate,
}

impl Column for TopicColumn {
    const ALL: &'static [Self] = &[
        TopicColumn::Name,
        TopicColumn::Partitions,
        TopicColumn::Replication,
        TopicColumn::UnderReplicated,
        TopicColumn::Records,
        TopicColumn::Rate,
    ];

    fn label(self) -> &'static str {
        match self {
            TopicColumn::Name => "Topic",
            TopicColumn::Partitions => "Parts",
            TopicColumn::Replication => "RF",
            TopicColumn::UnderReplicated => "Under-rep",
            TopicColumn::Records => "Records",
            TopicColumn::Rate => "Rate",
        }
    }

    fn descending_by_default(self) -> bool {
        self != TopicColumn::Name
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupColumn {
    Name,
    State,
    Members,
    Lag,
}

impl Column for GroupColumn {
    const ALL: &'static [Self] = &[
        GroupColumn::Name,
        GroupColumn::State,
        GroupColumn::Members,
        GroupColumn::Lag,
    ];

    fn label(self) -> &'static str {
        match self {
            GroupColumn::Name => "Group",
            GroupColumn::State => "State",
            GroupColumn::Members => "Members",
            GroupColumn::Lag => "Lag",
        }
    }

    fn descending_by_default(self) -> bool {
        !matches!(self, GroupColumn::Name | GroupColumn::State)
    }
}

/// The column a view is sorted by, and the direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sort<C> {
    pub column: C,
    pub descending: bool,
}

impl<C: Column> Sort<C> {
    pub fn by(column: C) -> Self {
        Sort {
            column,
            descending: column.descending_by_default(),
        }
    }

    /// The next column, in its natural direction.
    pub fn cycle(self) -> Self {
        let i = C::ALL.iter().position(|c| *c == self.column).unwrap_or(0);
        Sort::by(C::ALL[(i + 1) % C::ALL.len()])
    }

    pub fn reversed(self) -> Self {
        Sort {
            column: self.column,
            descending: !self.descending,
        }
    }

    /// `ordering` of the column, turned for the direction.
    fn apply(self, ordering: Ordering) -> Ordering {
        if self.descending {
            ordering.reverse()
        } else {
            ordering
        }
    }
}

/// Whether `text` contains `filter`, ignoring case; an empty filter matches.
pub fn matches_filter(filter: &str, text: &str) -> bool {
    filter.is_empty() || text.to_lowercase().contains(&filter.to_lowercase())
}

fn cmp_f64(a: Option<f64>, b: Option<f64>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.total_cmp(&b),
        (a, b) => a.is_some().cmp(&b.is_some()),
    }
}

/// Sorts `rows` by `sort`, then by name.
pub fn sort_topics(rows: &mut [&TopicRow], sort: Sort<TopicColumn>) {
    rows.sort_by(|a, b| {
        let by_column = match sort.column {
            TopicColumn::Name => Ordering::Equal,
            TopicColumn::Partitions => a.partitions.cmp(&b.partitions),
            TopicColumn::Replication => a.replication_factor.cmp(&b.replication_factor),
            TopicColumn::UnderReplicated => a.under_replicated.cmp(&b.under_replicated),
            TopicColumn::Records => a.records.cmp(&b.records),
            TopicColumn::Rate => cmp_f64(a.rate, b.rate),
        };
        let name = a.name.cmp(&b.name);
        if sort.column == TopicColumn::Name {
            sort.apply(name)
        } else {
            sort.apply(by_column).then(name)
        }
    });
}

/// Sorts `rows` by `sort`, then by name.
pub fn sort_groups(rows: &mut [&GroupRow], sort: Sort<GroupColumn>) {
    rows.sort_by(|a, b| {
        let by_column = match sort.column {
            GroupColumn::Name => Ordering::Equal,
            GroupColumn::State => a.state.as_str().cmp(b.state.as_str()),
            GroupColumn::Members => a.members.cmp(&b.members),
            GroupColumn::Lag => a.lag.cmp(&b.lag),
        };
        let name = a.group.cmp(&b.group);
        if sort.column == GroupColumn::Name {
            sort.apply(name)
        } else {
            sort.apply(by_column).then(name)
        }
    });
}

/// `1234567` as `1,234,567`.
pub fn format_count(n: i64) -> String {
    let digits = n.unsigned_abs().to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3 + 1);
    if n < 0 {
        out.push('-');
    }
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// A rate in records per second, `-` when unknown.
pub fn format_rate(rate: Option<f64>) -> String {
    match rate {
        None => "-".to_string(),
        Some(r) if r < 10.0 => format!("{r:.1}/s"),
        Some(r) if r < 1_000.0 => format!("{r:.0}/s"),
        Some(r) if r < 1_000_000.0 => format!("{:.1}k/s", r / 1_000.0),
        Some(r) => format!("{:.1}M/s", r / 1_000_000.0),
    }
}

/// How long ago something happened, e.g. `3s ago`, `2m 5s ago`.
pub fn format_age(age: Duration) -> String {
    let secs = age.as_secs();
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m {}s ago", secs / 60, secs % 60)
    } else {
        format!("{}h {}m ago", secs / 3600, (secs % 3600) / 60)
    }
}

/// What the event loop does after a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    None,
    Quit,
    Refresh,
}

/// Rows moved by PageUp and PageDown.
const PAGE: usize = 10;

/// The dashboard's state.
#[derive(Debug)]
pub struct App {
    /// The profile or the bootstrap servers, for the status line.
    pub cluster: String,
    pub interval: Duration,
    pub view: View,
    /// The last refresh that succeeded.
    pub data: Option<Dashboard>,
    /// When `data` was read.
    pub updated_at: Option<Instant>,
    pub fetching: bool,
    /// Why the last refresh failed or what it left out.
    pub error: Option<String>,
    pub filter: String,
    pub editing_filter: bool,
    pub topic_sort: Sort<TopicColumn>,
    pub group_sort: Sort<GroupColumn>,
    /// Selected row of each view, by [`View::index`].
    pub selected: [usize; 4],
    pub show_help: bool,
}

impl App {
    pub fn new(cluster: String, interval: Duration) -> Self {
        App {
            cluster,
            interval,
            view: View::Overview,
            data: None,
            updated_at: None,
            fetching: false,
            error: None,
            filter: String::new(),
            editing_filter: false,
            topic_sort: Sort::by(TopicColumn::Name),
            group_sort: Sort::by(GroupColumn::Lag),
            selected: [0; 4],
            show_help: false,
        }
    }

    /// Takes in what the fetch worker reported.
    pub fn apply(&mut self, update: Update) {
        match update {
            Update::Started => self.fetching = true,
            Update::Fetched {
                snapshot,
                watermarks,
                at,
                warning,
            } => {
                let prev = match (&self.data, self.updated_at) {
                    (Some(d), Some(t)) => Some((d, at.saturating_duration_since(t))),
                    _ => None,
                };
                let data = Dashboard::build(&snapshot, &watermarks, prev);
                self.data = Some(data);
                self.updated_at = Some(at);
                self.fetching = false;
                self.error = warning;
                self.clamp_selection();
            }
            Update::Failed(reason) => {
                self.fetching = false;
                self.error = Some(reason);
            }
        }
    }

    /// Topics passing the filter, in sort order.
    pub fn visible_topics(&self) -> Vec<&TopicRow> {
        let mut rows: Vec<&TopicRow> = self
            .data
            .iter()
            .flat_map(|d| &d.topics)
            .filter(|t| matches_filter(&self.filter, &t.name))
            .collect();
        sort_topics(&mut rows, self.topic_sort);
        rows
    }

    /// Groups passing the filter, in sort order.
    pub fn visible_groups(&self) -> Vec<&GroupRow> {
        let mut rows: Vec<&GroupRow> = self
            .data
            .iter()
            .flat_map(|d| &d.groups)
            .filter(|g| matches_filter(&self.filter, &g.group))
            .collect();
        sort_groups(&mut rows, self.group_sort);
        rows
    }

    /// Findings passing the filter, most severe first.
    pub fn visible_findings(&self) -> Vec<&HealthIssue> {
        self.data
            .iter()
            .flat_map(|d| &d.findings)
            .filter(|f| matches_filter(&self.filter, &f.message))
            .collect()
    }

    /// Rows of the current view.
    fn row_count(&self) -> usize {
        match self.view {
            View::Overview => 0,
            View::Topics => self.visible_topics().len(),
            View::Groups => self.visible_groups().len(),
            View::Findings => self.visible_findings().len(),
        }
    }

    /// Selected row of the current view.
    pub fn selected_row(&self) -> usize {
        self.selected[self.view.index()]
    }

    fn select(&mut self, row: usize) {
        let last = self.row_count().saturating_sub(1);
        self.selected[self.view.index()] = row.min(last);
    }

    fn move_by(&mut self, delta: isize) {
        let row = self.selected_row().saturating_add_signed(delta);
        self.select(row);
    }

    /// Keeps every view's selection inside its rows after new data.
    fn clamp_selection(&mut self) {
        let current = self.view;
        for view in View::ALL {
            self.view = view;
            let row = self.selected_row();
            self.select(row);
        }
        self.view = current;
    }

    fn set_filter(&mut self, filter: String) {
        self.filter = filter;
        self.selected = [0; 4];
    }

    /// Handles a key press.
    pub fn handle_key(&mut self, key: KeyEvent) -> Action {
        // Windows reports releases too; act on presses only.
        if key.kind != KeyEventKind::Press {
            return Action::None;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return Action::Quit;
        }
        if self.show_help {
            match key.code {
                KeyCode::Char('q') => return Action::Quit,
                KeyCode::Esc | KeyCode::Char('?') | KeyCode::Enter => self.show_help = false,
                _ => {}
            }
            return Action::None;
        }
        if self.editing_filter {
            match key.code {
                KeyCode::Esc => {
                    self.editing_filter = false;
                    self.set_filter(String::new());
                }
                KeyCode::Enter => self.editing_filter = false,
                KeyCode::Backspace => {
                    let mut filter = self.filter.clone();
                    filter.pop();
                    self.set_filter(filter);
                }
                KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                    let mut filter = self.filter.clone();
                    filter.push(c);
                    self.set_filter(filter);
                }
                _ => {}
            }
            return Action::None;
        }
        match key.code {
            KeyCode::Char('q') => return Action::Quit,
            KeyCode::Esc if self.filter.is_empty() => return Action::Quit,
            KeyCode::Esc => self.set_filter(String::new()),
            KeyCode::Char('r') => return Action::Refresh,
            KeyCode::Char('?') => self.show_help = true,
            KeyCode::Char('/') => self.editing_filter = true,
            KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => self.view = self.view.next(),
            KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => self.view = self.view.prev(),
            KeyCode::Char(c @ '1'..='4') => {
                let i = (c as usize).saturating_sub('1' as usize);
                if let Some(view) = View::ALL.get(i) {
                    self.view = *view;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => self.move_by(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_by(-1),
            KeyCode::PageDown => self.move_by(PAGE as isize),
            KeyCode::PageUp => self.move_by(-(PAGE as isize)),
            KeyCode::Home | KeyCode::Char('g') => self.select(0),
            KeyCode::End | KeyCode::Char('G') => self.select(usize::MAX),
            KeyCode::Char('s') => match self.view {
                View::Topics => self.topic_sort = self.topic_sort.cycle(),
                View::Groups => self.group_sort = self.group_sort.cycle(),
                View::Overview | View::Findings => {}
            },
            KeyCode::Char('S') => match self.view {
                View::Topics => self.topic_sort = self.topic_sort.reversed(),
                View::Groups => self.group_sort = self.group_sort.reversed(),
                View::Overview | View::Findings => {}
            },
            _ => {}
        }
        Action::None
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::model::cluster::{BrokerInfo, ConsumerGroupSummary, PartitionDetail, TopicSummary};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn broker(id: i32, online: bool, controller: bool) -> BrokerInfo {
        BrokerInfo {
            id,
            host: "localhost".into(),
            port: 9092,
            is_controller: controller,
            partition_leaders: 0,
            replicas_hosted: 0,
            is_online: online,
            log_dir_bytes: None,
            log_dirs: Vec::new(),
        }
    }

    fn partition(id: i32, leader: i32, replicas: &[i32], isr: &[i32]) -> PartitionDetail {
        PartitionDetail {
            partition_id: id,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            high_watermark: 0,
            low_watermark: 0,
        }
    }

    fn topic(name: &str, partitions: Vec<PartitionDetail>) -> TopicSummary {
        TopicSummary {
            name: name.into(),
            partition_count: partitions.len(),
            replication_factor: partitions.first().map(|p| p.replicas.len()).unwrap_or(0),
            total_messages: 0,
            cleanup_policy: "delete".into(),
            is_internal: name.starts_with("__"),
            size_bytes: None,
            msg_rate: None,
            partitions,
        }
    }

    fn group(name: &str, lag: i64) -> ConsumerGroupSummary {
        ConsumerGroupSummary {
            group_id: name.into(),
            state: ConsumerState::Stable,
            active_members: 1,
            target_topics: vec!["orders".into()],
            current_offset: 0,
            end_offset: 0,
            total_lag: lag,
            coordinator_node: "-".into(),
            consume_rate: None,
            partitions: Vec::new(),
        }
    }

    /// Two brokers online (1 is the controller), broker 3 offline; `orders`
    /// has one under-replicated and one leaderless partition.
    pub(crate) fn sample_snapshot() -> ClusterSnapshot {
        let mut s = ClusterSnapshot::empty("dev", "localhost:9092");
        s.cluster_id = "abc123".into();
        s.brokers = vec![
            broker(1, true, true),
            broker(2, true, false),
            broker(3, false, false),
        ];
        s.topics = vec![
            topic(
                "orders",
                vec![
                    partition(0, 1, &[1, 2, 3], &[1, 2]),
                    partition(1, -1, &[3], &[]),
                ],
            ),
            topic("payments", vec![partition(0, 2, &[1, 2], &[1, 2])]),
            topic(
                "__consumer_offsets",
                vec![partition(0, 1, &[1, 2], &[1, 2])],
            ),
        ];
        s.consumers = vec![group("billing", 500), group("audit", 0)];
        s
    }

    pub(crate) fn sample_watermarks(orders_high: i64) -> Watermarks {
        let mut w = Watermarks::new();
        w.insert(("orders".into(), 0), (0, orders_high));
        w.insert(("orders".into(), 1), (0, 10));
        w.insert(("payments".into(), 0), (5, 25));
        w
    }

    pub(crate) fn sample_app() -> App {
        let mut app = App::new("dev".into(), Duration::from_secs(5));
        app.apply(Update::Fetched {
            snapshot: Box::new(sample_snapshot()),
            watermarks: sample_watermarks(100),
            at: Instant::now(),
            warning: None,
        });
        app
    }

    #[test]
    fn overview_counts_brokers_partitions_and_replication_problems() {
        let d = Dashboard::build(&sample_snapshot(), &sample_watermarks(100), None);
        let o = &d.overview;
        assert_eq!(o.cluster_id, "abc123");
        assert_eq!(o.brokers_online, 2);
        assert_eq!(o.brokers_offline, vec![3]);
        assert_eq!(o.controller, Some(1));
        assert_eq!(o.topics, 2);
        assert_eq!(o.internal_topics, 1);
        assert_eq!(o.partitions, 4);
        // orders p0 (ISR 2 of 3) and p1 (ISR 0 of 1).
        assert_eq!(o.under_replicated, 2);
        assert_eq!(o.offline_partitions, 1);
        assert_eq!(o.groups, 2);
        assert_eq!(o.total_lag, 500);
        assert_eq!(o.produce_rate, None);
        assert!(!d.findings.is_empty());
    }

    #[test]
    fn topic_rows_skip_internal_topics_and_sum_watermarks() {
        let d = Dashboard::build(&sample_snapshot(), &sample_watermarks(100), None);
        let names: Vec<&str> = d.topics.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["orders", "payments"]);
        let orders = &d.topics[0];
        assert_eq!(orders.records, Some(110));
        assert_eq!(orders.end_offsets, Some(110));
        assert_eq!(orders.under_replicated, 2);
        assert_eq!(orders.offline, 1);
        assert_eq!(d.topics[1].records, Some(20));
    }

    #[test]
    fn missing_watermarks_leave_records_and_rate_unknown() {
        let mut marks = sample_watermarks(100);
        marks.remove(&("orders".to_string(), 1));
        let first = Dashboard::build(&sample_snapshot(), &marks, None);
        assert_eq!(first.topics[0].records, None);
        let second = Dashboard::build(
            &sample_snapshot(),
            &marks,
            Some((&first, Duration::from_secs(5))),
        );
        assert_eq!(second.topics[0].rate, None);
        assert_eq!(second.topics[1].rate, Some(0.0));
    }

    #[test]
    fn rate_comes_from_watermark_deltas_between_refreshes() {
        let first = Dashboard::build(&sample_snapshot(), &sample_watermarks(100), None);
        let second = Dashboard::build(
            &sample_snapshot(),
            &sample_watermarks(600),
            Some((&first, Duration::from_secs(5))),
        );
        assert_eq!(second.topics[0].rate, Some(100.0));
        assert_eq!(second.overview.produce_rate, Some(100.0));
    }

    #[test]
    fn rate_per_sec_handles_no_time_and_reset_offsets() {
        assert_eq!(rate_per_sec(0, 50, Duration::from_secs(10)), Some(5.0));
        assert_eq!(rate_per_sec(10, 10, Duration::from_secs(1)), Some(0.0));
        assert_eq!(rate_per_sec(0, 50, Duration::ZERO), None);
        assert_eq!(rate_per_sec(100, 50, Duration::from_secs(1)), None);
    }

    #[test]
    fn lag_trend_compares_with_the_previous_refresh() {
        assert_eq!(Trend::between(None, 5), Trend::Unknown);
        assert_eq!(Trend::between(Some(1), 5), Trend::Rising);
        assert_eq!(Trend::between(Some(9), 5), Trend::Falling);
        assert_eq!(Trend::between(Some(5), 5), Trend::Steady);

        let first = Dashboard::build(&sample_snapshot(), &sample_watermarks(100), None);
        assert_eq!(first.groups[0].trend, Trend::Unknown);
        let mut snap = sample_snapshot();
        snap.consumers[0].total_lag = 800;
        let second = Dashboard::build(
            &snap,
            &sample_watermarks(100),
            Some((&first, Duration::from_secs(5))),
        );
        assert_eq!(second.groups[0].trend, Trend::Rising);
        assert_eq!(second.groups[1].trend, Trend::Steady);
    }

    #[test]
    fn sorting_cycles_columns_in_their_natural_direction() {
        let s = Sort::by(TopicColumn::Name);
        assert!(!s.descending);
        let s = s.cycle();
        assert_eq!(s.column, TopicColumn::Partitions);
        assert!(s.descending);
        assert!(!s.reversed().descending);
        let last = Sort::by(TopicColumn::Rate).cycle();
        assert_eq!(last.column, TopicColumn::Name);
    }

    #[test]
    fn topics_sort_by_rate_with_unknown_rates_last() {
        let row = |name: &str, rate: Option<f64>| TopicRow {
            name: name.into(),
            partitions: 1,
            replication_factor: 1,
            under_replicated: 0,
            offline: 0,
            records: None,
            end_offsets: None,
            rate,
        };
        let (a, b, c) = (row("a", Some(1.0)), row("b", None), row("c", Some(9.0)));
        let mut rows = vec![&a, &b, &c];
        sort_topics(&mut rows, Sort::by(TopicColumn::Rate));
        let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["c", "a", "b"]);
        sort_topics(&mut rows, Sort::by(TopicColumn::Name).reversed());
        let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["c", "b", "a"]);
    }

    #[test]
    fn groups_sort_by_lag_then_name() {
        let app = sample_app();
        let names: Vec<&str> = app
            .visible_groups()
            .iter()
            .map(|g| g.group.as_str())
            .collect();
        assert_eq!(names, ["billing", "audit"]);
        let mut rows = app.visible_groups();
        sort_groups(&mut rows, Sort::by(GroupColumn::Name));
        assert_eq!(rows[0].group, "audit");
    }

    #[test]
    fn filter_ignores_case_and_resets_the_selection() {
        assert!(matches_filter("", "anything"));
        assert!(matches_filter("ORD", "orders"));
        assert!(!matches_filter("pay", "orders"));

        let mut app = sample_app();
        app.handle_key(key(KeyCode::Tab));
        assert_eq!(app.view, View::Topics);
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.selected_row(), 1);
        app.handle_key(key(KeyCode::Char('/')));
        assert!(app.editing_filter);
        for c in "PAY".chars() {
            app.handle_key(key(KeyCode::Char(c)));
        }
        assert_eq!(app.selected_row(), 0);
        app.handle_key(key(KeyCode::Enter));
        assert!(!app.editing_filter);
        let names: Vec<&str> = app
            .visible_topics()
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        assert_eq!(names, ["payments"]);
        // `q` typed while editing goes into the filter, not quit.
        app.handle_key(key(KeyCode::Char('/')));
        assert_eq!(app.handle_key(key(KeyCode::Char('q'))), Action::None);
        assert_eq!(app.filter, "PAYq");
        app.handle_key(key(KeyCode::Backspace));
        app.handle_key(key(KeyCode::Enter));
        // Esc clears an active filter first, then quits.
        assert_eq!(app.handle_key(key(KeyCode::Esc)), Action::None);
        assert!(app.filter.is_empty());
        assert_eq!(app.handle_key(key(KeyCode::Esc)), Action::Quit);
    }

    #[test]
    fn keys_switch_views_move_sort_refresh_and_quit() {
        let mut app = sample_app();
        app.handle_key(key(KeyCode::BackTab));
        assert_eq!(app.view, View::Findings);
        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.view, View::Overview);
        app.handle_key(key(KeyCode::Char('3')));
        assert_eq!(app.view, View::Groups);

        app.handle_key(key(KeyCode::End));
        assert_eq!(app.selected_row(), 1);
        app.handle_key(key(KeyCode::Char('j')));
        assert_eq!(app.selected_row(), 1, "stays on the last row");
        app.handle_key(key(KeyCode::Char('k')));
        app.handle_key(key(KeyCode::Up));
        assert_eq!(app.selected_row(), 0);

        app.handle_key(key(KeyCode::Char('s')));
        assert_eq!(app.group_sort.column, GroupColumn::Name);
        app.handle_key(key(KeyCode::Char('S')));
        assert!(app.group_sort.descending);

        assert_eq!(app.handle_key(key(KeyCode::Char('r'))), Action::Refresh);
        app.handle_key(key(KeyCode::Char('?')));
        assert!(app.show_help);
        app.handle_key(key(KeyCode::Esc));
        assert!(!app.show_help);
        assert_eq!(app.handle_key(key(KeyCode::Char('q'))), Action::Quit);
        assert_eq!(
            app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Action::Quit
        );
    }

    #[test]
    fn key_releases_are_ignored() {
        let mut app = sample_app();
        let mut release = key(KeyCode::Char('q'));
        release.kind = KeyEventKind::Release;
        assert_eq!(app.handle_key(release), Action::None);
    }

    #[test]
    fn failed_refresh_keeps_the_last_data() {
        let mut app = sample_app();
        app.apply(Update::Started);
        assert!(app.fetching);
        app.apply(Update::Failed("timed out".into()));
        assert!(!app.fetching);
        assert_eq!(app.error.as_deref(), Some("timed out"));
        assert!(app.data.is_some());
    }

    #[test]
    fn formatting_helpers() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_234_567), "1,234,567");
        assert_eq!(format_count(-1_000), "-1,000");
        assert_eq!(format_rate(None), "-");
        assert_eq!(format_rate(Some(2.4)), "2.4/s");
        assert_eq!(format_rate(Some(512.0)), "512/s");
        assert_eq!(format_rate(Some(12_345.0)), "12.3k/s");
        assert_eq!(format_rate(Some(2_500_000.0)), "2.5M/s");
        assert_eq!(format_age(Duration::from_secs(3)), "3s ago");
        assert_eq!(format_age(Duration::from_secs(125)), "2m 5s ago");
        assert_eq!(format_age(Duration::from_secs(7_260)), "2h 1m ago");
    }
}
