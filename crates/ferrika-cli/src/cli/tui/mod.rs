//! `ferrika top` on a terminal: a read-only, live dashboard of brokers,
//! topics, consumer group lag and health findings (ratatui).
//!
//! The cluster is read on a worker thread ([`fetch`]); this thread only
//! draws and reads keys, so a slow cluster shows a stale "updated" time and
//! an error in the status line instead of freezing the screen. Nothing here
//! writes to the cluster, and no record values are read. The company policy
//! has no read restrictions for topics or groups, so nothing is filtered by
//! it; it only gates changes, which this view never makes.

mod fetch;
pub mod state;
mod view;

use std::io::{self, Stdout};
use std::sync::mpsc::{self, TryRecvError};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use crate::cli::GlobalOpts;
use crate::error::{KafkaError, KafkaResult};
use state::{Action, App, GroupColumn, Sort, TopicColumn};

/// How often keys are polled and the "updated" age is redrawn.
const TICK: Duration = Duration::from_millis(250);

/// The view each view starts sorted by.
#[derive(Debug, Clone, Copy)]
pub struct InitialSort {
    pub topics: Sort<TopicColumn>,
    pub groups: Sort<GroupColumn>,
}

impl Default for InitialSort {
    fn default() -> Self {
        InitialSort {
            topics: Sort::by(TopicColumn::Name),
            groups: Sort::by(GroupColumn::Lag),
        }
    }
}

/// Runs the dashboard until the user quits. Blocks the calling thread.
pub fn run(global: &GlobalOpts, interval: Duration, sort: InitialSort) -> KafkaResult<()> {
    let cluster = global
        .resolved_profile_name()
        .unwrap_or_else(|| global.resolved_bootstrap_servers());
    let mut app = App::new(cluster, interval);
    app.topic_sort = sort.topics;
    app.group_sort = sort.groups;

    let (refresh_tx, refresh_rx) = mpsc::channel();
    let (updates_tx, updates_rx) = mpsc::channel();
    fetch::spawn(global.clone(), interval, refresh_rx, updates_tx)
        .map_err(|e| KafkaError::io(format!("Cannot start the refresh thread: {e}")))?;

    install_panic_hook();
    let mut screen = Screen::enter().map_err(terminal_error)?;
    let result = event_loop(&mut screen.terminal, &mut app, &refresh_tx, &updates_rx);
    let restored = screen.leave();
    result.map_err(terminal_error)?;
    restored.map_err(terminal_error)
}

fn terminal_error(e: io::Error) -> KafkaError {
    KafkaError::io(format!("Terminal error: {e}"))
}

fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut App,
    refresh: &mpsc::Sender<()>,
    updates: &mpsc::Receiver<state::Update>,
) -> io::Result<()> {
    loop {
        loop {
            match updates.try_recv() {
                Ok(update) => app.apply(update),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    app.fetching = false;
                    app.error = Some("The refresh thread stopped".to_string());
                    break;
                }
            }
        }
        terminal.draw(|frame| view::draw(frame, app, Instant::now()))?;
        if !event::poll(TICK)? {
            continue;
        }
        if let Event::Key(key) = event::read()? {
            match app.handle_key(key) {
                Action::Quit => return Ok(()),
                // A stopped worker shows its error already.
                Action::Refresh => {
                    let _ = refresh.send(());
                }
                Action::None => {}
            }
        }
    }
}

/// The terminal in raw mode on the alternate screen; put back by
/// [`Screen::leave`] or, on an early return, when dropped.
struct Screen {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    active: bool,
}

impl Screen {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(e) = execute!(stdout, EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(e);
        }
        let mut screen = Screen {
            terminal: Terminal::new(CrosstermBackend::new(stdout))?,
            active: true,
        };
        screen.terminal.hide_cursor()?;
        screen.terminal.clear()?;
        Ok(screen)
    }

    fn leave(mut self) -> io::Result<()> {
        self.active = false;
        let shown = self.terminal.show_cursor();
        restore_terminal()?;
        shown
    }
}

impl Drop for Screen {
    fn drop(&mut self) {
        if self.active {
            let _ = self.terminal.show_cursor();
            let _ = restore_terminal();
        }
    }
}

/// Leaves raw mode and the alternate screen.
fn restore_terminal() -> io::Result<()> {
    disable_raw_mode()?;
    execute!(io::stdout(), LeaveAlternateScreen, crossterm::cursor::Show)
}

/// Puts the terminal back before a panic message is printed, so it is
/// readable and the shell is usable afterwards.
fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = restore_terminal();
        previous(info);
    }));
}
