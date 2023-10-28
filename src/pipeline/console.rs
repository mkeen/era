use std::ops::{Deref, DerefMut};
use std::time::Instant;

use gasket::{
    framework::WorkerError,
    metrics::Reading,
    runtime::{StagePhase, TetherState},
};
use lazy_static::{__Deref, lazy_static};
use log::Log;
use ratatui::prelude::Layout;
use ratatui::widgets::{Borders, Padding};
use tokio::sync::Mutex;

use super::StageTypes;

use crossterm::{
    event::{self, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{prelude::*, widgets::Paragraph};
use std::io::{stdout, Stdout};

#[derive(clap::ValueEnum, Clone)]
pub enum Mode {
    /// shows progress as a plain sequence of logs
    Plain,
    /// shows aggregated progress and metrics
    TUI,
}

impl Default for Mode {
    fn default() -> Self {
        Mode::Plain
    }
}

struct MeteredNumber {
    value: u64,
}

impl Default for MeteredNumber {
    fn default() -> Self {
        Self { value: 0 }
    }
}

impl MeteredNumber {
    pub fn set(&mut self, to: u64) {
        self.value = to;
    }
}

struct MeteredString {
    value: String,
}

impl Default for MeteredString {
    fn default() -> Self {
        Self {
            value: "Byron".into(),
        }
    }
}

impl MeteredString {
    pub fn set(&mut self, to: &str) {
        self.value = to.to_string();
    }
}

enum MeteredValue {
    Numerical(MeteredNumber),
    Label(MeteredString),
}

impl MeteredValue {
    pub fn set_num(&mut self, to: u64) {
        match self {
            MeteredValue::Numerical(metered_number) => {
                metered_number.set(to);
            }
            MeteredValue::Label(_) => {}
        }
    }

    pub fn set_str(&mut self, to: &str) {
        match self {
            MeteredValue::Numerical(_) => {}
            MeteredValue::Label(metered_string) => {
                metered_string.set(to);
            }
        }
    }

    pub fn get_num(&self) -> u64 {
        match self {
            MeteredValue::Numerical(metered_number) => metered_number.value.clone(),
            MeteredValue::Label(_) => 0,
        }
    }

    pub fn get_str(&self) -> String {
        match self {
            MeteredValue::Numerical(_) => "".into(),
            MeteredValue::Label(metered_string) => metered_string.value.clone(),
        }
    }
}

pub struct MetricsSnapshot {
    chain_bar_depth: MeteredValue,
    chain_bar_progress: MeteredValue,
    chain_era: MeteredValue,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            chain_bar_depth: MeteredValue::Numerical(Default::default()),
            chain_bar_progress: MeteredValue::Numerical(Default::default()),
            chain_era: MeteredValue::Label(Default::default()),
        }
    }
}

struct TuiConsole {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl Deref for TuiConsole {
    type Target = Terminal<CrosstermBackend<std::io::Stdout>>;

    fn deref(&self) -> &Self::Target {
        &self.terminal
    }
}

impl DerefMut for TuiConsole {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.terminal
    }
}

pub fn i64_to_string(mut i: i64) -> String {
    let mut bytes = Vec::new();

    while i != 0 {
        let byte = (i & 0xFF) as u8;
        // Skip if it's a padding byte
        if byte != 0 {
            bytes.push(byte);
        }
        i >>= 8;
    }

    let s = std::string::String::from_utf8(bytes).unwrap();

    s.chars().rev().collect::<String>()
}

impl TuiConsole {
    fn new() -> Self {
        stdout().execute(EnterAlternateScreen).unwrap();
        enable_raw_mode().unwrap();
        Self {
            terminal: Terminal::new(CrosstermBackend::new(stdout())).unwrap(),
        }
    }

    fn draw(&mut self, snapshot: MetricsSnapshot) {
        let current_era = snapshot.chain_era.get_str();

        self.terminal.draw(|frame| {
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints(vec![
                    Constraint::Min(20),
                    Constraint::Length(3),
                    Constraint::Length(3),
                ])
                .split(frame.size());

            let progress_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints(vec![
                    Constraint::Length(15),
                    Constraint::Min(50),
                    Constraint::Length(15),
                ])
                .split(layout[1]);

            let progress_footer_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints(vec![
                    Constraint::Length(15),
                    Constraint::Min(25),
                    Constraint::Min(25),
                    Constraint::Length(15),
                ])
                .split(layout[2]);

            let progress = ratatui::widgets::Gauge::default()
                .block(
                    ratatui::widgets::Block::default()
                        .borders(Borders::ALL)
                        .padding(Padding::new(0, 0, 0, 0)),
                )
                .gauge_style(Style::new().light_blue())
                .percent(
                    match snapshot.chain_bar_depth.get_num() > 0
                        && snapshot.chain_bar_progress.get_num() > 0
                    {
                        true => (snapshot.chain_bar_progress.get_num() as f64
                            / snapshot.chain_bar_depth.get_num() as f64
                            * 100.0)
                            .round() as u16,
                        false => 0,
                    },
                );

            let bottom_pane = ratatui::widgets::Block::default().title("Hi");

            frame.render_widget(bottom_pane, layout[0]);

            frame.render_widget(
                Paragraph::new(snapshot.chain_era.get_str())
                    .block(ratatui::widgets::Block::new().padding(Padding::new(
                        0,                             // left
                        1,                             // right
                        progress_layout[0].height / 2, // top
                        0,                             // bottom
                    )))
                    .alignment(Alignment::Right),
                progress_layout[0],
            );

            frame.render_widget(
                Paragraph::new(snapshot.chain_bar_progress.get_str())
                    .block(ratatui::widgets::Block::new().padding(Padding::new(
                        1, // left
                        0, // right
                        0, // top
                        0, // bottom
                    )))
                    .alignment(Alignment::Left),
                progress_footer_layout[1],
            );

            frame.render_widget(progress, progress_layout[1]);

            frame.render_widget(
                Paragraph::new(snapshot.chain_bar_depth.get_str())
                    .block(ratatui::widgets::Block::new().padding(Padding::new(
                        1,                             // left
                        0,                             // right
                        progress_layout[0].height / 2, // top
                        0,                             // bottom
                    )))
                    .alignment(Alignment::Left),
                progress_layout[2],
            );

            // frame.render_widget(Paragraph::new("Bottom"), layout[0]);
            // frame.render_widget(Paragraph::new("Bottom"), layout[1]);
        });
    }

    fn refresh(&mut self, pipeline: &super::Pipeline) -> Result<(), WorkerError> {
        let mut snapshot = MetricsSnapshot::default();

        for tether in pipeline.tethers.iter() {
            let state = match tether.check_state() {
                TetherState::Dropped => "dropped!",
                TetherState::Blocked(_) => "blocked",
                TetherState::Alive(a) => match a {
                    StagePhase::Bootstrap => "bootstrapping...",
                    StagePhase::Working => "working",
                    StagePhase::Teardown => "tearing down...",
                    StagePhase::Ended => "ended...",
                },
            };

            if state == "blocked" {
                log::warn!("{} is blocked", tether.name());
            }

            let tether_type: StageTypes = tether.name().into();

            // match tether_type {
            //     StageTypes::Source => self.tether_source.set_message(state),
            //     StageTypes::Enrich => self.tether_enrich.set_message(state),
            //     StageTypes::Reduce => self.tether_reduce.set_message(state),
            //     StageTypes::Storage => self.tether_storage.set_message(state),
            //     StageTypes::Unknown => {}
            // }

            match tether.read_metrics() {
                Ok(readings) => {
                    for (key, value) in readings {
                        match (tether.name(), key, value) {
                            (_, "chain_tip", Reading::Gauge(x)) => {
                                if x > 0 {
                                    snapshot.chain_bar_depth.set_num(x as u64);
                                }
                            }
                            (_, "last_block", Reading::Gauge(x)) => {
                                if x > 0 {
                                    snapshot.chain_bar_progress.set_num(x as u64);
                                }
                            }
                            // (_, "received_blocks", Reading::Count(x)) => {
                            //     self.received_blocks.set_position(x);
                            //     self.received_blocks.set_message(state);
                            // }
                            // (_, "ops_count", Reading::Count(x)) => {
                            //     self.reducer_ops_count.set_position(x);
                            //     self.reducer_ops_count.set_message(state);
                            // }
                            // (_, "reducer_errors", Reading::Count(x)) => {
                            //     self.reducer_errors.set_position(x);
                            //     self.reducer_errors.set_message(state);
                            // }
                            // (_, "storage_ops", Reading::Count(x)) => {
                            //     self.storage_ops_count.set_position(x);
                            //     self.storage_ops_count.set_message(state);
                            // }
                            // (_, "enrich_inserts", Reading::Count(x)) => {
                            //     self.enrich_inserts.set_position(x);
                            //     self.enrich_inserts.set_message(state);
                            // }
                            // (_, "enrich_cancelled_empty_tx", Reading::Count(x)) => {
                            //     self.enrich_skipped_empty.set_position(x);
                            //     self.enrich_skipped_empty.set_message(state);
                            // }
                            // (_, "enrich_removes", Reading::Count(x)) => {
                            //     self.enrich_removes.set_position(x);
                            //     self.enrich_removes.set_message(state);
                            // }
                            // (_, "enrich_matches", Reading::Count(x)) => {
                            //     self.enrich_matches.set_position(x);
                            //     self.enrich_matches.set_message(state);
                            // }
                            // (_, "enrich_mismatches", Reading::Count(x)) => {
                            //     self.enrich_mismatches.set_position(x);
                            //     self.enrich_mismatches.set_message(state);
                            // }
                            // (_, "enrich_blocks", Reading::Count(x)) => {
                            //     self.enrich_blocks.set_position(x);
                            //     self.enrich_blocks.set_message(state);
                            // }
                            // (_, "historic_blocks", Reading::Count(x)) => {
                            //     self.historic_blocks.set_position(x);
                            //     self.historic_blocks.set_message("");
                            // }
                            // (_, "historic_blocks_removed", Reading::Count(x)) => {
                            //     self.historic_blocks_removed.set_position(x);
                            //     self.historic_blocks_removed.set_message("");
                            // }
                            (_, "chain_era", Reading::Gauge(x)) => {
                                if x > 0 {
                                    snapshot.chain_era.set_str(i64_to_string(x).as_str());
                                }
                            }
                            _ => (),
                        }
                    }
                }
                Err(_) => {
                    log::warn!("couldn't read metrics");
                }
            };
        }

        self.draw(snapshot);

        Ok(())
    }
}

impl Log for TuiConsole {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() >= log::Level::Info
    }

    fn log(&self, record: &log::Record) {
        // self.chainsync_progress
        //     .set_message(format!("{}", record.args()))
    }

    fn flush(&self) {}
}

struct PlainConsole {
    last_report: Mutex<Instant>,
}

impl PlainConsole {
    fn new() -> Self {
        Self {
            last_report: Mutex::new(Instant::now()),
        }
    }

    fn refresh(&self, pipeline: &super::Pipeline) -> Result<(), WorkerError> {
        for tether in pipeline.tethers.iter() {
            match tether.check_state() {
                gasket::runtime::TetherState::Dropped => {
                    log::error!("[{}] stage tether has been dropped", tether.name());
                }
                gasket::runtime::TetherState::Blocked(_) => {
                    log::warn!(
                        "[{}] stage tehter is blocked or not reporting state",
                        tether.name(),
                    );
                }
                gasket::runtime::TetherState::Alive(state) => {
                    log::debug!("[{}] stage is alive with state: {:?}", tether.name(), state);
                    match tether.read_metrics() {
                        Ok(readings) => {
                            for (key, value) in readings {
                                log::debug!("[{}] metric `{}` = {:?}", tether.name(), key, value);
                            }
                        }
                        Err(err) => {
                            log::error!("[{}] error reading metrics: {}", tether.name(), err)
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl log::Log for PlainConsole {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        eprintln!("{}", record.args())
    }

    fn flush(&self) {}
}

enum Logger {
    Tui(TuiConsole),
    Plain(PlainConsole),
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        match self {
            Self::Tui(tui_console) => tui_console.enabled(metadata),
            Self::Plain(plain_console) => plain_console.enabled(metadata),
        }
    }

    fn log(&self, record: &log::Record) {
        match self {
            Self::Tui(tui_console) => tui_console.log(record),
            Self::Plain(plain_console) => plain_console.log(record),
        }
    }

    fn flush(&self) {
        match self {
            Self::Tui(tui_console) => tui_console.flush(),
            Self::Plain(_plain_console) => {}
        }
    }
}

lazy_static! {
    static ref TUI_CONSOLE: Mutex<TuiConsole> = Mutex::new(TuiConsole::new());
}

lazy_static! {
    static ref PLAIN_CONSOLE: PlainConsole = PlainConsole::new();
}

pub async fn initialize(mode: Option<Mode>) {
    let logger = match mode {
        Some(Mode::TUI) => Logger::Tui(TuiConsole::new()),
        _ => Logger::Plain(PlainConsole::new()),
    };

    log::set_boxed_logger(Box::new(logger))
        .map(|_| log::set_max_level(log::LevelFilter::Info))
        .unwrap();
}

pub async fn refresh(mode: &Option<Mode>, pipeline: &super::Pipeline) -> Result<(), WorkerError> {
    let mode = match mode {
        Some(Mode::TUI) => TUI_CONSOLE.lock().await.refresh(pipeline),
        _ => PLAIN_CONSOLE.refresh(pipeline),
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    mode
}
