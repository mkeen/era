use std::f32::INFINITY;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use gasket::{
    framework::WorkerError,
    metrics::Reading,
    runtime::{StagePhase, TetherState},
};
use lazy_static::{__Deref, lazy_static};
use log::Log;
use ratatui::prelude::Layout;
use ratatui::widgets::{Axis, Block, BorderType, Borders, Chart, Dataset, GraphType, Padding};
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

#[derive(Clone)]
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

#[derive(Clone)]
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

#[derive(Clone)]
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

pub struct CollectedSnapshotRates {
    chain_bar_progress: f64,
    transactions: f64,
}

#[derive(Clone)]
pub struct MetricsSnapshot {
    timestamp: SystemTime,
    chain_bar_depth: MeteredValue,
    chain_bar_progress: MeteredValue,
    transactions: MeteredValue,
    chain_era: MeteredValue,
    sources_status: String,
    enrich_status: String,
    reducer_status: String,
    storage_status: String,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now(),
            chain_bar_depth: MeteredValue::Numerical(Default::default()),
            chain_bar_progress: MeteredValue::Numerical(Default::default()),
            chain_era: MeteredValue::Label(Default::default()),
            transactions: MeteredValue::Numerical(Default::default()),
            sources_status: "uninitialized".into(),
            enrich_status: "uninitialized".into(),
            reducer_status: "uninitialized".into(),
            storage_status: "uninitialized".into(),
        }
    }
}

impl MetricsSnapshot {
    fn indexed_stub(idx: usize, size: usize) -> Self {
        let mut timestamp = SystemTime::now();

        if idx + 1 != size {
            timestamp = timestamp - Duration::from_secs(1);
        }

        Self {
            timestamp,
            chain_bar_depth: MeteredValue::Numerical(Default::default()),
            chain_bar_progress: MeteredValue::Numerical(Default::default()),
            chain_era: MeteredValue::Label(Default::default()),
            transactions: MeteredValue::Numerical(Default::default()),
            sources_status: "uninitialized".into(),
            enrich_status: "uninitialized".into(),
            reducer_status: "uninitialized".into(),
            storage_status: "uninitialized".into(),
        }
    }

    pub fn set_timestamp(&mut self, val: SystemTime) -> &mut Self {
        self.timestamp = val;
        self
    }
}

pub struct RingBuffer {
    vec: Vec<MetricsSnapshot>,
    capacity: usize,
    start: usize,
    len: usize,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> RingBuffer {
        let mut vec = Vec::with_capacity(capacity);
        for i in 0..capacity {
            let element = MetricsSnapshot::indexed_stub(i, capacity);
            vec.push(element);
        }

        RingBuffer {
            vec,
            capacity,
            start: 0,
            len: capacity,
        }
    }

    pub fn push(&mut self, ele: MetricsSnapshot) {
        if self.len < self.capacity {
            self.vec.push(ele);
            self.len += 1;
        } else {
            self.vec[self.start] = ele.clone();
            self.start = (self.start + 1) % self.capacity;
        }
    }

    pub fn get(&self, index: usize) -> Option<&MetricsSnapshot> {
        if index < self.len {
            Some(&self.vec[(self.start + index) % self.capacity])
        } else {
            None
        }
    }

    pub fn timestamp_window(&self) -> (f64, f64) {
        let mut min: f64 = 0.0;
        let mut max: f64 = 0.0;

        for i in 0..self.len {
            if let Some(snapshot) = self.get(i) {
                if min == 0.0 {
                    min = snapshot
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64();
                }

                if snapshot
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64()
                    < min
                {
                    min = snapshot
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64();
                }

                if snapshot
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64()
                    > max
                {
                    max = snapshot
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64();
                }
            }
        }

        (min, max)
    }

    pub fn transaction_rate(&self) -> [(f64, f64); RING_DEPTH] {
        let mut rates = [(0.0, 0.0); RING_DEPTH];
        for i in 0..self.len {
            if let Some(snapshot) = self.get(i) {
                let next_snapshot = self.get((i + 1) % self.len).unwrap_or(snapshot);

                let next_duration = next_snapshot.timestamp.duration_since(UNIX_EPOCH).unwrap();
                let current_duration = snapshot.timestamp.duration_since(UNIX_EPOCH).unwrap();

                if next_duration > current_duration {
                    let time_diff = (next_duration - current_duration).as_secs_f64();
                    let value_diff = next_snapshot.transactions.get_num() as f64
                        - (snapshot.transactions.get_num() as f64);

                    let rate_of_increase = if time_diff != 0.0 {
                        value_diff / time_diff
                    } else {
                        0.0
                    };

                    rates[i] = (
                        snapshot
                            .timestamp
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs_f64(),
                        rate_of_increase,
                    );
                }
            }
        }
        rates
    }

    pub fn transaction_rate_window(&self) -> (f64, f64) {
        let mut min: f64 = 0.0;
        let mut max: f64 = 0.0;

        let transaction_rate = self.transaction_rate();

        for i in 0..transaction_rate.len() {
            if let Some(snapshot) = transaction_rate.get(i) {
                if min == 0.0 {
                    min = snapshot.1;
                }

                if (snapshot.1) < min {
                    min = snapshot.1;
                }

                if (snapshot.1) > max {
                    max = snapshot.1;
                }
            }
        }

        (min, max)
    }

    pub fn chain_bar_progress_rates(&self) -> [(f64, f64); RING_DEPTH] {
        let mut rates = [(0.0, 0.0); RING_DEPTH];
        for i in 0..self.len {
            if let Some(snapshot) = self.get(i) {
                let next_snapshot = self.get((i + 1) % self.len).unwrap_or(snapshot);

                let next_duration = next_snapshot.timestamp.duration_since(UNIX_EPOCH).unwrap();
                let current_duration = snapshot.timestamp.duration_since(UNIX_EPOCH).unwrap();

                if next_duration > current_duration {
                    let time_diff = (next_duration - current_duration).as_secs_f64();
                    let value_diff = next_snapshot.chain_bar_progress.get_num() as f64
                        - (snapshot.chain_bar_progress.get_num() as f64);

                    let rate_of_increase = if time_diff != 0.0 {
                        value_diff / time_diff
                    } else {
                        0.0
                    };

                    rates[i] = (
                        snapshot
                            .timestamp
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs_f64(),
                        rate_of_increase,
                    );
                }
            }
        }
        rates
    }

    pub fn chain_bar_progress_window(&self) -> (f64, f64) {
        let mut min: f64 = 0.0;
        let mut max: f64 = 0.0;

        let chain_bar_progress_rates = self.chain_bar_progress_rates();

        for i in 0..chain_bar_progress_rates.len() {
            if let Some(snapshot) = chain_bar_progress_rates.get(i) {
                if min == 0.0 {
                    min = snapshot.1;
                }

                if (snapshot.1) < min {
                    min = snapshot.1;
                }

                if (snapshot.1) > max {
                    max = snapshot.1;
                }
            }
        }

        (min, max)
    }

    pub fn chain_bar_progress_user_labels(&self) -> Vec<String> {
        let mut labels = vec!["".to_string(); 3];

        let progress_window = self.chain_bar_progress_window();

        labels[0] = progress_window.0.round().to_string();
        labels[1] = ((progress_window.0 + progress_window.1) / 2.0)
            .round()
            .to_string();
        labels[2] = progress_window.1.round().to_string();

        labels
    }
}

struct TuiConsole {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    metrics_buffer: RingBuffer,
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

const RING_DEPTH: usize = 100;

impl TuiConsole {
    fn new() -> Self {
        stdout().execute(EnterAlternateScreen).unwrap();
        enable_raw_mode().unwrap();
        Self {
            terminal: Terminal::new(CrosstermBackend::new(stdout())).unwrap(),
            metrics_buffer: RingBuffer::new(RING_DEPTH),
        }
    }

    fn draw(&mut self, snapshot: MetricsSnapshot) {
        let current_era = snapshot.chain_era.get_str();

        self.terminal.draw(|frame| {
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints(vec![
                    Constraint::Length(9),
                    Constraint::Min(20),
                    Constraint::Length(11),
                    Constraint::Length(3),
                    Constraint::Length(3),
                ])
                .split(frame.size());

            let top_status_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints(vec![Constraint::Length(26), Constraint::Min(60)])
                .split(layout[0]);

            let progress_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints(vec![
                    Constraint::Length(15),
                    Constraint::Min(50),
                    Constraint::Length(15),
                ])
                .split(layout[3]);

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

            //let bottom_pane = ratatui::widgets::Block::default().title("Hi");

            //frame.render_widget(bottom_pane, layout[0]);

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
                Paragraph::new(include_str!("./../../assets/boot.txt"))
                    .block(ratatui::widgets::Block::new().padding(Padding::new(
                        3, // left
                        0, // right
                        1, // top
                        0, // bottom
                    )))
                    .alignment(Alignment::Left),
                top_status_layout[0],
            );

            frame.render_widget(
                Paragraph::new("hi")
                    .block(ratatui::widgets::Block::new())
                    .alignment(Alignment::Left),
                layout[1],
            );

            frame.render_widget(
                Paragraph::new(format!(
                    "Source: {}\nEnrich: {}\nReduce: {}\nStorage: {}",
                    snapshot.sources_status,
                    snapshot.enrich_status,
                    snapshot.reducer_status,
                    snapshot.storage_status
                ))
                .block(
                    ratatui::widgets::Block::new()
                        .padding(Padding::new(
                            3, // left
                            0, // right
                            1, // top
                            0, // bottom
                        ))
                        .borders(Borders::LEFT)
                        .border_type(BorderType::Plain),
                )
                .alignment(Alignment::Left),
                top_status_layout[1],
            );

            frame.render_widget(
                Paragraph::new(snapshot.chain_bar_progress.get_str())
                    .block(Block::new().padding(Padding::new(
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
                    .block(Block::new().padding(Padding::new(
                        1,                             // left
                        0,                             // right
                        progress_layout[0].height / 2, // top
                        0,                             // bottom
                    )))
                    .alignment(Alignment::Left),
                progress_layout[2],
            );

            let metrics_test = self.metrics_buffer.chain_bar_progress_rates();
            let metrics_window_test = self.metrics_buffer.chain_bar_progress_window();
            let time_window = self.metrics_buffer.timestamp_window();

            let transaction_metrics = self.metrics_buffer.transaction_rate();
            let transaction_window = self.metrics_buffer.transaction_rate_window();

            let datasets = vec![
                Dataset::default()
                    .name("Blocks")
                    .marker(symbols::Marker::Braille)
                    .style(Style::default().fg(Color::Cyan))
                    .graph_type(GraphType::Line)
                    .data(&metrics_test),
                Dataset::default()
                    .name("Transactions")
                    .marker(symbols::Marker::Braille)
                    .style(Style::default().fg(Color::Green))
                    .graph_type(GraphType::Line)
                    .data(&transaction_metrics),
                Dataset::default(),
            ];

            let user_labels = self.metrics_buffer.chain_bar_progress_user_labels();

            let chart = Chart::new(datasets)
                .block(
                    Block::default()
                        .title("")
                        .borders(Borders::NONE)
                        .padding(Padding::new(1, 0, 0, 1)),
                )
                .y_axis(
                    Axis::default()
                        .title("")
                        .style(Style::default().fg(Color::Gray))
                        .labels(vec![
                            user_labels[0].as_str().bold(),
                            user_labels[1].as_str().bold(),
                            user_labels[2].as_str().bold(),
                        ])
                        .bounds([0.0, metrics_window_test.1]),
                )
                .x_axis(
                    Axis::default()
                        .title("")
                        .style(Style::default().fg(Color::Gray))
                        .bounds([time_window.0, time_window.1]),
                );
            frame.render_widget(chart, layout[2]);

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
                    StagePhase::Bootstrap => "bootstrapping",
                    StagePhase::Working => "working",
                    StagePhase::Teardown => "tearing down",
                    StagePhase::Ended => "ended",
                },
            };

            if state == "blocked" {
                log::warn!("{} is blocked", tether.name());
            }

            let tether_type: StageTypes = tether.name().into();

            match tether_type {
                StageTypes::Source => snapshot.sources_status = state.into(),
                StageTypes::Enrich => snapshot.enrich_status = state.into(),
                StageTypes::Reduce => snapshot.reducer_status = state.into(),
                StageTypes::Storage => snapshot.storage_status = state.into(),
                StageTypes::Unknown => {}
            }

            let mut transactions_increased = false;

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
                            (_, "enrich_inserts", Reading::Count(x)) => {
                                snapshot.transactions.set_num(x as u64);
                                transactions_increased = true;
                            }
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

        self.metrics_buffer.push(snapshot.clone());
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
