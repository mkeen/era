use std::time::{Duration, Instant};

use gasket::{framework::WorkerError, metrics::Reading};
use lazy_static::{__Deref, lazy_static};
use log::Log;
use tokio::sync::Mutex;

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

struct TuiConsole {
    last_report: Mutex<Instant>,
    chainsync_progress: indicatif::ProgressBar,
    received_blocks: indicatif::ProgressBar,
    reducer_ops_count: indicatif::ProgressBar,
    storage_ops_count: indicatif::ProgressBar,
    enrich_inserts: indicatif::ProgressBar,
    enrich_removes: indicatif::ProgressBar,
    enrich_matches: indicatif::ProgressBar,
    enrich_mismatches: indicatif::ProgressBar,
    enrich_blocks: indicatif::ProgressBar,
    historic_blocks: indicatif::ProgressBar,
    historic_blocks_removed: indicatif::ProgressBar,
    reducer_errors: indicatif::ProgressBar,
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
    fn build_counter_spinner(
        name: &str,
        container: &indicatif::MultiProgress,
    ) -> indicatif::ProgressBar {
        container.add(
            indicatif::ProgressBar::new_spinner().with_style(
                indicatif::ProgressStyle::default_spinner()
                    .template(&format!(
                        "{:<20} {{msg:<20}} {{pos:>8}} | {{per_sec}}",
                        name
                    ))
                    .unwrap(),
            ),
        )
    }

    fn new() -> Self {
        let container = indicatif::MultiProgress::new();

        Self {
            chainsync_progress: container.add(
                indicatif::ProgressBar::new(0).with_style(
                    indicatif::ProgressStyle::default_bar()
                        .template("Cardano ({prefix}): {wide_bar} {pos}/{len} eta: {eta}\n{msg}")
                        .unwrap(),
                ),
            ),
            received_blocks: Self::build_counter_spinner("received blocks", &container),
            enrich_inserts: Self::build_counter_spinner("enrich inserts", &container),
            enrich_removes: Self::build_counter_spinner("enrich removes", &container),
            enrich_matches: Self::build_counter_spinner("enrich matches", &container),
            enrich_mismatches: Self::build_counter_spinner("enrich mismatches", &container),
            enrich_blocks: Self::build_counter_spinner("enrich blocks", &container),
            reducer_ops_count: Self::build_counter_spinner("reducer ops", &container),
            reducer_errors: Self::build_counter_spinner("reducer errors", &container),
            storage_ops_count: Self::build_counter_spinner("storage ops", &container),
            historic_blocks: Self::build_counter_spinner("history inserts", &container),
            historic_blocks_removed: Self::build_counter_spinner("history removes", &container),
            last_report: Mutex::new(Instant::now()),
        }
    }

    async fn refresh(&self, pipeline: &super::Pipeline) -> Result<(), WorkerError> {
        for tether in pipeline.tethers.iter() {
            let state = match tether.check_state() {
                gasket::runtime::TetherState::Dropped => "dropped!",
                gasket::runtime::TetherState::Blocked(_) => "blocked",
                gasket::runtime::TetherState::Alive(x) => match x {
                    gasket::runtime::StagePhase::Bootstrap => "bootstrapping...",
                    gasket::runtime::StagePhase::Working => "working",
                    gasket::runtime::StagePhase::Teardown => "tearing down...",
                    gasket::runtime::StagePhase::Ended => "ended...",
                },
            };

            if state == "blocked" {
                log::warn!("{} is blocked", tether.name());
            }

            match tether.read_metrics() {
                Ok(readings) => {
                    for (key, value) in readings {
                        match (tether.name(), key, value) {
                            (_, "chain_tip", Reading::Gauge(x)) => {
                                self.chainsync_progress.set_length(x as u64);
                            }
                            (_, "last_block", Reading::Gauge(x)) => {
                                self.chainsync_progress.set_position(x as u64);
                            }
                            (_, "received_blocks", Reading::Count(x)) => {
                                self.received_blocks.set_position(x);
                                self.received_blocks.set_message(state);
                            }
                            (_, "ops_count", Reading::Count(x)) => {
                                self.reducer_ops_count.set_position(x);
                                self.reducer_ops_count.set_message(state);
                            }
                            (_, "reducer_errors", Reading::Count(x)) => {
                                self.reducer_errors.set_position(x);
                                self.reducer_errors.set_message(state);
                            }
                            (_, "storage_ops", Reading::Count(x)) => {
                                self.storage_ops_count.set_position(x);
                                self.storage_ops_count.set_message(state);
                            }
                            (_, "enrich_inserts", Reading::Count(x)) => {
                                self.enrich_inserts.set_position(x);
                                self.enrich_inserts.set_message(state);
                            }
                            (_, "enrich_removes", Reading::Count(x)) => {
                                self.enrich_removes.set_position(x);
                                self.enrich_removes.set_message(state);
                            }
                            (_, "enrich_matches", Reading::Count(x)) => {
                                self.enrich_matches.set_position(x);
                                self.enrich_matches.set_message(state);
                            }
                            (_, "enrich_mismatches", Reading::Count(x)) => {
                                self.enrich_mismatches.set_position(x);
                                self.enrich_mismatches.set_message(state);
                            }
                            (_, "enrich_blocks", Reading::Count(x)) => {
                                self.enrich_blocks.set_position(x);
                                self.enrich_blocks.set_message(state);
                            }
                            (_, "historic_blocks", Reading::Count(x)) => {
                                self.historic_blocks.set_position(x);
                                self.historic_blocks.set_message("");
                            }
                            (_, "historic_blocks_removed", Reading::Count(x)) => {
                                self.historic_blocks_removed.set_position(x);
                                self.historic_blocks_removed.set_message("");
                            }
                            (_, "chain_era", Reading::Gauge(x)) => {
                                self.chainsync_progress.set_prefix(i64_to_string(x));
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

        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        Ok(())
    }
}

impl Log for TuiConsole {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() >= log::Level::Info
    }

    fn log(&self, record: &log::Record) {
        self.chainsync_progress
            .set_message(format!("{}", record.args()))
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

    async fn refresh(&self, pipeline: &super::Pipeline) -> Result<(), WorkerError> {
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

lazy_static! {
    static ref TUI_CONSOLE: TuiConsole = TuiConsole::new();
}

lazy_static! {
    static ref PLAIN_CONSOLE: PlainConsole = PlainConsole::new();
}

pub fn initialize(mode: &Option<Mode>) {
    match mode {
        Some(Mode::TUI) => log::set_logger(TUI_CONSOLE.deref())
            .map(|_| log::set_max_level(log::LevelFilter::Info))
            .unwrap(),
        _ => env_logger::init(),
    }
}

pub async fn refresh(mode: &Option<Mode>, pipeline: &super::Pipeline) -> Result<(), WorkerError> {
    match mode {
        Some(Mode::TUI) => TUI_CONSOLE.refresh(pipeline).await,
        _ => PLAIN_CONSOLE.refresh(pipeline).await,
    }
}
