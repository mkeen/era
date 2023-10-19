use std::sync::Arc;

use clap;
use era::{
    crosscut, enrich,
    pipeline::{self, Context},
    reducers, sources, storage,
};
use gasket::runtime::spawn_stage;
use serde::Deserialize;
use tokio::sync::Mutex;

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum ChainConfig {
    Mainnet,
    Testnet,
    PreProd,
    Preview,
    Custom(crosscut::ChainWellKnownInfo),
}

impl Default for ChainConfig {
    fn default() -> Self {
        Self::Mainnet
    }
}

impl From<ChainConfig> for crosscut::ChainWellKnownInfo {
    fn from(other: ChainConfig) -> Self {
        match other {
            ChainConfig::Mainnet => crosscut::ChainWellKnownInfo::mainnet(),
            ChainConfig::Testnet => crosscut::ChainWellKnownInfo::testnet(),
            ChainConfig::PreProd => crosscut::ChainWellKnownInfo::preprod(),
            ChainConfig::Preview => crosscut::ChainWellKnownInfo::preview(),
            ChainConfig::Custom(x) => x,
        }
    }
}

#[derive(Deserialize)]
struct ConfigRoot {
    source: sources::Config,
    enrich: Option<enrich::Config>,
    reducers: Vec<reducers::Config>,
    storage: storage::Config,
    intersect: crosscut::IntersectConfig,
    finalize: Option<crosscut::FinalizeConfig>,
    chain: Option<ChainConfig>,
    blocks: Option<crosscut::historic::BlockConfig>,
    policy: Option<crosscut::policies::RuntimePolicy>,
}

impl ConfigRoot {
    pub fn new(explicit_file: &Option<std::path::PathBuf>) -> Result<Self, config::ConfigError> {
        let mut s = config::Config::builder();

        // our base config will always be in /etc/era
        s = s.add_source(config::File::with_name("/etc/era/config.toml").required(false));

        // but we can override it by having a file in the working dir
        s = s.add_source(config::File::with_name("config.toml").required(false));

        // if an explicit file was passed, then we load it as mandatory
        if let Some(explicit) = explicit_file.as_ref().and_then(|x| x.to_str()) {
            s = s.add_source(config::File::with_name(explicit).required(true));
        }

        // finally, we use env vars to make some last-step overrides
        s = s.add_source(config::Environment::with_prefix("ERA").separator("_"));

        s.build()?.try_deserialize()
    }
}

pub fn run(args: &Args) -> Result<(), era::Error> {
    let config = ConfigRoot::new(&args.config)
        .map_err(|err| era::Error::ConfigError(format!("{:?}", err)))?;

    spawn_stage(
        pipeline::Pipeline::bootstrap(
            Arc::new(Mutex::new(Context {
                chain: config.chain.unwrap_or_default().into(),
                intersect: config.intersect,
                finalize: config.finalize,
                error_policy: config.policy.unwrap_or_default(),
                block_buffer: config.blocks.unwrap().into(),
            })),
            config.source,
            config.enrich.unwrap_or_default(),
            config.reducers,
            config.storage,
            args.console.clone().unwrap_or_default(),
        ),
        Default::default(), // todo dont use default policy
    );

    Ok(())
}

#[derive(clap::Args)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(long, value_parser)]
    //#[clap(description = "config file to load by the daemon")]
    config: Option<std::path::PathBuf>,

    #[clap(long, value_parser)]
    //#[clap(description = "type of progress to display")],
    console: Option<pipeline::console::Mode>,
}
