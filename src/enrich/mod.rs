pub mod skip;
pub mod sled;

use gasket::messaging::tokio::{InputPort, OutputPort};
use serde::Deserialize;

use crate::{bootstrap, crosscut, model};

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    Skip,
    Sled(sled::Config),
}

impl Default for Config {
    fn default() -> Self {
        Self::Skip
    }
}

pub enum Stage {
    Skip(skip::Stage),
    Sled(sled::Stage),
}

impl Config {
    pub fn plugin(
        self,
        policy: &crosscut::policies::RuntimePolicy,
        blocks: &crosscut::historic::BlockConfig,
    ) -> Stage {
        match self {
            Config::Skip => Stage::Skip(skip::Config {}.bootstrapper()),
            Config::Sled(w) => Stage::Sled(w.bootstrapper(policy, blocks)),
        }
    }
}
