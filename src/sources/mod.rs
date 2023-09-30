use serde::Deserialize;

use crate::{crosscut, storage};

// #[cfg(target_family = "unix")]
// pub mod n2c;

pub mod n2n;
pub mod utils;

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    N2N(n2n::Config),
    // #[cfg(target_family = "unix")]
    // N2C(n2c::Config),
}

impl Config {
    pub fn plugin(
        &mut self,
        chain: &crosscut::ChainWellKnownInfo,
        block_config: &crosscut::historic::BlockConfig,
        intersect: &crosscut::IntersectConfig,
        finalize: &crosscut::FinalizeConfig,
        policy: &crosscut::policies::RuntimePolicy,
        cursor: &storage::Cursor,
    ) -> n2n::chainsync::Stage {
        match self {
            Config::N2N(c) => {
                c.bootstrapper(chain, block_config, intersect, finalize, policy, cursor)
            }
        }
    }
}

pub enum Stage {
    N2N(n2n::Bootstrapper),
    // N2C(n2c::Bootstrapper),
}
