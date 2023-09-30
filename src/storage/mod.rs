pub mod redis;
pub mod skip;

#[cfg(feature = "elastic")]
pub mod elastic;

use serde::Deserialize;

use crate::crosscut::{self, PointArg};

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    Skip(skip::Config),
    Redis(redis::Config),

    #[cfg(feature = "elastic")]
    Elastic(elastic::Config),
}

pub enum Stage {
    Skip(skip::Stage),
    Redis(redis::Stage),

    #[cfg(feature = "elastic")]
    Elastic(elastic::Stage),
}

impl Config {
    pub fn plugin(
        self,
        chain: &crosscut::ChainWellKnownInfo,
        intersect: &crosscut::IntersectConfig,
        policy: &crosscut::policies::RuntimePolicy,
    ) -> Stage {
        match self {
            Config::Skip(w) => Stage::Skip(w.bootstrapper()),
            Config::Redis(w) => Stage::Redis(w.bootstrapper(chain, intersect, policy)),

            #[cfg(feature = "elastic")]
            Config::Elastic(w) => Stage::Elastic(w.bootstrapper(chain, intersect, policy)),
        }
    }
}

pub enum Cursor {
    Skip(skip::Cursor),
    Redis(redis::Cursor),

    #[cfg(feature = "elastic")]
    Elastic(elastic::Cursor),
}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<PointArg>, crate::Error> {
        self.last_point()
    }
}
