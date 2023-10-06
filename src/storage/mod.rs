pub mod redis;
pub mod skip;

#[cfg(feature = "elastic")]
pub mod elastic;

use gasket::{messaging::tokio::InputPort, runtime::Tether};
use serde::Deserialize;

use crate::{
    bootstrap,
    crosscut::{self, PointArg},
    model,
};

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
    pub fn bootstrapper(
        self,
        chain: &crosscut::ChainWellKnownInfo,
        intersect: &crosscut::IntersectConfig,
    ) -> (Cursor, Option<Bootstrapper>) {
        match self {
            Config::Skip(c) => {
                let skip = Bootstrapper::Skip(c.clone().bootstrapper());
                let cursor = skip.build_cursor();
                (cursor, Some(Bootstrapper::Skip(c.bootstrapper())))
            }
            Config::Redis(c) => {
                let redis_s = Bootstrapper::Redis(c.bootstrapper());
                let cursor = redis_s.build_cursor();
                (cursor, Some(Bootstrapper::Redis(c.bootstrapper())))
            }

            #[cfg(feature = "elastic")]
            Config::Elastic(c) => {
                let elastic_s = Bootstrapper::Elastic(c.clone().bootstrapper(chain, intersect));
                let cursor = elastic_s.build_cursor();
                (
                    cursor,
                    Some(Bootstrapper::Elastic(c.bootstrapper(chain, intersect))),
                )
            }
        }
    }
}

#[derive(Clone)]
pub enum Cursor {
    Skip(skip::Cursor),
    Redis(redis::Cursor),

    #[cfg(feature = "elastic")]
    Elastic(elastic::Cursor),
}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<PointArg>, crate::Error> {
        match self {
            Cursor::Skip(x) => x.last_point(),
            Cursor::Redis(x) => x.last_point(),

            #[cfg(feature = "elastic")]
            Cursor::Elastic(x) => x.last_point(),
        }
    }
}

pub enum Bootstrapper {
    Skip(skip::Stage),
    Redis(redis::Stage),

    #[cfg(feature = "elastic")]
    Elastic(elastic::Stage),
}

impl Bootstrapper {
    pub fn build_cursor(self) -> Cursor {
        match self {
            Bootstrapper::Skip(x) => Cursor::Skip(x.cursor),
            Bootstrapper::Redis(x) => Cursor::Redis(x.cursor),

            #[cfg(feature = "elastic")]
            Bootstrapper::Elastic(x) => Cursor::Elastic(x.cursor),
        }
    }

    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<model::CRDTCommand> {
        match self {
            Bootstrapper::Skip(s) => &mut s.input,
            Bootstrapper::Redis(s) => &mut s.input,
            Bootstrapper::Elastic(s) => &mut s.input,
        }
    }

    pub fn spawn_stage(self, pipeline: &bootstrap::Pipeline) -> Tether {
        match self {
            Bootstrapper::Skip(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
            Bootstrapper::Redis(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
            Bootstrapper::Elastic(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
        }
    }
}
