pub mod redis;
pub mod skip;

#[cfg(feature = "elastic")]
pub mod elastic;

use std::sync::{Arc, Mutex};

use gasket::{messaging::tokio::InputPort, runtime::Tether};
use serde::Deserialize;

use crate::{
    crosscut::{self, PointArg},
    model, pipeline,
};

#[derive(Deserialize, Clone)]
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
        blocks_config: crosscut::historic::BlockConfig,
    ) -> Option<Bootstrapper> {
        match self {
            Config::Skip(c) => Some(Bootstrapper::Skip(c.bootstrapper(Arc::new(Mutex::new(
                crosscut::historic::BufferBlocks::from(blocks_config),
            ))))),
            Config::Redis(c) => Some(Bootstrapper::Redis(c.bootstrapper(Arc::new(Mutex::new(
                crosscut::historic::BufferBlocks::from(blocks_config),
            ))))),

            #[cfg(feature = "elastic")]
            Config::Elastic(c) => Some(Bootstrapper::Elastic(c.bootstrapper(Arc::new(
                Mutex::new(crosscut::historic::BufferBlocks::from(blocks_config)),
            )))),
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
    pub fn build_cursor(&mut self) -> Cursor {
        match self {
            Bootstrapper::Skip(x) => Cursor::Skip(x.cursor.clone()),
            Bootstrapper::Redis(x) => Cursor::Redis(x.cursor.clone()),

            #[cfg(feature = "elastic")]
            Bootstrapper::Elastic(x) => Cursor::Elastic(x.cursor.clone()),
        }
    }

    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<model::CRDTCommand> {
        match self {
            Bootstrapper::Skip(s) => &mut s.input,
            Bootstrapper::Redis(s) => &mut s.input,
            Bootstrapper::Elastic(s) => &mut s.input,
        }
    }

    pub fn borrow_blocks(&mut self) -> Arc<Mutex<crosscut::historic::BufferBlocks>> {
        match self {
            Bootstrapper::Skip(s) => s.blocks.clone(),
            Bootstrapper::Redis(s) => s.blocks.clone(),
            Bootstrapper::Elastic(s) => s.blocks.clone(),
        }
    }

    pub fn spawn_stage(self, pipeline: &pipeline::Pipeline) -> Tether {
        match self {
            Bootstrapper::Skip(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
            Bootstrapper::Redis(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
            Bootstrapper::Elastic(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
        }
    }
}
