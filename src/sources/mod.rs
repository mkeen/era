use std::sync::{Arc, Mutex};

use crate::{
    bootstrap, crosscut, model,
    storage::{self, Cursor},
};
use gasket::{messaging::tokio::OutputPort, runtime::Tether};
use serde::Deserialize;

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
    pub fn bootstrapper(
        self,
        ctx: &bootstrap::Context,
        cursor: Cursor,
        blocks: Arc<Mutex<crosscut::historic::BufferBlocks>>,
    ) -> Option<Bootstrapper> {
        match self {
            Config::N2N(c) => Some(Bootstrapper::N2N(c.bootstrapper(ctx, cursor, blocks))),
        }
    }
}

pub enum Bootstrapper {
    N2N(n2n::chainsync::Stage),
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<model::RawBlockPayload> {
        match self {
            Bootstrapper::N2N(s) => &mut s.output,
        }
    }

    pub fn borrow_blocks(mut self) -> Arc<Mutex<crosscut::historic::BufferBlocks>> {
        match self {
            Bootstrapper::N2N(s) => s.blocks,
        }
    }

    pub fn spawn_stage(mut self, pipeline: &bootstrap::Pipeline) -> Tether {
        match self {
            Bootstrapper::N2N(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
        }
    }
}
