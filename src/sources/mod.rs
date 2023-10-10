use std::sync::Arc;

use crate::{crosscut, model, pipeline, storage::Cursor};
use gasket::{messaging::tokio::OutputPort, runtime::Tether};
use serde::Deserialize;
use tokio::sync::Mutex;

// #[cfg(target_family = "unix")]
// pub mod n2c;

pub mod n2n;
pub mod utils;
pub mod utxorpc;

#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Config {
    N2N(n2n::Config),
    UTXORPC(utxorpc::Config),
}

impl Config {
    pub fn bootstrapper(
        self,
        ctx: &pipeline::Context,
        cursor: Cursor,
        blocks: Option<Arc<Mutex<crosscut::historic::BufferBlocks>>>,
    ) -> Option<Bootstrapper> {
        match self {
            Config::N2N(c) => Some(Bootstrapper::N2N(c.bootstrapper(
                ctx,
                cursor,
                blocks.unwrap(),
            ))),
            Config::UTXORPC(c) => Some(Bootstrapper::UTXORPC(c.bootstrapper(ctx, cursor))),
        }
    }
}

pub enum Bootstrapper {
    N2N(n2n::chainsync::Stage),
    UTXORPC(utxorpc::Stage),
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<model::RawBlockPayload> {
        match self {
            Bootstrapper::N2N(s) => &mut s.output,
            Bootstrapper::UTXORPC(s) => &mut s.output,
        }
    }

    pub fn borrow_blocks(self) -> Option<Arc<Mutex<crosscut::historic::BufferBlocks>>> {
        match self {
            Bootstrapper::N2N(s) => Some(s.blocks),
            Bootstrapper::UTXORPC(_) => None,
        }
    }

    pub fn spawn_stage(self, pipeline: &pipeline::Pipeline) -> Tether {
        match self {
            Bootstrapper::N2N(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
            Bootstrapper::UTXORPC(s) => gasket::runtime::spawn_stage(s, pipeline.policy.clone()),
        }
    }
}
