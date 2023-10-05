pub mod chainsync;
mod transport;

use elasticsearch::Elasticsearch;
use gasket::messaging::OutputPort;

use pallas::network::miniprotocols::Point;
use serde::Deserialize;

use crate::{
    bootstrap, crosscut, model,
    storage::{self, Cursor},
};

#[derive(Clone, Debug)]
pub enum ChainSyncInternalPayload {
    RollForward(Point),
    RollBack(Point),
}

impl ChainSyncInternalPayload {
    pub fn roll_forward(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollForward(point),
        }
    }

    pub fn roll_back(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::RollBack(point),
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    pub address: String,
    pub min_depth: Option<usize>,
}

impl Config {
    pub fn bootstrapper(self, ctx: &bootstrap::Context, cursor: Cursor) -> chainsync::Stage {
        chainsync::Stage {
            config: self,
            output: Default::default(),
            chain_tip: Default::default(),
            block_count: Default::default(),
            intersect: ctx.intersect.clone(),
            cursor,
            policy: ctx.policy.clone(),
            chain: ctx.chain.clone(),
            blocks: ctx.blocks.clone().into(),
            finalize: ctx.finalize.clone(),
        }
    }
}
