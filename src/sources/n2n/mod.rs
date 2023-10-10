pub mod chainsync;

use std::sync::Arc;

use pallas::network::miniprotocols::Point;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{crosscut, pipeline, storage::Cursor};

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

#[derive(Deserialize, Clone)]
pub struct Config {
    pub address: String,
    pub min_depth: Option<usize>,
}

impl Config {
    pub fn bootstrapper(
        self,
        ctx: &pipeline::Context,
        cursor: Cursor,
        blocks: Arc<Mutex<crosscut::historic::BufferBlocks>>,
    ) -> chainsync::Stage {
        chainsync::Stage {
            config: self,
            output: Default::default(),
            chain_tip: Default::default(),
            last_block: Default::default(),
            intersect: ctx.intersect.clone(),
            chain: ctx.chain.clone(),
            finalize: ctx.finalize.clone(),
            cursor,
            blocks,
        }
    }
}
