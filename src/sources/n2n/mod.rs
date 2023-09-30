pub mod chainsync;
mod transport;

use gasket::messaging::OutputPort;

use pallas::network::miniprotocols::Point;
use serde::Deserialize;

use crate::{crosscut, model, storage};

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
    pub policy: crosscut::policies::RuntimePolicy,
    pub finalize: crosscut::FinalizeConfig,
    pub intersect: crosscut::IntersectConfig,
    pub chain: crosscut::ChainWellKnownInfo,
    pub block_config: crosscut::historic::BlockConfig,
}

impl Config {
    pub fn bootstrapper(
        mut self,
        chain: &crosscut::ChainWellKnownInfo,
        block_config: &crosscut::historic::BlockConfig,
        intersect: &crosscut::IntersectConfig,
        finalize: &crosscut::FinalizeConfig,
        policy: &crosscut::policies::RuntimePolicy,
        cursor: &storage::Cursor,
    ) -> chainsync::Stage {
        self.policy = policy.clone(); // dk if i need clone
        self.finalize = finalize.clone(); // dk if i need clone, maybe its bad, idk
        self.intersect = intersect.clone();
        self.chain = chain.clone();
        self.block_config = block_config.clone();

        chainsync::Stage {
            config: self,
            cursor: *cursor,
            output: Default::default(),
            chain_tip: Default::default(),
            block_count: Default::default(),
        }
    }
}

pub struct Bootstrapper {
    config: Config,
    intersect: crosscut::IntersectConfig,
    finalize: Option<crosscut::FinalizeConfig>,
    policy: crosscut::policies::RuntimePolicy,
    chain: crosscut::ChainWellKnownInfo,
    blocks: crosscut::historic::BufferBlocks,
    output: OutputPort<model::RawBlockPayload, model::RawBlockPayload>,
}
