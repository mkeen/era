use std::time::Duration;

use gasket::messaging::tokio::{InputPort, OutputPort};
use gasket::runtime::spawn_stage;
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use crate::model::{CRDTCommand, EnrichedBlockPayload};
use crate::{bootstrap, crosscut, model};

pub mod macros;

pub mod ada_handle;
pub mod asset_metadata;
pub mod multi_asset_balances;
pub mod parameters;
pub mod policy_assets_moved;
pub mod stake_to_pool;
pub mod utxo_by_address;
pub mod utxo_owners;

mod worker;

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    UtxoOwners(utxo_owners::Config),
    UtxoByAddress(utxo_by_address::Config),
    Parameters(parameters::Config),
    AssetMetadata(asset_metadata::Config),
    PolicyAssetsMoved(policy_assets_moved::Config),
    MultiAssetBalances(multi_asset_balances::Config),
    AdaHandle(ada_handle::Config),
    StakeToPool(stake_to_pool::Config),
}

impl Config {
    fn plugin(
        self,
        chain: &crosscut::ChainWellKnownInfo,
        policy: &crosscut::policies::RuntimePolicy,
    ) -> Reducer {
        match self {
            Config::UtxoOwners(c) => c.plugin(policy),
            Config::UtxoByAddress(c) => c.plugin(policy),
            Config::Parameters(c) => c.plugin(chain),
            Config::AssetMetadata(c) => c.plugin(chain, policy),
            Config::PolicyAssetsMoved(c) => c.plugin(chain, policy),
            Config::MultiAssetBalances(c) => c.plugin(chain, policy),
            Config::AdaHandle(c) => c.plugin(chain),
            Config::StakeToPool(c) => c.plugin(),
        }
    }
}

pub struct Bootstrapper {
    input: InputPort<EnrichedBlockPayload>,
    output: OutputPort<CRDTCommand>,
    reducers: Vec<Reducer>,
    policy: crosscut::policies::RuntimePolicy,
}

impl Bootstrapper {
    pub fn new(
        configs: Vec<Config>,
        chain: &crosscut::ChainWellKnownInfo,
        policy: &crosscut::policies::RuntimePolicy,
    ) -> Self {
        Self {
            reducers: configs
                .into_iter()
                .map(|x| x.plugin(chain, policy))
                .collect(),
            input: Default::default(),
            output: Default::default(),
            policy: policy.clone(),
        }
    }

    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<EnrichedBlockPayload> {
        &mut self.input
    }

    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<CRDTCommand> {
        &mut self.output
    }
}

pub enum Reducer {
    UtxoOwners(utxo_owners::Reducer),
    UtxoByAddress(utxo_by_address::Reducer),
    Parameters(parameters::Reducer),
    AssetMetadata(asset_metadata::Reducer),
    PolicyAssetsMoved(policy_assets_moved::Reducer),
    MultiAssetBalances(multi_asset_balances::Reducer),
    AdaHandle(ada_handle::Reducer),
    StakeToPool(stake_to_pool::Reducer),
}

impl Reducer {
    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        rollback: bool,
        output: &mut OutputPort<CRDTCommand>,
    ) -> Result<(), gasket::error::Error> {
        match self {
            Reducer::UtxoOwners(x) => x.reduce_block(block, ctx, rollback, output),
            Reducer::UtxoByAddress(x) => x.reduce_block(block, ctx, rollback, output),
            Reducer::Parameters(x) => x.reduce_block(block, rollback, output),
            Reducer::AssetMetadata(x) => x.reduce_block(block, rollback, output),
            Reducer::PolicyAssetsMoved(x) => x.reduce_block(block, output),
            Reducer::MultiAssetBalances(x) => x.reduce_block(block, ctx, rollback, output),
            Reducer::AdaHandle(x) => x.reduce_block(block, ctx, rollback, output),
            Reducer::StakeToPool(x) => x.reduce_block(block, rollback, output),
        }
    }
}
