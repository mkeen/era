use std::time::Duration;

use gasket::messaging::tokio::{InputPort, OutputPort};
use gasket::runtime::{spawn_stage, Tether};
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use crate::bootstrap::Context;
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

pub mod worker;

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
    fn bootstrapper(self, ctx: &Context) -> Reducer {
        match self {
            Config::UtxoOwners(c) => c.plugin(ctx.policy.clone()),
            Config::UtxoByAddress(c) => c.plugin(ctx.policy.clone()),
            Config::Parameters(c) => c.plugin(ctx.chain.clone()),
            Config::AssetMetadata(c) => c.plugin(ctx.chain.clone(), ctx.policy.clone()),
            Config::PolicyAssetsMoved(c) => c.plugin(ctx.chain.clone(), ctx.policy.clone()),
            Config::MultiAssetBalances(c) => c.plugin(ctx),
            Config::AdaHandle(c) => c.plugin(ctx.chain.clone()),
            Config::StakeToPool(c) => c.plugin(),
        }
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

struct Stub {}

impl Reducer {
    pub async fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        rollback: bool,
        output: &mut OutputPort<CRDTCommand>,
    ) -> Result<(), gasket::error::Error> {
        match self {
            Reducer::UtxoOwners(x) => x.reduce_block(block, ctx, rollback, output).await,
            Reducer::UtxoByAddress(x) => x.reduce_block(block, ctx, rollback, output).await,
            Reducer::Parameters(x) => x.reduce_block(block, rollback, output).await,
            Reducer::AssetMetadata(x) => x.reduce_block(block, rollback, output).await,
            Reducer::PolicyAssetsMoved(x) => x.reduce_block(block, output).await,
            Reducer::MultiAssetBalances(x) => x.reduce_block(block, ctx, rollback, output).await,
            Reducer::AdaHandle(x) => x.reduce_block(block, ctx, rollback, output).await,
            Reducer::StakeToPool(x) => x.reduce_block(block, rollback, output).await,
        }
    }
}

pub struct Bootstrapper {
    reducers: Vec<Reducer>,
    stage: worker::Stage,
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort<EnrichedBlockPayload> {
        &mut self.stage.input
    }

    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<CRDTCommand> {
        &mut self.stage.output
    }

    pub fn spawn_stage(self, pipeline: &bootstrap::Pipeline) -> Tether {
        spawn_stage(self.stage, pipeline.policy.clone())
    }
}
