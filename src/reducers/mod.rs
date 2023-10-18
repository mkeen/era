use std::sync::Arc;

use futures::Future;
use gasket::messaging::tokio::OutputPort;
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::model::CRDTCommand;
use crate::pipeline::Context;
use crate::{crosscut, model};

pub mod macros;

pub mod utils;

pub mod assets_balances;
pub mod assets_last_moved;
pub mod handles;
pub mod metadata;
pub mod parameters;
pub mod stake_to_pool;
pub mod utxo;
pub mod utxo_owners;

pub mod worker;

#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Config {
    UtxoOwners(utxo_owners::Config),
    Utxo(utxo::Config),
    Parameters(parameters::Config),
    Metadata(metadata::Config),
    AssetsLastMoved(assets_last_moved::Config),
    AssetsBalances(assets_balances::Config),
    Handles(handles::Config),
    StakeToPool(stake_to_pool::Config),
}

impl Config {
    fn bootstrapper(self, ctx: &Context) -> Reducer {
        match self {
            Config::UtxoOwners(c) => c.plugin(),
            Config::Utxo(c) => c.plugin(),
            Config::Parameters(c) => c.plugin(ctx.chain.clone()),
            Config::Metadata(c) => c.plugin(ctx.chain.clone()),
            Config::AssetsLastMoved(c) => c.plugin(ctx.chain.clone()),
            Config::AssetsBalances(c) => c.plugin(ctx),
            Config::Handles(c) => c.plugin(ctx.chain.clone()),
            Config::StakeToPool(c) => c.plugin(),
        }
    }
}

#[derive(Clone)]
pub enum Reducer {
    UtxoOwners(utxo_owners::Reducer),
    Utxo(utxo::Reducer),
    Parameters(parameters::Reducer),
    Metadata(metadata::Reducer),
    AssetsLastMoved(assets_last_moved::Reducer),
    AssetsBalances(assets_balances::Reducer),
    Handle(handles::Reducer),
    StakeToPool(stake_to_pool::Reducer),
}

impl Reducer {
    pub async fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        rollback: bool,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        errs: &crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::framework::WorkerError> {
        match self {
            Reducer::UtxoOwners(x) => x.reduce(block, ctx, rollback, output, errs).await,
            Reducer::Utxo(x) => x.reduce(block, ctx, rollback, output, errs).await,
            Reducer::Parameters(x) => x.reduce(block, rollback, output, errs).await,
            Reducer::Metadata(x) => x.reduce(block, rollback, output, errs).await,
            Reducer::AssetsLastMoved(x) => x.reduce(block, output, errs).await,
            Reducer::AssetsBalances(x) => x.reduce(block, ctx, rollback, output, errs).await,
            Reducer::Handle(x) => x.reduce(block, ctx, rollback, output, errs).await,
            Reducer::StakeToPool(x) => x.reduce(block, rollback, output, errs).await,
        }
    }
}
