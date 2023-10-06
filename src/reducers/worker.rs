use async_trait;
use pallas::{ledger::traverse::MultiEraBlock, network::miniprotocols::Point};

use crate::bootstrap::Context;
use crate::model::{CRDTCommand, EnrichedBlockPayload};
use crate::{crosscut, model, prelude::*, reducers};

use super::Reducer;

use gasket::framework::*;
use gasket::messaging::tokio::{InputPort, OutputPort};

struct Config {
    chain: crosscut::ChainWellKnownInfo,
}

pub struct Worker {}

impl Worker {
    async fn reduce_block<'b>(
        &mut self,
        block: &'b Vec<u8>,
        rollback: bool,
        ctx: &model::BlockContext,
        last_good_block_rollback_info: (Point, i64),
        final_block_in_rollback_batch: bool,
        output: &mut OutputPort<CRDTCommand>,
        reducers: &mut Vec<Reducer>,
    ) -> Option<i64> {
        let block = MultiEraBlock::decode(block).unwrap();

        let (point, block_number) = match rollback {
            true => last_good_block_rollback_info,
            false => (
                Point::Specific(block.slot(), block.hash().to_vec()),
                block.number() as i64,
            ),
        };

        output
            .send(model::CRDTCommand::block_starting(&block).into())
            .await;

        for reducer in reducers {
            reducer.reduce_block(&block, ctx, rollback, output).await;
        }

        output
            .send(
                model::CRDTCommand::block_finished(
                    point,
                    !rollback || final_block_in_rollback_batch,
                )
                .into(),
            )
            .await;

        Some(block_number)
    }
}

pub fn bootstrap(ctx: &Context, reducers: Vec<reducers::Config>) -> Stage {
    Stage {
        config: Config {
            chain: ctx.chain.clone(),
        },
        reducers: reducers.into_iter().map(|x| x.bootstrapper(&ctx)).collect(),
        input: Default::default(),
        output: Default::default(),
        chain_tip: Default::default(),
        ops_count: Default::default(),
    }
}

#[derive(Stage)]
#[stage(
    name = "n2n-reducers",
    unit = "EnrichedBlockPayload",
    worker = "Worker"
)]
pub struct Stage {
    config: Config,

    reducers: Vec<Reducer>,

    pub input: InputPort<EnrichedBlockPayload>,
    pub output: OutputPort<CRDTCommand>,

    #[metric]
    chain_tip: gasket::metrics::Gauge,

    #[metric]
    ops_count: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {})
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<EnrichedBlockPayload>, WorkerError> {
        match stage.input.recv().await {
            Ok(c) => Ok(WorkSchedule::Unit(c.payload)),
            Err(_) => Err(WorkerError::Retry),
        }
    }

    async fn execute(
        &mut self,
        unit: &EnrichedBlockPayload,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        match unit {
            model::EnrichedBlockPayload::RollForward(block, ctx) => {
                self.reduce_block(
                    &block,
                    false,
                    &ctx,
                    (Point::Origin, 0),
                    false,
                    &mut stage.output,
                    &mut stage.reducers,
                )
                .await;
            }
            model::EnrichedBlockPayload::RollBack(
                block,
                ctx,
                last_block_rollback_info,
                final_block_in_batch,
            ) => {
                self.reduce_block(
                    &block,
                    true,
                    &ctx,
                    last_block_rollback_info.clone(),
                    final_block_in_batch.clone(),
                    &mut stage.output,
                    &mut stage.reducers,
                )
                .await;
            }
        }

        Ok(())
    }
}
