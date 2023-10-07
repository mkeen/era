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
        block_raw: &'b Vec<u8>,
        rollback: bool,
        ctx: &model::BlockContext,
        last_good_block_rollback_info: (Point, i64),
        final_block_in_rollback_batch: bool,
        output: &mut OutputPort<CRDTCommand>,
        error_policy: &crosscut::policies::RuntimePolicy,
        reducers: &mut Vec<Reducer>,
    ) -> Option<i64> {
        let block_parsed = MultiEraBlock::decode(block_raw).unwrap();

        let (point, block_number) = match rollback {
            true => last_good_block_rollback_info,
            false => (
                Point::Specific(block_parsed.slot(), block_parsed.hash().to_vec()),
                block_parsed.number() as i64,
            ),
        };

        output
            .send(model::CRDTCommand::block_starting(&block_parsed).into())
            .await
            .unwrap();

        for reducer in reducers {
            reducer
                .reduce_block(&block_parsed, ctx, rollback, output, error_policy)
                .await
                .unwrap();
        }

        output
            .send(
                model::CRDTCommand::block_finished(
                    point,
                    !rollback || final_block_in_rollback_batch,
                    block_raw.clone(),
                )
                .into(),
            )
            .await
            .unwrap();

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
        last_block: Default::default(),
        error_policy: ctx.error_policy.clone(),
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
    error_policy: crosscut::policies::RuntimePolicy,

    pub input: InputPort<EnrichedBlockPayload>,
    pub output: OutputPort<CRDTCommand>,

    #[metric]
    chain_tip: gasket::metrics::Gauge,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    last_block: gasket::metrics::Gauge,
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
                stage.last_block.set(
                    self.reduce_block(
                        &block,
                        false,
                        &ctx,
                        (Point::Origin, 0),
                        false,
                        &mut stage.output,
                        &stage.error_policy,
                        &mut stage.reducers,
                    )
                    .await
                    .unwrap(),
                );
            }
            model::EnrichedBlockPayload::RollBack(
                block,
                ctx,
                last_block_rollback_info,
                final_block_in_batch,
            ) => {
                stage.last_block.set(
                    self.reduce_block(
                        &block,
                        true,
                        &ctx,
                        last_block_rollback_info.clone(),
                        final_block_in_batch.clone(),
                        &mut stage.output,
                        &stage.error_policy,
                        &mut stage.reducers,
                    )
                    .await
                    .unwrap(),
                );
            }
        }

        Ok(())
    }
}
