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
        last_good_block_rollback_info: Option<(Point, u64)>,
        output: &mut OutputPort<CRDTCommand>,
        error_policy: &crosscut::policies::RuntimePolicy,
        reducers: &mut Vec<Reducer>,
    ) -> Option<u64> {
        let block_parsed = MultiEraBlock::decode(block_raw).unwrap();

        let point = match rollback {
            true => {
                let (last_point, _) = last_good_block_rollback_info.clone().unwrap();
                last_point
            }
            false => Point::Specific(block_parsed.slot(), block_parsed.hash().to_vec()),
        };

        let number = match rollback {
            true => {
                let (_, last_number) = last_good_block_rollback_info.unwrap();
                last_number
            }
            false => block_parsed.number(),
        };

        output
            .send(model::CRDTCommand::block_starting(&block_parsed).into())
            .await
            .unwrap();

        if rollback {
            log::warn!(
                "rolling back {}@{}",
                block_parsed.hash().to_string(),
                block_parsed.slot()
            );
        }

        for reducer in reducers {
            reducer
                .reduce_block(&block_parsed, ctx, rollback, output, error_policy)
                .await
                .unwrap();
        }

        output
            .send(model::CRDTCommand::block_finished(point, block_raw.clone(), rollback).into())
            .await
            .unwrap();

        Some(number)
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
                        None,
                        &mut stage.output,
                        &stage.error_policy,
                        &mut stage.reducers,
                    )
                    .await
                    .unwrap() as i64,
                );
            }
            model::EnrichedBlockPayload::RollBack(block, ctx, last_block_rollback_info) => {
                stage.last_block.set(
                    self.reduce_block(
                        &block,
                        true,
                        &ctx,
                        Some(last_block_rollback_info.clone()),
                        &mut stage.output,
                        &stage.error_policy,
                        &mut stage.reducers,
                    )
                    .await
                    .unwrap() as i64,
                );
            }
        }

        Ok(())
    }
}
