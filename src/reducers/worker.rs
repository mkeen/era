use std::sync::Arc;

use async_trait;
use pallas::{ledger::traverse::MultiEraBlock, network::miniprotocols::Point};
use tokio::sync::Mutex;

use crate::model::{CRDTCommand, EnrichedBlockPayload};
use crate::pipeline::Context;

use crate::{crosscut, model, prelude::*, reducers};

use super::Reducer;

use gasket::framework::*;
use gasket::messaging::tokio::{connect_ports, InputPort, OutputPort};

struct Config {
    chain: crosscut::ChainWellKnownInfo,
}

pub struct Worker {}

impl Worker {
    async fn reduce_block<'b>(
        &mut self,
        block_raw: &'b Vec<u8>,
        block_parsed: &'b MultiEraBlock<'b>,
        rollback: bool,
        ctx: &'b model::BlockContext,
        last_good_block_rollback_info: Option<(Point, u64)>,
        output: &'b Arc<Mutex<OutputPort<CRDTCommand>>>,
        error_policy: &'b crosscut::policies::RuntimePolicy,
        reducers: &'b mut Vec<Reducer>,
    ) -> Option<u64> {
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

        match output {
            _ => {
                output
                    .lock()
                    .await
                    .send(model::CRDTCommand::block_starting(&block_parsed).into())
                    .await
                    .unwrap();
            }
        }

        if rollback {
            log::warn!(
                "rolling back {}.{}",
                block_parsed.slot(),
                block_parsed.hash().to_string(),
            );
        }

        let mut handles = Vec::new();
        for reducer in reducers {
            handles.push(reducer.reduce_block(&block_parsed, &ctx, rollback, output, error_policy));
        }

        let results = futures::future::join_all(handles).await;

        for res in results {
            match res {
                Ok(_) => {}
                Err(_) => {
                    panic!("Reducer error")
                }
            };
        }

        output
            .lock()
            .await
            .send(
                model::CRDTCommand::block_finished(point.clone(), block_raw.clone(), rollback)
                    .into(),
            )
            .await
            .unwrap();

        Some(number)
    }
}

pub async fn bootstrap(
    ctx: &Context,
    reducers: Vec<reducers::Config>,
    storage_input: &mut InputPort<CRDTCommand>,
) -> Stage {
    let mut output: OutputPort<CRDTCommand> = Default::default();
    connect_ports(&mut output, storage_input, 100);

    let stage = Stage {
        config: Config {
            chain: ctx.chain.clone(),
        },
        reducers: reducers.into_iter().map(|x| x.bootstrapper(&ctx)).collect(),
        input: Default::default(),
        output: Arc::new(Mutex::new(output)),
        ops_count: Default::default(),
        last_block: Default::default(),
        error_policy: ctx.error_policy.clone(),
    };

    stage
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
    pub output: Arc<Mutex<OutputPort<CRDTCommand>>>,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    last_block: gasket::metrics::Gauge,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_: &Stage) -> Result<Self, WorkerError> {
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
                        &MultiEraBlock::decode(block).unwrap(),
                        false,
                        &ctx,
                        None,
                        &stage.output,
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
                        &MultiEraBlock::decode(block).unwrap(),
                        true,
                        &ctx,
                        Some(last_block_rollback_info.clone()),
                        &stage.output,
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
