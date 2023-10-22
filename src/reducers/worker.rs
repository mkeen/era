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
        block_parsed: MultiEraBlock<'b>,
        rollback: bool,
        block_ctx: &model::BlockContext,
        last_good_block_rollback_info: Option<(Point, u64)>,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        reducers: &mut Vec<Reducer>,
        ops_count: &gasket::metrics::Counter,
        reducer_errors: &gasket::metrics::Counter,
    ) -> Result<(), WorkerError> {
        let point = match rollback {
            true => {
                let (last_point, _) = last_good_block_rollback_info.clone().unwrap();
                last_point
            }
            false => Point::Specific(block_parsed.slot(), block_parsed.hash().to_vec()),
        };

        output
            .lock()
            .await
            .send(model::CRDTCommand::block_starting(&block_parsed).into())
            .await
            .map_err(|_| WorkerError::Send)?;

        if rollback {
            log::warn!(
                "rolling back {}.{}",
                block_parsed.slot(),
                block_parsed.hash().to_string(),
            );
        }

        let mut handles = Vec::new();
        for reducer in reducers {
            handles.push(reducer.reduce_block(
                block_parsed.clone(),
                block_ctx.clone(),
                rollback.clone(),
                output.clone(),
            ));
        }

        let results = futures::future::join_all(handles).await;

        let mut errors = false;

        for (i, res) in results.into_iter().enumerate() {
            match res {
                Ok(_) => {
                    ops_count.inc(1);
                }
                Err(_) => {
                    errors = true;
                    reducer_errors.inc(1);
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
            .map_err(|_| WorkerError::Send);

        match errors {
            true => Err(WorkerError::Panic),
            false => Ok(()),
        }
    }
}

pub fn bootstrap(
    ctx: Arc<Mutex<Context>>,
    reducers: Vec<reducers::Config>,
    storage_input: &mut InputPort<CRDTCommand>,
) -> Stage {
    let mut output: OutputPort<CRDTCommand> = Default::default();
    connect_ports(&mut output, storage_input, 100);

    let stage = Stage {
        ctx: ctx.clone(),
        reducers: reducers
            .into_iter()
            .map(|x| x.bootstrapper(ctx.clone()))
            .collect(),
        input: Default::default(),
        output: Arc::new(Mutex::new(output)),
        ops_count: Default::default(),
        reducer_errors: Default::default(),
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
    reducers: Vec<Reducer>,
    ctx: Arc<Mutex<Context>>,

    pub input: InputPort<EnrichedBlockPayload>,
    pub output: Arc<Mutex<OutputPort<CRDTCommand>>>,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    reducer_errors: gasket::metrics::Counter,
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
                let parsed_block = MultiEraBlock::decode(block).unwrap();

                self.reduce_block(
                    &block,
                    parsed_block,
                    false,
                    &ctx,
                    None,
                    stage.output.clone(),
                    &mut stage.reducers,
                    &stage.ops_count,
                    &stage.reducer_errors,
                )
            }

            model::EnrichedBlockPayload::RollBack(block, ctx, last_block_rollback_info) => {
                let parsed_block = MultiEraBlock::decode(block).unwrap();
                self.reduce_block(
                    &block,
                    parsed_block,
                    true,
                    &ctx,
                    Some(last_block_rollback_info.clone()),
                    stage.output.clone(),
                    &mut stage.reducers,
                    &stage.ops_count,
                    &stage.reducer_errors,
                )
            }
        }
        .await
    }
}
