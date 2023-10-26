use std::sync::Arc;

use gasket::framework::*;
use gasket::messaging::tokio::InputPort;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{crosscut, model::CRDTCommand, pipeline::Context};

#[derive(Deserialize, Clone)]
pub struct Config {}

impl Config {
    pub fn bootstrapper(self, ctx: Arc<Mutex<Context>>) -> Stage {
        Stage {
            _config: self.clone(),
            input: Default::default(),
            ctx,
            ops_count: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct Cursor {}

impl Cursor {
    pub fn last_point(&self) -> Result<Option<crosscut::PointArg>, crate::Error> {
        Ok(None)
    }
}

pub struct Worker {}

#[derive(Stage)]
#[stage(name = "storage-skip", unit = "CRDTCommand", worker = "Worker")]
pub struct Stage {
    _config: Config,
    pub ctx: Arc<Mutex<Context>>,

    pub input: InputPort<CRDTCommand>,

    #[metric]
    ops_count: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {})
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<CRDTCommand>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, _: &CRDTCommand, stage: &mut Stage) -> Result<(), WorkerError> {
        stage.ops_count.inc(1);
        Ok(())
    }
}
