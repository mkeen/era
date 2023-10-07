use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use gasket::framework::*;
use gasket::messaging::tokio::InputPort;
use serde::Deserialize;

use crate::{crosscut, model::CRDTCommand};

#[derive(Deserialize, Clone)]
pub struct Config {
    last_point: crosscut::PointArg,
}

impl Config {
    pub fn bootstrapper(self, blocks: Arc<Mutex<crosscut::historic::BufferBlocks>>) -> Stage {
        let cursor = Cursor {
            last_point: Arc::new(Mutex::new(None)),
        };

        Stage {
            config: self.clone(),
            input: Default::default(),
            cursor,
            blocks,
            ops_count: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct Cursor {
    last_point: Arc<Mutex<Option<crosscut::PointArg>>>,
}

impl Cursor {
    pub fn last_point(&self) -> Result<Option<crosscut::PointArg>, crate::Error> {
        Ok(None)
    }
}

pub struct Worker {}

#[derive(Stage)]
#[stage(name = "storage-skip", unit = "CRDTCommand", worker = "Worker")]
pub struct Stage {
    config: Config,
    pub cursor: Cursor,
    pub blocks: Arc<Mutex<crosscut::historic::BufferBlocks>>,

    pub input: InputPort<CRDTCommand>,

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
    ) -> Result<WorkSchedule<CRDTCommand>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &CRDTCommand, stage: &mut Stage) -> Result<(), WorkerError> {
        stage.ops_count.inc(1);
        Ok(())
    }
}
