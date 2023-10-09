pub mod console;

use crate::{crosscut, enrich, reducers, sources, storage};
use std::time::Duration;

use gasket::{
    framework::*,
    messaging::tokio::connect_ports,
    retries,
    runtime::{spawn_stage, Policy, Tether},
};

#[derive(Stage)]
#[stage(name = "pipeline-bootstrapper", unit = "()", worker = "Pipeline")]
pub struct Stage {
    pub ctx: Option<Context>,
    pub sources_config: Option<sources::Config>,
    pub enrich_config: Option<enrich::Config>,
    pub reducer_config: Option<Vec<reducers::Config>>,
    pub storage_config: Option<storage::Config>,
}

// Way too many clones down in here... need to tighten up
#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Pipeline {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let mut pipe = Self {
            policy: Policy {
                tick_timeout: Some(Duration::from_secs(3)),
                bootstrap_retry: retries::Policy::default(),
                work_retry: retries::Policy::default(),
                teardown_retry: retries::Policy::default(),
            },

            tethers: Default::default(),
        };

        let enrich = stage.enrich_config.clone().unwrap();
        let mut enrich_stage = enrich.bootstrapper(&stage.ctx.clone().unwrap()).unwrap();

        let enrich_input_port = enrich_stage.borrow_input_port();

        let storage = stage.storage_config.clone().unwrap();

        let mut storage_stage = storage
            .clone()
            .bootstrapper(stage.ctx.clone().unwrap().block)
            .unwrap();

        let source = stage.sources_config.clone().unwrap();

        let mut source_stage = source
            .clone()
            .bootstrapper(
                &stage.ctx.clone().unwrap(),
                storage_stage.build_cursor(),
                Some(storage_stage.borrow_blocks()),
            )
            .unwrap();

        let mut reducer = reducers::worker::bootstrap(
            &stage.ctx.as_ref().unwrap().clone(),
            stage.reducer_config.clone().unwrap(),
            storage_stage.borrow_input_port(),
        )
        .await;

        connect_ports(source_stage.borrow_output_port(), enrich_input_port, 100);
        connect_ports(enrich_stage.borrow_output_port(), &mut reducer.input, 100);
        // connect_ports(
        //     &mut reducer.output.lock().await.to_owned(),
        //     storage_stage.borrow_input_port(),
        //     100,
        // );

        pipe.tethers.push(storage_stage.spawn_stage(&pipe));
        pipe.tethers.push(spawn_stage(reducer, pipe.policy.clone()));
        pipe.tethers.push(enrich_stage.spawn_stage(&pipe));
        pipe.tethers.push(source_stage.spawn_stage(&pipe));

        return Ok(pipe);
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        std::thread::sleep(Duration::from_secs(60));
        Ok(WorkSchedule::Unit(()))
    }

    async fn execute(&mut self, _: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        std::thread::sleep(Duration::from_secs(60));
        Ok(())
    }
}

pub struct Pipeline {
    pub policy: Policy,
    pub tethers: Vec<Tether>,
}

impl Pipeline {
    pub fn bootstrap(
        ctx: &Context,
        sources_config: sources::Config,
        enrich_config: enrich::Config,
        config_reducer: Vec<reducers::Config>,
        storage_config: storage::Config,
        args_console: console::Mode,
    ) -> Stage {
        console::initialize(&Some(args_console));

        Stage {
            ctx: Some(ctx.clone()),
            sources_config: Some(sources_config),
            storage_config: Some(storage_config),
            enrich_config: Some(enrich_config),
            reducer_config: Some(config_reducer),
        }
    }

    pub fn should_stop(&mut self) -> bool {
        self.tethers
            .iter()
            .any(|tether| match tether.check_state() {
                gasket::runtime::TetherState::Alive(_) => false,
                _ => true,
            })
    }

    pub fn shutdown(self) {
        for tether in self.tethers {
            let state = tether.check_state();
            log::warn!("dismissing stage: {} with state {:?}", tether.name(), state);
            tether.dismiss_stage().expect("stage stops");

            // Can't join the stage because there's a risk of deadlock, usually
            // because a stage gets stuck sending into a port which depends on a
            // different stage not yet dismissed. The solution is to either create a
            // DAG of dependencies and dismiss in the correct order, or implement a
            // 2-phase teardown where ports are disconnected and flushed
            // before joining the stage.

            //tether.join_stage();
        }
    }
}

#[derive(Clone)]
pub struct Context {
    pub chain: crosscut::ChainWellKnownInfo,
    pub intersect: crosscut::IntersectConfig,
    pub finalize: Option<crosscut::FinalizeConfig>,
    pub block: crosscut::historic::BlockConfig,
    pub error_policy: crosscut::policies::RuntimePolicy,
}
