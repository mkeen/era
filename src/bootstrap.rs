use crate::{bootstrap, crosscut, enrich, reducers, sources, storage};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use gasket::{
    messaging::tokio::connect_ports,
    retries,
    runtime::{spawn_stage, Policy, Tether},
};

pub struct Pipeline {
    pub policy: Policy,
    pub tethers: Vec<Tether>,
}

impl Pipeline {
    pub fn bootstrap(
        ctx: &Context,
        sources_config: sources::Config,
        enrich: Option<enrich::Bootstrapper>,
        mut reducer: reducers::worker::Stage,
        storage_config: storage::Config,
    ) -> Self {
        let mut pipe = Self {
            policy: Policy {
                tick_timeout: Some(Duration::from_secs(3)),
                bootstrap_retry: retries::Policy::default(),
                work_retry: retries::Policy::default(),
                teardown_retry: retries::Policy::default(),
            },
            tethers: Vec::new(),
        };

        let mut enrich = enrich.unwrap();

        let mut storage = storage_config.bootstrapper(ctx.block.clone()).unwrap();

        let mut source = sources_config
            .bootstrapper(&ctx, storage.build_cursor(), Some(storage.borrow_blocks()))
            .unwrap();

        connect_ports(source.borrow_output_port(), enrich.borrow_input_port(), 100);
        connect_ports(enrich.borrow_output_port(), &mut reducer.input, 100);
        connect_ports(&mut reducer.output, storage.borrow_input_port(), 100);

        pipe.register_stage(storage.spawn_stage(&pipe));
        pipe.register_stage(spawn_stage(reducer, pipe.policy.clone()));
        pipe.register_stage(enrich.spawn_stage(&pipe));
        pipe.register_stage(source.spawn_stage(&pipe));

        pipe
    }

    pub fn register_stage(&mut self, tether: Tether) {
        self.tethers.push(tether);
    }
}

pub struct Context {
    pub chain: crosscut::ChainWellKnownInfo,
    pub intersect: crosscut::IntersectConfig,
    pub finalize: Option<crosscut::FinalizeConfig>,
    pub block: crosscut::historic::BlockConfig,
    pub error_policy: crosscut::policies::RuntimePolicy,
}
