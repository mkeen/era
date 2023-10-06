use crate::{bootstrap, crosscut, enrich, reducers, sources, storage};
use std::time::{Duration, Instant};

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
        source: Option<sources::Bootstrapper>,
        enrich: Option<enrich::Bootstrapper>,
        reducer: reducers::worker::Stage,
        storage: Option<storage::Bootstrapper>,
    ) -> Self {
        let source = source.unwrap();
        let enrich = enrich.unwrap();
        let storage = storage.unwrap();

        let mut pipe = Self {
            tethers: Vec::new(),
            policy: Policy {
                tick_timeout: Some(Duration::from_secs(10)),
                bootstrap_retry: retries::Policy::default(),
                work_retry: retries::Policy::default(),
                teardown_retry: retries::Policy::default(),
            },
        };

        pipe.build(source, enrich, reducer, storage);

        pipe
    }

    pub fn register_stage(&mut self, tether: Tether) {
        self.tethers.push(tether);
    }

    pub fn build(
        &mut self,
        mut source: sources::Bootstrapper,
        mut enrich: enrich::Bootstrapper,
        mut reducer: reducers::worker::Stage,
        mut storage: storage::Bootstrapper,
    ) {
        let reducer_input = &mut reducer.input;
        connect_ports(source.borrow_output_port(), enrich.borrow_input_port(), 100);

        connect_ports(enrich.borrow_output_port(), reducer_input, 100);

        connect_ports(&mut reducer.output, storage.borrow_input_port(), 100);

        self.register_stage(storage.spawn_stage(self));
        self.register_stage(spawn_stage(reducer, self.policy.clone()));
        self.register_stage(enrich.spawn_stage(self));
        self.register_stage(source.spawn_stage(self));
    }
}

pub struct Context {
    pub chain: crosscut::ChainWellKnownInfo,
    pub intersect: crosscut::IntersectConfig,
    pub finalize: Option<crosscut::FinalizeConfig>,
    pub blocks: crosscut::historic::BlockConfig,
}
