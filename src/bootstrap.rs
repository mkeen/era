use crate::{bootstrap, crosscut, enrich, reducers, sources, storage};
use std::time::{Duration, Instant};

use gasket::{
    messaging::tokio::connect_ports,
    retries,
    runtime::{Policy, Tether},
};

pub struct Pipeline {
    pub policy: Policy,

    pub tethers: Vec<Tether>,
}

impl Pipeline {
    pub fn bootstrap(
        mut source: sources::Bootstrapper,
        mut enrich: enrich::Bootstrapper,
        mut reducer: reducers::Bootstrapper,
        mut storage: storage::Bootstrapper,
    ) -> Self {
        let mut pipe = Self {
            tethers: Vec::new(),
            policy: Policy {
                tick_timeout: Some(Duration::from_secs(30)),
                bootstrap_retry: retries::Policy::no_retry(),
                work_retry: retries::Policy::no_retry(),
                teardown_retry: retries::Policy::no_retry(),
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
        mut reducer: reducers::Bootstrapper,
        mut storage: storage::Bootstrapper,
    ) {
        connect_ports(source.borrow_output_port(), enrich.borrow_input_port(), 100);

        connect_ports(
            enrich.borrow_output_port(),
            reducer.borrow_input_port(),
            100,
        );

        connect_ports(
            enrich.borrow_output_port(),
            reducer.borrow_input_port(),
            100,
        );

        connect_ports(
            reducer.borrow_output_port(),
            storage.borrow_input_port(),
            100,
        );

        self.register_stage(storage.spawn_stage(self));

        self.register_stage(source.spawn_stage(self));

        self.register_stage(enrich.spawn_stage(self));

        self.register_stage(reducer.spawn_stage(self));
    }
}

pub struct Context {
    pub chain: crosscut::ChainWellKnownInfo,
    pub intersect: crosscut::IntersectConfig,
    pub cursor: storage::Cursor,
    pub finalize: Option<crosscut::FinalizeConfig>,
    pub blocks: crosscut::historic::BlockConfig,
    pub policy: crosscut::policies::RuntimePolicy,
}
