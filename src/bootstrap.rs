use crate::{enrich, reducers, sources, storage};

use gasket::runtime::spawn_stage;
use gasket::{messaging::tokio::connect_ports, runtime::Tether};

use std::time::{Duration, Instant};

pub struct Pipeline {
    pub tethers: Vec<Tether>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            tethers: Vec::new(),
        }
    }

    pub fn register_stage(&mut self, tether: Tether) {
        self.tethers.push(tether);
    }
}

pub fn build(
    mut sources: sources::Stage,
    mut enrich: enrich::Stage,
    mut reducer: reducers::Stage,
    mut storage: storage::Stage,
    mut cursor: storage::Cursor,
) -> Result<Pipeline, crate::Error> {
    let policy = gasket::runtime::Policy {
        tick_timeout: Some(Duration::from_secs(3)),
        bootstrap_retry: gasket::retries::Policy::no_retry(),
        work_retry: gasket::retries::Policy::no_retry(),
        teardown_retry: gasket::retries::Policy::no_retry(),
    };

    let mut pipeline = Pipeline::new();

    let storage = match storage {
        storage::Stage::Redis(s) => s,
        storage::Stage::Elastic(s) => s,
        storage::Stage::Skip(s) => s,
    };

    connect_ports(sources.output, enrich.input, 100);
    connect_ports(enrich.output, reducer.input, 100);
    connect_ports(&mut reducer.output, &mut storage.input, 100);

    let sources = match sources {
        sources::Stage::N2N(s) => s,
    };

    pipeline.register_stage(spawn_stage(sources, policy.clone()));
    pipeline.register_stage(spawn_stage(enrich, policy.clone()));
    pipeline.register_stage(spawn_stage(reducer, policy.clone()));
    pipeline.register_stage(spawn_stage(storage, policy.clone()));

    for tether in pipeline.tethers {
        tether.dismiss_stage().expect("stage stops");
        tether.join_stage();
    }

    Ok(pipeline)
}
