use pallas::{ledger::traverse::MultiEraBlock, network::miniprotocols::Point};

use crate::{crosscut, model, prelude::*};

use super::Reducer;

type InputPort = gasket::messaging::TwoPhaseInputPort<model::EnrichedBlockPayload>;
type OutputPort = gasket::messaging::OutputPort<model::CRDTCommand>;

pub struct Worker {
    input: InputPort,
    output: OutputPort,
    reducers: Vec<Reducer>,
    policy: crosscut::policies::RuntimePolicy,
    ops_count: gasket::metrics::Counter,
    last_block: gasket::metrics::Gauge,
}

impl Worker {
    pub fn new(
        reducers: Vec<Reducer>,
        input: InputPort,
        output: OutputPort,
        policy: crosscut::policies::RuntimePolicy,
    ) -> Self {
        Worker {
            reducers,
            input,
            output,
            policy,
            ops_count: Default::default(),
            last_block: Default::default(),
        }
    }

    fn reduce_block<'b>(
        &mut self,
        block: &'b Vec<u8>,
        rollback: bool,
        ctx: &model::BlockContext,
        last_good_block_rollback_info: (Point, i64),
        final_block_in_rollback_batch: bool,
    ) -> Result<(), gasket::error::Error> {
        let block = MultiEraBlock::decode(block)
            .map_err(crate::Error::cbor)
            .apply_policy(&self.policy)
            .or_panic()?;

        let block = match block {
            Some(x) => x,
            None => return Ok(()),
        };

        let (point, block_number) = match rollback {
            true => last_good_block_rollback_info,
            false => (
                Point::Specific(block.slot(), block.hash().to_vec()),
                block.number() as i64,
            ),
        };

        self.last_block.set(block_number);

        self.output.send(gasket::messaging::Message::from(
            model::CRDTCommand::block_starting(&block),
        ))?;

        for reducer in self.reducers.iter_mut() {
            reducer.reduce_block(&block, ctx, rollback, &mut self.output)?;
            self.ops_count.inc(1);
        }

        self.output.send(gasket::messaging::Message::from(
            model::CRDTCommand::block_finished(point, !rollback || final_block_in_rollback_batch),
        ))?;

        Ok(())
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("ops_count", &self.ops_count)
            .with_gauge("last_block", &self.last_block)
            .build()
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.input.recv_or_idle()?;

        match msg.payload {
            model::EnrichedBlockPayload::RollForward(block, ctx) => {
                self.reduce_block(&block, false, &ctx, (Point::Origin, 0), false)?
            }
            model::EnrichedBlockPayload::RollBack(
                block,
                ctx,
                last_block_rollback_info,
                final_block_in_batch,
            ) => self.reduce_block(
                &block,
                true,
                &ctx,
                last_block_rollback_info,
                final_block_in_batch,
            )?,
        }

        self.input.commit();
        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
