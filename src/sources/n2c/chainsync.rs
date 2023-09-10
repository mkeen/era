use gasket::error::AsWorkError;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::miniprotocols::chainsync::BlockContent;
use pallas::network::miniprotocols::{chainsync, Point};
use pallas::network::multiplexer::StdChannel;

use crate::prelude::*;
use crate::{crosscut, model, sources::utils, storage, Error};

use super::transport::Transport;

fn to_traverse<'b>(block: &'b BlockContent) -> Result<MultiEraBlock<'b>, Error> {
    MultiEraBlock::decode(&block).map_err(Error::cbor)
}

type OutputPort = gasket::messaging::OutputPort<model::RawBlockPayload>;

pub struct Worker {
    socket: String,
    min_depth: usize,
    policy: crosscut::policies::RuntimePolicy,
    chain_buffer: chainsync::RollbackBuffer,
    chain: crosscut::ChainWellKnownInfo,
    blocks: crosscut::historic::BufferBlocks,
    intersect: crosscut::IntersectConfig,
    cursor: storage::Cursor,
    finalize: Option<crosscut::FinalizeConfig>,
    chainsync: Option<chainsync::N2CClient<StdChannel>>,
    output: OutputPort,
    block_count: gasket::metrics::Counter,
    chain_tip: gasket::metrics::Gauge,
}

impl Worker {
    pub fn new(
        socket: String,
        min_depth: usize,
        policy: crosscut::policies::RuntimePolicy,
        chain: crosscut::ChainWellKnownInfo,
        blocks: crosscut::historic::BufferBlocks,
        intersect: crosscut::IntersectConfig,
        finalize: Option<crosscut::FinalizeConfig>,
        cursor: storage::Cursor,
        output: OutputPort,
    ) -> Self {
        Self {
            socket,
            min_depth,
            policy,
            chain,
            blocks,
            intersect,
            finalize,
            cursor,
            output,
            chainsync: None,
            block_count: Default::default(),
            chain_tip: Default::default(),
            chain_buffer: chainsync::RollbackBuffer::new(),
        }
    }

    fn on_roll_forward(
        &mut self,
        content: chainsync::BlockContent,
    ) -> Result<(), gasket::error::Error> {
        // parse the header and extract the point of the chain
        let block = to_traverse(&content)
            .apply_policy(&self.policy)
            .or_panic()?;

        let block = match block {
            Some(x) => x,
            None => return Ok(()),
        };

        let point = Point::Specific(block.slot(), block.hash().to_vec());

        // track the new point in our memory buffer
        log::debug!("rolling forward to point {:?}", point);
        self.chain_buffer.roll_forward(point);

        Ok(())
    }

    fn on_rollback(&mut self, point: &Point) -> Result<(), gasket::error::Error> {
        log::debug!("rolling block to point {:?}", point);

        match self.chain_buffer.roll_back(point) {
            chainsync::RollbackEffect::Handled => {
                log::debug!("handled rollback within buffer {:?}", point);
            }
            chainsync::RollbackEffect::OutOfScope => {
                self.blocks.enqueue_rollback_batch(point);
            }
        }

        Ok(())
    }

    fn request_next(&mut self) -> Result<(), gasket::error::Error> {
        log::info!("requesting next block");

        let next = self
            .chainsync
            .as_mut()
            .unwrap()
            .request_next()
            .or_restart()?;

        match next {
            chainsync::NextResponse::RollForward(h, t) => {
                self.blocks.insert_block(&t.0, &h.0);
                self.on_roll_forward(h)?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::RollBackward(p, t) => {
                self.on_rollback(&p)?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::Await => {
                log::info!("chain-sync reached the tip of the chain");
                Ok(())
            }
        }
    }

    fn await_next(&mut self) -> Result<(), gasket::error::Error> {
        log::info!("awaiting next block (blocking)");

        let next = self
            .chainsync
            .as_mut()
            .unwrap()
            .recv_while_must_reply()
            .or_restart()?;

        match next {
            chainsync::NextResponse::RollForward(h, t) => {
                self.blocks.insert_block(&t.0, &h.0);
                self.on_roll_forward(h)?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::RollBackward(p, t) => {
                self.on_rollback(&p)?;
                self.chain_tip.set(t.1 as i64);
                Ok(())
            }
            _ => unreachable!("protocol invariant not respected in chain-sync state machine"),
        }
    }
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("received_blocks", &self.block_count)
            .with_gauge("chain_tip", &self.chain_tip)
            .build()
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let transport = Transport::setup(&self.socket, self.chain.magic).or_retry()?;

        let mut chainsync = chainsync::N2CClient::new(transport.channel5);

        let start =
            utils::define_chainsync_start(&self.intersect, &mut self.cursor, &mut chainsync)
                .or_retry()?;

        let start = start.ok_or(Error::IntersectNotFound).or_panic()?;

        log::info!("chain-sync intersection is {:?}", start);

        self.chainsync = Some(chainsync);
        Ok(())
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let mut rolled_back = false;

        loop {
            let pop_rollback_block = self.blocks.rollback_pop();

            if let Some(cbor) = pop_rollback_block {
                let block = MultiEraBlock::decode(&cbor)
                    .map_err(crate::Error::cbor)
                    .apply_policy(&self.policy);

                if let Ok(block) = block {
                    rolled_back = true;
                    self.output
                        .send(model::RawBlockPayload::roll_back(cbor.clone()))?;
                    self.block_count.inc(1);

                    if let Some(block) = block {
                        let last_point = Point::Specific(block.slot(), block.hash().to_vec());

                        if crosscut::should_finalize(&self.finalize, &last_point) {
                            return Ok(gasket::runtime::WorkOutcome::Done);
                        }
                    }
                }
            } else {
                break;
            }
        }

        if rolled_back {
            return Ok(gasket::runtime::WorkOutcome::Partial);
        }

        match self.chainsync.as_ref().unwrap().has_agency() {
            true => self.request_next()?,
            false => self.await_next()?,
        };

        // see if we have points that already reached certain depth
        let ready = self.chain_buffer.pop_with_depth(self.min_depth);
        log::debug!("found {} points with required min depth", ready.len());

        // find confirmed block in memory and send down the pipeline
        for point in ready {
            let block = self.blocks.get_block_at_point(&point);

            if let Some(block) = block {
                self.output
                    .send(model::RawBlockPayload::roll_forward(block))?;
                self.block_count.inc(1);
            } else {
                log::warn!(
                    "couldn't find block in buffer: {}",
                    &point.slot_or_default()
                )
            }

            // evaluate if we should finalize the thread according to config
            if crosscut::should_finalize(&self.finalize, &point) {
                return Ok(gasket::runtime::WorkOutcome::Done);
            }
        }

        Ok(gasket::runtime::WorkOutcome::Partial)
    }
}
