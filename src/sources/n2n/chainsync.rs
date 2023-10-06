use config::builder::DefaultState;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader};
use pallas::network::miniprotocols::chainsync::{HeaderContent, NextResponse};
use pallas::network::miniprotocols::{blockfetch, chainsync, Point};

use gasket::framework::*;

use crate::model::RawBlockPayload;
use crate::sources::n2n::transport::Transport;
use crate::{crosscut, model, sources, sources::utils, storage, Error};

use crate::prelude::*;

fn to_traverse<'b>(header: &'b HeaderContent) -> Result<MultiEraHeader<'b>, Error> {
    MultiEraHeader::decode(
        header.variant,
        header.byron_prefix.map(|x| x.0),
        &header.cbor,
    )
    .map_err(Error::cbor)
}

pub type OutputPort = gasket::messaging::tokio::OutputPort<RawBlockPayload>;

pub struct Worker {
    address: String,
    min_depth: usize,
    chainsync: Option<chainsync::N2NClient>,
    blockfetch: Option<blockfetch::Client>,
    chain_buffer: chainsync::RollbackBuffer,
}

impl Worker {
    fn on_roll_forward(
        &mut self,
        content: chainsync::HeaderContent,
    ) -> Result<(), gasket::error::Error> {
        // parse the header and extract the point of the chain

        let header = to_traverse(&content).unwrap();

        let point = Point::Specific(header.slot(), header.hash().to_vec());

        // track the new point in our memory buffer
        log::warn!("rolling forward to point {:?}", point);
        self.chain_buffer.roll_forward(point);

        Ok(())
    }

    fn on_rollback(
        &mut self,
        point: &Point,
        blocks: &mut crosscut::historic::BufferBlocks,
    ) -> Result<(), gasket::error::Error> {
        match self.chain_buffer.roll_back(point) {
            chainsync::RollbackEffect::Handled => {
                log::warn!("handled rollback within buffer {:?}", point);
                Ok(())
            }
            chainsync::RollbackEffect::OutOfScope => {
                log::warn!("rolling backward to point {:?}", point);
                blocks.enqueue_rollback_batch(point);
                Ok(())
            }
        }
    }

    async fn request_next(
        &mut self,
        chain_tip: &gasket::metrics::Gauge,
    ) -> Result<(), gasket::error::Error> {
        log::info!("requesting next block");

        let next = self
            .chainsync
            .as_mut()
            .unwrap()
            .request_next()
            .await
            .or_restart()
            .unwrap();

        match next {
            chainsync::NextResponse::RollForward(h, t) => {
                self.on_roll_forward(h)?;
                chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::RollBackward(p, t) => {
                self.chain_buffer.roll_back(&p); // just rollback the chain buffer... dont do anything with rollback data on initial sync
                chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::Await => {
                log::info!("chain-sync reached the tip of the chain");
                Ok(())
            }
        }
    }

    async fn await_next(
        &mut self,
        chain_tip: &gasket::metrics::Gauge,
        blocks: &mut crosscut::historic::BufferBlocks,
    ) -> Result<(), gasket::error::Error> {
        log::info!("awaiting next block (blocking)");

        let next = self
            .chainsync
            .as_mut()
            .unwrap()
            .recv_while_must_reply()
            .await
            .or_restart()
            .unwrap();

        match next {
            chainsync::NextResponse::RollForward(h, t) => {
                self.on_roll_forward(h)?;
                chain_tip.set(t.1 as i64);
                Ok(())
            }
            chainsync::NextResponse::RollBackward(p, t) => {
                self.on_rollback(&p, blocks)?;
                chain_tip.set(t.1 as i64);
                Ok(())
            }
            _ => unreachable!("protocol invariant not respected in chain-sync state machine"),
        }
    }
}

#[derive(Stage)]
#[stage(name = "n2n-chainsync", unit = "()", worker = "Worker")]
pub struct Stage {
    pub config: sources::n2n::Config,
    pub cursor: storage::Cursor,
    pub intersect: crosscut::IntersectConfig,
    pub chain: crosscut::ChainWellKnownInfo,
    pub blocks: crosscut::historic::BufferBlocks,
    pub finalize: Option<crosscut::FinalizeConfig>,

    pub output: OutputPort,

    #[metric]
    pub chain_tip: gasket::metrics::Gauge,

    #[metric]
    pub block_count: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let transport = Transport::setup(&stage.config.address, stage.chain.magic)
            .await
            .unwrap();

        let mut chainsync = chainsync::N2NClient::new(transport.channel2);

        let start = utils::define_chainsync_start(
            &stage.intersect,
            &mut stage.cursor.clone(),
            &mut chainsync,
        )
        .await
        .or_retry()?;

        let start = start.ok_or(Error::IntersectNotFound).or_panic()?;

        log::info!("chain-sync intersection is {:?}", start);

        let blockfetch = blockfetch::Client::new(transport.channel3);

        Ok(Self {
            address: stage.config.address.clone(),
            min_depth: stage.config.min_depth.unwrap_or(10 as usize),
            blockfetch: Some(blockfetch),
            chainsync: Some(chainsync),
            chain_buffer: chainsync::RollbackBuffer::new(),
        })
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        let client = &mut self.chainsync.as_mut().unwrap();

        Ok(WorkSchedule::Unit(match client.has_agency() {
            true => {
                log::info!("requesting next block");
                self.request_next(&stage.chain_tip)
                    .await
                    .or_restart()
                    .unwrap();
                ()
            }
            false => {
                log::info!("awaiting next block (blocking)");
                self.await_next(&stage.chain_tip, &mut stage.blocks)
                    .await
                    .or_restart()
                    .unwrap();
                ()
            }
        }))
    }

    async fn execute(&mut self, _: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        let mut rolled_back = false;

        let blocks = &mut stage.blocks;

        loop {
            let pop_rollback_block = blocks.rollback_pop();

            if let Some(rollback_cbor) = pop_rollback_block {
                if let Some(last_good_block) = blocks.get_block_latest() {
                    if let Ok(parsed_last_good_block) = MultiEraBlock::decode(&last_good_block) {
                        rolled_back = true;

                        let point = Point::Specific(
                            parsed_last_good_block.slot(),
                            parsed_last_good_block.hash().to_vec(),
                        );

                        stage
                            .output
                            .send(model::RawBlockPayload::roll_back(
                                rollback_cbor.clone(),
                                (point, parsed_last_good_block.number() as i64),
                                blocks.get_current_queue_depth() == 0,
                            ))
                            .await
                            .unwrap();

                        stage.block_count.inc(1);

                        // if crosscut::should_finalize(&self.finalize, &last_point) {
                        //     return Ok(gasket::runtime::WorkOutcome::Done);
                        // }
                    }
                }
            } else {
                break;
            }
        }

        if rolled_back {
            return Ok(());
        }

        // see if we have points that already reached certain depth
        let ready = self.chain_buffer.pop_with_depth(self.min_depth);

        for point in ready {
            let block = self
                .blockfetch
                .as_mut()
                .unwrap()
                .fetch_single(point.clone())
                .await
                .or_restart()?;

            blocks.insert_block(&point, &block);

            stage
                .output
                .send(model::RawBlockPayload::roll_forward(block))
                .await
                .unwrap();

            stage.block_count.inc(1);

            // evaluate if we should finalize the thread according to config

            // if crosscut::should_finalize(&Some(stage.config.finalize.clone()), &point) {
            //     return Ok(()); // i think this is effectively a no-op that does nothing now... but this is designed to never shut down anyway... night fully remove should_finalize
            // }
        }

        Ok(())
    }
}
