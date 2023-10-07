use std::sync::{Arc, Mutex};

use config::builder::DefaultState;
use log::*;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader};
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::chainsync::{HeaderContent, NextResponse, RollbackBuffer};
use pallas::network::miniprotocols::{chainsync, Point};

use gasket::framework::*;

use crate::crosscut::IntersectConfig;
use crate::model::RawBlockPayload;
use crate::storage::Cursor;
use crate::{crosscut, model, sources, storage, Error};

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

async fn intersect_from_config(
    peer: &mut PeerClient,
    intersect: &IntersectConfig,
    mut cursor: Cursor,
) -> Result<Option<Point>, WorkerError> {
    let chainsync = peer.chainsync();

    match cursor.last_point().unwrap() {
        Some(x) => {
            log::info!("found existing cursor in storage plugin: {:?}", x);
            let point = x.try_into().unwrap();
            chainsync
                .find_intersect(vec![point])
                .await
                .map_err(crate::Error::ouroboros)
                .unwrap();
        }
        None => {
            log::info!("no cursor found in storage plugin");
        }
    };

    match &intersect {
        crosscut::IntersectConfig::Origin => {
            let point = chainsync
                .intersect_origin()
                .await
                .map_err(crate::Error::ouroboros)
                .unwrap();
            Ok(Some(point))
        }
        crosscut::IntersectConfig::Tip => {
            let point = chainsync
                .intersect_tip()
                .await
                .map_err(crate::Error::ouroboros)
                .unwrap();
            Ok(Some(point))
        }
        crosscut::IntersectConfig::Point(_, _) => {
            let point = intersect.get_point().expect("point value");
            let (point, _) = chainsync
                .find_intersect(vec![point])
                .await
                .map_err(crate::Error::ouroboros)
                .unwrap();
            Ok(point)
        }
        crosscut::IntersectConfig::Fallbacks(_) => {
            let points = intersect.get_fallbacks().expect("fallback values");
            let (point, _) = chainsync
                .find_intersect(points)
                .await
                .map_err(crate::Error::ouroboros)
                .unwrap();
            Ok(point)
        }
    }
}

pub struct Worker {
    min_depth: usize,
    peer: PeerClient,
    chain_buffer: RollbackBuffer,
}

impl Worker {
    fn on_roll_forward(&mut self, content: &HeaderContent) -> Result<(), gasket::error::Error> {
        let header = to_traverse(&content).unwrap();
        let point = Point::Specific(header.slot(), header.hash().to_vec());
        // track the new point in our memory buffer
        log::warn!("queueing point for processing {:?}", point);
        self.chain_buffer.roll_forward(point);

        Ok(())
    }
}

#[derive(Stage)]
#[stage(
    name = "n2n-chainsync",
    unit = "NextResponse<HeaderContent>",
    worker = "Worker"
)]
pub struct Stage {
    pub config: sources::n2n::Config,
    pub cursor: storage::Cursor,
    pub intersect: crosscut::IntersectConfig,
    pub chain: crosscut::ChainWellKnownInfo,
    pub blocks: Arc<Mutex<crosscut::historic::BufferBlocks>>,
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
        let mut peer_session = PeerClient::connect(&stage.config.address, stage.chain.magic)
            .await
            .unwrap();

        intersect_from_config(&mut peer_session, &stage.intersect, stage.cursor.clone())
            .await
            .unwrap();

        Ok(Self {
            min_depth: stage.config.min_depth.unwrap_or(10 as usize),
            peer: peer_session,
            chain_buffer: chainsync::RollbackBuffer::new(),
        })
    }

    async fn schedule(
        &mut self,
        _: &mut Stage,
    ) -> Result<WorkSchedule<NextResponse<HeaderContent>>, WorkerError> {
        let client = self.peer.chainsync();

        Ok(WorkSchedule::Unit(match client.has_agency() {
            true => {
                log::info!("requesting next block");
                client.request_next().await.or_restart().unwrap()
            }
            false => {
                log::info!("awaiting next block (blocking)");
                client.recv_while_must_reply().await.or_restart().unwrap()
            }
        }))
    }

    async fn execute(
        &mut self,
        unit: &NextResponse<HeaderContent>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        let mut blocks = stage.blocks.lock().unwrap();

        match unit {
            NextResponse::RollForward(h, t) => {
                self.on_roll_forward(&h).unwrap();
                stage.chain_tip.set(t.1 as i64);
            }
            NextResponse::RollBackward(p, t) => {
                match self.chain_buffer.roll_back(&p) {
                    chainsync::RollbackEffect::Handled => {
                        log::warn!("handled rollback within buffer for point {:?}", p);
                    }
                    chainsync::RollbackEffect::OutOfScope => {
                        log::warn!("rolling backward away from point {:?}", p);

                        blocks.enqueue_rollback_batch(&p);
                    }
                }
                stage.chain_tip.set(t.1 as i64);
            }
            NextResponse::Await => {
                log::info!("chain-sync reached the tip of the chain");
            }
        }

        loop {
            let pop_rollback_block = blocks.rollback_pop();

            if let Some(rollback_cbor) = pop_rollback_block {
                if let Some(last_good_block) = blocks.get_block_latest() {
                    if let Ok(parsed_last_good_block) = MultiEraBlock::decode(&last_good_block) {
                        warn!("backward happening");

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
                    }
                }
            } else {
                break;
            }
        }

        // see if we have points that already reached certain depth
        if self.chain_buffer.size() >= self.min_depth {
            let ready = self.chain_buffer.pop_with_depth(self.min_depth);

            for point in ready {
                let block = self
                    .peer
                    .blockfetch()
                    .fetch_single(point.clone())
                    .await
                    .or_restart()?;

                stage.block_count.inc(1);

                stage
                    .output
                    .send(model::RawBlockPayload::roll_forward(block))
                    .await
                    .unwrap();

                // evaluate if we should finalize the thread according to config

                // if crosscut::should_finalize(&Some(stage.config.finalize.clone()), &point) {
                //     return Ok(()); // i think this is effectively a no-op that does nothing now... but this is designed to never shut down anyway... night fully remove should_finalize
                // }
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        self.peer.abort();

        Ok(())
    }
}
