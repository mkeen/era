use std::sync::{Arc, Mutex};

use gasket::messaging::tokio::OutputPort;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader};
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::blockfetch::Range;
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
        self.chain_buffer.roll_forward(point);

        Ok(())
    }
}

#[derive(Stage)]
#[stage(
    name = "sources-n2n",
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

    pub output: OutputPort<RawBlockPayload>,

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

        let chainsync = peer_session.chainsync();

        match stage.cursor.clone().last_point().unwrap() {
            Some(x) => {
                log::info!("found existing cursor in storage plugin: {:?}", x);
                let point = x.try_into().unwrap();
                chainsync
                    .find_intersect(vec![point])
                    .await
                    .map_err(crate::Error::ouroboros)
                    .unwrap();
            }
            None => match &stage.intersect {
                crosscut::IntersectConfig::Origin => {
                    chainsync
                        .intersect_origin()
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Tip => {
                    chainsync
                        .intersect_tip()
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Point(_, _) => {
                    let point = &stage.intersect.get_point().expect("point value");
                    chainsync
                        .find_intersect(vec![point.clone()])
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Fallbacks(_) => {
                    let points = &stage.intersect.get_fallbacks().expect("fallback values");
                    chainsync
                        .find_intersect(points.clone())
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
            },
        }

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

        loop {
            let pop_rollback_block = blocks.rollback_pop();

            if let Some(rollback_cbor) = pop_rollback_block {
                if let Some(last_good_block) = blocks.get_block_latest() {
                    if let Ok(parsed_last_good_block) = MultiEraBlock::decode(&last_good_block) {
                        let point = Point::Specific(
                            parsed_last_good_block.slot(),
                            parsed_last_good_block.hash().to_vec(),
                        );

                        stage
                            .output
                            .send(model::RawBlockPayload::roll_back(
                                rollback_cbor.clone(),
                                (point, parsed_last_good_block.number()),
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

        match unit {
            NextResponse::RollForward(h, t) => {
                self.on_roll_forward(&h).unwrap();
                stage.chain_tip.set(t.1 as i64);
            }
            NextResponse::RollBackward(p, t) => {
                match self.chain_buffer.roll_back(&p) {
                    chainsync::RollbackEffect::Handled => {
                        log::warn!("rolled back before side effects {:?}", p);
                    }
                    chainsync::RollbackEffect::OutOfScope => {
                        log::warn!("preparing to undo side effects for {:?}", p);

                        blocks.enqueue_rollback_batch(&p);
                    }
                }
                stage.chain_tip.set(t.1 as i64);
            }
            NextResponse::Await => {
                log::info!("chain-sync reached the tip of the chain");
            }
        }

        // see if we have points that already reached certain depth
        let ready = self.chain_buffer.pop_with_depth(self.min_depth);

        if ready.len() > 0 {
            let range = (
                ready.first().unwrap().clone(),
                ready.last().unwrap().clone(),
            );

            let blocks = self
                .peer
                .blockfetch()
                .fetch_range(Range::from(range.clone()))
                .await
                .or_retry()?;

            for block in blocks {
                stage.block_count.inc(1);

                stage
                    .output
                    .send(model::RawBlockPayload::roll_forward(block))
                    .await
                    .or_retry()?;
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        self.peer.abort();

        Ok(())
    }
}
