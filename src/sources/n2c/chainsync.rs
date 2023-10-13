use std::sync::Arc;
use std::time::Duration;

use gasket::messaging::tokio::OutputPort;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::facades::NodeClient;
use pallas::network::miniprotocols::chainsync::{self, NextResponse};
use pallas::network::miniprotocols::Point;

use gasket::framework::*;
use tokio::sync::Mutex;

use crate::model::RawBlockPayload;
use crate::{crosscut, model, sources, storage, Error};

use crate::prelude::*;

pub struct Worker {
    min_depth: usize,
    peer: Option<NodeClient>,
}

impl Worker {}

#[derive(Stage)]
#[stage(name = "sources-n2c", unit = "Vec<RawBlockPayload>", worker = "Worker")]
pub struct Stage {
    pub config: sources::n2c::Config,
    pub cursor: storage::Cursor,
    pub intersect: crosscut::IntersectConfig,
    pub chain: crosscut::ChainWellKnownInfo,
    pub blocks: Arc<Mutex<crosscut::historic::BufferBlocks>>,
    pub finalize: Option<crosscut::FinalizeConfig>,

    pub output: OutputPort<RawBlockPayload>,

    #[metric]
    pub chain_tip: gasket::metrics::Gauge,
    //#[metric]
    //pub last_block: gasket::metrics::Gauge,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let peer_session = NodeClient::connect(&stage.config.path, stage.chain.magic)
            .await
            .unwrap();

        let mut worker = Self {
            min_depth: stage.config.min_depth.unwrap_or(10 as usize),
            peer: Some(peer_session),
        };

        let peer = worker.peer.as_mut().unwrap();

        match stage.cursor.clone().last_point().unwrap() {
            Some(x) => {
                log::info!("found existing cursor in storage plugin: {:?}", x);
                let point: Point = x.try_into().unwrap();
                //stage.last_block.set(point.slot_or_default() as i64);
                peer.chainsync
                    .find_intersect(vec![point])
                    .await
                    .map_err(crate::Error::ouroboros)
                    .unwrap();
            }
            None => match &stage.intersect {
                crosscut::IntersectConfig::Origin => {
                    peer.chainsync
                        .intersect_origin()
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Tip => {
                    peer.chainsync
                        .intersect_tip()
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Point(_, _) => {
                    let point = &stage.intersect.get_point().expect("point value");
                    peer.chainsync
                        .find_intersect(vec![point.clone()])
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
                crosscut::IntersectConfig::Fallbacks(_) => {
                    let points = &stage.intersect.get_fallbacks().expect("fallback values");
                    peer.chainsync
                        .find_intersect(points.clone())
                        .await
                        .map_err(crate::Error::ouroboros)
                        .unwrap();
                }
            },
        }

        Ok(worker)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<Vec<RawBlockPayload>>, WorkerError> {
        let peer = self.peer.as_mut().unwrap();

        async fn flush_buffered_blocks(
            blocks_client: &mut crosscut::historic::BufferBlocks,
        ) -> Vec<RawBlockPayload> {
            match blocks_client.block_mem_take_all().await {
                Some(blocks) => blocks,
                None => vec![],
            }
        }

        Ok(WorkSchedule::Unit(match peer.chainsync.has_agency() {
            true => match peer.chainsync.request_next().await.or_restart() {
                Ok(next) => {
                    let mut blocks_client = stage.blocks.lock().await;

                    match next {
                        NextResponse::RollForward(cbor, t) => {
                            stage.chain_tip.set(t.1 as i64);
                            blocks_client
                                .block_mem_add(RawBlockPayload::RollForward(cbor.0))
                                .await;

                            match blocks_client.block_mem_size().await >= self.min_depth {
                                true => flush_buffered_blocks(&mut blocks_client).await,

                                false => {
                                    vec![]
                                }
                            }
                        }

                        NextResponse::RollBackward(p, t) => {
                            let mut blocks = flush_buffered_blocks(&mut blocks_client).await;

                            blocks_client.enqueue_rollback_batch(&p);

                            loop {
                                let pop_rollback_block = blocks_client.rollback_pop();

                                if let Some(last_good_block) = blocks_client.get_block_latest() {
                                    if let Ok(parsed_last_good_block) =
                                        MultiEraBlock::decode(&last_good_block)
                                    {
                                        let last_good_point = Point::Specific(
                                            parsed_last_good_block.slot(),
                                            parsed_last_good_block.hash().to_vec(),
                                        );

                                        stage.chain_tip.set(parsed_last_good_block.slot() as i64);

                                        if let Some(rollback_cbor) = pop_rollback_block {
                                            blocks.push(RawBlockPayload::RollBack(
                                                rollback_cbor.clone(),
                                                (last_good_point, parsed_last_good_block.number()),
                                            ));
                                        } else {
                                            break;
                                        }
                                    }
                                } else {
                                    if let Some(rollback_cbor) = pop_rollback_block {
                                        stage.chain_tip.set(0 as i64);

                                        blocks.push(RawBlockPayload::RollBack(
                                            rollback_cbor.clone(),
                                            (Point::Origin, 0),
                                        ));
                                    } else {
                                        break;
                                    }
                                }
                            }

                            blocks
                        }

                        NextResponse::Await => vec![],
                    }
                }
                Err(_) => {
                    log::warn!("got no response");
                    vec![]
                }
            },
            false => {
                log::info!("awaiting next block (blocking)");
                match peer.chainsync.recv_while_must_reply().await.or_restart() {
                    Ok(n) => {
                        let mut blocks_client = stage.blocks.lock().await;
                        let mut blocks = flush_buffered_blocks(&mut blocks_client).await;

                        match n {
                            NextResponse::RollForward(cbor, t) => {
                                log::warn!("rolling forward");

                                blocks.push(RawBlockPayload::RollForward(cbor.0));

                                blocks
                            }
                            NextResponse::RollBackward(p, t) => {
                                log::warn!("rolling back");
                                let mut blocks_client = stage.blocks.lock().await;

                                blocks_client.enqueue_rollback_batch(&p);

                                loop {
                                    let pop_rollback_block = blocks_client.rollback_pop();

                                    if let Some(last_good_block) = blocks_client.get_block_latest()
                                    {
                                        if let Ok(parsed_last_good_block) =
                                            MultiEraBlock::decode(&last_good_block)
                                        {
                                            let last_good_point = Point::Specific(
                                                parsed_last_good_block.slot(),
                                                parsed_last_good_block.hash().to_vec(),
                                            );

                                            if let Some(rollback_cbor) = pop_rollback_block {
                                                blocks.push(RawBlockPayload::RollBack(
                                                    rollback_cbor.clone(),
                                                    (
                                                        last_good_point,
                                                        parsed_last_good_block.number(),
                                                    ),
                                                ));
                                            } else {
                                                break;
                                            }
                                        }
                                    } else {
                                        if let Some(rollback_cbor) = pop_rollback_block {
                                            blocks.push(RawBlockPayload::RollBack(
                                                rollback_cbor.clone(),
                                                (Point::Origin, 0),
                                            ));
                                        } else {
                                            break;
                                        }
                                    }
                                }

                                blocks
                            }
                            NextResponse::Await => blocks,
                        }
                    }
                    Err(_) => vec![],
                }
            }
        }))
    }

    async fn execute(
        &mut self,
        unit: &Vec<RawBlockPayload>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        for raw_block_payload in unit {
            stage
                .output
                .send(gasket::messaging::Message {
                    payload: raw_block_payload.clone(),
                })
                .await
                .or_retry();
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        self.peer.as_mut().unwrap().abort();

        Ok(())
    }
}