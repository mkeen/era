use std::sync::{Arc, Mutex};
use std::time::Duration;

use gasket::messaging::tokio::OutputPort;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader};
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::blockfetch::{self, Range};
use pallas::network::miniprotocols::chainsync::{
    self, HeaderContent, NextResponse, RollbackBuffer,
};
use pallas::network::miniprotocols::Point;

use gasket::framework::*;

use crate::model::RawBlockPayload;
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
    peer: Option<PeerClient>,
}

impl Worker {}

#[derive(Stage)]
#[stage(name = "sources-n2n", unit = "Vec<RawBlockPayload>", worker = "Worker")]
pub struct Stage {
    pub config: sources::n2n::Config,
    pub cursor: storage::Cursor,
    pub intersect: crosscut::IntersectConfig,
    pub chain: crosscut::ChainWellKnownInfo,
    pub blocks: Arc<Mutex<crosscut::historic::BufferBlocks>>,
    pub finalize: Option<crosscut::FinalizeConfig>,

    pub busy: bool,

    pub output: OutputPort<RawBlockPayload>,

    #[metric]
    pub chain_tip: gasket::metrics::Gauge,

    #[metric]
    pub block_count: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let peer_session = PeerClient::connect(&stage.config.address, stage.chain.magic)
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
                let point = x.try_into().unwrap();
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

        std::thread::sleep(Duration::from_millis(1000));
        log::warn!("requesting next block");

        Ok(WorkSchedule::Unit(match peer.chainsync.has_agency() {
            true => match peer.chainsync.request_next().await.or_restart() {
                Ok(next) => {
                    let mut blocks = vec![];

                    let mut blocks_client = stage.blocks.lock().unwrap();

                    let mut to_fetch = vec![];

                    match next {
                        NextResponse::RollForward(h, t) => {
                            stage.chain_tip.set(t.1 as i64);
                            let parsed_headers = to_traverse(&h);
                            match parsed_headers {
                                Ok(parsed_headers) => {
                                    to_fetch.push(parsed_headers);
                                    // match peer
                                    //     .blockfetch
                                    //     .fetch_single(Point::Specific(
                                    //         parsed_headers.slot(),
                                    //         parsed_headers.hash().to_vec(),
                                    //     ))
                                    //     .await
                                    // {
                                    //     Ok(static_single) => {
                                    //         blocks
                                    //             .push(RawBlockPayload::RollForward(static_single));
                                    //     }
                                    //     Err(_) => {}
                                    //}
                                }
                                Err(_) => {}
                            }

                            for _ in 1..5000 {
                                match peer.chainsync.request_next().await.or_restart() {
                                    Ok(next) => match next {
                                        NextResponse::RollForward(h1, t1) => {
                                            stage.chain_tip.set(t1.1 as i64);
                                            let parsed_headers = to_traverse(&h1);
                                            match parsed_headers {
                                                Ok(parsed_headers) => {
                                                    match peer
                                                        .blockfetch
                                                        .fetch_single(Point::Specific(
                                                            parsed_headers.slot(),
                                                            parsed_headers.hash().to_vec(),
                                                        ))
                                                        .await
                                                    {
                                                        Ok(static_single) => {
                                                            blocks.push(
                                                                RawBlockPayload::RollForward(
                                                                    static_single,
                                                                ),
                                                            );
                                                        }
                                                        Err(_) => {
                                                            break;
                                                        }
                                                    }
                                                }
                                                Err(_) => {
                                                    break;
                                                }
                                            }
                                        }

                                        NextResponse::RollBackward(p1, t1) => {
                                            blocks_client.enqueue_rollback_batch(&p1);

                                            loop {
                                                let pop_rollback_block =
                                                    blocks_client.rollback_pop();

                                                if let Some(last_good_block) =
                                                    blocks_client.get_block_latest()
                                                {
                                                    if let Ok(parsed_last_good_block) =
                                                        MultiEraBlock::decode(&last_good_block)
                                                    {
                                                        let last_good_point = Point::Specific(
                                                            parsed_last_good_block.slot(),
                                                            parsed_last_good_block.hash().to_vec(),
                                                        );

                                                        stage
                                                            .chain_tip
                                                            .set(last_good_point.slot_or_default()
                                                                as i64);

                                                        if let Some(rollback_cbor) =
                                                            pop_rollback_block
                                                        {
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
                                                    if let Some(rollback_cbor) = pop_rollback_block
                                                    {
                                                        blocks.push(RawBlockPayload::RollBack(
                                                            rollback_cbor.clone(),
                                                            (Point::Origin, 0),
                                                        ));
                                                    } else {
                                                        break;
                                                    }
                                                }
                                            }

                                            break;
                                        }

                                        NextResponse::Await => break,
                                    },
                                    Err(_) => break,
                                }
                            }

                            match (to_fetch.first(), to_fetch.last()) {
                                (Some(to_fetch_first), Some(to_fetch_last)) => {
                                    match peer
                                        .blockfetch
                                        .fetch_range((
                                            Point::Specific(
                                                to_fetch_first.slot(),
                                                to_fetch_first.hash().to_vec(),
                                            ),
                                            Point::Specific(
                                                to_fetch_last.slot(),
                                                to_fetch_last.hash().to_vec(),
                                            ),
                                        ))
                                        .await
                                    {
                                        Ok(incoming_blocks) => {
                                            for block in incoming_blocks {
                                                blocks.push(RawBlockPayload::RollForward(block));
                                            }
                                        }
                                        Err(_) => {}
                                    }
                                }

                                _ => {}
                            };

                            blocks
                        }

                        NextResponse::RollBackward(p, t) => {
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
                Err(_) => {
                    log::warn!("got no response");
                    vec![]
                }
            },
            false => {
                log::info!("awaiting next block (blocking)");
                match peer.chainsync.recv_while_must_reply().await.or_restart() {
                    Ok(n) => {
                        let mut blocks = vec![];

                        let mut blocks_client = stage.blocks.lock().unwrap();

                        match n {
                            NextResponse::RollForward(h, t) => {
                                log::warn!("rolling forward");

                                let parsed_headers = to_traverse(&h);
                                match parsed_headers {
                                    Ok(parsed_headers) => {
                                        match peer
                                            .blockfetch
                                            .fetch_single(Point::Specific(
                                                parsed_headers.slot(),
                                                parsed_headers.hash().to_vec(),
                                            ))
                                            .await
                                        {
                                            Ok(static_single) => {
                                                blocks.push(RawBlockPayload::RollForward(
                                                    static_single,
                                                ));
                                            }
                                            Err(_) => {}
                                        }
                                    }
                                    Err(_) => {}
                                }

                                blocks
                            }
                            NextResponse::RollBackward(p, t) => {
                                log::warn!("rolling back");

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
                .or_retry()
                .unwrap();
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        self.peer.as_mut().unwrap().abort();

        Ok(())
    }
}
