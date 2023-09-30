use gasket::framework::*;
use std::time::Duration;

use gasket::{
    messaging::tokio::{InputPort, OutputPort},
    runtime::spawn_stage,
};

use pallas::{
    codec::minicbor,
    ledger::traverse::{Era, MultiEraBlock, MultiEraTx, OutputRef},
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Deserialize;
use sled::IVec;

use crate::{
    bootstrap, crosscut,
    model::{self, BlockContext, EnrichedBlockPayload, RawBlockPayload},
    prelude::AppliesPolicy,
};

#[derive(Deserialize, Clone)]
pub struct Config {
    pub db_path: String,
    pub rollback_db_path: Option<String>,
    pub policy: crosscut::policies::RuntimePolicy,
}

impl Config {
    pub fn bootstrapper(
        mut self,
        policy: &crosscut::policies::RuntimePolicy,
        blocks: &crosscut::historic::BlockConfig,
    ) -> Stage {
        self.rollback_db_path = Some(blocks.rollback_db_path.clone());

        Stage {
            config: self,
            input: Default::default(),
            output: Default::default(),
            inserts_count: Default::default(),
            blocks_counter: Default::default(),
        }
    }
}

struct SledTxValue(u16, Vec<u8>);

impl TryInto<IVec> for SledTxValue {
    type Error = crate::Error;

    fn try_into(self) -> Result<IVec, Self::Error> {
        let SledTxValue(era, body) = self;
        minicbor::to_vec((era, body))
            .map(|x| IVec::from(x))
            .map_err(crate::Error::cbor)
    }
}

impl TryFrom<IVec> for SledTxValue {
    type Error = crate::Error;

    fn try_from(value: IVec) -> Result<Self, Self::Error> {
        let (tag, body): (u16, Vec<u8>) = minicbor::decode(&value).map_err(crate::Error::cbor)?;

        Ok(SledTxValue(tag, body))
    }
}

#[inline]
fn fetch_referenced_utxo<'a>(
    db: &sled::Db,
    utxo_ref: &OutputRef,
) -> Result<Option<(OutputRef, Era, Vec<u8>)>, crate::Error> {
    if let Some(ivec) = db
        .get(utxo_ref.to_string().as_bytes())
        .map_err(crate::Error::storage)?
    {
        let SledTxValue(era, cbor) = ivec.try_into().map_err(crate::Error::storage)?;
        let era: Era = era.try_into().map_err(crate::Error::storage)?;
        Ok(Some((utxo_ref.clone(), era, cbor)))
    } else {
        Ok(None)
    }
}

struct Sled {
    enrich_db: Option<sled::Db>,
    rollback_db: Option<sled::Db>,
}

impl Sled {
    fn db_refs_all(&self) -> Result<Option<(&sled::Db, &sled::Db)>, ()> {
        match (self.db_ref_enrich(), self.db_ref_rollback()) {
            (Some(db), Some(consumed_ring)) => Ok(Some((db, consumed_ring))),
            _ => Err(()),
        }
    }

    fn db_ref_enrich(&self) -> Option<&sled::Db> {
        match self.enrich_db.as_ref() {
            None => None,
            Some(db) => Some(db),
        }
    }

    fn db_ref_rollback(&self) -> Option<&sled::Db> {
        match self.rollback_db.as_ref() {
            None => None,
            Some(db) => Some(db),
        }
    }

    fn insert_produced_utxos(&self, db: &sled::Db, txs: &[MultiEraTx]) -> Result<(), crate::Error> {
        let mut insert_batch = sled::Batch::default();

        for tx in txs.iter() {
            for (idx, output) in tx.produces() {
                let key = format!("{}#{}", tx.hash(), idx);

                let era = tx.era().into();
                let body = output.encode();
                let value: IVec = SledTxValue(era, body).try_into()?;

                insert_batch.insert(key.as_bytes(), value)
            }
        }

        let batch_results = db.apply_batch(insert_batch).or_retry();

        batch_results.map_err(crate::Error::storage)
    }

    fn remove_produced_utxos(&self, db: &sled::Db, txs: &[MultiEraTx]) -> Result<(), crate::Error> {
        let mut remove_batch = sled::Batch::default();

        for tx in txs.iter() {
            for (idx, _) in tx.produces() {
                remove_batch.remove(format!("{}#{}", tx.hash(), idx).as_bytes());
            }
        }

        db.apply_batch(remove_batch).map_err(crate::Error::storage)
    }

    #[inline]
    fn par_fetch_referenced_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<BlockContext, crate::Error> {
        let mut ctx = BlockContext::default();

        let required: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.requires())
            .map(|input| input.output_ref())
            .collect();

        let matches: Result<Vec<_>, crate::Error> = required
            .par_iter()
            .map(|utxo_ref| fetch_referenced_utxo(db, utxo_ref))
            .collect();

        for m in matches? {
            if let Some((key, era, cbor)) = m {
                ctx.import_ref_output(&key, era, cbor);
            }
        }

        Ok(ctx)
    }

    fn get_removed(
        &self,
        consumed_ring: &sled::Db,
        key: &[u8],
    ) -> Result<Option<IVec>, crate::Error> {
        consumed_ring.get(key).map_err(crate::Error::storage)
    }

    fn remove_consumed_utxos(
        &self,
        db: &sled::Db,
        consumed_ring: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<(), ()> {
        let mut remove_batch = sled::Batch::default();
        let mut current_values_batch = sled::Batch::default();

        let keys: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.consumes())
            .map(|i| i.output_ref())
            .collect();

        for key in keys.iter() {
            if let Some(current_value) = db
                .get(key.to_string())
                .map_err(crate::Error::storage)
                .unwrap()
            {
                current_values_batch.insert(key.to_string().as_bytes(), current_value);
            }

            remove_batch.remove(key.to_string().as_bytes());
        }

        let result: Result<(), ()> = match (
            db.apply_batch(remove_batch),
            consumed_ring.apply_batch(current_values_batch),
        ) {
            (Ok(()), Ok(())) => Ok(()),
            _ => Err(()),
        };

        result
    }

    fn replace_consumed_utxos(
        &self,
        db: &sled::Db,
        consumed_ring: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<(), crate::Error> {
        let mut insert_batch = sled::Batch::default();
        let mut remove_batch = sled::Batch::default();

        let keys: Vec<_> = txs
            .iter()
            .flat_map(|tx| tx.consumes())
            .map(|i| i.output_ref())
            .collect();

        for key in keys.iter().rev() {
            if let Ok(Some(existing_value)) =
                self.get_removed(consumed_ring, key.to_string().as_bytes())
            {
                insert_batch.insert(key.to_string().as_bytes(), existing_value);
                remove_batch.remove(key.to_string().as_bytes());
            }
        }

        let result = match (
            db.apply_batch(insert_batch),
            consumed_ring.apply_batch(remove_batch),
        ) {
            (Ok(_), Ok(_)) => Ok(()),
            _ => Err(crate::Error::storage("failed to roll back consumed utxos")),
        };

        result
    }
}

#[derive(Stage)]
#[stage(name = "enrich-sled", unit = "RawBlockPayload", worker = "Worker")]
pub struct Stage {
    config: Config,

    pub input: InputPort<RawBlockPayload>,
    pub output: OutputPort<EnrichedBlockPayload>,

    #[metric]
    inserts_count: gasket::metrics::Counter,
    blocks_counter: gasket::metrics::Counter,
}

pub struct Worker {
    sled: Sled,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let sled = Sled {
            enrich_db: Some(
                sled::open(&stage.config.db_path.clone())
                    .or_retry()
                    .unwrap(),
            ),
            rollback_db: Some(
                sled::open(stage.config.rollback_db_path.clone().unwrap_or_default())
                    .or_retry()
                    .unwrap(),
            ),
        };

        log::warn!("finished opening enrich databases");
        Ok(Self { sled })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<RawBlockPayload>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(
        &mut self,
        unit: &RawBlockPayload,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        let all_dbs = self.sled.db_refs_all().unwrap();

        if let Some((db, consumed_ring)) = all_dbs {
            match unit {
                model::RawBlockPayload::RollForward(cbor) => {
                    let block = MultiEraBlock::decode(&cbor)
                        .map_err(crate::Error::cbor)
                        .apply_policy(&stage.config.clone().policy)
                        .or_panic()?;

                    let block = match block {
                        Some(x) => x,
                        None => return Ok(()),
                    };

                    let txs = &block.txs();

                    self.sled.insert_produced_utxos(db, txs).or_panic()?;
                    let ctx = self.sled.par_fetch_referenced_utxos(db, &txs).or_panic()?;

                    // and finally we remove utxos consumed by the block
                    self.sled
                        .remove_consumed_utxos(db, consumed_ring, &txs)
                        .expect("todo panic");

                    stage
                        .output
                        .send(model::EnrichedBlockPayload::roll_forward(*cbor, ctx))
                        .await;
                }

                model::RawBlockPayload::RollBack(cbor, last_known_block_info, finalize) => {
                    let block = MultiEraBlock::decode(&cbor)
                        .map_err(crate::Error::cbor)
                        .apply_policy(&stage.config.clone().policy);

                    if let Ok(block) = block {
                        if let Some(block) = block {
                            let txs = block.txs();

                            // Revert Anything to do with this block
                            self.sled
                                .remove_produced_utxos(db, &txs)
                                .expect("todo: panic error");
                            self.sled
                                .replace_consumed_utxos(db, consumed_ring, &txs)
                                .expect("todo: panic error");

                            let ctx = self
                                .sled
                                .par_fetch_referenced_utxos(db, &txs)
                                .or_restart()?;

                            stage
                                .output
                                .send(model::EnrichedBlockPayload::roll_back(
                                    *cbor,
                                    ctx,
                                    *last_known_block_info,
                                    *finalize,
                                ))
                                .await;
                        }
                    }

                    stage.blocks_counter.inc(1);
                }
            };
        }

        // todo propagate the workererror from above down here
        Ok(())
    }
}
