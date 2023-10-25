use std::sync::Arc;

use gasket::framework::*;

use gasket::messaging::tokio::{InputPort, OutputPort};

use pallas::crypto::hash::Hash;
use pallas::ledger::configs::byron::{genesis_utxos, GenesisUtxo};

use pallas::{
    codec::minicbor,
    ledger::traverse::{Era, MultiEraBlock, MultiEraTx, OutputRef},
};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Deserialize;
use sled::IVec;
use tokio::sync::Mutex;

use crate::pipeline::Context;
use crate::prelude::AppliesPolicy;
use crate::Error;
use crate::{
    model::{self, BlockContext, EnrichedBlockPayload, RawBlockPayload},
    pipeline,
};

#[derive(Deserialize, Clone)]
pub struct Config {
    pub db_path: String,
    pub rollback_db_path: Option<String>,
}

impl Config {
    pub fn bootstrapper(
        mut self,
        ctx: Arc<Mutex<pipeline::Context>>,
        rollback_db_path: String,
    ) -> Stage {
        self.rollback_db_path = Some(rollback_db_path);

        Stage {
            config: self,
            input: Default::default(),
            output: Default::default(),
            enrich_inserts: Default::default(),
            enrich_removes: Default::default(),
            enrich_matches: Default::default(),
            enrich_mismatches: Default::default(),
            enrich_blocks: Default::default(),
            enrich_cancelled_empty_tx: Default::default(),
            ctx,
        }
    }
}

// bool - true = genesis txs
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
    log::warn!("trying to find {}", utxo_ref.to_string());

    if let Some(ivec) = db
        .get(utxo_ref.to_string().as_bytes())
        .map_err(crate::Error::storage)?
    {
        let SledTxValue(era, cbor) = ivec.try_into().map_err(crate::Error::storage)?;
        let era: Era = era.try_into().map_err(crate::Error::storage)?;
        log::warn!("found {}", utxo_ref.to_string());

        Ok(Some((utxo_ref.clone(), era, cbor)))
    } else {
        Ok(None)
    }
}

pub struct Worker {
    enrich_db: Option<sled::Db>,
    rollback_db: Option<sled::Db>,
}

impl Worker {
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

    fn insert_produced_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
        inserts: &gasket::metrics::Counter,
    ) -> Result<(), crate::Error> {
        let mut insert_batch = sled::Batch::default();

        for tx in txs.iter() {
            for (idx, output) in tx.produces() {
                let value: IVec = SledTxValue(tx.era() as u16, output.encode()).try_into()?;
                insert_batch.insert(format!("{}#{}", tx.hash(), idx).as_bytes(), value);
                inserts.inc(1);
            }
        }

        db.apply_batch(insert_batch).map_err(crate::Error::storage)
    }

    fn insert_genesis_utxo(
        &self,
        db: &sled::Db,
        genesis_utxo: &GenesisUtxo,
        inserts: &gasket::metrics::Counter,
    ) -> Result<(), crate::Error> {
        let mut encoded_genesis_utxo = vec![];

        minicbor::encode(genesis_utxo, &mut encoded_genesis_utxo).unwrap();

        let value: IVec = SledTxValue(0, encoded_genesis_utxo).try_into()?;
        inserts.inc(1);

        db.insert(format!("{}#{}", genesis_utxo.0, "0").as_bytes(), value)
            .map_err(Error::storage)?;

        Ok(())
    }

    fn remove_produced_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
        enrich_removes: &gasket::metrics::Counter,
    ) -> Result<(), crate::Error> {
        let mut remove_batch = sled::Batch::default();

        for tx in txs.iter() {
            for (idx, _) in tx.produces() {
                enrich_removes.inc(txs.len() as u64);

                remove_batch.remove(format!("{}#{}", tx.hash(), idx).as_bytes());
            }
        }

        db.apply_batch(remove_batch).map_err(crate::Error::storage)
    }

    #[inline]
    fn par_fetch_referenced_utxos(
        &self,
        db: &sled::Db,
        block_number: u64,
        txs: &[MultiEraTx],
    ) -> Result<(BlockContext, u64, u64), crate::Error> {
        log::warn!("getting thing for {:?}", block_number);

        let mut ctx = BlockContext::default();

        let mut match_count: u64 = 0;
        let mut mismatch_count: u64 = 0;

        ctx.block_number = block_number.clone();

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
                match_count += 1;
            } else {
                mismatch_count += 1;
            }
        }

        Ok((ctx, match_count, mismatch_count))
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
    ) -> Result<(Result<(), ()>, u64), ()> {
        let mut remove_batch = sled::Batch::default();
        let mut current_values_batch = sled::Batch::default();

        let mut remove_count: u64 = 0;

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
            remove_count += 1;
        }

        let result: Result<(), ()> = match (
            db.apply_batch(remove_batch),
            consumed_ring.apply_batch(current_values_batch),
        ) {
            (Ok(()), Ok(())) => Ok(()),
            _ => Err(()),
        };

        Ok((result, remove_count))
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
    pub config: Config,

    pub ctx: Arc<Mutex<Context>>,

    pub input: InputPort<RawBlockPayload>,
    pub output: OutputPort<EnrichedBlockPayload>,

    #[metric]
    pub enrich_inserts: gasket::metrics::Counter,
    #[metric]
    pub enrich_blocks: gasket::metrics::Counter,
    #[metric]
    pub enrich_removes: gasket::metrics::Counter,
    #[metric]
    pub enrich_matches: gasket::metrics::Counter,
    #[metric]
    pub enrich_mismatches: gasket::metrics::Counter,
    #[metric]
    pub enrich_cancelled_empty_tx: gasket::metrics::Counter,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let sled = Worker {
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

        Ok(sled)
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
        let policy = stage.ctx.lock().await.error_policy.clone();

        match self.db_refs_all() {
            Ok(db_refs) => match db_refs {
                Some((db, consumed_ring)) => match unit {
                    model::RawBlockPayload::RollForwardGenesis => {
                        log::warn!("rolling forward genesis provided, working it");

                        let all = genesis_utxos(&stage.ctx.lock().await.genesis_file).clone();

                        for utxo in all {
                            self.insert_genesis_utxo(&db, &utxo, &stage.enrich_inserts)
                                .map_err(crate::Error::storage)
                                .apply_policy(&policy)
                                .or_panic()?;
                        }

                        stage
                            .output
                            .send(model::EnrichedBlockPayload::roll_forward_genesis(
                                genesis_utxos(&stage.ctx.lock().await.genesis_file),
                            ))
                            .await
                            .or_panic()?;

                        Ok(())
                    }

                    model::RawBlockPayload::RollForward(cbor) => {
                        let block = MultiEraBlock::decode(&cbor)
                            .map_err(crate::Error::cbor)
                            .apply_policy(&policy)
                            .or_panic()?;

                        let block = block.unwrap();

                        let txs = block.txs();

                        if txs.is_empty() {
                            stage.enrich_cancelled_empty_tx.inc(1);
                            return Ok(());
                        }

                        match self
                            .par_fetch_referenced_utxos(db, block.number(), &txs)
                            .apply_policy(&policy)
                            .or_restart()?
                        {
                            Some((ctx, match_count, mismatch_count)) => {
                                stage.enrich_matches.inc(match_count);
                                stage.enrich_mismatches.inc(mismatch_count);

                                let (_, removed_count) =
                                    self.remove_consumed_utxos(db, consumed_ring, &txs).unwrap(); // not handling error, todo

                                stage.enrich_removes.inc(removed_count);

                                stage.enrich_blocks.inc(1);

                                stage
                                    .output
                                    .send(model::EnrichedBlockPayload::roll_forward(
                                        cbor.clone(),
                                        ctx,
                                    ))
                                    .await
                                    .map_err(|_| WorkerError::Send)
                                    .or_panic()?;

                                match self
                                    .insert_produced_utxos(db, &txs, &stage.enrich_inserts)
                                    .map_err(crate::Error::storage)
                                    .apply_policy(&policy)
                                    .or_panic()?
                                {
                                    Some(_) => Ok(()),

                                    None => Err(WorkerError::Panic),
                                }
                            }
                            None => Err(WorkerError::Panic),
                        }
                    }

                    model::RawBlockPayload::RollBack(
                        cbor,
                        (last_known_point, last_known_block_number),
                    ) => {
                        log::warn!("rolling back");

                        let block = MultiEraBlock::decode(&cbor);

                        if let Ok(block) = block {
                            let txs = block.txs();

                            self.replace_consumed_utxos(db, consumed_ring, &txs)
                                .or_panic()?;

                            let (ctx, match_count, mismatch_count) = self
                                .par_fetch_referenced_utxos(
                                    db,
                                    last_known_block_number.clone(),
                                    &txs,
                                )
                                .or_panic()?;

                            stage.enrich_matches.inc(match_count);
                            stage.enrich_mismatches.inc(mismatch_count);

                            // Revert Anything to do with this block from consumed ring
                            self.remove_produced_utxos(db, &txs, &stage.enrich_removes)
                                .map_err(crate::Error::storage)
                                .apply_policy(&policy)
                                .or_panic()?;

                            stage
                                .output
                                .send(model::EnrichedBlockPayload::roll_back(
                                    cbor.clone(),
                                    ctx,
                                    (last_known_point.clone(), last_known_block_number.clone()),
                                ))
                                .await
                                .or_panic()?;
                        }

                        stage.enrich_blocks.inc(1);

                        Ok(())
                    }
                },

                None => Err(WorkerError::Retry),
            },

            Err(_) => Err(WorkerError::Retry),
        }
    }
}
