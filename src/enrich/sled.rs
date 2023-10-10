use gasket::framework::*;

use gasket::messaging::tokio::{InputPort, OutputPort};

use pallas::{
    codec::minicbor,
    ledger::traverse::{Era, MultiEraBlock, MultiEraTx, OutputRef},
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Deserialize;
use sled::IVec;

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
    pub fn bootstrapper(mut self, ctx: &pipeline::Context) -> Stage {
        self.rollback_db_path = Some(ctx.block.rollback_db_path.clone());

        Stage {
            config: self,
            input: Default::default(),
            output: Default::default(),
            inserts_counter: Default::default(),
            remove_counter: Default::default(),
            matches_counter: Default::default(),
            mismatches_counter: Default::default(),
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
    ) -> Result<u64, crate::Error> {
        let mut insert_batch = sled::Batch::default();

        let mut produced: u64 = 0;

        for tx in txs.iter() {
            for (idx, output) in tx.produces() {
                let key = format!("{}#{}", tx.hash(), idx);

                let era = tx.era().into();
                let body = output.encode();
                let value: IVec = SledTxValue(era, body).try_into()?;

                produced += 1;

                insert_batch.insert(key.as_bytes(), value)
            }
        }

        let batch_results = db.apply_batch(insert_batch).or_retry();

        batch_results.map_err(crate::Error::storage).unwrap();

        Ok(produced)
    }

    fn remove_produced_utxos(
        &self,
        db: &sled::Db,
        txs: &[MultiEraTx],
    ) -> Result<u64, crate::Error> {
        let mut remove_batch = sled::Batch::default();

        let mut removed_count: u64 = 0;

        for tx in txs.iter() {
            for (idx, _) in tx.produces() {
                removed_count += 1;
                remove_batch.remove(format!("{}#{}", tx.hash(), idx).as_bytes());
            }
        }

        db.apply_batch(remove_batch)
            .map_err(crate::Error::storage)
            .and(Ok(removed_count))
    }

    #[inline]
    fn par_fetch_referenced_utxos(
        &self,
        db: &sled::Db,
        block_number: u64,
        txs: &[MultiEraTx],
    ) -> Result<(BlockContext, u64, u64), crate::Error> {
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

    pub input: InputPort<RawBlockPayload>,
    pub output: OutputPort<EnrichedBlockPayload>,

    #[metric]
    pub inserts_counter: gasket::metrics::Counter,
    #[metric]
    pub blocks_counter: gasket::metrics::Counter,
    #[metric]
    pub remove_counter: gasket::metrics::Counter,
    #[metric]
    pub matches_counter: gasket::metrics::Counter,
    #[metric]
    pub mismatches_counter: gasket::metrics::Counter,
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
        let all_dbs = self.db_refs_all().unwrap();

        if let Some((db, consumed_ring)) = all_dbs {
            match unit {
                model::RawBlockPayload::RollForward(cbor) => {
                    let block = MultiEraBlock::decode(&cbor).unwrap();

                    let txs = &block.txs();
                    stage
                        .inserts_counter
                        .inc(self.insert_produced_utxos(db, txs).or_panic().unwrap());

                    let (ctx, match_count, mismatch_count) = self
                        .par_fetch_referenced_utxos(db, block.number(), &txs)
                        .or_panic()?;

                    stage.matches_counter.inc(match_count);
                    stage.mismatches_counter.inc(mismatch_count);

                    // and finally we remove utxos consumed by the block
                    let (_, removed_count) = self
                        .remove_consumed_utxos(db, consumed_ring, &txs)
                        .expect("todo panic");

                    stage.remove_counter.inc(removed_count);

                    stage.blocks_counter.inc(1);

                    stage
                        .output
                        .send(model::EnrichedBlockPayload::roll_forward(cbor.clone(), ctx))
                        .await
                        .or_panic()
                }

                model::RawBlockPayload::RollBack(cbor, (last_known_point, last_known_number)) => {
                    let block = MultiEraBlock::decode(&cbor);

                    if let Ok(block) = block {
                        let txs = block.txs();

                        // Revert Anything to do with this block
                        stage
                            .remove_counter
                            .inc(self.remove_produced_utxos(db, &txs).unwrap());

                        self.replace_consumed_utxos(db, consumed_ring, &txs)
                            .expect("todo: panic error");

                        let (ctx, match_count, mismatch_count) = self
                            .par_fetch_referenced_utxos(db, last_known_number.clone(), &txs)
                            .or_restart()?;

                        stage.matches_counter.inc(match_count);
                        stage.mismatches_counter.inc(mismatch_count);

                        return stage
                            .output
                            .send(model::EnrichedBlockPayload::roll_back(
                                cbor.clone(),
                                ctx,
                                (last_known_point.clone(), last_known_number.clone()),
                            ))
                            .await
                            .or_panic();
                    }

                    stage.blocks_counter.inc(1);

                    Ok(())
                }
            }
            .unwrap();
        }

        Ok(())
    }
}
