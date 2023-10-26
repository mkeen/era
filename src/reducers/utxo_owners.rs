use std::sync::Arc;

use gasket::framework::{AsWorkError, WorkerError};
use gasket::messaging::tokio::OutputPort;
use pallas::codec::utils::CborWrap;
use pallas::crypto::hash::Hash;
use pallas::ledger::configs::byron::GenesisUtxo;
use pallas::ledger::primitives::babbage::{PlutusData, PseudoDatumOption};
use pallas::ledger::primitives::Fragment;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};
use pallas::ledger::traverse::{MultiEraOutput, OriginalHash};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;

use crate::model::{BlockOrigination, CRDTCommand};
use crate::pipeline::Context;
use crate::{crosscut, model};

use super::utils::AssetFingerprint;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub coin_key_prefix: Option<String>,
    pub address_as_key: Option<bool>,
}

#[derive(Clone)]
pub struct Reducer {
    config: Config,
    ctx: Arc<Mutex<Context>>,
}

pub fn resolve_datum(utxo: &BlockOrigination, tx: &MultiEraTx) -> Result<PlutusData, ()> {
    match utxo.datum() {
        Some(PseudoDatumOption::Data(CborWrap(pd))) => Ok(pd.unwrap()),
        Some(PseudoDatumOption::Hash(datum_hash)) => {
            for raw_datum in tx.clone().plutus_data() {
                if raw_datum.original_hash().eq(&datum_hash) {
                    return Ok(raw_datum.clone().unwrap());
                }
            }

            return Err(());
        }
        _ => Err(()),
    }
}

impl Reducer {
    fn get_key_value<'b>(
        &self,
        utxo: &'b BlockOrigination<'b>,
        tx: &MultiEraTx,
        output_ref: &(Hash<32>, u64),
    ) -> Option<(String, String)> {
        if let Some(address) = utxo.address().map(|addr| addr.to_string()).ok() {
            let mut data = serde_json::Value::Object(serde_json::Map::new());
            let address_as_key = self.config.address_as_key.unwrap_or(false);
            let key: String;

            if address_as_key {
                key = address;
                data["tx_hash"] = serde_json::Value::String(hex::encode(output_ref.0.to_vec()));
                data["output_index"] =
                    serde_json::Value::from(serde_json::Number::from(output_ref.1));
            } else {
                key = format!("{}#{}", hex::encode(output_ref.0.to_vec()), output_ref.1); // this should be good
                data["address"] = serde_json::Value::String(address);
            }

            if let Some(datum) = resolve_datum(utxo, tx).ok() {
                data["datum"] =
                    serde_json::Value::String(hex::encode(datum.encode_fragment().ok().unwrap()));
            } else if let Some(PseudoDatumOption::Hash(h)) = utxo.datum() {
                data["datum_hash"] = serde_json::Value::String(hex::encode(h.to_vec()));
            }

            let mut assets: Vec<serde_json::Value> = Vec::new();

            assets.push(json!({
                "unit": "lovelace",
                "quantity": format!("{}", utxo.lovelace_amount())
            }));

            for policy_group in utxo.non_ada_assets() {
                for asset in policy_group.assets() {
                    let fingerprint = AssetFingerprint::from_parts(
                        &hex::encode(asset.policy()),
                        &hex::encode(asset.name()),
                    )
                    .unwrap()
                    .fingerprint()
                    .unwrap();

                    assets.push(json!({
                        "unit": fingerprint,
                        "quantity": format!("{}", asset.output_coin().unwrap())
                    }));
                }
            }

            data["amount"] = serde_json::Value::Array(assets);
            return Some((key, data.to_string()));
        }

        None
    }

    pub async fn reduce<'b>(
        &mut self,
        block: Option<MultiEraBlock<'b>>,
        block_ctx: Option<model::BlockContext>,
        genesis_utxos: Option<Vec<GenesisUtxo>>,
        genesis_hash: Option<Hash<32>>,
        rollback: bool,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
    ) -> Result<(), gasket::framework::WorkerError> {
        if rollback {
            return Ok(());
        }

        let prefix = &self.config.key_prefix.clone().unwrap_or("tx".to_string());

        match (block, block_ctx, genesis_utxos, genesis_hash) {
            (Some(block), Some(block_ctx), None, None) => {
                for tx in block.txs() {
                    for consumed in tx.consumes() {
                        let output_ref = consumed.output_ref();

                        if let Ok(utxo) = block_ctx.find_utxo(&output_ref) {
                            if let Some((key, value)) = self.get_key_value(
                                &utxo,
                                &tx,
                                &(output_ref.hash().clone(), output_ref.index().clone()),
                            ) {
                                output
                                    .lock()
                                    .await
                                    .send(
                                        model::CRDTCommand::set_remove(
                                            Some(prefix),
                                            &key.as_str(),
                                            value,
                                        )
                                        .into(),
                                    )
                                    .await
                                    .map_err(|_| WorkerError::Send)
                                    .or_panic()?;
                            }
                        }
                    }

                    for (index, produced) in tx.produces() {
                        let output_ref = (tx.hash().clone(), index as u64);
                        let block_txo = BlockOrigination::Chain(produced);
                    }
                }

                Ok(())
            }

            (None, None, Some(genesis_utxos), Some(genesis_hash)) => {
                for (i, utxo) in genesis_utxos.iter().enumerate() {
                    output
                        .lock()
                        .await
                        .send(
                            model::CRDTCommand::set_add(
                                Some(prefix),
                                format!("{}#{}", hex::encode(genesis_hash.to_vec()), i).as_str(),
                                "This is unused".to_string(),
                            )
                            .into(),
                        )
                        .await
                        .map_err(|e| WorkerError::Send)
                        .or_panic()?;
                }

                Ok(())
            }

            _ => Err(WorkerError::Panic),
        }
    }
}

impl Config {
    pub fn plugin(self, ctx: Arc<Mutex<Context>>) -> super::Reducer {
        let reducer = Reducer { config: self, ctx };

        super::Reducer::UtxoOwners(reducer)
    }
}
