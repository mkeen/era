use std::sync::Arc;

use gasket::messaging::tokio::OutputPort;
use pallas::codec::utils::CborWrap;
use pallas::crypto::hash::Hash;
use pallas::ledger::primitives::babbage::{PlutusData, PseudoDatumOption};
use pallas::ledger::primitives::Fragment;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};
use pallas::ledger::traverse::{MultiEraOutput, OriginalHash};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;

use crate::model::CRDTCommand;
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
}

pub fn resolve_datum(utxo: &MultiEraOutput, tx: &MultiEraTx) -> Result<PlutusData, ()> {
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
    fn get_key_value(
        &self,
        utxo: &MultiEraOutput,
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
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        rollback: bool,
        output: &Arc<Mutex<OutputPort<CRDTCommand>>>,
        error_policy: &crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::error::Error> {
        if rollback {
            return Ok(());
        }

        let prefix = &self.config.key_prefix.clone().unwrap_or("tx".to_string());

        let out = &mut output.lock().await;

        for tx in block.txs() {
            for consumed in tx.consumes() {
                let output_ref = consumed.output_ref();

                if let Ok(utxo) = ctx.find_utxo(&output_ref) {
                    if let Some((key, value)) = self.get_key_value(
                        &utxo,
                        &tx,
                        &(output_ref.hash().clone(), output_ref.index().clone()),
                    ) {
                        out.send(
                            model::CRDTCommand::set_remove(Some(prefix), &key.as_str(), value)
                                .into(),
                        )
                        .await
                        .unwrap();
                    }
                }
            }

            for (index, produced) in tx.produces() {
                let output_ref = (tx.hash().clone(), index as u64);
                if let Some((key, value)) = self.get_key_value(&produced, &tx, &output_ref) {
                    log::warn!("i see a tx {:?}", prefix);
                    out.send(model::CRDTCommand::set_add(Some(prefix), &key, value).into())
                        .await
                        .unwrap();
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer { config: self };

        super::Reducer::UtxoOwners(reducer)
    }
}
