use std::ops::Deref;

use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

use pallas::codec::utils::KeyValuePairs;
use pallas::ledger::primitives::alonzo::{Metadata, Metadatum, MetadatumLabel};
use pallas::ledger::primitives::Fragment;
use pallas::ledger::traverse::{assets, MultiEraBlock, MultiEraTx};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use hex::{self};

use crate::model::{CRDTCommand, Delta};
use crate::{crosscut, model};

#[derive(Copy, Clone, Deserialize, Serialize)]
pub enum Projection {
    Cbor,
    Json,
}

impl Default for Projection {
    fn default() -> Self {
        Self::Json
    }
}

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub historical_metadata: Option<bool>,
    pub policy_asset_index: Option<bool>,
    pub royalty_metadata: Option<bool>,
    pub projection: Option<Projection>,
    pub filter: Option<crosscut::filters::Predicate>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    time: crosscut::time::NaiveProvider,
}

const CIP25_META_NFT: u64 = 721;
const CIP27_META_ROYALTIES: u64 = 777;

#[inline]
fn kv_pairs_to_hashmap(
    kv_pairs: &KeyValuePairs<Metadatum, Metadatum>,
) -> serde_json::Map<String, Value> {
    #[inline]
    fn metadatum_to_value(m: &Metadatum) -> Value {
        match m {
            Metadatum::Int(int_value) => Value::String(int_value.to_string()),
            Metadatum::Bytes(bytes) => Value::String(hex::encode(bytes.as_slice())),
            Metadatum::Text(text) => Value::String(text.clone()),
            Metadatum::Array(array) => {
                let json_array: Vec<Value> = array.iter().map(metadatum_to_value).collect();
                Value::Array(json_array)
            }
            Metadatum::Map(kv_pairs) => {
                let json_object = kv_pairs_to_hashmap(kv_pairs);
                Value::Object(json_object)
            }
        }
    }

    let mut hashmap = serde_json::Map::new();
    for (key, value) in kv_pairs.deref() {
        if let Metadatum::Text(key_str) = key {
            hashmap.insert(key_str.clone(), metadatum_to_value(value));
        }
    }

    hashmap
}

impl Reducer {
    fn find_metadata_policy_assets(
        &self,
        metadata: &Metadatum,
        target_policy_id: &str,
    ) -> Option<KeyValuePairs<Metadatum, Metadatum>> {
        match metadata {
            Metadatum::Map(kv) => {
                for (policy_label, policy_contents) in kv.iter() {
                    if let Metadatum::Text(policy_label) = policy_label {
                        if policy_label == target_policy_id {
                            if let Metadatum::Map(policy_inner_map) = policy_contents {
                                return Some(policy_inner_map.clone());
                            }
                        }
                    }
                }

                None
            }
            _ => None,
        }
    }

    fn asset_fingerprint(&self, data_list: [&str; 2]) -> Result<String, bech32::Error> {
        let combined_parts = data_list.join("");
        let raw = hex::decode(combined_parts).unwrap();

        let mut hasher = Blake2bVar::new(20).unwrap();
        hasher.update(&raw);
        let mut buf = [0u8; 20];
        hasher.finalize_variable(&mut buf).unwrap();
        let base32_combined = buf.to_base32();
        bech32::encode("asset", base32_combined, Variant::Bech32)
    }

    fn get_asset_label(&self, l: Metadatum) -> Result<String, &str> {
        match l {
            Metadatum::Text(l) => Ok(l),
            Metadatum::Int(l) => Ok(l.to_string()),
            Metadatum::Bytes(l) => Ok(String::from_utf8(l.to_vec())
                .unwrap_or_default()
                .to_string()),
            _ => Err("Malformed metadata"),
        }
    }

    fn get_wrapped_metadata_fragment(
        &self,
        cip: u64,
        asset_name: String,
        policy_id: String,
        asset_metadata: &KeyValuePairs<Metadatum, Metadatum>,
    ) -> Metadata {
        let asset_map = KeyValuePairs::from(vec![
            (
                Metadatum::Text(asset_name),
                Metadatum::Map(asset_metadata.clone())
            );
            1
        ]);

        let policy_map = KeyValuePairs::from(vec![(
            Metadatum::Text(policy_id.clone()),
            Metadatum::Map(asset_map),
        )]);

        let meta_wrapper = vec![(
            MetadatumLabel::from(cip),
            Metadatum::Map(policy_map.clone()),
        )];

        Metadata::from(meta_wrapper)
    }

    fn get_metadata_fragment(
        &self,
        asset_name: String,
        policy_id: String,
        asset_metadata: &KeyValuePairs<Metadatum, Metadatum>,
        cip: u64,
    ) -> String {
        let mut std_wrap_map = serde_json::Map::new();
        let mut policy_wrap_map = serde_json::Map::new();
        let mut asset_wrap_map = serde_json::Map::new();
        let asset_map = kv_pairs_to_hashmap(asset_metadata);

        asset_wrap_map.insert(asset_name, Value::Object(asset_map));

        if cip == CIP27_META_ROYALTIES {
            std_wrap_map.insert(cip.to_string(), Value::Object(asset_wrap_map));
        } else if cip == CIP25_META_NFT {
            policy_wrap_map.insert(policy_id, Value::Object(asset_wrap_map));
            std_wrap_map.insert(cip.to_string(), Value::Object(policy_wrap_map));
        }

        serde_json::to_string(&std_wrap_map).unwrap()
    }

    fn extract_and_aggregate_cip_metadata(
        &self,
        output: &mut super::OutputPort<()>,
        cip: u64,
        policy_map: &Metadatum,
        policy_id_str: String,
        asset_name_str: String,
        slot_no: u64,
        rollback: bool,
    ) {
        let prefix = self.config.key_prefix.as_deref().unwrap_or("m");
        let projection = self.config.projection.unwrap_or_default();
        let should_keep_asset_index = self.config.policy_asset_index.unwrap_or(false);
        let should_keep_historical_metadata = self.config.historical_metadata.unwrap_or(false);
        let should_store_royalty_metadata = self.config.royalty_metadata.unwrap_or(true);

        if let Some(policy_assets) = self.find_metadata_policy_assets(&policy_map, &policy_id_str) {
            let filtered_policy_assets = policy_assets.iter().find(|(l, _)| {
                let asset_label = self.get_asset_label(l.clone()).unwrap();
                asset_label.as_str() == asset_name_str
            });

            if let Some((_, Metadatum::Map(asset_metadata))) = filtered_policy_assets {
                if let Ok(fingerprint_str) = self.asset_fingerprint([
                    &policy_id_str.clone(),
                    hex::encode(&asset_name_str).as_str(),
                ]) {
                    let timestamp = self.time.slot_to_wallclock(slot_no);
                    let metadata_final: Metadata = self.get_wrapped_metadata_fragment(
                        cip,
                        asset_name_str.clone(),
                        policy_id_str.clone(),
                        asset_metadata,
                    );

                    let meta_payload = match projection {
                        Projection::Json => self.get_metadata_fragment(
                            asset_name_str,
                            policy_id_str.clone(),
                            asset_metadata,
                            cip,
                        ),

                        Projection::Cbor => {
                            let cbor_enc = metadata_final.encode_fragment().unwrap_or(vec![]);
                            String::from_utf8(cbor_enc).unwrap_or("".to_string())
                        }
                    };

                    if !meta_payload.is_empty() {
                        if should_store_royalty_metadata && cip == CIP27_META_ROYALTIES {
                            if should_keep_historical_metadata {
                                if !rollback {
                                    let _ = output.send(
                                        CRDTCommand::LastWriteWins(
                                            format!("{}.r.{}", prefix, policy_id_str.clone()),
                                            meta_payload.clone().into(),
                                            timestamp,
                                        )
                                        .into(),
                                    );
                                } else {
                                    let _ = output.send(
                                        CRDTCommand::SortedSetRemove(
                                            format!("{}.r.{}", prefix, policy_id_str.clone()),
                                            meta_payload.clone().into(),
                                            Delta::from(timestamp as i64),
                                        )
                                        .into(),
                                    );
                                }
                            } else {
                                if !rollback {
                                    let _ = output.send(
                                        CRDTCommand::AnyWriteWins(
                                            format!("{}.r.{}", prefix, policy_id_str.clone()),
                                            meta_payload.clone().into(),
                                        )
                                        .into(),
                                    );
                                } else {
                                    // output.send(CRDTCommand::UnsetKey(
                                    //     format!("{}.r.{}", prefix, policy_id_str.clone())
                                    // ).into());
                                }
                            }
                        } else {
                            if should_keep_historical_metadata {
                                if !rollback {
                                    let _ = output.send(
                                        CRDTCommand::LastWriteWins(
                                            format!("{}.{}", prefix, fingerprint_str.clone()),
                                            meta_payload.clone().into(),
                                            timestamp,
                                        )
                                        .into(),
                                    );
                                } else {
                                    let _ = output.send(
                                        CRDTCommand::SortedSetRemove(
                                            format!("{}.{}", prefix, fingerprint_str.clone()),
                                            meta_payload.clone().into(),
                                            timestamp as i64,
                                        )
                                        .into(),
                                    );
                                }
                            } else {
                                // Can't roll this back with the way it currently works
                                let _ = output.send(
                                    CRDTCommand::AnyWriteWins(
                                        format!("{}.{}", prefix, fingerprint_str.clone()),
                                        model::Value::String(meta_payload.clone()),
                                    )
                                    .into(),
                                );
                            };

                            if should_keep_asset_index {
                                let _ = output.send(
                                    CRDTCommand::LastWriteWins(
                                        format!("{}.{}", prefix, policy_id_str),
                                        fingerprint_str.clone().into(),
                                        timestamp,
                                    )
                                    .into(),
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    fn send(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        rollback: bool,
        output: &mut super::OutputPort<()>,
    ) -> Result<(), gasket::error::Error> {
        let metadata = tx.metadata();

        for asset_group in tx.mints() {
            for multi_asset in asset_group.assets() {
                let policy_id_str = hex::encode(multi_asset.policy());
                if let Some(quantity) = multi_asset.mint_coin() {
                    if let Ok(asset_name_str) = String::from_utf8(multi_asset.name().to_vec()) {
                        if !policy_id_str.is_empty() {
                            for supported_metadata_cip in vec![CIP25_META_NFT, CIP27_META_ROYALTIES]
                            {
                                if let Some(policy_map) =
                                    metadata.find(MetadatumLabel::from(supported_metadata_cip))
                                {
                                    if quantity > -1 {
                                        self.extract_and_aggregate_cip_metadata(
                                            output,
                                            supported_metadata_cip,
                                            policy_map,
                                            policy_id_str.to_owned(),
                                            asset_name_str.to_owned(),
                                            block.slot().to_owned(),
                                            rollback,
                                        )
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        rollback: bool,
        output: &mut super::OutputPort<()>,
    ) -> Result<(), gasket::error::Error> {
        for tx in &block.txs() {
            if tx.metadata().as_alonzo().iter().any(|meta| {
                meta.iter()
                    .any(|(key, _)| *key == CIP25_META_NFT || *key == CIP27_META_ROYALTIES)
            }) {
                self.send(block, tx, rollback, output)?;
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(
        self,
        chain: &crosscut::ChainWellKnownInfo,
        policy: &crosscut::policies::RuntimePolicy,
    ) -> super::Reducer {
        let worker = Reducer {
            config: self,
            policy: policy.clone(),
            time: crosscut::time::NaiveProvider::new(chain.clone()),
        };

        super::Reducer::AssetMetadata(worker)
    }
}
