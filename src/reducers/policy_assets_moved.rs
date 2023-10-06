use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use std::str::FromStr;

use gasket::messaging::tokio::OutputPort;

use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::{MultiEraAsset, MultiEraBlock, MultiEraPolicyAssets};
use serde::Deserialize;

use crate::{crosscut, model};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub policy_ids_hex: Option<Vec<String>>,
}

fn asset_fingerprint(data_list: [&str; 2]) -> Result<String, bech32::Error> {
    let combined_parts = data_list.join("");
    let raw = hex::decode(combined_parts).unwrap();
    let mut hasher = Blake2bVar::new(20).unwrap();
    hasher.update(&raw);
    let mut buf = [0u8; 20];
    hasher.finalize_variable(&mut buf).unwrap();
    let base32_combined = buf.to_base32();
    bech32::encode("asset", base32_combined, Variant::Bech32)
}

pub struct Reducer {
    config: Config,
    chain: crosscut::ChainWellKnownInfo,
    policy_ids: Option<Vec<Hash<28>>>,
    time: crosscut::time::NaiveProvider,
}

impl Reducer {
    fn is_policy_id_accepted(&self, policy_id: &Hash<28>) -> bool {
        return match &self.policy_ids {
            Some(pids) => pids.contains(&policy_id),
            None => true,
        };
    }

    async fn process_asset(
        &mut self,
        policy: &Hash<28>,
        fingerprint: &str,
        timestamp: &str,
        output: &mut super::OutputPort<model::CRDTCommand>,
    ) -> Result<(), gasket::error::Error> {
        if !self.is_policy_id_accepted(&policy) {
            return Ok(());
        }

        let key = match &self.config.key_prefix {
            Some(prefix) => prefix.to_string(),
            None => "policy".to_string(),
        };

        let crdt = model::CRDTCommand::HashSetValue(
            format!("{}.{}", key, hex::encode(policy)),
            fingerprint.to_string(),
            timestamp.to_string().into(),
        );
        output.send(crdt.into()).await;

        Ok(())
    }

    pub async fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        output: &mut OutputPort<model::CRDTCommand>,
        error_policy: &crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            for (_, out) in tx.produces().iter() {
                for asset_group in out.non_ada_assets() {
                    for asset in asset_group.assets() {
                        let asset_name = hex::encode(asset.name());
                        let policy_hex = hex::encode(asset.policy());

                        if let Ok(fingerprint) =
                            asset_fingerprint([&policy_hex, asset_name.as_str()])
                        {
                            if !fingerprint.is_empty() {
                                self.process_asset(
                                    &asset.policy(),
                                    &fingerprint,
                                    &self.time.slot_to_wallclock(block.slot()).to_string(),
                                    output,
                                )
                                .await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, chain: crosscut::ChainWellKnownInfo) -> super::Reducer {
        let policy_ids: Option<Vec<Hash<28>>> = match &self.policy_ids_hex {
            Some(pids) => {
                let ps = pids
                    .iter()
                    .map(|pid| Hash::<28>::from_str(pid).expect("invalid policy_id"))
                    .collect();

                Some(ps)
            }
            None => None,
        };

        let reducer = Reducer {
            config: self,
            chain: chain.clone(),
            time: crosscut::time::NaiveProvider::new(chain.clone()),
            policy_ids,
        };

        super::Reducer::PolicyAssetsMoved(reducer)
    }
}
