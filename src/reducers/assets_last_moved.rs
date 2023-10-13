use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use std::sync::Arc;
use tokio::sync::Mutex;

use gasket::messaging::tokio::OutputPort;

use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use crate::model::CRDTCommand;
use crate::{crosscut, model};

#[derive(Deserialize, Clone)]
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

#[derive(Clone)]
pub struct Reducer {
    config: Config,
    time: crosscut::time::NaiveProvider,
}

impl Reducer {
    async fn process_asset(
        &mut self,
        policy: &Hash<28>,
        fingerprint: &str,
        timestamp: &str,
        output: &mut super::OutputPort<model::CRDTCommand>,
    ) -> Result<(), gasket::error::Error> {
        let key = match &self.config.key_prefix {
            Some(prefix) => prefix.to_string(),
            None => "policy".to_string(),
        };

        let crdt = model::CRDTCommand::HashSetValue(
            format!("{}.{}", key, hex::encode(policy)),
            fingerprint.to_string(),
            timestamp.to_string().into(),
        );

        output.send(crdt.into()).await?;

        Ok(())
    }

    pub async fn reduce<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        output: &Arc<Mutex<OutputPort<CRDTCommand>>>,
        error_policy: &crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::error::Error> {
        let out = &mut output.lock().await;

        for tx in block.txs().into_iter() {
            for (_, outp) in tx.produces().iter() {
                for asset_group in outp.non_ada_assets() {
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
                                    out,
                                )
                                .await?;
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
        let reducer = Reducer {
            config: self,
            time: crosscut::time::NaiveProvider::new(chain.clone()),
        };

        super::Reducer::AssetsLastMoved(reducer)
    }
}
