use std::sync::Arc;

use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::traverse::{MultiEraAsset, MultiEraBlock};
use serde::Deserialize;

use gasket::messaging::tokio::OutputPort;
use tokio::sync::Mutex;

use crate::model::CRDTCommand;
use crate::{crosscut, model};

use super::utils::AssetFingerprint;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub policy_id: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            key_prefix: Some(String::from("h")),
            policy_id: None,
        }
    }
}

#[derive(Clone)]
pub struct Reducer {
    config: Config,
}

impl Reducer {
    fn to_string_output(&self, asset: MultiEraAsset) -> Option<String> {
        let policy_id = hex::encode(asset.policy());

        if policy_id.eq(self.config.policy_id.clone().unwrap().as_str()) {
            if let MultiEraAsset::AlonzoCompatibleOutput(_, name, _) = asset {
                return match std::str::from_utf8(name) {
                    Ok(a) => Some(a.to_string()),
                    Err(_) => None,
                };
            }
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
        let mut out = output.lock().await;

        for tx in block.txs().iter() {
            if rollback {
                for input in tx.consumes() {
                    if let Ok(txo) = ctx.find_utxo(&input.output_ref()) {
                        let mut asset_names: Vec<String> = vec![];

                        for asset_list in txo.non_ada_assets() {
                            for asset in asset_list.assets() {
                                asset_names.push(hex::encode(asset.name()).to_string())
                            }
                        }

                        if asset_names.is_empty() {
                            return Ok(());
                        }

                        let address = &(txo.address().unwrap());
                        let soa = match address {
                            Address::Shelley(s) => match StakeAddress::try_from(s.clone()).ok() {
                                Some(x) => x.to_bech32().unwrap_or(x.to_hex()),
                                _ => address.to_bech32().unwrap_or(address.to_string()),
                            },

                            Address::Byron(_) => address.to_bech32().unwrap_or(address.to_string()),
                            Address::Stake(stake) => {
                                stake.to_bech32().unwrap_or(address.to_string())
                            }
                        };

                        for asset in asset_names {
                            out.send(
                                model::CRDTCommand::any_write_wins(
                                    Some(
                                        self.config.key_prefix.clone().unwrap_or_default().as_str(),
                                    ),
                                    asset.clone(),
                                    soa.to_string(),
                                )
                                .into(),
                            )
                            .await
                            .unwrap();

                            out.send(
                                model::CRDTCommand::any_write_wins(
                                    Some(
                                        self.config.key_prefix.clone().unwrap_or_default().as_str(),
                                    ),
                                    soa.to_string(),
                                    asset,
                                )
                                .into(),
                            )
                            .await
                            .unwrap();
                        }
                    }
                }
            } else {
                for (_, txo) in tx.produces() {
                    let mut asset_names: Vec<String> = vec![];

                    for asset_list in txo.non_ada_assets() {
                        for asset in asset_list.assets() {
                            match String::from_utf8(asset.name().to_vec()) {
                                Ok(asset_name) => asset_names.push(asset_name),
                                Err(_) => log::warn!(
                                    "could not parse asset name {} not a valid ada handle?",
                                    AssetFingerprint::from_parts(
                                        hex::encode(asset.name()).as_str(),
                                        hex::encode(asset.policy()).as_str()
                                    )
                                    .unwrap()
                                    .fingerprint()
                                    .unwrap()
                                    .as_str()
                                ),
                            };
                        }
                    }

                    if asset_names.is_empty() {
                        return Ok(());
                    }

                    let address = &(txo.address().unwrap());
                    let soa = match address {
                        Address::Shelley(s) => match StakeAddress::try_from(s.clone()).ok() {
                            Some(x) => x.to_bech32().unwrap_or(x.to_hex()),
                            _ => address.to_bech32().unwrap_or(address.to_string()),
                        },

                        Address::Byron(_) => address.to_bech32().unwrap_or(address.to_string()),
                        Address::Stake(stake) => stake.to_bech32().unwrap_or(address.to_string()),
                    };

                    for asset in asset_names {
                        out.send(
                            model::CRDTCommand::any_write_wins(
                                Some(self.config.key_prefix.clone().unwrap_or_default().as_str()),
                                asset.clone(),
                                soa.to_string(),
                            )
                            .into(),
                        )
                        .await
                        .unwrap();

                        out.send(
                            model::CRDTCommand::any_write_wins(
                                Some(self.config.key_prefix.clone().unwrap_or_default().as_str()),
                                soa.to_string(),
                                asset,
                            )
                            .into(),
                        )
                        .await
                        .unwrap();
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
            config: Self {
                key_prefix: self.key_prefix,
                policy_id: Some(chain.adahandle_policy),
            },
        };
        super::Reducer::Handle(reducer)
    }
}