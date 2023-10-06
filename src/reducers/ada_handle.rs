use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{MultiEraAsset, MultiEraBlock};
use serde::Deserialize;

use gasket::messaging::tokio::OutputPort;

use crate::model::CRDTCommand;
use crate::{crosscut, model};

#[derive(Deserialize)]
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

pub struct Reducer {
    config: Config,
}

impl Reducer {
    fn to_string_output(&self, asset: MultiEraAsset) -> Option<String> {
        if let policy_id = hex::encode(asset.policy()) {
            if policy_id.eq(self.config.policy_id.clone().unwrap().as_str()) {
                if let MultiEraAsset::AlonzoCompatibleOutput(_, name, _) = asset {
                    return match std::str::from_utf8(name) {
                        Ok(a) => Some(a.to_string()),
                        Err(_) => None,
                    };
                }
            }
        }

        None
    }

    pub async fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        rollback: bool,
        output: &mut OutputPort<CRDTCommand>,
        error_policy: &crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().iter() {
            if rollback {
                for input in tx.consumes() {
                    if let Ok(txo) = ctx.find_utxo(&input.output_ref()) {
                        let mut asset_names: Vec<String> = vec![];

                        for asset_list in txo.non_ada_assets() {
                            for asset in asset_list.assets() {
                                asset_names
                                    .push(std::str::from_utf8(asset.name()).unwrap().to_string())
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
                            output
                                .send(
                                    model::CRDTCommand::any_write_wins(
                                        Some(
                                            self.config
                                                .key_prefix
                                                .clone()
                                                .unwrap_or_default()
                                                .as_str(),
                                        ),
                                        asset.clone(),
                                        soa.to_string(),
                                    )
                                    .into(),
                                )
                                .await;

                            output
                                .send(
                                    model::CRDTCommand::any_write_wins(
                                        Some(
                                            self.config
                                                .key_prefix
                                                .clone()
                                                .unwrap_or_default()
                                                .as_str(),
                                        ),
                                        soa.to_string(),
                                        asset,
                                    )
                                    .into(),
                                )
                                .await;
                        }
                    }
                }
            } else {
                for (_, txo) in tx.produces() {
                    let mut asset_names: Vec<String> = vec![];

                    for asset_list in txo.non_ada_assets() {
                        for asset in asset_list.assets() {
                            asset_names.push(std::str::from_utf8(asset.name()).unwrap().to_string())
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
                        output
                            .send(
                                model::CRDTCommand::any_write_wins(
                                    Some(
                                        self.config.key_prefix.clone().unwrap_or_default().as_str(),
                                    ),
                                    asset.clone(),
                                    soa.to_string(),
                                )
                                .into(),
                            )
                            .await;

                        output
                            .send(
                                model::CRDTCommand::any_write_wins(
                                    Some(
                                        self.config.key_prefix.clone().unwrap_or_default().as_str(),
                                    ),
                                    soa.to_string(),
                                    asset,
                                )
                                .into(),
                            )
                            .await;
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
        super::Reducer::AdaHandle(reducer)
    }
}
