use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::traverse::MultiEraOutput;
use pallas::ledger::traverse::{Asset, MultiEraBlock};
use serde::Deserialize;

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
    fn to_string_output(&self, asset: Asset) -> Option<String> {
        if let Some(policy_id) = asset.policy_hex() {
            if policy_id.eq(self.config.policy_id.clone().unwrap().as_str()) {
                if let Asset::NativeAsset(_, name, _) = asset {
                    return String::from_utf8(name).ok();
                }
            }
        }

        None
    }

    pub fn process_txo(
        &self,
        txo: &MultiEraOutput,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let asset_names: Vec<_> = txo
            .non_ada_assets()
            .into_iter()
            .filter_map(|x| self.to_string_output(x))
            .collect();

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
            output.send(
                model::CRDTCommand::any_write_wins(
                    Some(self.config.key_prefix.clone().unwrap_or_default().as_str()),
                    asset.clone(),
                    soa.to_string(),
                )
                .into(),
            )?;

            output.send(
                model::CRDTCommand::any_write_wins(
                    Some(self.config.key_prefix.clone().unwrap_or_default().as_str()),
                    soa.to_string(),
                    asset,
                )
                .into(),
            )?;
        }

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        rollback: bool,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().iter() {
            if rollback {
                for input in tx.consumes() {
                    if let Ok(txo) = ctx.find_utxo(&input.output_ref()) {
                        self.process_txo(&txo, output)?;
                    }
                }
            } else {
                for (_, txo) in tx.produces() {
                    self.process_txo(&txo, output)?;
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, chain: &crosscut::ChainWellKnownInfo) -> super::Reducer {
        let reducer = Reducer {
            config: Self {
                key_prefix: self.key_prefix,
                policy_id: Some(chain.adahandle_policy.clone()),
            },
        };
        super::Reducer::AdaHandle(reducer)
    }
}
