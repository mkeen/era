use std::sync::Arc;

use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::primitives::alonzo::PolicyId;
use pallas::ledger::traverse::{MultiEraAsset, MultiEraOutput};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx, OutputRef};
use serde::{Deserialize, Serialize};

use gasket::messaging::tokio::{InputPort, OutputPort};
use tokio::sync::Mutex;

use crate::crosscut::policies::AppliesPolicy;
use crate::model::CRDTCommand;
use crate::{crosscut, model, prelude::*};

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub coin_key_prefix: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            key_prefix: Some("tx".to_string()),
            coin_key_prefix: Some("c".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct Reducer {
    config: Config,
}

// hash and index are stored in the key
#[derive(Deserialize, Serialize)]
pub struct DropKingMultiAssetUTXO {
    policy_id: String,
    name: String,
    quantity: u64,
    tx_address: String,
    fingerprint: String,
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

impl Reducer {
    fn stake_or_address_from_address(&self, address: &Address) -> String {
        match address {
            Address::Shelley(s) => match StakeAddress::try_from(s.clone()).ok() {
                Some(x) => x.to_bech32().unwrap_or(x.to_hex()),
                _ => address.to_bech32().unwrap_or(address.to_string()),
            },

            Address::Byron(_) => address.to_bech32().unwrap_or(address.to_string()),
            Address::Stake(stake) => stake.to_bech32().unwrap_or(address.to_string()),
        }
    }

    async fn tx_state(
        &mut self,
        output: &Arc<Mutex<OutputPort<CRDTCommand>>>,
        soa: &str,
        tx_str: &str,
        should_exist: bool,
    ) -> Option<()> {
        let mut out = output.lock().await;

        match should_exist {
            true => {
                let _ = out.send(
                    model::CRDTCommand::set_add(
                        self.config.key_prefix.as_deref(),
                        &soa,
                        tx_str.to_string(),
                    )
                    .into(),
                );
            }

            _ => {
                let _ = out.send(
                    model::CRDTCommand::set_remove(
                        self.config.key_prefix.as_deref(),
                        &soa,
                        tx_str.to_string(),
                    )
                    .into(),
                );
            }
        }

        Some(())
    }

    async fn coin_state(
        &mut self,
        output: &Arc<Mutex<OutputPort<CRDTCommand>>>,
        address: &str,
        tx_str: &str,
        lovelace_amt: &str,
        should_exist: bool,
    ) -> Option<()> {
        let mut out = output.lock().await;

        match should_exist {
            true => {
                out.send(
                    model::CRDTCommand::set_add(
                        self.config.coin_key_prefix.as_deref(),
                        tx_str,
                        format!("{}/{}", address, lovelace_amt),
                    )
                    .into(),
                )
                .await
                .unwrap();
            }

            _ => {
                out.send(
                    model::CRDTCommand::set_remove(
                        self.config.coin_key_prefix.as_deref(),
                        tx_str,
                        format!("{}/{}", address, lovelace_amt),
                    )
                    .into(),
                )
                .await
                .unwrap();
            }
        };

        Some(())
    }

    async fn token_state(
        &mut self,
        output: &Arc<Mutex<OutputPort<CRDTCommand>>>,
        address: &str,
        tx_str: &str,
        policy_id: &str,
        fingerprint: &str,
        quantity: &str,
        should_exist: bool,
    ) -> Result<(), gasket::error::Error> {
        let mut out = output.lock().await;

        match should_exist {
            true => out
                .send(
                    model::CRDTCommand::set_add(
                        self.config.key_prefix.as_deref(),
                        tx_str,
                        format!("{}/{}/{}/{}", address, policy_id, fingerprint, quantity),
                    )
                    .into(),
                )
                .await
                .unwrap(),

            _ => out
                .send(
                    model::CRDTCommand::set_remove(
                        self.config.key_prefix.as_deref(),
                        tx_str,
                        format!("{}/{}/{}/{}", address, policy_id, fingerprint, quantity),
                    )
                    .into(),
                )
                .await
                .unwrap(),
        };

        Ok(())
    }

    async fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutputRef,
        output: &Arc<Mutex<OutputPort<CRDTCommand>>>,
        rollback: bool,
        error_policy: &crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::error::Error> {
        let utxo = ctx
            .find_utxo(input)
            .apply_policy(error_policy)
            .or_panic()
            .unwrap();

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(()),
        };

        let address = utxo.address().map(|x| x.to_string()).unwrap();

        if let Ok(raw_address) = &utxo.address() {
            let soa = self.stake_or_address_from_address(raw_address);
            self.tx_state(
                output,
                soa.as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                rollback,
            )
            .await
            .unwrap();

            self.coin_state(
                output,
                raw_address
                    .to_bech32()
                    .unwrap_or(raw_address.to_string())
                    .as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                utxo.lovelace_amount().to_string().as_str(),
                rollback,
            )
            .await
            .unwrap();
        }

        // Spend Native Tokens
        for asset_group in utxo.non_ada_assets() {
            for asset in asset_group.assets() {
                if let MultiEraAsset::AlonzoCompatibleOutput(policy_id, asset_name, quantity) =
                    asset.clone()
                {
                    let asset_name = hex::encode(asset_name.to_vec());

                    if let Ok(fingerprint) =
                        asset_fingerprint([&hex::encode(policy_id), &asset_name])
                    {
                        // todo confirm this check is unneeded
                        if !fingerprint.is_empty() {
                            self.token_state(
                                output,
                                &address,
                                format!("{}#{}", input.hash(), input.index()).as_str(),
                                &hex::encode(policy_id),
                                &fingerprint,
                                quantity.to_string().as_str(),
                                rollback,
                            )
                            .await
                            .unwrap();
                        }
                    }
                };
            }
        }

        Ok(())
    }

    async fn process_produced_txo<'b>(
        &mut self,
        tx_hash: &Hash<32>,
        tx_output: &MultiEraOutput<'b>,
        output_idx: usize,
        output: &Arc<Mutex<OutputPort<CRDTCommand>>>,
        rollback: bool,
    ) -> Result<(), gasket::error::Error> {
        if let Ok(raw_address) = &tx_output.address() {
            let tx_address = raw_address.to_bech32().unwrap_or(raw_address.to_string());

            self.coin_state(
                output,
                tx_address.as_str(),
                &format!("{}#{}", tx_hash, output_idx),
                tx_output.lovelace_amount().to_string().as_str(),
                !rollback,
            )
            .await
            .unwrap();

            for asset_group in tx_output.non_ada_assets() {
                for asset in asset_group.assets() {
                    if let MultiEraAsset::AlonzoCompatibleOutput(policy_id, asset_name, quantity) =
                        asset
                    {
                        let asset_name = hex::encode(asset_name.to_vec());
                        let policy_id_str = hex::encode(policy_id);

                        if let Ok(fingerprint) =
                            asset_fingerprint([&policy_id_str, asset_name.as_str()])
                        {
                            if !fingerprint.is_empty() {
                                self.token_state(
                                    output,
                                    &tx_address,
                                    format!("{}#{}", tx_hash, output_idx).as_str(),
                                    &policy_id_str,
                                    &fingerprint,
                                    quantity.to_string().as_str(),
                                    !rollback,
                                )
                                .await
                                .unwrap();
                            }
                        }
                    };
                }
            }

            let soa = self.stake_or_address_from_address(raw_address);
            self.tx_state(
                output,
                soa.as_str(),
                &format!("{}#{}", tx_hash, output_idx),
                !rollback,
            )
            .await
            .unwrap();
        }

        Ok(())
    }

    pub async fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        rollback: bool,
        output: &Arc<Mutex<OutputPort<CRDTCommand>>>,
        error_policy: &crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs() {
            for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                self.process_consumed_txo(&ctx, &consumed, output, rollback, error_policy)
                    .await
                    .expect("TODO: panic message");
            }

            for (idx, produced) in tx.produces().iter() {
                self.process_produced_txo(&tx.hash(), &produced, idx.clone(), output, rollback)
                    .await
                    .expect("TODO: panic message");
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer { config: self };

        super::Reducer::UtxoByAddress(reducer)
    }
}
