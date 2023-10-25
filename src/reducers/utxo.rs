use std::collections::HashMap;
use std::sync::Arc;

use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::configs::byron::GenesisUtxo;
use pallas::ledger::traverse::{MultiEraAsset, MultiEraOutput};
use pallas::ledger::traverse::{MultiEraBlock, OutputRef};
use serde::{Deserialize, Serialize};

use gasket::messaging::tokio::OutputPort;
use tokio::sync::Mutex;

use crate::model::CRDTCommand;
use crate::pipeline::Context;
use crate::{crosscut, model, prelude::*};

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub coin_key_prefix: Option<String>,
    pub datum_key_prefix: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            key_prefix: Some("tx".to_string()),
            coin_key_prefix: Some("c".to_string()),
            datum_key_prefix: Some("d".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct Reducer {
    config: Config,
    ctx: Arc<Mutex<Context>>,
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
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        soa: &str,
        tx_str: &str,
        should_exist: bool,
    ) -> Result<(), gasket::framework::WorkerError> {
        output
            .lock()
            .await
            .send(gasket::messaging::Message::from(match should_exist {
                true => CRDTCommand::set_add(
                    self.config.key_prefix.clone().as_deref(),
                    &soa,
                    tx_str.to_string(),
                ),

                false => CRDTCommand::set_remove(
                    self.config.key_prefix.clone().as_deref(),
                    &soa,
                    tx_str.to_string(),
                ),
            }))
            .await
            .or_panic()
    }

    async fn coin_state(
        &mut self,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        address: &str,
        tx_str: &str,
        lovelace_amt: &str,
        should_exist: bool,
    ) -> Result<(), gasket::framework::WorkerError> {
        output
            .lock()
            .await
            .send(gasket::messaging::Message::from(match should_exist {
                true => CRDTCommand::set_add(
                    self.config.coin_key_prefix.clone().as_deref(),
                    tx_str,
                    format!("{}/{}", address, lovelace_amt),
                ),

                false => CRDTCommand::set_remove(
                    self.config.coin_key_prefix.clone().as_deref(),
                    tx_str,
                    format!("{}/{}", address, lovelace_amt),
                ),
            }))
            .await
            .or_panic()
    }

    async fn token_state(
        &mut self,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        address: &str,
        tx_str: &str,
        policy_id: &str,
        fingerprint: &str,
        quantity: &str,
        should_exist: bool,
    ) -> Result<(), gasket::framework::WorkerError> {
        output
            .lock()
            .await
            .send(gasket::messaging::Message::from(match should_exist {
                true => CRDTCommand::set_add(
                    self.config.key_prefix.clone().as_deref(),
                    tx_str,
                    format!("{}/{}/{}/{}", address, policy_id, fingerprint, quantity),
                ),

                _ => CRDTCommand::set_remove(
                    self.config.key_prefix.clone().as_deref(),
                    tx_str,
                    format!("{}/{}/{}/{}", address, policy_id, fingerprint, quantity),
                ),
            }))
            .await
            .or_panic()
    }

    async fn datum_state<'b>(
        &mut self,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        address: &str,
        tx_str: &str,
        utxo: &'b MultiEraOutput<'b>,
        should_exist: bool,
    ) -> Result<(), gasket::framework::WorkerError> {
        match utxo.datum() {
            Some(datum) => match datum {
                pallas::ledger::primitives::babbage::PseudoDatumOption::Data(datum) => {
                    let raw_cbor_bytes: &[u8] = datum.0.raw_cbor();

                    output
                        .lock()
                        .await
                        .send(gasket::messaging::Message::from(match should_exist {
                            true => CRDTCommand::set_add(
                                self.config.datum_key_prefix.clone().as_deref(),
                                tx_str,
                                format!("{}/{}", address, hex::encode(raw_cbor_bytes)),
                            ),
                            false => CRDTCommand::set_remove(
                                self.config.datum_key_prefix.clone().as_deref(),
                                tx_str,
                                format!("{}/{}", address, hex::encode(raw_cbor_bytes)),
                            ),
                        }))
                        .await
                        .or_panic()?;
                }

                _ => {}
            },
            None => {}
        }

        Ok(())
    }

    async fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutputRef,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        rollback: bool,
        error_policy: &crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::framework::WorkerError> {
        let utxo = ctx.find_utxo(input).apply_policy(error_policy).or_panic()?;

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(()),
        };

        let address = utxo.address().map(|x| x.to_string()).unwrap();

        if let Ok(raw_address) = &utxo.address() {
            let soa = self.stake_or_address_from_address(raw_address);
            self.tx_state(
                output.clone(),
                soa.as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                rollback,
            )
            .await?;

            self.datum_state(
                output.clone(),
                soa.as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                &utxo,
                rollback,
            )
            .await?;

            self.coin_state(
                output.clone(),
                raw_address
                    .to_bech32()
                    .unwrap_or(raw_address.to_string())
                    .as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                utxo.lovelace_amount().to_string().as_str(),
                rollback,
            )
            .await?;
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
                                output.clone(),
                                &address,
                                format!("{}#{}", input.hash(), input.index()).as_str(),
                                &hex::encode(policy_id),
                                &fingerprint,
                                quantity.to_string().as_str(),
                                rollback,
                            )
                            .await
                            .or_panic()?;
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
        tx_output: &'b MultiEraOutput<'b>,
        output_idx: usize,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        rollback: bool,
    ) -> Result<(), gasket::framework::WorkerError> {
        if let Ok(raw_address) = &tx_output.address() {
            let tx_address = raw_address.to_bech32().unwrap_or(raw_address.to_string());

            self.coin_state(
                output.clone(),
                &tx_address,
                &format!("{}#{}", tx_hash, output_idx),
                tx_output.lovelace_amount().to_string().as_str(),
                !rollback,
            )
            .await?;

            self.datum_state(
                output.clone(),
                &tx_address,
                &format!("{}#{}", tx_hash, output_idx),
                &tx_output,
                !rollback,
            )
            .await?;

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
                                    output.clone(),
                                    &tx_address,
                                    format!("{}#{}", tx_hash, output_idx).as_str(),
                                    &policy_id_str,
                                    &fingerprint,
                                    quantity.to_string().as_str(),
                                    !rollback,
                                )
                                .await?
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
            .await?
        }

        Ok(())
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
        let policy = self.ctx.lock().await.error_policy.clone();

        match (block, genesis_utxos, genesis_hash) {
            (Some(block), _, _) => {
                for tx in block.txs() {
                    if tx.is_valid() {
                        match block_ctx.clone() {
                            Some(block_ctx) => {
                                for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                                    self.process_consumed_txo(
                                        &block_ctx,
                                        &consumed,
                                        output.clone(),
                                        rollback,
                                        &policy,
                                    )
                                    .await?;
                                }
                            }

                            None => {}
                        }

                        for (idx, produced) in tx.produces().iter() {
                            self.process_produced_txo(
                                &tx.hash(),
                                &produced,
                                idx.clone(),
                                output.clone(),
                                rollback,
                            )
                            .await?;
                        }
                    }
                }

                Ok(())
            }

            (_, Some(genesis_utxos), _) => {
                // let mut address_lovelace_agg: HashMap<String, u64> = HashMap::new();
                for utxo in genesis_utxos {
                    // *address_lovelace_agg
                    //     .entry(hex::encode(utxo.1.to_vec()))
                    //     .or_insert(0) += utxo.2;
                    let address = hex::encode(utxo.1.to_vec());
                    let key = format!("{}#{}", hex::encode(utxo.0), 0);

                    self.tx_state(output.clone(), &address, &key, true).await?;

                    self.coin_state(
                        output.clone(),
                        &address,
                        &key,
                        &utxo.2.to_string(),
                        !rollback,
                    )
                    .await?;
                }

                //for (address, lovelace) in &address_lovelace_agg {} // todo determine if we can merge this with utxo_owners or whatever it is for wallet management.. stubbed for now

                Ok(())
            }

            _ => Err(gasket::framework::WorkerError::Panic),
        }
    }
}

impl Config {
    pub fn plugin(self, ctx: Arc<Mutex<Context>>) -> super::Reducer {
        let reducer = Reducer { config: self, ctx };

        super::Reducer::Utxo(reducer)
    }
}
