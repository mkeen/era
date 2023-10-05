use bech32::{ToBase32, Variant};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use pallas::ledger::addresses::{Address, StakeAddress};
use pallas::ledger::primitives::alonzo::PolicyId;
use pallas::ledger::traverse::{MultiEraAsset, MultiEraOutput};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx, OutputRef};
use serde::{Deserialize, Serialize};

use gasket::messaging::tokio::{InputPort, OutputPort};

use crate::model::CRDTCommand;
use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<Vec<String>>,
    pub coin_key_prefix: Option<String>,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
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

    fn tx_state(
        &mut self,
        output: &mut OutputPort<CRDTCommand>,
        soa: &str,
        tx_str: &str,
        should_exist: bool,
    ) {
        match should_exist {
            true => {
                let _ = output.send(
                    model::CRDTCommand::set_add(
                        self.config.key_prefix.as_deref(),
                        &soa,
                        tx_str.to_string(),
                    )
                    .into(),
                );
            }

            _ => {
                let _ = output.send(
                    model::CRDTCommand::set_remove(
                        self.config.key_prefix.as_deref(),
                        &soa,
                        tx_str.to_string(),
                    )
                    .into(),
                );
            }
        }
    }

    fn coin_state(
        &mut self,
        output: &mut OutputPort<CRDTCommand>,
        address: &str,
        tx_str: &str,
        lovelace_amt: &str,
        should_exist: bool,
    ) {
        match should_exist {
            true => {
                let _ = output.send(
                    model::CRDTCommand::set_add(
                        self.config.coin_key_prefix.as_deref(),
                        tx_str,
                        format!("{}/{}", address, lovelace_amt),
                    )
                    .into(),
                );
            }

            _ => {
                let _ = output.send(
                    model::CRDTCommand::set_remove(
                        self.config.coin_key_prefix.as_deref(),
                        tx_str,
                        format!("{}/{}", address, lovelace_amt),
                    )
                    .into(),
                );
            }
        }
    }

    fn token_state(
        &mut self,
        output: &mut OutputPort<CRDTCommand>,
        address: &str,
        tx_str: &str,
        policy_id: &str,
        fingerprint: &str,
        quantity: &str,
        should_exist: bool,
    ) -> Result<(), gasket::error::Error> {
        match should_exist {
            true => output.send(
                model::CRDTCommand::set_add(
                    self.config.key_prefix.as_deref(),
                    tx_str,
                    format!("{}/{}/{}/{}", address, policy_id, fingerprint, quantity),
                )
                .into(),
            ),

            _ => output.send(
                model::CRDTCommand::set_remove(
                    self.config.key_prefix.as_deref(),
                    tx_str,
                    format!("{}/{}/{}/{}", address, policy_id, fingerprint, quantity),
                )
                .into(),
            ),
        };

        Ok(())
    }

    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutputRef,
        output: &mut OutputPort<CRDTCommand>,
        rollback: bool,
    ) -> Result<(), gasket::error::Error> {
        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).unwrap();

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(()),
        };

        let address = utxo.address().map(|x| x.to_string()).unwrap();

        if let Some(addresses) = &self.config.filter {
            if let Err(_) = addresses.binary_search(&address) {
                return Ok(());
            }
        }

        if let Ok(raw_address) = &utxo.address() {
            let soa = self.stake_or_address_from_address(raw_address);
            self.tx_state(
                output,
                soa.as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                rollback,
            );
            self.coin_state(
                output,
                raw_address
                    .to_bech32()
                    .unwrap_or(raw_address.to_string())
                    .as_str(),
                &format!("{}#{}", input.hash(), input.index()),
                utxo.lovelace_amount().to_string().as_str(),
                rollback,
            );
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
                            )?;
                        }
                    }
                };
            }
        }

        Ok(())
    }

    fn process_produced_txo(
        &mut self,
        tx: &MultiEraTx,
        tx_output: &MultiEraOutput,
        output_idx: usize,
        output: &mut OutputPort<CRDTCommand>,
        rollback: bool,
    ) -> Result<(), gasket::error::Error> {
        if let Ok(raw_address) = &tx_output.address() {
            let tx_hash = tx.hash();
            let tx_address = raw_address.to_bech32().unwrap_or(raw_address.to_string());

            self.coin_state(
                output,
                tx_address.as_str(),
                &format!("{}#{}", tx_hash, output_idx),
                tx_output.lovelace_amount().to_string().as_str(),
                !rollback,
            );

            if let Some(addresses) = &self.config.filter {
                if let Err(_) = addresses.binary_search(&tx_address) {
                    return Ok(());
                }
            }

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
                                )?;
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
            );
        }

        Ok(())
    }

    pub async fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        rollback: bool,
        output: &mut OutputPort<CRDTCommand>,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                self.process_consumed_txo(&ctx, &consumed, output, rollback)
                    .expect("TODO: panic message");
            }

            for (idx, produced) in tx.produces() {
                self.process_produced_txo(&tx, &produced, idx, output, rollback)
                    .expect("TODO: panic message");
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, policy: crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            policy,
        };

        super::Reducer::UtxoByAddress(reducer)
    }
}
