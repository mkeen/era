use pallas::ledger::primitives::alonzo;
use pallas::ledger::primitives::alonzo::{PoolKeyhash, StakeCredential};
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use crate::model;

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
}

pub struct Reducer {
    config: Config,
}

impl Reducer {
    fn registration(
        &mut self,
        cred: &StakeCredential,
        pool: &PoolKeyhash,
        output: &mut super::OutputPort<()>,
    ) -> Result<(), gasket::error::Error> {
        let key = match cred {
            StakeCredential::AddrKeyhash(x) => x.to_string(),
            StakeCredential::Scripthash(x) => x.to_string(),
        };

        let value = pool.to_string();

        let crdt =
            model::CRDTCommand::any_write_wins(self.config.key_prefix.as_deref(), &key, value);

        output.send(gasket::messaging::Message::from(crdt))?;

        Ok(())
    }

    fn deregistration(
        &mut self,
        cred: &StakeCredential,
        output: &mut super::OutputPort<()>,
    ) -> Result<(), gasket::error::Error> {
        let key = match cred {
            StakeCredential::AddrKeyhash(x) => x.to_string(),
            StakeCredential::Scripthash(x) => x.to_string(),
        };

        let crdt = model::CRDTCommand::spoil(self.config.key_prefix.as_deref(), &key);

        output.send(gasket::messaging::Message::from(crdt))?;

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        rollback: bool,
        output: &mut super::OutputPort<()>,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs() {
            if tx.is_valid() {
                for cert in tx.certs() {
                    if let Some(cert) = cert.as_alonzo() {
                        match cert {
                            alonzo::Certificate::StakeDelegation(cred, pool) => {
                                if !rollback {
                                    self.registration(cred, pool, output)?
                                } else {
                                    self.deregistration(cred, output)?
                                }
                            }

                            alonzo::Certificate::StakeDeregistration(cred) => {
                                if !rollback {
                                    self.deregistration(cred, output)?
                                } else {
                                }
                            }

                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer { config: self };
        super::Reducer::StakeToPool(reducer)
    }
}
