use pallas::ledger::primitives::alonzo;
use pallas::ledger::primitives::alonzo::{PoolKeyhash, StakeCredential};
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use gasket::messaging::tokio::OutputPort;

use crate::model::CRDTCommand;

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
}

pub struct Reducer {
    config: Config,
}

impl Reducer {
    fn registration(&mut self, cred: &StakeCredential, pool: &PoolKeyhash) -> CRDTCommand {
        let key = match cred {
            StakeCredential::AddrKeyhash(x) => x.to_string(),
            StakeCredential::Scripthash(x) => x.to_string(),
        };

        let value = pool.to_string();

        CRDTCommand::any_write_wins(self.config.key_prefix.as_deref(), &key, value)
    }

    fn deregistration(&mut self, cred: &StakeCredential) -> CRDTCommand {
        let key = match cred {
            StakeCredential::AddrKeyhash(x) => x.to_string(),
            StakeCredential::Scripthash(x) => x.to_string(),
        };

        CRDTCommand::spoil(self.config.key_prefix.as_deref(), &key)
    }

    pub async fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        rollback: bool,
        output: &mut OutputPort<CRDTCommand>,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs() {
            if tx.is_valid() {
                for cert in tx.certs() {
                    if let Some(cert) = cert.as_alonzo() {
                        match cert {
                            alonzo::Certificate::StakeDelegation(cred, pool) => {
                                if !rollback {
                                    output.send(self.registration(cred, pool).into()).await;
                                } else {
                                    output.send(self.deregistration(cred).into()).await;
                                }
                            }

                            alonzo::Certificate::StakeDeregistration(cred) => {
                                if !rollback {
                                    output.send(self.deregistration(cred).into()).await;
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
