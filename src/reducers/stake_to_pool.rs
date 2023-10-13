use std::sync::Arc;

use pallas::ledger::primitives::alonzo;
use pallas::ledger::primitives::alonzo::{PoolKeyhash, StakeCredential};
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;

use gasket::messaging::tokio::OutputPort;
use tokio::sync::Mutex;

use crate::model::CRDTCommand;

use crate::crosscut;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
}

#[derive(Clone)]
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

    pub async fn reduce<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        rollback: bool,
        output: &Arc<Mutex<OutputPort<CRDTCommand>>>,
        error_policy: &crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::error::Error> {
        let mut out = output.lock().await;

        for tx in block.txs() {
            if tx.is_valid() {
                for cert in tx.certs() {
                    if let Some(cert) = cert.as_alonzo() {
                        match cert {
                            alonzo::Certificate::StakeDelegation(cred, pool) => {
                                if !rollback {
                                    out.send(self.registration(cred, pool).into()).await?;
                                } else {
                                    out.send(self.deregistration(cred).into()).await?;
                                }
                            }

                            alonzo::Certificate::StakeDeregistration(cred) => {
                                if !rollback {
                                    out.send(self.deregistration(cred).into()).await?;
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
