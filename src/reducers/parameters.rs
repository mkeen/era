use std::sync::Arc;

use crate::crosscut::epochs::block_epoch;
use crate::model::{CRDTCommand, Value};
use crate::{crosscut, model, prelude::*};
use gasket::messaging::tokio::OutputPort;
use pallas::ledger::traverse::MultiEraBlock;
use serde::Deserialize;
use tokio::sync::Mutex;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
}

#[derive(Clone)]
pub struct Reducer {
    config: Config,
    chain: crosscut::ChainWellKnownInfo,
}

impl Reducer {
    pub async fn reduce<'b>(
        &mut self,
        block: MultiEraBlock<'b>,
        rollback: bool,
        output: Arc<Mutex<OutputPort<CRDTCommand>>>,
        error_policy: crosscut::policies::RuntimePolicy,
    ) -> Result<(), gasket::framework::WorkerError> {
        if rollback {
            return Ok(());
        }

        let def_key_prefix = "last_block";

        let key = match &self.config.key_prefix {
            Some(prefix) => format!("{}", prefix),
            None => format!("{}", def_key_prefix.to_string()),
        };

        let mut member_keys = vec![
            "epoch_no".into(),
            "height".into(),
            "slot_no".into(),
            "block_hash".into(),
            "block_era".into(),
            "transactions_count".into(),
        ];
        let mut member_values = vec![
            Value::BigInt(block_epoch(&self.chain, &block).into()),
            Value::BigInt(block.number().into()),
            Value::BigInt(block.slot().into()),
            block.hash().to_string().into(),
            block.era().to_string().into(),
            Value::String(block.tx_count().to_string().into()), // using a string here to move fast.. some other shits up with bigint for this .into()
        ];

        if let Some(first_tx_hash) = block.txs().first() {
            member_keys.push("first_transaction_hash".into());
            member_values.push(first_tx_hash.hash().to_string().into())
        }

        if let Some(last_tx_hash) = block.txs().last() {
            member_keys.push("last_transaction_hash".into());
            member_values.push(last_tx_hash.hash().to_string().into())
        }

        output
            .lock()
            .await
            .send(gasket::messaging::Message::from(
                model::CRDTCommand::HashSetMulti(key, member_keys, member_values),
            ))
            .await
            .or_retry()
    }
}

impl Config {
    pub fn plugin(self, chain: crosscut::ChainWellKnownInfo) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            chain,
        };

        super::Reducer::Parameters(reducer)
    }
}
