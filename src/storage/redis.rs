use std::str::FromStr;
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use gasket::framework::*;
use gasket::messaging::tokio::InputPort;

use pallas::ledger::traverse::MultiEraBlock;
use redis::{Cmd, Commands, ConnectionLike, ToRedisArgs};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::model::{CRDTCommand, Member, Value};
use crate::pipeline::Context;
use crate::{crosscut, model};

impl ToRedisArgs for model::Value {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            model::Value::String(x) => x.write_redis_args(out),
            model::Value::BigInt(x) => x.to_string().write_redis_args(out),
            model::Value::Cbor(x) => x.write_redis_args(out),
            model::Value::Json(x) => todo!("{}", x),
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub connection_params: String,
    pub cursor_key: Option<String>,
}

impl Config {
    pub fn bootstrapper(&self, ctx: Arc<Mutex<Context>>) -> Stage {
        Stage {
            config: self.clone(),
            cursor: Cursor {
                config: self.clone(),
            },
            input: Default::default(),
            storage_ops: Default::default(),
            chain_era: Default::default(),
            ctx,
        }
    }

    pub fn cursor_key(&self) -> &str {
        self.cursor_key.as_deref().unwrap_or("_cursor")
    }
}

#[derive(Clone)]
pub struct Cursor {
    config: Config,
}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<crosscut::PointArg>, crate::Error> {
        let mut connection = redis::Client::open(self.config.connection_params.clone())
            .and_then(|x| x.get_connection())
            .map_err(crate::Error::storage)?;

        let raw: Option<String> = connection
            .get(&self.config.cursor_key())
            .map_err(crate::Error::storage)?;

        let point = match raw {
            Some(x) => Some(crosscut::PointArg::from_str(&x)?),
            None => None,
        };

        Ok(point)
    }
}

#[derive(Stage)]
#[stage(name = "storage-redis", unit = "CRDTCommand", worker = "Worker")]
pub struct Stage {
    config: Config,
    pub cursor: Cursor,
    pub ctx: Arc<Mutex<Context>>,

    pub input: InputPort<CRDTCommand>,

    #[metric]
    storage_ops: gasket::metrics::Counter,

    #[metric]
    chain_era: gasket::metrics::Gauge,
}

pub struct Worker {
    connection: Option<redis::Connection>,
}

// Hack to encode era
pub fn string_to_i64(s: String) -> i64 {
    let bytes = s.into_bytes();
    let mut result: i64 = 0;

    for &b in bytes.iter() {
        assert!(b < 128); // Ensures ascii
        result <<= 8;
        result |= i64::from(b);
    }

    // If the string is less than 8 characters, left pad with zeros.
    for _ in 0..8usize.saturating_sub(bytes.len()) {
        result <<= 8;
    }

    result
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        log::debug!("starting redis");
        let connection = redis::Client::open(stage.config.connection_params.clone())
            .and_then(|c| c.get_connection())
            .or_retry()?;

        log::debug!("redis connection opened");
        Ok(Self {
            connection: Some(connection),
        })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<CRDTCommand>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &CRDTCommand, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            model::CRDTCommand::Noop => Ok(())?,
            model::CRDTCommand::BlockStarting(_) => {
                // start redis transaction
                redis::cmd("MULTI")
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;
            }
            model::CRDTCommand::GrowOnlySetAdd(key, member) => {
                self.connection
                    .as_mut()
                    .unwrap()
                    .sadd(key, member)
                    .or_restart()?;
            }
            model::CRDTCommand::SetAdd(key, member) => {
                log::debug!("adding to set [{}], value [{}]", key, member);

                self.connection
                    .as_mut()
                    .unwrap()
                    .sadd(key, member)
                    .or_restart()?;
            }
            model::CRDTCommand::SetRemove(key, member) => {
                log::debug!("removing from set [{}], value [{}]", key, member);

                self.connection
                    .as_mut()
                    .unwrap()
                    .srem(key, member)
                    .or_restart()?;
            }
            model::CRDTCommand::LastWriteWins(key, member, ts) => {
                log::debug!("last write for [{}], slot [{}]", key, ts);

                self.connection
                    .as_mut()
                    .unwrap()
                    .zadd(key, member, ts)
                    .or_restart()?;
            }
            model::CRDTCommand::SortedSetAdd(key, member, delta) => {
                log::debug!(
                    "sorted set add [{}], value [{}], delta [{}]",
                    key,
                    member,
                    delta
                );

                self.connection
                    .as_mut()
                    .unwrap()
                    .zincr(key, member, delta)
                    .or_restart()?;
            }
            model::CRDTCommand::SortedSetMemberRemove(key, member) => {
                log::debug!("sorted set member remove [{}], value [{}]", key, member);

                self.connection
                    .as_mut()
                    .unwrap()
                    .zrem(&key, member)
                    .or_restart()?;
            }
            model::CRDTCommand::SortedSetRemove(key, member, delta) => {
                log::debug!(
                    "sorted set member with score remove [{}], value [{}], delta [{}]",
                    key,
                    member,
                    delta
                );

                self.connection
                    .as_mut()
                    .unwrap()
                    .zrembyscore(&key, member, delta)
                    .or_restart()?;
            }
            model::CRDTCommand::Spoil(key) => {
                log::debug!("overwrite [{}]", key);

                self.connection.as_mut().unwrap().del(key).or_restart()?;
            }
            model::CRDTCommand::AnyWriteWins(key, value) => {
                log::debug!("overwrite [{}]", key);

                self.connection
                    .as_mut()
                    .unwrap()
                    .set(key, value)
                    .or_restart()?;
            }
            model::CRDTCommand::PNCounter(key, delta) => {
                log::debug!("increasing counter [{}], by [{}]", key, delta);

                self.connection
                    .as_mut()
                    .unwrap()
                    .req_command(
                        &Cmd::new()
                            .arg("INCRBYFLOAT")
                            .arg(key)
                            .arg(delta.to_string()),
                    )
                    .or_restart()?;
            }
            model::CRDTCommand::HashSetMulti(key, members, values) => {
                log::debug!(
                    "setting hash multi on key {} for {} members and {} values",
                    key,
                    members.len(),
                    values.len()
                );

                let mut tuples: Vec<(Member, Value)> = vec![];
                for (index, member) in members.iter().enumerate() {
                    tuples.push((member.to_owned(), values[index].clone()));
                }

                self.connection
                    .as_mut()
                    .unwrap()
                    .hset_multiple(key, &tuples)
                    .or_restart()?;
            }
            model::CRDTCommand::HashSetValue(key, member, value) => {
                log::debug!("setting hash key {} member {}", key, member);

                self.connection
                    .as_mut()
                    .unwrap()
                    .hset(key, member, value)
                    .or_restart()?;
            }
            model::CRDTCommand::HashCounter(key, member, delta) => {
                log::debug!(
                    "increasing hash key {} member {} by {}",
                    key.clone(),
                    member.clone(),
                    delta
                );

                self.connection
                    .as_mut()
                    .unwrap()
                    .req_command(
                        &Cmd::new()
                            .arg("HINCRBYFLOAT")
                            .arg(key.clone())
                            .arg(member.clone())
                            .arg(delta.to_string()),
                    )
                    .or_restart()?;
            }
            model::CRDTCommand::HashUnsetKey(key, member) => {
                log::debug!("deleting hash key {} member {}", key, member);

                self.connection
                    .as_mut()
                    .unwrap()
                    .hdel(member, key)
                    .or_restart()?;
            }
            model::CRDTCommand::UnsetKey(key) => {
                log::debug!("deleting key {}", key);

                self.connection.as_mut().unwrap().del(key).or_restart()?;
            }
            model::CRDTCommand::BlockFinished(point, block_bytes, rollback) => {
                let cursor_str = crosscut::PointArg::from(point.clone()).to_string();

                let block = MultiEraBlock::decode(&block_bytes);
                stage
                    .chain_era
                    .set(string_to_i64(block.unwrap().era().to_string()));

                if !rollback {
                    self.connection
                        .as_mut()
                        .unwrap()
                        .set(stage.config.cursor_key(), &cursor_str)
                        .or_restart()?;
                }

                log::info!(
                    "new cursor saved to redis {} {}",
                    &stage.config.cursor_key(),
                    &cursor_str
                );

                // end redis transaction
                redis::cmd("EXEC")
                    .query(self.connection.as_mut().unwrap())
                    .or_restart()?;

                if !rollback {
                    stage
                        .ctx
                        .lock()
                        .await
                        .block_buffer
                        .insert_block(&point, &block_bytes);
                } else {
                    stage.ctx.lock().await.block_buffer.remove_block(&point); // todo make these return a Result so we can use error handling
                }
            }
        };

        stage.storage_ops.inc(1);

        Ok(())
    }
}
