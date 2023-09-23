use crate::Error;
use gasket::error::AsWorkError;
use pallas::network::miniprotocols::Point;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub struct BlockConfig {
    pub db_path: String,
    pub rollback_db_path: String,
}

impl Default for BlockConfig {
    fn default() -> Self {
        BlockConfig {
            db_path: "/opt/era/block_buffer".to_string(),
            rollback_db_path: "/opt/era/consumed_buffer".to_string(),
        }
    }
}

impl From<BlockConfig> for BufferBlocks {
    fn from(config: BlockConfig) -> Self {
        BufferBlocks::open_db(config)
    }
}

#[derive(Clone)]
pub struct BufferBlocks {
    db: Option<sled::Db>,
    db_depth: Option<usize>,
    queue: Vec<(String, Vec<u8>)>,
}

impl BufferBlocks {
    fn open_db(config: BlockConfig) -> Self {
        let db = sled::open(config.db_path).or_retry().unwrap();
        let queue: Vec<(String, Vec<u8>)> = Vec::default();

        BufferBlocks {
            db_depth: Some(db.len() as usize), // o(n) to get the initial size, but should only be called once
            db: Some(db),
            queue,
        }
    }

    pub fn insert_block(&mut self, point: &Point, block: &Vec<u8>) {
        let key = point.slot_or_default();
        let db = self.get_db_ref();
        db.insert(key.to_string().as_bytes(), sled::IVec::from(block.clone()))
            .expect("todo map storage error");

        self.db_depth_up();
        if self.drop_old_block_if_buffer_max() {
            self.db_depth_down();
        }
    }

    pub fn get_block_at_point(&self, point: &Point) -> Option<Vec<u8>> {
        match self
            .get_db_ref()
            .get(point.slot_or_default().to_string().as_bytes())
        {
            Ok(block) => match block {
                Some(block) => Some(block.to_vec()),
                None => None,
            },
            Err(_) => None,
        }
    }

    pub fn get_block_latest(&self) -> Option<Vec<u8>> {
        match self.get_db_ref().last() {
            Ok(block) => match block {
                Some((_, block)) => Some(block.to_vec()),
                None => None,
            },
            Err(_) => None,
        }
    }

    pub fn close(&self) {
        self.get_db_ref().flush().unwrap_or_default();
    }

    pub fn enqueue_rollback_batch(&mut self, from: &Point) -> usize {
        self.get_rollback_range(from).len()
    }

    pub fn rollback_pop(&mut self) -> Option<Vec<u8>> {
        match self.queue.pop() {
            None => None,
            Some((popped_key, popped)) => {
                let _ = self
                    .get_db_ref()
                    .remove(popped_key.as_bytes())
                    .map_err(Error::storage);
                Some(popped)
            }
        }
    }

    pub fn get_current_queue_depth(&mut self) -> usize {
        self.queue.len()
    }

    fn get_db_ref(&self) -> &sled::Db {
        self.db.as_ref().unwrap()
    }

    fn get_rollback_range(&mut self, from: &Point) -> Vec<(String, Vec<u8>)> {
        let mut blocks_to_roll_back: Vec<(String, Vec<u8>)> = vec![];

        let db = self.get_db_ref();

        let slot = from.slot_or_default().to_string();

        let current_block = match db.get(slot.as_bytes()).unwrap() {
            None => vec![],
            Some(value) => value.to_vec(),
        };

        if !current_block.is_empty() {
            blocks_to_roll_back.push((slot.clone(), current_block.to_vec()));
        }

        //let mut clear_blocks = sled::Batch::default();

        let mut last_seen_slot = slot.to_string();
        while let Some((next_key, next_block)) = db.get_gt(last_seen_slot.as_bytes()).unwrap() {
            last_seen_slot = String::from_utf8(next_key.to_vec()).unwrap();
            blocks_to_roll_back.push((last_seen_slot.clone(), next_block.to_vec()));
        }

        if !blocks_to_roll_back.is_empty() {
            self.queue = blocks_to_roll_back;
        }

        self.queue.clone()
    }

    fn drop_old_block_if_buffer_max(&mut self) -> bool {
        let db = self.get_db_ref();
        let mut dropped = false;

        if self.db_depth.unwrap() > 50000 {
            let first = match db.first() {
                Ok(first) => first,
                Err(_) => None,
            };

            if let Some((first, _)) = first {
                db.remove(first).expect("todo: map storage error");
                dropped = true;
            }
        }

        dropped
    }

    fn db_depth_down(&mut self) -> usize {
        let current_db_depth = self.db_depth.unwrap();
        if current_db_depth > 0 {
            return current_db_depth - 1;
        }

        self.db_depth = Some(current_db_depth);

        return current_db_depth;
    }

    fn db_depth_up(&mut self) -> usize {
        let current_db_depth = self.db_depth.unwrap();
        if current_db_depth > 0 {
            return current_db_depth + 1;
        }

        self.db_depth = Some(current_db_depth);

        return current_db_depth;
    }
}
