use std::collections::BTreeMap;

use axum::body::Bytes;
use bytemuck::{Pod, Zeroable};
use fastbloom::BloomFilter;
use snafu::ResultExt as _;
use tokio::{fs::File, io::AsyncWriteExt as _};

use crate::{Error, IoSnafu};

pub const MAX_IN_MEMORY_VALUES: usize = 1024;
pub const BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.01;

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
struct KeyValueRef {
    key_offset: u64,
    value_offset: u64,
    key_len: u32,
    value_len: u32,
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
struct SSTableFileHeader {
    version: u32,
    key_filter_size: u32,
    num_values: u64,
}

pub struct InMemoryTable {
    key_filter: BloomFilter,
    data: BTreeMap<String, Bytes>,
}

impl Default for InMemoryTable {
    fn default() -> Self {
        Self {
            key_filter: BloomFilter::with_false_pos(BLOOM_FILTER_FALSE_POSITIVE_RATE)
                .expected_items(MAX_IN_MEMORY_VALUES),
            data: BTreeMap::default(),
        }
    }
}

impl InMemoryTable {
    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.data.get(key)
    }

    pub fn insert(&mut self, key: String, value: Bytes) {
        self.key_filter.insert(&key);
        self.data.insert(key, value);
    }

    pub fn needs_flush(&self) -> bool {
        self.data.len() >= MAX_IN_MEMORY_VALUES
    }

    pub async fn write(&self, f: &mut File) -> Result<(), Error> {
        // file format:
        // version, number of keys
        // bloom filter
        // index: sorted tuples of (key offset, key len, value offset, value len)
        // the keys in one blob
        // the values in another blob

        // build index & bloom filter
        let mut slices = Vec::with_capacity(self.data.len() * std::mem::size_of::<KeyValueRef>());
        let mut next_key_offset = 0;
        let mut next_val_offset = 0;
        for (k, v) in self.data.iter() {
            slices.extend_from_slice(bytemuck::bytes_of(&KeyValueRef {
                key_offset: next_key_offset,
                value_offset: next_val_offset,
                key_len: k.len() as u32,
                value_len: v.len() as u32,
            }));
            next_key_offset += k.len() as u64;
            next_val_offset += v.len() as u64;
        }
        let header = SSTableFileHeader {
            version: 0,
            num_values: slices.len() as u64,
            key_filter_size: self.key_filter.as_slice().len() as u32,
        };

        f.write_all(bytemuck::bytes_of(&header))
            .await
            .context(IoSnafu {
                cause: "write SSTable header",
            })?;
        f.write_all(bytemuck::cast_slice(self.key_filter.as_slice()))
            .await
            .context(IoSnafu {
                cause: "write bloom filter for keys",
            })?;
        f.write_all(slices.as_slice()).await.context(IoSnafu {
            cause: "write key/value slices",
        })?;

        for k in self.data.keys() {
            f.write_all(k.as_bytes())
                .await
                .context(IoSnafu { cause: "write key" })?;
        }

        for v in self.data.values() {
            f.write_all(v).await.context(IoSnafu {
                cause: "write value",
            })?;
        }

        Ok(())
    }
}
