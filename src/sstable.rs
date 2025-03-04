use std::{cmp::Ordering, collections::BTreeMap, io::SeekFrom, path::Path};

use axum::body::Bytes;
use bytemuck::{Pod, Zeroable};
use fastbloom::BloomFilter;
use snafu::{ResultExt as _, ensure};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt as _, AsyncWrite, AsyncWriteExt as _, BufReader},
};
use tracing::{debug, trace};

use crate::{CONFIG, Error, FileFormatVersionSnafu, IoSnafu, SerializeSnafu, kv::write_key_value};

pub const BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.01;

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
struct FileHeader {
    version: u32,
    key_filter_len: u32,
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
                .expected_items(CONFIG.max_in_memory_values),
            data: BTreeMap::default(),
        }
    }
}

impl InMemoryTable {
    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.data.get(key).filter(|v| !v.is_empty())
    }

    pub fn insert(&mut self, key: String, value: Bytes) {
        self.key_filter.insert(&key);
        self.data.insert(key, value);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn needs_flush(&self) -> bool {
        self.data.len() >= CONFIG.max_in_memory_values
    }

    pub fn extend_with(&mut self, items: Vec<(String, Bytes)>) {
        for (k, _) in items.iter() {
            self.key_filter.insert(k);
        }
        self.data.extend(items);
    }

    pub async fn write(&self, f: &mut (impl AsyncWrite + Unpin)) -> Result<(), Error> {
        // file format:
        // version, number of keys
        // bloom filter
        // sequence of: key len; value len; key bytes; value bytes

        let filter_ser = postcard::to_allocvec(&self.key_filter).context(SerializeSnafu)?;

        let header = FileHeader {
            version: 0,
            num_values: self.data.len() as u64,
            key_filter_len: filter_ser.len() as u32,
        };

        f.write_all(bytemuck::bytes_of(&header))
            .await
            .context(IoSnafu {
                cause: "write SSTable header",
            })?;
        f.write_all(&filter_ser).await.context(IoSnafu {
            cause: "write bloom filter for keys",
        })?;

        for (key, value) in self.data.iter() {
            write_key_value(f, key, value).await.context(IoSnafu {
                cause: "write key/value pair",
            })?;
        }

        f.flush().await.context(IoSnafu {
            cause: "flush stream",
        })?;

        Ok(())
    }
}

pub struct OnDiskTable {
    file: BufReader<File>,
    header: FileHeader,
}

impl OnDiskTable {
    pub async fn open(path: &Path) -> Result<Self, Error> {
        let mut file = BufReader::new(File::open(path).await.context(IoSnafu {
            cause: "open sstable on disk",
        })?);
        let mut header = FileHeader::zeroed();
        file.read_exact(bytemuck::bytes_of_mut(&mut header))
            .await
            .context(IoSnafu {
                cause: "read sstable header",
            })?;
        ensure!(header.version == 0, FileFormatVersionSnafu);
        debug!(?header, path=?path.display(), "opening on-disk SSTable");
        Ok(Self { file, header })
    }

    #[tracing::instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, Error> {
        self.file
            .seek(SeekFrom::Start(size_of::<FileHeader>() as u64))
            .await
            .context(IoSnafu {
                cause: "seek to key filter",
            })?;
        let mut buf = vec![0; self.header.key_filter_len as usize];
        self.file.read_exact(&mut buf).await.context(IoSnafu {
            cause: "read key filter",
        })?;
        let key_filter: BloomFilter = postcard::from_bytes(&buf).context(SerializeSnafu)?;
        trace!("checking key filter for key");
        if !key_filter.contains(key) {
            return Ok(None);
        }

        trace!("scanning for value");
        for _ in 0..self.header.num_values {
            // read the lengths of the key and value
            let key_len = self.file.read_u32_le().await.context(IoSnafu {
                cause: "read key len",
            })?;
            let val_len = self.file.read_u32_le().await.context(IoSnafu {
                cause: "read value len",
            })?;

            // read the full key
            let mut key_buf = vec![0u8; key_len as usize];
            self.file
                .read_exact(&mut key_buf)
                .await
                .context(IoSnafu { cause: "read key" })?;

            // compare the key to the one we are looking for
            match key_buf.as_slice().cmp(key.as_bytes()) {
                Ordering::Less => {
                    // keep going, skip this value
                    self.file
                        .seek(SeekFrom::Current(val_len as i64))
                        .await
                        .context(IoSnafu {
                            cause: "skip value",
                        })?;
                    continue;
                }
                Ordering::Equal => {
                    // we found the key
                    if val_len > 0 {
                        // there is a value, read/return it
                        let mut val_buf = vec![0u8; val_len as usize];
                        self.file.read_exact(&mut val_buf).await.context(IoSnafu {
                            cause: "read value",
                        })?;
                        return Ok(Some(val_buf.into()));
                    } else {
                        // the value was deleted
                        return Ok(None);
                    }
                }
                // we've gotten past the point in the table where we expect to find the key, so we
                // are finished searching
                Ordering::Greater => return Ok(None),
            }
        }

        Ok(None)
    }
}

pub async fn merge(
    tables: &[OnDiskTable],
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<(), Error> {
    todo!()
}
