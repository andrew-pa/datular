mod merge;

use std::{
    cmp::Ordering, collections::BTreeMap, fmt::Debug, io::SeekFrom, path::Path, sync::LazyLock,
};

use axum::body::Bytes;
use bincode::Options;
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

const BINCODE: LazyLock<
    bincode::config::WithOtherIntEncoding<
        bincode::config::DefaultOptions,
        bincode::config::FixintEncoding,
    >,
> = LazyLock::new(|| bincode::options().with_fixint_encoding());

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

async fn write_header(
    f: &mut (impl AsyncWrite + Unpin),
    num_values: u64,
    key_filter: &BloomFilter,
) -> Result<(), Error> {
    let filter_ser = BINCODE.serialize(&key_filter).context(SerializeSnafu)?;

    let header = FileHeader {
        version: 0,
        num_values,
        key_filter_len: filter_ser.len() as u32,
    };

    trace!(?header, "writing SSTable header");

    f.write_all(bytemuck::bytes_of(&header))
        .await
        .context(IoSnafu {
            cause: "write SSTable header",
        })?;
    f.write_all(&filter_ser).await.context(IoSnafu {
        cause: "write bloom filter for keys",
    })?;

    Ok(())
}

impl InMemoryTable {
    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.data.get(key).filter(|v| !v.is_empty())
    }

    pub fn insert(&mut self, key: String, value: Bytes) {
        self.key_filter.insert(key.as_bytes());
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
            self.key_filter.insert(k.as_bytes());
        }
        self.data.extend(items);
    }

    pub async fn write(&self, f: &mut (impl AsyncWrite + Unpin)) -> Result<(), Error> {
        // file format:
        // version, number of keys
        // bloom filter
        // sequence of: key len; value len; key bytes; value bytes

        write_header(f, self.data.len() as u64, &self.key_filter).await?;

        for (key, value) in self.data.iter() {
            write_key_value(f, key.as_bytes(), value)
                .await
                .context(IoSnafu {
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

    async fn skip_to_data(&mut self) -> Result<(), Error> {
        self.file
            .seek(SeekFrom::Start(
                size_of::<FileHeader>() as u64 + self.header.key_filter_len as u64,
            ))
            .await
            .context(IoSnafu {
                cause: "seek to key filter",
            })
            .map(|_| ())
    }

    async fn read_next_key(&mut self) -> Result<(u32, Vec<u8>), tokio::io::Error> {
        // read the lengths of the key and value
        let key_len = self.file.read_u32_le().await?;
        let val_len = self.file.read_u32_le().await?;

        // read the full key
        let mut key_buf = vec![0u8; key_len as usize];
        self.file.read_exact(&mut key_buf).await?;

        Ok((val_len, key_buf))
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
        let key_filter: BloomFilter = BINCODE.deserialize(&buf).context(SerializeSnafu)?;
        trace!("checking key filter for key");
        if !key_filter.contains(key.as_bytes()) {
            return Ok(None);
        }

        trace!("scanning for value");
        for _ in 0..self.header.num_values {
            let (val_len, key_buf) = self.read_next_key().await.context(IoSnafu {
                cause: "read next key",
            })?;

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
