use std::path::{Path, PathBuf};

use axum::body::Bytes;
use futures::{StreamExt as _, TryStreamExt};
use snafu::{ResultExt, Whatever};
use tokio::{
    fs::{self, File},
    io::BufWriter,
    sync::{Mutex, RwLock, RwLockWriteGuard},
};

use tokio_stream::wrappers::ReadDirStream;
use tracing::{debug, error, info, trace};

use crate::sstable::{InMemoryTable, OnDiskTable};
use crate::{Error, IoSnafu, wal::WriteAheadLog};

struct TruncatedBytes<'a> {
    bytes: &'a Bytes,
    max_len: usize,
}

impl<'a> TruncatedBytes<'a> {
    fn new(bytes: &'a Bytes, max_len: usize) -> Self {
        Self { bytes, max_len }
    }
}

impl<'a> std::fmt::Display for TruncatedBytes<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.bytes.len();
        let truncated = if len > self.max_len {
            &self.bytes[..self.max_len]
        } else {
            &self.bytes[..]
        };

        write!(f, "{:?}", truncated)?;

        if len > self.max_len {
            write!(f, " … ({} more)", len - self.max_len)?;
        }

        Ok(())
    }
}

pub struct DataStore {
    sstable_dir: PathBuf,
    sstable_index: RwLock<Vec<usize>>,
    in_memory: RwLock<(WriteAheadLog, InMemoryTable)>,
}

impl DataStore {
    pub async fn new(data_path: &Path) -> Result<Self, Whatever> {
        let log_path = data_path.join("log");
        let sst_path = data_path.join("data");
        info!(log_path=%log_path.display(), data_path=%sst_path.display(), "initializing data store");

        let write_log_and_init_recovered_table = WriteAheadLog::open(log_path).await?;

        fs::create_dir_all(&sst_path)
            .await
            .whatever_context("create data directory")?;

        let sstables_on_disk = ReadDirStream::new(
            fs::read_dir(&sst_path)
                .await
                .whatever_context("open sstable directory")?,
        );
        let mut tables: Vec<usize> = sstables_on_disk
            .try_filter_map(async |e| Ok(e.file_name().to_string_lossy().parse::<usize>().ok()))
            .try_collect()
            .await
            .whatever_context("scan sstable directory")?;
        tables.sort();

        Ok(Self {
            sstable_dir: sst_path,
            sstable_index: RwLock::new(tables),
            in_memory: RwLock::new(write_log_and_init_recovered_table),
        })
    }

    #[tracing::instrument(skip_all)]
    pub async fn get(&self, key: &str) -> Result<Bytes, Error> {
        debug!(key, "get");
        if let Some(value) = self.in_memory.read().await.1.get(key) {
            debug!(value=%TruncatedBytes::new(value, 32), "got");
            Ok(value.clone())
        } else {
            let tables = self.sstable_index.read().await;
            for table_index in tables.iter().rev() {
                let path = self.sstable_dir.join(table_index.to_string());
                let mut table = OnDiskTable::open(&path).await?;
                if let Some(v) = table.get(key).await? {
                    debug!(value=%TruncatedBytes::new(&v, 32), "got value from disk");
                    return Ok(v);
                }
            }
            Err(Error::NotFound)
        }
    }

    async fn flush_in_memory(
        &self,
        in_mem: &mut RwLockWriteGuard<'_, (WriteAheadLog, InMemoryTable)>,
    ) -> Result<(), Error> {
        let old_log_segment_id = in_mem.0.rotate_log().await?;
        debug!(segment_id = old_log_segment_id, "flushing in-memory store");

        let data = core::mem::take(&mut in_mem.1);

        let path = self.sstable_dir.join(old_log_segment_id.to_string());

        trace!(path=%path.display(), "creating new SSTable file");
        let mut f = BufWriter::new(File::create_new(path).await.context(IoSnafu {
            cause: "create SSTable file",
        })?);

        match data.write(&mut f).await {
            Ok(()) => {
                drop(f);
            }
            Err(e) => {
                error!(
                    old_log_segment_id,
                    "failed to create new SSTable on disk: {}",
                    snafu::Report::from_error(e)
                );
                panic!("what is the right thing to do here?");
            }
        }

        self.sstable_index.write().await.push(old_log_segment_id);

        match in_mem.0.delete_old_segment(old_log_segment_id).await {
            Ok(()) => {}
            Err(e) => error!(
                old_log_segment_id,
                "failed to delete old log segement: {}",
                snafu::Report::from_error(e)
            ),
        };

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn put(&self, key: String, value: Bytes) -> Result<(), Error> {
        debug!(key, value=%TruncatedBytes::new(&value, 32), "put");
        let mut in_mem = self.in_memory.write().await;

        if in_mem.1.needs_flush() {
            self.flush_in_memory(&mut in_mem).await?;
        }

        in_mem.0.log_write(&key, &value).await?;

        trace!("inserting value into in-memory store");
        in_mem.1.insert(key, value);

        Ok(())
    }
}
