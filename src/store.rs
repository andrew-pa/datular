use std::{
    collections::{BTreeMap, BTreeSet},
    fs::FileType,
    path::{Path, PathBuf},
};

use axum::body::Bytes;
use futures::TryStreamExt;
use snafu::{ResultExt, Whatever};
use tokio::{
    fs::{self, File},
    io::BufWriter,
    sync::{RwLock, RwLockWriteGuard},
};

use tokio_stream::wrappers::ReadDirStream;
use tracing::{debug, error, info, trace, warn};

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
    sstable_index: RwLock<BTreeMap<usize, BTreeSet<usize>>>,
    in_memory: RwLock<(WriteAheadLog, InMemoryTable)>,
}

impl DataStore {
    async fn find_sstables_on_disk(
        sst_path: &Path,
    ) -> Result<BTreeMap<usize, BTreeSet<usize>>, std::io::Error> {
        // enter each subdirectory representing a level
        ReadDirStream::new(fs::read_dir(&sst_path).await?).try_filter_map(async |e| {
            let ty = e.file_type().await?;
            if let Some(lvl) = ty.is_dir().then(|| e.file_name().to_string_lossy().parse::<usize>().ok()).flatten() {
                fs::read_dir(e.path())
                    .await
                    .map(move |s| Some(ReadDirStream::new(s).map_ok(move |e| (lvl, e))))
            } else {
                warn!(entry=?e, "unexpected entry in data directory");
                Ok(None)
            }
        })
        .try_flatten_unordered(None)
        // parse segment index from file names
        .try_filter_map(async |(lvl, e)| {
            let ty = e.file_type().await?;
            Ok(
                ty.is_file().then(|| e.file_name().to_string_lossy().parse::<usize>().ok()).flatten()
                .map(|i| (lvl, i))
            )
        })
        // collect/sort all segments by level
        .try_fold(BTreeMap::<usize, BTreeSet<usize>>::new(), |mut acc, (lvl, idx)| {
            if !acc.entry(lvl).or_default().insert(idx) {
                panic!("duplicate segements with the same id detected on disk, level {lvl}, index {idx}");
            }
            futures::future::ok(acc)
        })
        .await
    }

    pub async fn new(data_path: &Path) -> Result<Self, Whatever> {
        let log_path = data_path.join("log");
        let sst_path = data_path.join("data");
        info!(log_path=%log_path.display(), data_path=%sst_path.display(), "initializing data store");

        let write_log_and_init_recovered_table = WriteAheadLog::open(log_path).await?;

        // make sure both the root data directory and the level 0 directory exist
        fs::create_dir_all(sst_path.join("0"))
            .await
            .whatever_context("create data directory")?;

        let mut tables = Self::find_sstables_on_disk(&sst_path)
            .await
            .whatever_context("scan sstable directory")?;

        if tables.is_empty() {
            tables.insert(0, BTreeSet::default());
        }

        debug!(index=?tables, "initial SSTable index");

        Ok(Self {
            sstable_dir: sst_path,
            sstable_index: RwLock::new(tables),
            in_memory: RwLock::new(write_log_and_init_recovered_table),
        })
    }

    fn table_path(&self, level: usize, index: usize) -> PathBuf {
        self.sstable_dir.join(format!("{level}/{index}"))
    }

    #[tracing::instrument(skip_all)]
    pub async fn get(&self, key: &str) -> Result<Bytes, Error> {
        debug!(key, "get");
        if let Some(value) = self.in_memory.read().await.1.get(key) {
            debug!(value=%TruncatedBytes::new(value, 32), "got value from memory");
            Ok(value.clone())
        } else {
            let tables = self.sstable_index.read().await;
            for path in tables.iter().flat_map(|(&lvl, v)| {
                v.iter()
                    .copied()
                    .rev()
                    .map(move |i| self.table_path(lvl, i))
            }) {
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

        let path = self.table_path(0, old_log_segment_id);

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

        self.sstable_index
            .write()
            .await
            .first_entry()
            .expect("at least one level present in SSTable index")
            .get_mut()
            .insert(old_log_segment_id);

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
