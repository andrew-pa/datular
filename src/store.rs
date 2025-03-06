use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::body::Bytes;
use futures::TryStreamExt;
use snafu::{ResultExt, Whatever};
use tokio::{
    fs::{self, DirEntry, File},
    io::BufWriter,
    sync::{
        OnceCell, RwLock, RwLockWriteGuard,
        mpsc::{Receiver, Sender, channel},
    },
    task::JoinHandle,
};

use tokio_stream::wrappers::ReadDirStream;
use tracing::{Instrument, debug, error, info, trace, warn};

use crate::{
    CONFIG,
    sstable::{InMemoryTable, OnDiskTable},
};
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

impl std::fmt::Display for TruncatedBytes<'_> {
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
    pending_flush: RwLock<Option<InMemoryTable>>,
    merge_queue: Sender<usize>,
    merge_worker: OnceCell<JoinHandle<()>>,
}

fn dir_entry_to_index(e: &DirEntry) -> Option<usize> {
    e.file_name().to_string_lossy().parse::<usize>().ok()
}

impl DataStore {
    async fn find_sstables_on_disk(
        sst_path: &Path,
    ) -> Result<BTreeMap<usize, BTreeSet<usize>>, std::io::Error> {
        // enter each subdirectory representing a level
        ReadDirStream::new(fs::read_dir(&sst_path).await?).try_filter_map(async |e| {
            let ty = e.file_type().await?;
            if let Some(lvl) = ty.is_dir().then(|| dir_entry_to_index(&e)).flatten() {
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
                ty.is_file().then(|| dir_entry_to_index(&e)).flatten()
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

    async fn process_merge_task(&self, level: usize) -> Result<(), Error> {
        info!("performing SSTable merge");
        // TODO: if we're at the maximum level?
        let next_level = level + 1;

        // open tables
        let s = fs::read_dir(self.sstable_dir.join(level.to_string()))
            .await
            .context(IoSnafu {
                cause: "read sstable dir for level",
            })?;
        let mut table_paths: Vec<_> = ReadDirStream::new(s)
            .try_filter_map(async |e| {
                let ty = e.file_type().await?;
                Ok(ty
                    .is_file()
                    .then(|| dir_entry_to_index(&e))
                    .flatten()
                    .map(|i| (i, e.path())))
            })
            .try_collect()
            .await
            .context(IoSnafu {
                cause: "scan sstable dir for level",
            })?;

        trace!(?table_paths, "tables at level");

        if table_paths.len() < CONFIG.num_sstables_to_merge {
            debug!(
                num_tables = table_paths.len(),
                "skipping merge, not enough SSTables at level"
            );
            return Ok(());
        }

        table_paths.sort_by_key(|(i, _)| std::cmp::Reverse(*i));

        let next_index = table_paths.first().unwrap().0;

        let mut tables = Vec::new();
        for (_, path) in table_paths.iter().take(CONFIG.num_sstables_to_merge) {
            tables.push(OnDiskTable::open(path).await?);
        }

        // output destination file
        trace!("creating output for merge");
        fs::create_dir_all(self.sstable_dir.join(next_level.to_string()))
            .await
            .context(IoSnafu {
                cause: "ensure sstable directory level exists",
            })?;
        let output_tmp_name = self.sstable_dir.join(format!("{next_level}/__"));
        let mut output = File::create(&output_tmp_name).await.context(IoSnafu {
            cause: "create sstable merge output file",
        })?;

        OnDiskTable::merge(&mut tables, &mut output).await?;

        output.sync_data().await.context(IoSnafu {
            cause: "sync new sstable",
        })?;

        // rename destination file to cause it to become visible
        trace!(
            lvl = next_level,
            idx = next_index,
            "making merged SSTable visible"
        );
        fs::rename(output_tmp_name, self.table_path(next_level, next_index))
            .await
            .context(IoSnafu {
                cause: "failed to rename merge output",
            })?;

        // update the sstable index
        trace!("updating in-memory SSTable index");
        {
            let mut index = self.sstable_index.write().await;
            let nl = index.entry(next_level).or_default();
            nl.insert(next_index);
            if nl.len() > CONFIG.num_sstables_to_merge {
                self.merge_queue.send(next_level).await.expect("send merge request");
            }
            let lvl_index = index.get_mut(&level).expect("index has set for level");
            for (i, _) in table_paths.iter().take(CONFIG.num_sstables_to_merge) {
                lvl_index.remove(i);
            }
        }

        // delete old sstables
        trace!("deleting old SSTables on disk");
        drop(tables);
        for (_, p) in table_paths.iter().take(CONFIG.num_sstables_to_merge) {
            fs::remove_file(p).await.context(IoSnafu {
                cause: "delete old sstable ",
            })?;
        }

        Ok(())
    }

    async fn merge_worker(self: Arc<Self>, mut merge_queue: Receiver<usize>) {
        trace!("starting merge worker");
        while let Some(level) = merge_queue.recv().await {
            let span = tracing::info_span!("merge", level);
            match self
                .process_merge_task(level)
                .instrument(span.clone())
                .await
            {
                Ok(()) => {
                    let _enter = span.enter();
                    debug!("merge successful!")
                }
                Err(e) => {
                    let _enter = span.enter();
                    error!("merge failed: {}", snafu::Report::from_error(e));
                    panic!()
                }
            }
        }
    }

    pub async fn new(data_path: &Path) -> Result<Arc<Self>, Whatever> {
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

        let (merge_queue, merge_queue_recv) = channel(256);

        let this = Arc::new(Self {
            sstable_dir: sst_path,
            sstable_index: RwLock::new(tables),
            in_memory: RwLock::new(write_log_and_init_recovered_table),
            pending_flush: RwLock::default(),
            merge_queue,
            merge_worker: OnceCell::new(),
        });

        this.merge_worker
            .set(tokio::spawn(Self::merge_worker(
                this.clone(),
                merge_queue_recv,
            )))
            .expect("store merge worker join handle");

        Ok(this)
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
        } else if let Some(value) = self.pending_flush.read().await.as_ref().and_then(|t| t.get(key)) {
            debug!(value=%TruncatedBytes::new(value, 32), "got value from memory (pending flush)");
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

    #[tracing::instrument(skip(self))]
    async fn flush_in_memory(
        self: Arc<Self>,
        old_log_segment_id: usize,
    ) {
        debug!(segment_id = old_log_segment_id, "flushing in-memory store");

        let path = self.table_path(0, old_log_segment_id);

        {
            trace!(path=%path.display(), "creating new SSTable file");
            let mut f = BufWriter::new(match File::create_new(&path).await {
                Ok(f) => f,
                Err(e) => {
                    error!(error=%e, path=%path.display(), "failed to create new SSTable file");
                    return;
                }
            });

            let pending_flush = self.pending_flush.read().await;

            match pending_flush.as_ref().unwrap().write(&mut f).await {
                Ok(()) => { }
                Err(e) => {
                    error!(
                        old_log_segment_id,
                        "failed to create new SSTable on disk: {}",
                        snafu::Report::from_error(e)
                    );
                    panic!("what is the right thing to do here?");
                }
            }
        }

        {
            let mut ssi = self.sstable_index.write().await;
            let mut level = ssi
                .first_entry()
                .expect("at least one level present in SSTable index");
            level.get_mut().insert(old_log_segment_id);
            if level.get().len() >= CONFIG.num_sstables_to_merge {
                self.merge_queue
                    .send(*level.key())
                    .await
                    .expect("send merge request");
            }
        }

        // drop the old pending table
        self.pending_flush.write().await.take();

        match self.in_memory.read().await.0.delete_old_segment(old_log_segment_id).await {
            Ok(()) => {}
            Err(e) => error!(
                old_log_segment_id,
                "failed to delete old log segement: {}",
                snafu::Report::from_error(e)
            ),
        };
    }

    #[tracing::instrument(skip_all)]
    pub async fn put(self: &Arc<Self>, key: String, value: Bytes) -> Result<(), Error> {
        debug!(key, value=%TruncatedBytes::new(&value, 32), "put");
        let mut in_mem = self.in_memory.write().await;

        if in_mem.1.needs_flush() {
            let mut pending_flush = self.pending_flush.write().await;
            if pending_flush.is_none() {
                // no flush in progress, so we can flush here
                let old_log_segment_id = in_mem.0.rotate_log().await?;
                *pending_flush = Some(core::mem::take(&mut in_mem.1));
                tokio::spawn(Self::flush_in_memory(self.clone(), old_log_segment_id));
            } else {
                warn!("in-memory flush required but flushing previous data still in progress, overfilling current in-memory table!");
            }
        }

        in_mem.0.log_write(&key, &value).await?;

        trace!("inserting value into in-memory store");
        in_mem.1.insert(key, value);

        Ok(())
    }
}
