use std::path::PathBuf;

use axum::body::Bytes;
use snafu::{ResultExt, Whatever};
use tokio::{
    fs::{self, File},
    sync::{RwLock, RwLockWriteGuard},
};

use tracing::{debug, error, info, trace};

use crate::sstable::InMemoryTable;
use crate::{
    Error, IoSnafu,
    wal::{LogRecord, WriteLogFile},
};

pub struct DataStore {
    sstable_dir: PathBuf,
    in_memory: RwLock<(WriteLogFile, InMemoryTable)>,
}

impl DataStore {
    pub async fn new(data_path: PathBuf) -> Result<Self, Whatever> {
        let log_path = data_path.join("log");
        let sst_path = data_path.join("data");
        info!(log_path=?log_path.display(), data_path=?sst_path.display(), "initializing data store");
        let write_log = WriteLogFile::new(log_path).await?;
        fs::create_dir_all(&sst_path)
            .await
            .whatever_context("create data directory")?;
        Ok(Self {
            sstable_dir: sst_path,
            in_memory: RwLock::new((write_log, InMemoryTable::default())),
        })
    }

    #[tracing::instrument(skip_all)]
    pub async fn get(&self, key: &str) -> Result<Bytes, Error> {
        debug!(key, "get");
        if let Some(value) = self.in_memory.read().await.1.get(key) {
            trace!(?value, "got");
            Ok(value.clone())
        } else {
            Err(Error::NotFound)
        }
    }

    async fn flush_in_memory(
        &self,
        in_mem: &mut RwLockWriteGuard<'_, (WriteLogFile, InMemoryTable)>,
    ) -> Result<(), Error> {
        let old_log_segment_id = in_mem.0.rotate_log().await?;
        trace!(segment_id = old_log_segment_id, "flushing in-memory store");

        let data = core::mem::take(&mut in_mem.1);

        let path = self.sstable_dir.join(old_log_segment_id.to_string());

        debug!(path=%path.display(), "creating new SSTable file");
        let mut f = File::create_new(path).await.context(IoSnafu {
            cause: "create SSTable file",
        })?;

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
        debug!(key, ?value, "put");
        let record = LogRecord {
            key: &key,
            value: &value,
        };
        let mut in_mem = self.in_memory.write().await;

        if in_mem.1.needs_flush() {
            self.flush_in_memory(&mut in_mem).await?;
        }

        in_mem.0.log_write(record).await?;

        trace!("inserting value into in-memory store");
        in_mem.1.insert(key, value);

        Ok(())
    }
}
