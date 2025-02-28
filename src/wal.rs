use std::path::{Path, PathBuf};

use axum::body::Bytes;
use futures::TryStreamExt as _;
use serde::Serialize;
use snafu::{ResultExt as _, Whatever};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt as _,
};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{trace, warn};

use crate::{
    Error, IoSnafu, SerializeSnafu,
    kv::{read_next_key_value, write_key_value},
    sstable::InMemoryTable,
};

#[derive(Serialize)]
pub struct LogRecord<'k, 'v> {
    #[serde(borrow)]
    pub key: &'k str,
    #[serde(borrow)]
    pub value: &'v [u8],
}

pub struct WriteAheadLog {
    log_dir: PathBuf,
    current_segment: File,
    current_segment_id: usize,
}

async fn open_segment_file(
    log_dir: &Path,
    current_segment_id: usize,
) -> Result<File, tokio::io::Error> {
    File::options()
        .append(true)
        .create(true)
        .write(true)
        .open(log_dir.join(current_segment_id.to_string()))
        .await
}

impl WriteAheadLog {
    pub async fn open(log_dir_path: impl Into<PathBuf>) -> Result<(Self, InMemoryTable), Whatever> {
        let log_dir = log_dir_path.into();
        fs::create_dir_all(&log_dir)
            .await
            .whatever_context("create log directory")?;
        let log_dir_s = ReadDirStream::new(
            fs::read_dir(&log_dir)
                .await
                .whatever_context("open log directory")?,
        );
        let mut log_segments: Vec<(usize, Vec<(String, Bytes)>)> = log_dir_s
            .try_filter_map(
                async |entry| match entry.file_name().to_string_lossy().parse() {
                    Ok(id) => {
                        let mut table = Vec::new();
                        let mut f = File::open(entry.path()).await?;
                        while let Some(kv) = read_next_key_value(&mut f).await? {
                            table.push(kv);
                        }
                        Ok(Some((id, table)))
                    }
                    Err(e) => {
                        warn!(?entry, "found unusual file in log directory: {e}");
                        Ok(None)
                    }
                },
            )
            .try_collect()
            .await
            .whatever_context("read old log")?;
        log_segments.sort_by_key(|(i, _)| *i);
        let current_segment_id = log_segments.last().map(|(i, _)| *i).unwrap_or_default();
        let recovered_table =
            log_segments
                .into_iter()
                .fold(InMemoryTable::default(), |mut rt, (_, table)| {
                    rt.extend_with(table);
                    rt
                });
        trace!(current_segment_id, recovered_values = recovered_table.len());
        let current_segment = open_segment_file(&log_dir, current_segment_id)
            .await
            .whatever_context("open write log file")?;
        Ok((
            Self {
                log_dir,
                current_segment,
                current_segment_id,
            },
            recovered_table,
        ))
    }

    pub async fn log_write(&mut self, key: &str, value: &Bytes) -> Result<(), Error> {
        trace!("writing to log");
        write_key_value(&mut self.current_segment, key, value)
            .await
            .context(IoSnafu {
                cause: "write key/value pair to log",
            })?;
        self.current_segment.sync_data().await.context(IoSnafu {
            cause: "sync write log",
        })?;
        Ok(())
    }

    pub async fn rotate_log(&mut self) -> Result<usize, Error> {
        let old_segment = self.current_segment_id;
        let new_segment = old_segment + 1;
        trace!(new_segment_id = new_segment, "rotating log");
        self.current_segment = open_segment_file(&self.log_dir, new_segment)
            .await
            .context(IoSnafu {
                cause: "rotate logs",
            })?;
        self.current_segment_id = new_segment;
        Ok(old_segment)
    }

    pub async fn delete_old_segment(&self, segment_id: usize) -> Result<(), Error> {
        trace!(segment_id, "deleting old log segment");
        assert_ne!(segment_id, self.current_segment_id);
        fs::remove_file(self.log_dir.join(segment_id.to_string()))
            .await
            .context(IoSnafu {
                cause: "delete log segment",
            })?;
        Ok(())
    }
}
