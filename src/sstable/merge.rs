use std::io::ErrorKind;
use tokio::io::AsyncSeek;

use super::*;

#[derive(Clone)]
enum CursorState {
    ReadNext,
    Pending { val_len: u32, key: Vec<u8> },
    Finished,
}

impl Debug for CursorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadNext => write!(f, "ReadNext"),
            Self::Pending { val_len, key } => f
                .debug_struct("Pending")
                .field("val_len", val_len)
                .field("key", &String::from_utf8_lossy(key))
                .finish(),
            Self::Finished => write!(f, "Finished"),
        }
    }
}

struct Cursor<'t> {
    table: &'t mut OnDiskTable,
    table_index: usize,
    state: CursorState,
}

impl Debug for Cursor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cursor")
            .field("table", &self.table_index)
            .field("state", &self.state)
            .finish()
    }
}

impl Cursor<'_> {
    /// If the cursor is in the `ReadNext` state, then read the next key in the file and transition
    /// to the `Pending` state. If EOF is encountered, then the cursor moves to the `Finished`
    /// state.
    async fn fetch_step(&mut self) -> Result<(), Error> {
        if let CursorState::ReadNext = self.state {
            self.state = loop {
                match self.table.read_next_key().await {
                    // skip deleted keys. we don't need to seek because the next kv
                    // pair follows immediatly (since the value is of length zero)
                    Ok((0, _)) => continue,
                    Ok((val_len, key)) => break CursorState::Pending { val_len, key },
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        break CursorState::Finished;
                    }
                    Err(e) => {
                        return Err(Error::Io {
                            cause: "read next key".into(),
                            source: e,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Has the EOF been hit for this file?
    fn finished(&self) -> bool {
        matches!(self.state, CursorState::Finished)
    }

    /// Which cursor's key comes first, then which cursor is from a more recent table?
    /// Finished cursors always come last.
    fn key_order(&self, other: &Cursor<'_>) -> Ordering {
        match (&self.state, &other.state) {
            (CursorState::Pending { key: key1, .. }, CursorState::Pending { key: key2, .. }) => {
                key1.cmp(key2)
                    .then(self.table_index.cmp(&other.table_index).reverse())
            }
            (CursorState::Finished, _) => Ordering::Greater,
            (_, CursorState::Finished) => Ordering::Less,
            _ => self.table_index.cmp(&other.table_index).reverse(),
        }
    }

    async fn consume_pending_key_value_pair(
        &mut self,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        match std::mem::replace(&mut self.state, CursorState::ReadNext) {
            CursorState::Pending { val_len, key } => {
                assert_ne!(val_len, 0);
                let mut val = vec![0u8; val_len as usize];
                self.table
                    .file
                    .read_exact(&mut val)
                    .await
                    .context(IoSnafu {
                        cause: "read value",
                    })?;
                Ok(Some((key, val)))
            }
            CursorState::ReadNext => unreachable!(),
            CursorState::Finished => {
                self.state = CursorState::Finished;
                Ok(None)
            }
        }
    }

    async fn skip_pending_key_value_pair_with_key(&mut self, query: &[u8]) -> Result<(), Error> {
        match &self.state {
            CursorState::Pending { val_len, key } if key == query => {
                self.table
                    .file
                    .seek(SeekFrom::Current(*val_len as i64))
                    .await
                    .context(IoSnafu {
                        cause: "seek past skipped value",
                    })?;
                self.state = CursorState::ReadNext;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl OnDiskTable {
    /// tables: newest to oldest
    // TODO: this function is way too big/complex
    pub async fn merge(
        tables: &mut [OnDiskTable],
        output: &mut (impl AsyncWrite + AsyncSeek + Unpin),
    ) -> Result<(), Error> {
        let max_items: u64 = tables.iter().map(|t| t.header.num_values).sum();
        trace!(
            num_items = max_items,
            num_tables = tables.len(),
            "merging SSTables on disk"
        );

        let mut key_filter = BloomFilter::with_false_pos(BLOOM_FILTER_FALSE_POSITIVE_RATE)
            .expected_items(max_items as usize);
        let filter_len = BINCODE
            .serialized_size(&key_filter)
            .context(SerializeSnafu)?;

        // we're going to come back and write the header at the end
        output
            .seek(SeekFrom::Start(filter_len + size_of::<FileHeader>() as u64))
            .await
            .context(IoSnafu {
                cause: "seek past header",
            })?;

        trace!("seeking to start of data at each input table");
        for table in tables.iter_mut() {
            table.skip_to_data().await?;
        }

        let mut num_values: u64 = 0;
        let mut cursors = tables
            .iter_mut()
            .enumerate()
            .map(|(table_index, table)| Cursor {
                table,
                table_index,
                state: CursorState::ReadNext,
            })
            .collect::<Vec<_>>();
        while !cursors.iter().all(Cursor::finished) {
            // fetch new keys
            for cursor in cursors.iter_mut() {
                cursor.fetch_step().await?;
            }
            trace!(?cursors);
            // pick the smallest key, then pick the newest data
            let next = cursors.iter_mut().min_by(|a, b| a.key_order(b)).unwrap();
            // write the smallest key/value to the output and move to the next pair for all matching tables
            trace!(table = next.table_index, "selected next cursor");
            match next.consume_pending_key_value_pair().await? {
                Some((key, val)) => {
                    num_values += 1;
                    key_filter.insert(key.as_slice());
                    write_key_value(output, &key, &val).await.context(IoSnafu {
                        cause: "write value",
                    })?;

                    // if there were any old matching keys, skip their values
                    for c in cursors.iter_mut() {
                        c.skip_pending_key_value_pair_with_key(&key).await?;
                    }
                }
                None => break,
            }
        }

        output.seek(SeekFrom::Start(0)).await.context(IoSnafu {
            cause: "rewind to start of merge output",
        })?;

        write_header(output, num_values, &key_filter).await?;

        output.flush().await.context(IoSnafu {
            cause: "flush merged SSTable",
        })?;

        Ok(())
    }
}
