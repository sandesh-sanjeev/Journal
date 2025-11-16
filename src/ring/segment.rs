//! Portion of ring buffer that stores a contiguous sequence of logs.

use super::{file::SegFile, index::SegIndex};
use crate::{BufError, BufResult, LogBuf, schema::LogSerializer};
use monoio::buf::IoBuf;
use std::{
    cell::Cell,
    io::{Error, Result},
    path::Path,
};

/// Contiguous sequence of log records indexed in memory and stored on disk.
pub(super) struct Segment {
    file: SegFile,
    index: SegIndex,
    state: Cell<SegState>,
}

impl Segment {
    /// Create a new segment.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to persistent storage on disk.
    /// * `index` - A sparse index used to store
    /// * `after` - Logs after this seq_no will be appended into segment.
    pub(super) async fn create<P: AsRef<Path>>(path: P, mut index: SegIndex, after: u64) -> Result<Self> {
        // Clear any accumulated state in the index.
        index.clear();

        Ok(Self {
            index,
            file: SegFile::create(path).await?,
            state: Cell::new(SegState::new(after)),
        })
    }

    /// Current state of the segment.
    pub(super) fn state(&self) -> SegState {
        self.state.get()
    }

    /// Number of additional log records that can be appended.
    pub(super) fn remaining(&self) -> usize {
        let state = self.state();
        self.index.remaining(state.count)
    }

    /// Append a buffer of log records.
    ///
    /// Returns true when log records are accepted into the segment. Returns false
    /// when segment does not have enough capacity for the buffer of logs. When that
    /// happens, none of the logs are accepted by the segment.
    ///
    /// # Arguments
    ///
    /// * `buf` - Log buffer to append into segment.
    pub(super) async fn append(&self, mut buf: LogBuf) -> BufResult<(bool, LogBuf), Error> {
        // Return empty if buffer is empty.
        let Some(buf_after) = buf.after() else {
            return Ok((true, buf));
        };

        // Read snapshot of current state.
        let state = self.state();
        let mut new_state = state;

        // Panic because this is a bug.
        let prev = new_state.prev.unwrap_or(new_state.after);
        assert_eq!(buf_after, prev, "LogBuf append out of order");

        // Make sure we have enough space in allocated memory to hold logs.
        if self.index.remaining(new_state.count) < buf.count() {
            return Ok((false, buf));
        }

        // Figure out bytes to write to disk.
        let start = buf.start;
        let bytes = buf.replace_memory(Vec::new());
        let src = bytes.slice(start..);

        // Write log bytes to disk.
        let buf = match self.file.append(src).await {
            Ok(src) => {
                buf.replace_memory(src.into_inner());
                buf
            }

            Err(BufError(error, src)) => {
                buf.replace_memory(src.into_inner());
                return Err(BufError(error, (false, buf)));
            }
        };

        // Sweet, update index with log offsets.
        for log in &buf {
            // If we have skipped enough logs, create one now.
            if self.index.create_index(new_state.count) {
                self.index.append(log.seq_no(), new_state.file_len, false);
            }

            // Update for next iteration.
            new_state.count += 1;
            new_state.file_len += log.size() as u64;
            new_state.prev = Some(log.seq_no());
            if new_state.first.is_none() {
                new_state.first = Some(log.seq_no());
            }
        }

        // A series of sanity checks.
        // TODO: We can add more of these.
        assert_eq!(state.after, new_state.after, "After changed during append");
        assert_eq!(buf.last(), new_state.prev, "Prev seq_no mismatch during append");
        assert_eq!(self.file.size(), new_state.file_len, "File size mismatch during append");

        // Finally update state and return.
        self.state.replace(new_state);
        Ok((true, buf))
    }

    /// Query for log records.
    ///
    /// Logs are returned in ascending order of their sequence number,
    /// without any gaps between log records.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Query for log records after this number.
    /// * `buf` - Buffer to populated with the discovered logs.
    pub(super) async fn query_after(&self, seq_no: u64, mut buf: LogBuf) -> BufResult<LogBuf, Error> {
        // Clear state for completeness.
        buf.clear();

        // Read snapshot of current state.
        let state = self.state();

        // Return early if there is nothing to read.
        if state.count == 0 {
            return Ok(buf);
        }

        // Attempting to read log records that do not (yet maybe) exist.
        if let Some(prev) = state.prev
            && seq_no >= prev
        {
            return Ok(buf);
        }

        // Figure out minimum range of bytes to read.
        let (start, maybe_end) = self.index.seek_range(seq_no);
        let min_end = maybe_end.unwrap_or(state.file_len);

        // Figure of number of bytes to read.
        let mut dst = buf.replace_memory(Vec::new());
        let min_capacity = min_end.saturating_sub(start) as usize;
        let read_len = std::cmp::max(min_capacity, dst.capacity());

        // Prepare buffer for reads.
        dst.resize(read_len, 0);

        // Finally read the requested range of bytes from file.
        match self.file.read_at(start, dst).await {
            Ok((read, mut dst)) => {
                dst.truncate(read); // Get rid of additional zeros.
                buf.replace_memory(dst);
                Ok(buf)
            }

            Err(BufError(error, (_, mut dst))) => {
                dst.clear(); // Contents of the buffer are undefined.
                buf.replace_memory(dst);
                Err(BufError(error, buf))
            }
        }
    }

    /// Gracefully shutdown the segment.
    ///
    /// If successful, any changes made to the segment since construction
    /// are guaranteed to be durably stored on persistent storage.
    pub(super) async fn close(self) -> Result<()> {
        self.file.close().await
    }

    /// Consume the segment returning parts of the segment.
    ///
    /// This can be used to destroy the underlying file storage, and
    /// more importantly to reclaim allocated memory.
    pub(super) fn into_inner(self) -> (SegFile, SegIndex) {
        (self.file, self.index)
    }
}

/// Current state of a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SegState {
    pub(super) after: u64,
    pub(super) count: u64,
    pub(super) file_len: u64,
    pub(super) first: Option<u64>,
    pub(super) prev: Option<u64>,
}

impl SegState {
    /// Initialize new state after provided seq_no.
    fn new(after: u64) -> Self {
        Self {
            after,
            first: None,
            prev: None,
            count: 0,
            file_len: 0,
        }
    }
}
