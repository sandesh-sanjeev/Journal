//! Sequenced, user supplied log record and associated buffers.

use crate::schema::LogSerializer;
use std::borrow::Cow;
use thiserror::Error;

/// Results from log interactions.
pub type Result<T> = std::result::Result<T, Error>;

/// Different types errors when working with log records.
#[derive(Debug, Error)]
pub enum Error {
    /// Error when log record is constructed with invalid sequence number.
    #[error("seq_no {0} <= prev_seq_no {1}")]
    InvalidSequence(u64, u64),

    /// Error when log records are encountered out of order.
    #[error("seq_no {0}, expected prev {1} but got {2}")]
    OutOfSequence(u64, u64, u64),

    /// Error when size of log data exceeds limits.
    #[error("seq_no {0} data size {1} exceeds limit {limit}", limit = Log::DATA_SIZE_LIMIT)]
    DataExceedsLimit(u64, usize),

    /// Error when corruption is detected.
    #[error("seq_no: {0}, expected hash {1}, but got {2}")]
    MalformedBytes(u64, u64, u64),
}

/// A sequenced log record that can be appended into Journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log<'a>
where
    [u8]: ToOwned<Owned = Vec<u8>>,
{
    pub(crate) seq_no: u64,
    pub(crate) prev_seq_no: u64,
    pub(crate) data: Cow<'a, [u8]>,
}

impl Log<'_> {
    /// Maximum allowed size of log data.
    #[cfg(not(test))]
    pub const DATA_SIZE_LIMIT: usize = u32::MAX as usize;

    /// Maximum allowed size of log data.
    #[cfg(test)]
    pub const DATA_SIZE_LIMIT: usize = u16::MAX as usize;

    /// Create new log from borrowed data.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number of the log.
    /// * `prev_seq_no` - Sequence number of previous log.
    /// * `data` - Payload of the log.
    pub fn new_borrowed(seq_no: u64, prev_seq_no: u64, data: &[u8]) -> Result<Log<'_>> {
        Log::new(seq_no, prev_seq_no, Cow::Borrowed(data))
    }

    /// Create new log from owned data.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number of the log.
    /// * `prev_seq_no` - Sequence number of previous log.
    /// * `data` - Payload of the log.
    pub fn new_owned(seq_no: u64, prev_seq_no: u64, data: Vec<u8>) -> Result<Log<'static>> {
        Log::new(seq_no, prev_seq_no, Cow::Owned(data))
    }

    /// Sequence number of the log record.
    pub fn seq_no(&self) -> u64 {
        self.seq_no
    }

    /// Sequence number of the previous log record.
    pub fn prev_seq_no(&self) -> u64 {
        self.prev_seq_no
    }

    /// Reference to data held in log.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn new(seq_no: u64, prev_seq_no: u64, data: Cow<'_, [u8]>) -> Result<Log<'_>> {
        // Sequence numbers should be monotonically increasing.
        if seq_no <= prev_seq_no {
            return Err(Error::InvalidSequence(seq_no, prev_seq_no));
        }

        // Size of log payload has practical limits applied to them.
        if data.len() > Self::DATA_SIZE_LIMIT {
            return Err(Error::DataExceedsLimit(seq_no, data.len()));
        }

        Ok(Log {
            seq_no,
            prev_seq_no,
            data,
        })
    }
}

/// A re-usable buffer of log contiguous log records.
pub struct LogBuf {
    pub(crate) buf: Vec<u8>,
    pub(crate) start: usize,
    pub(crate) count: usize,
    pub(crate) after: Option<u64>,
    pub(crate) first: Option<u64>,
    pub(crate) last: Option<u64>,
}

impl LogBuf {
    /// Create a new log buffer with some pre-allocated capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Amount of bytes to pre-allocate during construction.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            start: 0,
            count: 0,
            after: None,
            first: None,
            last: None,
            buf: Vec::with_capacity(capacity),
        }
    }

    /// Currently allocated capacity for the buffer.
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Number of bytes currently held in buffer.
    pub fn length(&self) -> usize {
        self.buf.len()
    }

    /// Sequence number of log before this buffer.
    pub fn after(&self) -> Option<u64> {
        self.after
    }

    /// Sequence number of last log record in buffer.
    pub fn first(&self) -> Option<u64> {
        self.first
    }

    /// Sequence number of the first log record in buffer.
    pub fn last(&self) -> Option<u64> {
        self.last
    }

    /// Number of log records in the buffer.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Append a log record.
    ///
    /// # Arguments
    ///
    /// * `log` - Log record to append into buffer.
    pub fn append(&mut self, log: &Log<'_>) -> Result<usize> {
        // Make sure logs are appended in order.
        if let Some(last) = self.last
            && last != log.prev_seq_no()
        {
            return Err(Error::OutOfSequence(log.seq_no(), log.prev_seq_no(), last));
        }

        // Write serialized log bytes into buffer.
        log.write(&mut self.buf);

        // Update internal metadata.
        self.count += 1;
        self.last = Some(log.seq_no());
        if self.first.is_none() {
            self.first = Some(log.seq_no());
            self.after = Some(log.prev_seq_no());
        }

        // Return the number of bytes written to buffer.
        Ok(log.size())
    }

    /// Clear accumulated state from the buffer.
    ///
    /// Note that memory allocated for the buffer remains as is.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.start = 0;
        self.count = 0;
        self.after = None;
        self.first = None;
        self.last = None;
    }

    /// Shrinks the allocated capacity of the buffer.
    ///
    /// Existing data in buffer will be unaffected. So for best results release
    /// capacity after a [`LogBuf::clear`].
    ///
    /// # Arguments
    ///
    /// * `len` - Minimum size of the buffer.
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.buf.shrink_to(min_capacity);
    }

    /// Returns reference to log bytes held in the buffer.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.buf[self.start..]
    }

    /// Replace underlying byte buffer with a new one.
    ///
    /// Returns the byte buffer that is currently being tracked.
    ///
    /// # Arguments
    ///
    /// * `new_buf` - Buffer to replace.
    pub(crate) fn replace_memory(&mut self, new_buf: Vec<u8>) -> Vec<u8> {
        std::mem::replace(&mut self.buf, new_buf)
    }

    /// Validate log records and rebuild internal state.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Ignore log records with seq_no <= provided seq_no.
    pub(crate) fn initialize(&mut self, seq_no: u64) -> Result<()> {
        let mut len = 0;
        let mut src = &self.buf[..];

        // Clear state other than buffer and start parsing through.
        self.start = 0;
        self.count = 0;
        self.after = None;
        self.first = None;
        self.last = None;
        while let Some(log) = Log::read(src) {
            // We bail early on first detection of corruption.
            let log = log?;

            // Consume log bytes.
            len += log.size();
            src = &src[log.size()..];

            // Check if the log record should be filtered out.
            if log.seq_no() <= seq_no {
                self.start += log.size();
                continue;
            }

            // Make sure logs are seen in order.
            if let Some(prev) = self.last
                && log.prev_seq_no() != prev
            {
                return Err(Error::OutOfSequence(log.seq_no(), log.prev_seq_no(), prev));
            }

            // Everything checks out, log can be fully consumed.
            self.count += 1;
            self.last = Some(log.seq_no());
            if self.first.is_none() {
                self.first = Some(log.seq_no());
                self.after = Some(log.prev_seq_no());
            }
        }

        // Truncate excess bytes in the buffer.
        self.buf.truncate(len);
        Ok(())
    }
}

/// An iterator to iterate through logs in the buffer.
pub struct LogBufIter<'a>(&'a [u8]);

impl<'a> Iterator for LogBufIter<'a> {
    type Item = Log<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // Safety: LogBuf is always validated prior to use.
        let (log, _) = Log::read_unchecked(self.0)?;

        // Update state for next iteration and return.
        self.0 = &self.0[log.size()..];
        Some(log)
    }
}

impl<'a> IntoIterator for &'a LogBuf {
    type Item = Log<'a>;
    type IntoIter = LogBufIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LogBufIter(self.bytes())
    }
}
