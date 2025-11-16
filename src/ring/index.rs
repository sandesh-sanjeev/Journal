//! Index that tracks offset of log records on a file.

use std::cell::RefCell;

/// A sparse index that hold positions of log records on a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SegIndex {
    skip_size: u64,
    entries: RefCell<Vec<Entry>>,
}

impl SegIndex {
    /// Create index with some pre-allocated max capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of log (skip included) index can hold.
    /// * `skip_size` - Number of logs to skip between entries.
    pub(super) fn with_capacity(capacity: usize, skip_size: u64) -> Self {
        let capacity = (capacity / skip_size as usize) + 1;

        Self {
            skip_size,
            entries: RefCell::new(Vec::with_capacity(capacity)),
        }
    }

    /// Returns true if index has space for logs.
    ///
    /// # Arguments
    ///
    /// * `count` - Total number of logs in segment.
    pub(super) fn remaining(&self, count: u64) -> usize {
        let entries = self.entries.borrow();
        let capacity = entries.capacity() as u64 * self.skip_size;
        capacity.saturating_sub(count) as _
    }

    /// Returns true if the log index should be create, false otherwise.
    ///
    /// # Arguments
    ///
    /// * `count` - Index (0 addressed) of the log in segment.
    pub(super) fn create_index(&self, count: u64) -> bool {
        count.is_multiple_of(self.skip_size)
    }

    /// Append new entry into index.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number of log record.
    /// * `offset` - Offset of log record on file.
    pub(super) fn append(&self, seq_no: u64, offset: u64, force: bool) {
        // If index is not sorted, it's results don't make sense.
        let mut entries = self.entries.borrow_mut();
        if let Some(last) = entries.last()
            && last.seq_no() >= seq_no
        {
            // Panic cause this is a bug.
            panic!("Attempting to appended seq_no out of order");
        }

        // We do not want to re-allocate at runtime, if possible.
        assert!(force || entries.len() < entries.capacity(), "SegIndex overflow");
        entries.push(Entry { offset, seq_no });
    }

    /// Seek a matching offset range for a sequence number.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number to search.
    pub(super) fn seek_range(&self, seq_no: u64) -> (u64, Option<u64>) {
        let entries = self.entries.borrow();

        // Return early if there is nothing to match.
        if entries.is_empty() {
            return (0, None);
        }

        // Binary search for the matching offset.
        // For examples below: [4, 6, 7, 14]
        let (fwd_i, offset) = match entries.binary_search_by_key(&seq_no, Entry::seq_no) {
            // Seq no < smallest seq_no in the index.
            // If seq_no == 1, i == 0.
            // If seq_no == 3, i == 0.
            Err(0) => (2, 0),

            // Exact match found.
            // If seq_no == 4, i == 0.
            // If seq_no == 6, i == 1.
            Ok(i) => (i + 2, entries[i].offset()),

            // Seq no might exist on file now or in future.
            // If seq_no == 5, i == 1.
            // If seq_no == 15, i == 4.
            Err(i) => (i + 1, entries[i - 1].offset()),
        };

        // Look a little bit ahead to find approximate end, if one exists.
        (offset, entries.get(fwd_i).map(Entry::offset))
    }

    /// Clear entries accumulated in the index.
    pub(super) fn clear(&mut self) {
        self.entries.borrow_mut().clear();
    }
}

/// An entry stored in the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    offset: u64,
    seq_no: u64,
}

impl Entry {
    /// Sequence number of log record.
    fn seq_no(&self) -> u64 {
        self.seq_no
    }

    /// Offset of log record on a file.
    fn offset(&self) -> u64 {
        self.offset
    }
}
