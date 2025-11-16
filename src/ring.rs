//! Portion of a Journal that stores a contiguous sequence of logs.

pub(super) mod file;
pub(super) mod incin;
pub(super) mod index;
pub(super) mod segment;

use self::{incin::Incinerator, index::SegIndex, segment::Segment};
use crate::{BufError, BufResult, LogBuf};
use async_lock::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::{
    cell::RefCell,
    collections::VecDeque,
    io::{Error, Result},
    num::{NonZeroU16, NonZeroUsize},
    path::{Path, PathBuf},
};

/// A ring buffer of log records.
///
/// When the ring buffer becomes full, memory and disk space is reclaimed
/// from the oldest log records to make space for new log records.
pub(crate) struct Ring {
    path: PathBuf,
    opts: RingOpts,
    incinerator: Incinerator,
    free: RefCell<Vec<SegIndex>>,
    segments: RwLock<VecDeque<Segment>>,
}

impl Ring {
    /// Create a new ring buffer.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to directory that holds ring buffer files.
    /// * `after` - Logs will be appended to ring buffer after this seq_no.
    /// * `opts` - Options to use to initialize ring buffer.
    pub(crate) async fn create<P>(path: P, after: u64, opts: RingOpts) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        // Allocate all the memory necessary for the ring buffer.
        let segments = opts.segment_count.get().into();
        let capacity = opts.segment_capacity.get();
        let skip_size = opts.index_skip_size.get().into();
        let mut free: Vec<_> = (0..segments)
            .map(|_| SegIndex::with_capacity(capacity, skip_size))
            .collect();

        // A ring buffer must have at least one segment.
        let seg_path = Self::segment_path(path.as_ref(), after);
        let seg_index = free.pop().expect("Should have free memory");
        let segment = Segment::create(seg_path, seg_index, after).await?;

        // Slots on the ring buffer.
        let mut segments = VecDeque::with_capacity(segments);
        segments.push_back(segment);

        // Start the background file incinerator.
        let incinerator = Incinerator::new();

        // Return the newly created ring buffer.
        Ok(Self {
            opts,
            incinerator,
            free: RefCell::new(free),
            segments: RwLock::new(segments),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Append log records.
    ///
    /// # Arguments
    ///
    /// * `buf` - A buffer of log records.
    pub(crate) async fn append(&self, buf: LogBuf) -> BufResult<LogBuf, Error> {
        // Return early if there is nothing to append.
        if buf.count() == 0 {
            return Ok(buf);
        };

        // We take a read lock with the right to upgrade to a write lock.
        // There can be at most one such read lock, which works for us.
        // It's a bug for there to be multiple concurrent writes.
        let mut read_lock = self
            .segments
            .try_upgradable_read()
            .expect("There should be only one writer");

        // Find the latest segment in ring buffer.
        let (remaining, state) = read_lock
            .back()
            .map(|segment| (segment.remaining(), segment.state()))
            .expect("Every Ring must have at least one writeable segment");

        // If the latest segment does not have enough space,
        // a new segment must be created to hold new log records.
        if remaining < buf.count() {
            // Just to make our lives easier.
            // Given disk storage, this should be quite generous.
            let seg_capacity = self.opts.segment_capacity.get();
            assert!(buf.count() <= seg_capacity, "LogBuf exceeds segment capacity");

            // Get memory to back the new segment.
            let index = match self.free.borrow_mut().pop() {
                Some(index) => index,
                None => {
                    // Unlink the oldest segment from the ring buffer.
                    let mut write_lock = RwLockUpgradableReadGuard::upgrade(read_lock).await;
                    let reclaimed = write_lock.pop_front().expect("Ring must have at least one segment");
                    read_lock = RwLockWriteGuard::downgrade_to_upgradable(write_lock);

                    // Reclaim disk and memory.
                    match self.reclaim_segment(reclaimed).await {
                        Ok(index) => index,
                        Err(e) => return Err(BufError(e, buf)),
                    }
                }
            };

            // Create the new segment for the new logs.
            let prev = state.prev.unwrap_or(state.after);
            let path = Self::segment_path(&self.path, prev);
            let segment = match Segment::create(path, index, prev).await {
                Ok(segment) => segment,
                Err(error) => return Err(BufError(error, buf)),
            };

            // Link segment to the ring.
            let mut write_lock = RwLockUpgradableReadGuard::upgrade(read_lock).await;
            write_lock.push_back(segment);
            read_lock = RwLockWriteGuard::downgrade_to_upgradable(write_lock);
        }

        // Find the latest segment in ring buffer.
        let segment = read_lock
            .back()
            .expect("Every Ring must have at least one writeable segment");

        // Finally, let's go!
        match segment.append(buf).await {
            Ok((true, buf)) => Ok(buf),
            Ok(_) => panic!("Buffer size exceeds segment capacity"),
            Err(BufError(error, (_, buf))) => return Err(BufError(error, buf)),
        }
    }

    /// Query for log records.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Logs will be listed after this sequence number.
    pub(crate) async fn query_after(&self, seq_no: u64, mut buf: LogBuf) -> BufResult<LogBuf, Error> {
        // Clear any accumulated state.
        buf.clear();

        // Get a lock on all the segments we are tracking.
        let segments = self.segments.read().await;

        // Figure out what segment contains logs we are looking for.
        let Some(segment) = segments.iter().rev().find(|segment| seq_no >= segment.state().after) else {
            return Ok(buf);
        };

        // Find matching logs from the segment.
        segment.query_after(seq_no, buf).await
    }

    /// Gracefully shutdown the ring.
    ///
    /// Last error, if any, is returned back.
    pub(crate) async fn close(self) -> Result<()> {
        // Wait for the incinerator to close.
        self.incinerator.close().await;

        // Close all the segments.
        let mut error = None;
        for segment in self.segments.into_inner() {
            if let Err(e) = segment.close().await {
                error = Some(e);
            };
        }

        // I don't know of a nicer way of express this :/
        if let Some(error) = error { Err(error) } else { Ok(()) }
    }

    /// Path to a segment in the home directory.
    ///
    /// # Arguments
    ///
    /// * `home` - Path to the home directory of the ring buffer.
    /// * `after` - Unique identity of the ring buffer.
    fn segment_path(home: &Path, after: u64) -> PathBuf {
        home.join(format!("{after:0>20}"))
    }

    /// Reclaim disk and memory from a segment.
    ///
    /// # Arguments
    ///
    /// * `segment` - Segment to reclaim.
    async fn reclaim_segment(&self, segment: Segment) -> Result<SegIndex> {
        // Disassemble the segment into various components.
        let (file, index) = segment.into_inner();

        // Close the reclaimed file gracefully.
        let path = file.path().to_path_buf();
        file.close().await?;

        // Destroy the file to reclaim disk space.
        self.incinerator.incinerate(path).await;

        // Return reclaimed memory for the new segment.
        Ok(index)
    }
}

/// Options to initialize the ring buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RingOpts {
    pub(crate) segment_capacity: NonZeroUsize,
    pub(crate) segment_count: NonZeroU16,
    pub(crate) index_skip_size: NonZeroU16,
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::Log;
    use anyhow::Result;
    use tempfile::tempdir;

    // Macos: Write time: 24.600324083s, with rate 10162467.74 lps
    // Mac + Ubuntu:
    #[monoio::test]
    async fn test_run() -> Result<()> {
        // Options to create ring buffer with.
        let opts = RingOpts {
            segment_capacity: NonZeroUsize::new(1000000).unwrap(),
            segment_count: NonZeroU16::new(10).unwrap(),
            index_skip_size: NonZeroU16::new(10).unwrap(),
        };

        // Create a new ring buffer.
        let temp_dir = tempdir()?;
        println!("Temp path: {:?}", temp_dir.path());
        let ring = Ring::create(temp_dir.path(), 0, opts).await?;

        // Add a whole bunch of records into the ring buffer.
        let start = Instant::now();
        let data = vec![0; 250];
        let mut prev_seq_no = 0;
        let mut buf = LogBuf::with_capacity(5000);
        for _ in 0..50000 {
            // Prepare buffer for append.
            buf.clear();
            for _ in 0..5000 {
                // Add new log.
                let seq_no = prev_seq_no + 1;
                let log = Log::new_borrowed(seq_no, prev_seq_no, &data)?;
                buf.append(&log)?;

                // Update for next iteration.
                prev_seq_no = seq_no;
            }

            // Append logs using the buffer.
            buf = match ring.append(buf).await {
                Ok(buf) => buf,
                Err(BufError(e, _)) => Err(e)?,
            };
        }

        // Print results.
        let logs = prev_seq_no as f64;
        let total = start.elapsed().as_secs_f64();
        let rate = if total != 0.0 { logs / total } else { logs };
        println!("Write time: {total}s, with rate {rate} lps");

        // Shutdown and return.
        Ok(ring.close().await?)
    }
}
