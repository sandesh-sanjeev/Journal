//! Append only file for persistent storage.

use monoio::fs::{File, OpenOptions};
use std::{
    cell::Cell,
    io::{Error, Result},
    path::{Path, PathBuf},
};
use thiserror::Error;

/// Result returned from file interactions.
pub type BufResult<T> = std::result::Result<T, BufError>;

/// Different types of errors that can occur when sharing buffers.
#[derive(Debug, Error)]
#[error("I/O error: {1:?}")]
pub struct BufError(Error, Vec<u8>);

/// An asynchronous append only file.
#[derive(Debug)]
pub struct AoFile {
    // Current known size of the file.
    len: Cell<u64>,

    // File handle to the underlying page.
    file: File,

    // Path to the file on disk.
    path: PathBuf,
}

impl AoFile {
    /// Create a new file on disk.
    ///
    /// Returns an error if file already exists at path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file on disk.
    pub async fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Create a new file asserting that one doesn't already exist.
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(path.as_ref())
            .await?;

        // Return the newly created file.
        Ok(Self {
            file,
            len: Cell::new(0),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Open an existing file on disk.
    ///
    /// Returns an error if file does not exist at path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file on disk.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Create a new file asserting that one doesn't already exist.
        let file = OpenOptions::new()
            .create(false)
            .read(true)
            .append(true)
            .open(path.as_ref())
            .await?;

        // Get current size of the file.
        let metadata = file.metadata().await?;

        // Return the newly created file.
        Ok(Self {
            file,
            len: Cell::new(metadata.len()),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Path to the file on disk.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append some blob of bytes into file.
    ///
    /// State of the underlying file is undefined if this results in an error.
    /// It is recommended to abort and run maintenance on the file before use.
    /// However any bytes that to make it to disk won't be visible and will be
    /// overwritten by subsequent successful appends.
    ///
    /// # Arguments
    ///
    /// * `src` - Bytes to append to file.
    pub async fn append(&self, src: Vec<u8>) -> BufResult<Vec<u8>> {
        // Return early if there is nothing to append.
        if src.is_empty() {
            return Ok(src);
        }

        // Append always happens at end of the file.
        let pos = self.len.get();

        // Attempt to append bytes to file.
        let (result, src) = self.file.write_all_at(src, pos).await;
        if let Err(error) = result {
            return Err(BufError(error, src));
        }

        // Update state and return.
        // It's probably impossible to reach u64::MAX.
        self.len.set(pos + src.len() as u64);
        Ok(src)
    }

    /// Read a range of bytes from file.
    ///
    /// It is not an error for file to return lesser than requested number of bytes,
    /// even if those bytes exist in file. Attempting to start read beyond the current
    /// known end of the file is a no-op. However an attempt will be made to read any
    /// many bytes as the buffer has capacity for.
    ///
    /// # Arguments
    ///
    /// * `offset` - Offset to begin read.
    /// * `dst` - Destination buffer to write bytes read from file.
    pub async fn read_at(&self, offset: u64, dst: Vec<u8>) -> BufResult<(usize, Vec<u8>)> {
        // Check how many bytes can be safely read from the file.
        // Safely as in write known to have completed successfully.
        let file_len = self.len.get();
        let remaining = file_len.saturating_sub(offset);
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);

        // Return early if there is nothing to read.
        let to_read = std::cmp::min(remaining, dst.len());
        if to_read == 0 {
            return Ok((0, dst));
        }

        // Attempt to read from the file.
        let (result, dst) = self.file.read_at(dst, offset).await;

        // Return results from the read.
        match result {
            Err(error) => Err(BufError(error, dst)),
            Ok(read) => Ok((read, dst)),
        }
    }

    /// Sync any changes to data in file to disk.
    ///
    /// If successful, guarantees that any intermediate buffers are flushed
    /// and bytes are durably stored on disk.
    pub async fn sync(&self) -> Result<()> {
        self.file.sync_data().await
    }

    /// Gracefully close the file.
    ///
    /// If successful, changes file changes are guaranteed to be durably stored on disk.
    pub async fn close(self) -> Result<()> {
        self.sync().await?;
        self.file.close().await
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use assert2::{check, let_assert};
    use monoio::IoUringDriver;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use std::io::ErrorKind;
    use tempfile::tempdir;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn read_after_append(bufs in arb_append_bufs()) {
            let temp_dir = tempdir()?;
            let mut runtime = monoio::RuntimeBuilder::<IoUringDriver>::new().enable_all().build()?;

            runtime.block_on(async {
                // Create file in path.
                let path = temp_dir.path().join("test.aof");
                let aof = AoFile::create(&path).await?;

                // A re-usable buffer for reads.
                let buf_capacity = bufs.iter().map(Vec::len).sum();
                let mut read_buf = Vec::with_capacity(buf_capacity);

                // Append to file and make sure visible.
                let mut read;
                let mut offset = 0;
                for buf in &bufs {
                    // Append bytes into file.
                    aof.append(buf.clone()).await?;

                    // Read from the file at expected offset.
                    prep_sink_buf(&mut read_buf, buf_capacity);
                    (read, read_buf) = aof.read_at(offset, read_buf).await?;

                    // Make sure expected bytes.
                    prop_assert_eq!(&read_buf[..read], buf);
                    offset += buf.len() as u64;
                }

                // Expected contents of the entire file.
                let super_buf: Vec<_> = bufs.into_iter().flatten().collect();

                // Read the complete file and make sure correct.
                prep_sink_buf(&mut read_buf, buf_capacity);
                (read, read_buf) = aof.read_at(0, read_buf).await?;
                prop_assert_eq!(&read_buf[..read], &super_buf);

                // Close file and return.
                 Ok(aof.close().await?)
            })?;
        }

        #[test]
        fn read_after_append_reopen(bufs in arb_append_bufs()) {
            let temp_dir = tempdir()?;
            let mut runtime = monoio::RuntimeBuilder::<IoUringDriver>::new().enable_all().build()?;

            runtime.block_on(async {
                // Create file in path.
                let path = temp_dir.path().join("test.aof");
                let mut aof = AoFile::create(&path).await?;

                // A re-usable buffer for reads.
                let buf_capacity = bufs.iter().map(Vec::len).sum();
                let mut read_buf = Vec::with_capacity(buf_capacity);

                // Append to file and make sure visible.
                let mut read;
                let mut offset = 0;
                for buf in &bufs {
                    // Append bytes into file.
                    aof.append(buf.clone()).await?;

                    // Close and open the file.
                    aof = re_open(aof, &path).await?;

                    // Read from the file at expected offset.
                    prep_sink_buf(&mut read_buf, buf_capacity);
                    (read, read_buf) = aof.read_at(offset, read_buf).await?;

                    // Make sure expected bytes.
                    prop_assert_eq!(&read_buf[..read], buf);
                    offset += buf.len() as u64;
                }

                // Expected contents of the entire file.
                let super_buf: Vec<_> = bufs.into_iter().flatten().collect();

                // Close and open the file.
                aof = re_open(aof, &path).await?;

                // Read the complete file and make sure correct.
                prep_sink_buf(&mut read_buf, buf_capacity);
                (read, read_buf) = aof.read_at(0, read_buf).await?;
                prop_assert_eq!(&read_buf[..read], &super_buf);

                // Close file and return.
                Ok(aof.close().await?)
            })?;
        }
    }

    #[monoio::test]
    async fn create_already_exists_returns_error() -> Result<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("test.aof");

        // Create file.
        let aof = AoFile::create(&path).await?;
        aof.close().await?;

        // Create file again, should return error.
        let_assert!(Err(error) = AoFile::create(&path).await);
        check!(error.kind() == ErrorKind::AlreadyExists);
        Ok(())
    }

    #[monoio::test]
    async fn open_does_not_exist_returns_error() -> Result<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("test.aof");

        // Open file without creating, should return error.
        let_assert!(Err(error) = AoFile::open(&path).await);
        check!(error.kind() == ErrorKind::NotFound);
        Ok(())
    }

    #[monoio::test]
    async fn append_error_returns_buf() -> Result<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("test.aof");
        let mut aof = AoFile::create(&path).await?;

        // Remove read permissions from file handle.
        let file = OpenOptions::new().create(false).read(true).open(aof.path()).await?;
        std::mem::replace(&mut aof.file, file).close().await?; // Close prev file

        // Append should fail because file handle does not have append/write permissions.
        let src = Vec::from(b"batman");
        let_assert!(Err(BufError(_, buf)) = aof.append(src).await);
        check!(&buf == b"batman");

        Ok(aof.close().await?)
    }

    #[monoio::test]
    async fn read_at_error_returns_buf() -> Result<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("test.aof");
        let mut aof = AoFile::create(&path).await?;

        // Append some bytes into the file.
        let src = Vec::from(b"batman");
        let src = aof.append(src).await.map_err(|e| e.0)?;

        // Remove read permissions from file handle.
        let file = OpenOptions::new().create(false).append(true).open(aof.path()).await?;
        std::mem::replace(&mut aof.file, file).close().await?;

        let dst = vec![0; src.len()];
        let_assert!(Err(BufError(_, buf)) = aof.read_at(0, dst).await);
        check!(buf.len() == src.len());

        Ok(aof.close().await?)
    }

    /// An arbitrary list of byte arrays that has combined maximum size of 1MB.
    fn arb_append_bufs() -> impl Strategy<Value = Vec<Vec<u8>>> {
        vec(vec(any::<u8>(), 0..1024), 0..1024)
    }

    /// Prepare buffer for reads from file.
    fn prep_sink_buf(buf: &mut Vec<u8>, len: usize) {
        buf.clear();
        buf.resize(len, 0);
    }

    /// Re-open a file.
    async fn re_open<P: AsRef<Path>>(file: AoFile, path: P) -> Result<AoFile> {
        file.close().await?;
        AoFile::open(path).await
    }
}
