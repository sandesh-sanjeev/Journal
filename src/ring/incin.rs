//! A background worker to perform asynchronous file deletions.

use std::{
    path::PathBuf,
    thread::{self, JoinHandle},
};

/// A background thread that destroys files asynchronously.
pub(super) struct Incinerator {
    handle: JoinHandle<()>,
    close_recv: oneshot::Receiver<()>,
    work_send: flume::Sender<PathBuf>,
}

impl Incinerator {
    /// Create a new file incinerator.
    ///
    /// # Arguments
    ///
    /// * `queue_size` - Size of the bounded channel used to talk to incinerator.
    pub(super) fn new() -> Self {
        // Create channels to communicate with the async world.
        let (close_send, close_recv) = oneshot::channel();
        let (work_send, work_recv) = flume::unbounded();

        // Spawn the thread that drives the incinerator.
        let handle = thread::spawn(move || {
            while let Ok(path) = work_recv.recv() {
                //println!("Request to delete file: {path:?}");
                let _ = std::fs::remove_file(path); // FIXME
            }

            // Let async components know when the incinerator has closed.
            // Nothing we can do if there are no receivers of close notification.
            let _ = close_send.send(());
        });

        Self {
            handle,
            work_send,
            close_recv,
        }
    }

    /// Destroy the file at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file on disk.
    pub(super) async fn incinerate(&self, path: PathBuf) {
        // // Channel to know fate of a request.
        // let (notify_send, notify_recv) = oneshot::channel();

        // Share path to incinerate to the background thread.
        if self.work_send.send_async(path).await.is_err() {
            // Means that the background thread has unexpected terminated.
            // Which should not happen, so we propagate a panic.
            panic!("Background incinerator thread terminated unexpectedly");
        }

        // // Await results from the background thread.
        // println!("Waiting for incin");
        // let Ok(result) = notify_recv.await else {
        //     // Means that the background thread has unexpected terminated.
        //     // Which should not happen, so we propagate a panic.
        //     panic!("Background incinerator thread terminated unexpectedly");
        // };

        // // Return results from the background thread.
        // result
    }

    /// Gracefully shutdown the background worker.
    pub(super) async fn close(self) {
        // Drop the last sender (we don't allow clones).
        drop(self.work_send);

        // Wait for the close notification to complete.
        // If the background thread has already closed (returns error),
        // we just ignore it cause close is completed.
        // let _ = self.close_recv.await;
    }
}
