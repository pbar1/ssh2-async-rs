#[cfg(feature = "tokio")]
mod tokio;

use std::future::Future;
use std::io;
use std::task::Poll;

use ssh2::Error;

/// Async runtime used to process nonblocking IO readiness.
pub trait Runtime: Clone + Send + Sync {
    /// Runtime-specific resources that are shared across all operations on a
    /// session and its derivatives (channels, sftp, etc).
    type Context: Clone + Send + Sync;

    /// Creates a runtime context from a session. Called once to when session is
    /// created.
    fn create_context(&self, session: &ssh2::Session) -> Result<Self::Context, Error>;

    /// Runs `func` with a session, retrying on EAGAIN after waiting for socket
    /// readiness.
    fn with_async<'a, T, F>(
        ctx: &'a Self::Context,
        session: &'a ssh2::Session,
        func: F,
    ) -> impl Future<Output = Result<T, Error>> + Send
    where
        T: Send,
        F: FnMut(&ssh2::Session) -> Result<T, Error> + Send;

    /// Runs `func` with a mutable session, retrying on EAGAIN after waiting for
    /// socket readiness.
    fn with_async_mut<'a, T, F>(
        ctx: &'a Self::Context,
        session: &'a mut ssh2::Session,
        func: F,
    ) -> impl Future<Output = Result<T, Error>> + Send
    where
        T: Send,
        F: FnMut(&mut ssh2::Session) -> Result<T, Error> + Send;

    /// Runs `func`, retrying on [`io::ErrorKind::WouldBlock`] after waiting for
    /// readiness.
    fn with_async_io<'a, T, F>(
        ctx: &'a Self::Context,
        session: &'a ssh2::Session,
        func: F,
    ) -> impl Future<Output = io::Result<T>> + Send
    where
        T: Send,
        F: FnMut() -> io::Result<T> + Send;

    /// Poll for IO readiness. Used for [`futures::AsyncRead`] and
    /// [`futures::AsyncWrite`] implementations.
    fn poll_io<T, F>(
        cx: &mut std::task::Context<'_>,
        ctx: &Self::Context,
        session: &ssh2::Session,
        func: F,
    ) -> Poll<io::Result<T>>
    where
        F: FnMut() -> io::Result<T>;
}
