#[cfg(feature = "tokio")]
mod tokio;

use std::future::Future;
use std::io;
use std::task::Context;
use std::task::Poll;

use ssh2::BlockDirections;
use ssh2::Error;

#[cfg(feature = "tokio")]
pub use self::tokio::TokioContext;
use crate::consts::ERROR_BAD_SOCKET;
use crate::consts::ERROR_EAGAIN;

/// Async runtime context.
///
/// Concrete implementations of this are shared among a [`Session`] and
/// its derivatives.
///
/// [`Session`]: crate::Session
pub trait RuntimeContext: Clone + Send + Sync + Sized {
    /// Create runtime-specific context from a session.
    fn new(session: &ssh2::Session) -> Result<Self, Error>;

    /// Wait for session readiness.
    fn wait_ready(
        &self,
        directions: BlockDirections,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Poll for session readiness.
    fn poll_ready(&self, cx: &mut Context, directions: &BlockDirections) -> Poll<io::Result<()>>;

    /// Wrap a nonblocking session function with retries until it succeeds.
    fn with_async<'a, T, F>(
        &'a self,
        session: &'a ssh2::Session,
        mut func: F,
    ) -> impl Future<Output = Result<T, Error>> + Send
    where
        T: Send,
        F: FnMut(&ssh2::Session) -> Result<T, Error> + Send,
    {
        async move {
            loop {
                match func(session) {
                    Ok(t) => return Ok(t),
                    Err(e) if would_block_ssh(&e) => self
                        .wait_ready(session.block_directions())
                        .await
                        .map_err(|_| Error::new(ERROR_BAD_SOCKET, "socket wait failed"))?,
                    Err(e) => return Err(e),
                }
            }
        }
    }

    /// Like [`RuntimeContext::with_async`] but for mutable sessions.
    fn with_async_mut<'a, T, F>(
        &'a self,
        session: &'a ssh2::Session,
        mut func: F,
    ) -> impl Future<Output = Result<T, Error>> + Send
    where
        T: Send,
        F: FnMut(&ssh2::Session) -> Result<T, Error> + Send,
    {
        async move {
            loop {
                match func(session) {
                    Ok(t) => return Ok(t),
                    Err(e) if would_block_ssh(&e) => self
                        .wait_ready(session.block_directions())
                        .await
                        .map_err(|_| Error::new(ERROR_BAD_SOCKET, "socket wait failed"))?,
                    Err(e) => return Err(e),
                }
            }
        }
    }

    /// Wrap a function with polling until it succeeds.
    ///
    /// Used for implementing `AsyncRead` and `AsyncWrite` on channels and
    /// streams.
    fn with_poll<T, F>(
        &self,
        cx: &mut Context<'_>,
        session: &ssh2::Session,
        mut func: F,
    ) -> Poll<io::Result<T>>
    where
        F: FnMut() -> io::Result<T>,
    {
        loop {
            match func() {
                Ok(t) => return Poll::Ready(Ok(t)),
                Err(e) if would_block_io(&e) => {
                    match self.poll_ready(cx, &session.block_directions()) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

fn would_block_ssh(error: &Error) -> bool {
    error.code() == ERROR_EAGAIN
}

fn would_block_io(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
}
