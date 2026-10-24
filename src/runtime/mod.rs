#[cfg(all(feature = "tokio", unix))]
mod tokio;

use std::future::Future;
use std::io;
use std::task::Context;
use std::task::Poll;

use ssh2::BlockDirections;
use ssh2::Error;

#[cfg(all(feature = "tokio", unix))]
pub use self::tokio::TokioContext;
#[cfg(all(feature = "tokio", unix))]
pub use self::tokio::TokioTransport;
use crate::consts::ERROR_BAD_SOCKET;
use crate::consts::ERROR_EAGAIN;

/// Async runtime context.
///
/// Concrete implementations of this are shared among a [`Session`] and
/// its derivatives.
///
/// [`Session`]: crate::Session
pub trait RuntimeContext: Clone + Send + Sync + Sized {
    /// Owned transport accepted by this runtime.
    ///
    /// Transports must be safe to move with the native session:
    ///
    /// ```compile_fail
    /// use std::{io, rc::Rc, task::{Context, Poll}};
    /// use ssh2_async::{RuntimeContext, ssh2::{BlockDirections, Error, Session}};
    /// #[derive(Clone)]
    /// struct Local;
    /// impl RuntimeContext for Local {
    ///     type Transport = Rc<()>;
    ///     fn new(_: Session, _: Self::Transport) -> Result<Self, Error> { Ok(Self) }
    ///     fn block_directions(&self) -> BlockDirections { BlockDirections::None }
    ///     async fn wait_ready(&self) -> io::Result<()> { Ok(()) }
    ///     fn poll_ready(&self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
    ///         Poll::Ready(Ok(()))
    ///     }
    /// }
    /// ```
    type Transport: Send;

    /// Install the transport and create runtime-specific context for a session.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be registered with the runtime.
    /// Implementations must keep the registration's socket alive until
    /// deregistration, including when a native session alias replaces
    /// transport.
    fn new(session: ssh2::Session, transport: Self::Transport) -> Result<Self, Error>;

    /// Get the blocking direction of the session.
    fn block_directions(&self) -> BlockDirections;

    /// Wait for session readiness.
    fn wait_ready(&self) -> impl Future<Output = io::Result<()>> + Send;

    /// Poll for session readiness.
    ///
    /// Clones must retain independent poll waiters. Each I/O direction uses
    /// its own clone so one pending operation cannot replace another's waker.
    fn poll_ready(&self, cx: &mut Context) -> Poll<io::Result<()>>;

    /// Wrap a nonblocking function with retries until it succeeds.
    fn with_async<T, F>(&self, mut func: F) -> impl Future<Output = Result<T, Error>> + Send
    where
        T: Send,
        F: FnMut() -> Result<T, Error> + Send,
    {
        async move {
            loop {
                match func() {
                    Ok(t) => return Ok(t),
                    Err(e) if would_block_ssh(&e) => self
                        .wait_ready()
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
    fn with_poll<T, F>(&self, cx: &mut Context<'_>, mut func: F) -> Poll<io::Result<T>>
    where
        F: FnMut() -> io::Result<T>,
    {
        loop {
            match func() {
                Ok(t) => return Poll::Ready(Ok(t)),
                Err(e) if would_block_io(&e) => match self.poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                },
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
