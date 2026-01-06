use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::task::Poll;

use ssh2::BlockDirections;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

use super::Runtime;
use super::would_block_io;
use super::would_block_ssh;
use crate::consts::ERROR_BAD_SOCKET;
use crate::consts::ERROR_SOCKET_RECV;
use crate::consts::ERROR_SOCKET_SEND;

/// Tokio runtime implementation.
#[derive(Clone, Copy)]
pub struct Tokio;

/// Shared context for session running in Tokio. Holds the [`AsyncFd`] wrapped
/// in an [`Arc`] so that it can be cheaply cloned to channels, sftp handles,
/// etc.
///
/// This avoids repeated `epoll_ctl` syscalls that would cause kernel lock
/// contention under high concurrency.
#[derive(Clone)]
pub struct TokioContext {
    async_fd: Arc<AsyncFd<RawFd>>,
}

impl TokioContext {
    fn new(fd: RawFd) -> io::Result<Self> {
        let async_fd = AsyncFd::with_interest(fd, Interest::READABLE | Interest::WRITABLE)?;
        let async_fd = Arc::new(async_fd);
        Ok(Self { async_fd })
    }
}

impl Runtime for Tokio {
    type Context = TokioContext;

    fn create_context(&self, session: &ssh2::Session) -> Result<Self::Context, ssh2::Error> {
        TokioContext::new(session.as_raw_fd())
            .map_err(|_| ssh2::Error::from_errno(ERROR_BAD_SOCKET))
    }

    async fn with_async<'a, T, F>(
        ctx: &'a Self::Context,
        session: &'a ssh2::Session,
        mut func: F,
    ) -> Result<T, ssh2::Error>
    where
        T: Send,
        F: FnMut(&ssh2::Session) -> Result<T, ssh2::Error> + Send,
    {
        loop {
            match func(session) {
                Ok(t) => return Ok(t),
                Err(e) if would_block_ssh(&e) => {
                    wait_for_ready(&ctx.async_fd, session.block_directions()).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn with_async_mut<'a, T, F>(
        ctx: &'a Self::Context,
        session: &'a mut ssh2::Session,
        mut func: F,
    ) -> Result<T, ssh2::Error>
    where
        T: Send,
        F: FnMut(&mut ssh2::Session) -> Result<T, ssh2::Error> + Send,
    {
        loop {
            match func(session) {
                Ok(t) => return Ok(t),
                Err(e) if would_block_ssh(&e) => {
                    wait_for_ready(&ctx.async_fd, session.block_directions()).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn with_async_io<'a, T, F>(
        ctx: &'a Self::Context,
        session: &'a ssh2::Session,
        mut func: F,
    ) -> io::Result<T>
    where
        T: Send,
        F: FnMut() -> io::Result<T> + Send,
    {
        loop {
            match func() {
                Ok(t) => return Ok(t),
                Err(e) if would_block_io(&e) => {
                    wait_for_ready_io(&ctx.async_fd, session.block_directions()).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn poll_io<T, F>(
        cx: &mut std::task::Context<'_>,
        ctx: &Self::Context,
        session: &ssh2::Session,
        mut func: F,
    ) -> std::task::Poll<io::Result<T>>
    where
        F: FnMut() -> io::Result<T>,
    {
        loop {
            match func() {
                Ok(t) => return Poll::Ready(Ok(t)),
                Err(e) => {
                    if would_block_io(&e) {
                        match poll_for_ready(cx, &ctx.async_fd, &session.block_directions()) {
                            Poll::Ready(Ok(())) => {}
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                }
            }
        }
    }
}

async fn wait_for_ready(
    async_fd: &AsyncFd<RawFd>,
    directions: BlockDirections,
) -> Result<(), ssh2::Error> {
    match directions {
        BlockDirections::None => tokio::task::yield_now().await,
        BlockDirections::Inbound => async_fd
            .readable()
            .await
            .map_err(|_| ssh2::Error::from_errno(ERROR_SOCKET_RECV))?
            .clear_ready(),
        BlockDirections::Outbound => async_fd
            .writable()
            .await
            .map_err(|_| ssh2::Error::from_errno(ERROR_SOCKET_SEND))?
            .clear_ready(),
        BlockDirections::Both => {
            tokio::select! {
                result = async_fd.readable() => {
                    result
                        .map_err(|_| ssh2::Error::from_errno(ERROR_SOCKET_RECV))?
                        .clear_ready();
                },
                result = async_fd.writable() => {
                    result
                        .map_err(|_| ssh2::Error::from_errno(ERROR_SOCKET_SEND))?
                        .clear_ready();
                },
            }
        }
    }
    Ok(())
}

async fn wait_for_ready_io(
    async_fd: &AsyncFd<RawFd>,
    directions: BlockDirections,
) -> io::Result<()> {
    match directions {
        BlockDirections::None => tokio::task::yield_now().await,
        BlockDirections::Inbound => async_fd
            .readable()
            .await
            .map_err(io::Error::other)?
            .clear_ready(),
        BlockDirections::Outbound => async_fd
            .writable()
            .await
            .map_err(io::Error::other)?
            .clear_ready(),
        BlockDirections::Both => {
            tokio::select! {
                result = async_fd.readable() => {
                    result.map_err(io::Error::other)?.clear_ready();
                },
                result = async_fd.writable() => {
                    result.map_err(io::Error::other)?.clear_ready();
                },
            }
        }
    }
    Ok(())
}

fn poll_for_ready(
    cx: &mut std::task::Context,
    async_fd: &AsyncFd<RawFd>,
    directions: &BlockDirections,
) -> Poll<io::Result<()>> {
    match directions {
        BlockDirections::None => {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
        BlockDirections::Inbound => match async_fd.poll_read_ready(cx) {
            Poll::Ready(Ok(mut guard)) => {
                guard.clear_ready();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        },
        BlockDirections::Outbound => match async_fd.poll_write_ready(cx) {
            Poll::Ready(Ok(mut guard)) => {
                guard.clear_ready();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        },
        BlockDirections::Both => {
            match async_fd.poll_read_ready(cx) {
                Poll::Ready(Ok(mut guard)) => {
                    guard.clear_ready();
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
            match async_fd.poll_write_ready(cx) {
                Poll::Ready(Ok(mut guard)) => {
                    guard.clear_ready();
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
