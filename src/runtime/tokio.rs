use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use ssh2::BlockDirections;
use ssh2::Error;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::io::unix::AsyncFdReadyGuard;

use super::RuntimeContext;
use crate::consts::ERROR_BAD_SOCKET;

/// Tokio runtime context.
///
/// Contains an `AsyncFd` wrapped within an [`Arc`] so it can be cheaply
/// cloned to session derivatives. This avoids repeated syscalls (ie,
/// `epoll_ctl`) that would cause kernel lock contention under high concurrency.
#[derive(Clone)]
pub struct TokioContext {
    session: ssh2::Session,
    fd: Arc<AsyncFd<RawFd>>,
}

impl RuntimeContext for TokioContext {
    fn new(session: ssh2::Session) -> Result<Self, Error> {
        let fd =
            AsyncFd::with_interest(session.as_raw_fd(), Interest::READABLE | Interest::WRITABLE)
                .map_err(|_| {
                    Error::new(ERROR_BAD_SOCKET, "failed extracting AsyncFd from session")
                })?;
        Ok(Self {
            session,
            fd: Arc::new(fd),
        })
    }

    fn block_directions(&self) -> BlockDirections {
        self.session.block_directions()
    }

    async fn wait_ready(&self) -> io::Result<()> {
        match self.block_directions() {
            BlockDirections::None => tokio::task::yield_now().await,
            BlockDirections::Inbound => {
                self.fd.readable().await?.clear_ready();
            }
            BlockDirections::Outbound => {
                self.fd.writable().await?.clear_ready();
            }
            BlockDirections::Both => {
                tokio::select! {
                    result = self.fd.readable() => result?.clear_ready(),
                    result = self.fd.writable() => result?.clear_ready(),
                }
            }
        }
        Ok(())
    }

    fn poll_ready(&self, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.block_directions() {
            BlockDirections::None => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            BlockDirections::Inbound => clear_ready(self.fd.poll_read_ready(cx)),
            BlockDirections::Outbound => clear_ready(self.fd.poll_write_ready(cx)),
            BlockDirections::Both => {
                if let Poll::Ready(r) = clear_ready(self.fd.poll_read_ready(cx)) {
                    return Poll::Ready(r);
                }
                clear_ready(self.fd.poll_write_ready(cx))
            }
        }
    }
}

fn clear_ready(
    poll_result: Poll<io::Result<AsyncFdReadyGuard<'_, RawFd>>>,
) -> Poll<io::Result<()>> {
    poll_result.map_ok(|mut guard| {
        guard.clear_ready();
    })
}
