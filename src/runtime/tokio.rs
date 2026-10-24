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
    // Fields drop in declaration order: deregister before the last session
    // reference can close the socket, otherwise Tokio can leak its
    // registration.
    fd: Arc<AsyncFd<RawFd>>,
    session: ssh2::Session,
}

impl RuntimeContext for TokioContext {
    fn new(session: ssh2::Session) -> Result<Self, Error> {
        let fd =
            AsyncFd::with_interest(session.as_raw_fd(), Interest::READABLE | Interest::WRITABLE)
                .map_err(|_| {
                    Error::new(ERROR_BAD_SOCKET, "failed extracting AsyncFd from session")
                })?;
        Ok(Self {
            fd: Arc::new(fd),
            session,
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

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixStream;
    use std::sync::OnceLock;
    use std::sync::Weak;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use super::Arc;
    use super::AsRawFd;
    use super::AsyncFd;
    use super::RawFd;
    use super::RuntimeContext;
    use super::TokioContext;

    struct DropCheckedStream {
        stream: UnixStream,
        registration: Arc<OnceLock<Weak<AsyncFd<RawFd>>>>,
        dropped: Arc<AtomicBool>,
    }

    impl AsRawFd for DropCheckedStream {
        fn as_raw_fd(&self) -> RawFd {
            self.stream.as_raw_fd()
        }
    }

    impl Drop for DropCheckedStream {
        fn drop(&mut self) {
            assert!(
                self.registration.get().unwrap().upgrade().is_none(),
                "socket dropped before its AsyncFd"
            );
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn deregisters_before_closing_socket() {
        check_drop_order(false);
    }

    #[tokio::test]
    async fn last_clone_deregisters_before_closing_socket() {
        check_drop_order(true);
    }

    fn check_drop_order(clone: bool) {
        let (stream, _peer) = UnixStream::pair().unwrap();
        stream.set_nonblocking(true).unwrap();
        let registration = Arc::new(OnceLock::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let mut session = ssh2::Session::new().unwrap();
        session.set_tcp_stream(DropCheckedStream {
            stream,
            registration: Arc::clone(&registration),
            dropped: Arc::clone(&dropped),
        });
        let ctx = TokioContext::new(session).unwrap();
        registration.set(Arc::downgrade(&ctx.fd)).unwrap();

        if clone {
            let cloned = ctx.clone();
            assert!(Arc::ptr_eq(&ctx.fd, &cloned.fd));
            drop(ctx);
            assert!(!dropped.load(Ordering::SeqCst));
            drop(cloned);
        } else {
            drop(ctx);
        }
        assert!(dropped.load(Ordering::SeqCst));
    }
}
