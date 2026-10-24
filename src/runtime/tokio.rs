use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use ssh2::BlockDirections;
use ssh2::Error;
use tokio::io::Interest;
use tokio::net::TcpStream;
use tokio::net::UnixStream;

use super::RuntimeContext;

/// Owned transport accepted by the Tokio backend.
///
/// Standard-library and Tokio TCP/Unix streams convert into this type.
/// Standard streams are set to nonblocking before registration.
pub enum TokioTransport {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl From<TcpStream> for TokioTransport {
    fn from(stream: TcpStream) -> Self {
        Self::Tcp(stream)
    }
}

impl From<UnixStream> for TokioTransport {
    fn from(stream: UnixStream) -> Self {
        Self::Unix(stream)
    }
}

impl TryFrom<std::net::TcpStream> for TokioTransport {
    type Error = io::Error;

    fn try_from(stream: std::net::TcpStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        TcpStream::from_std(stream).map(Self::Tcp)
    }
}

impl TryFrom<std::os::unix::net::UnixStream> for TokioTransport {
    type Error = io::Error;

    fn try_from(stream: std::os::unix::net::UnixStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        UnixStream::from_std(stream).map(Self::Unix)
    }
}

impl AsRawFd for TokioTransport {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Self::Tcp(stream) => stream.as_raw_fd(),
            Self::Unix(stream) => stream.as_raw_fd(),
        }
    }
}

struct SharedTransport(Arc<TokioTransport>);

impl AsRawFd for SharedTransport {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl TokioTransport {
    async fn ready(&self, interest: Interest) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.ready(interest).await?,
            Self::Unix(stream) => stream.ready(interest).await?,
        };
        Ok(())
    }

    fn try_io(&self, interest: Interest, func: impl FnOnce() -> io::Result<()>) {
        let _ = match self {
            Self::Tcp(stream) => stream.try_io(interest, func),
            Self::Unix(stream) => stream.try_io(interest, func),
        };
    }
}

/// Tokio runtime context sharing one owned socket with the native session.
///
/// Clones share the existing Tokio registration. No descriptor is duplicated
/// or registered through `AsyncFd`.
pub struct TokioContext {
    poll_wait: Mutex<Option<(BlockDirections, BoxFuture<'static, io::Result<()>>)>>,
    // Keep each native call and its direction lookup atomic across clones.
    io: Arc<Mutex<()>>,
    socket: Arc<TokioTransport>,
    session: ssh2::Session,
}

impl TokioContext {
    fn check_transport(&self) -> io::Result<()> {
        if self.session.as_raw_fd() == self.socket.as_raw_fd() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native transport was replaced; create a new async session",
            ))
        }
    }
    async fn wait_for(&self, direction: BlockDirections) -> io::Result<()> {
        self.check_transport()?;
        match direction {
            BlockDirections::None => tokio::task::yield_now().await,
            BlockDirections::Inbound => self.socket.ready(Interest::READABLE).await?,
            BlockDirections::Outbound => self.socket.ready(Interest::WRITABLE).await?,
            BlockDirections::Both => {
                self.socket
                    .ready(Interest::READABLE | Interest::WRITABLE)
                    .await?;
            }
        }
        self.check_transport()
    }

    fn poll_for(&self, cx: &mut Context, direction: BlockDirections) -> Poll<io::Result<()>> {
        self.check_transport()?;
        let mut wait = self.poll_wait.lock().unwrap();
        if direction == BlockDirections::None {
            *wait = None;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if wait
            .as_ref()
            .is_some_and(|(previous, _)| *previous != direction)
        {
            *wait = None;
        }
        let (_, future) = wait.get_or_insert_with(|| {
            let socket = self.socket.clone();
            let interest = match direction {
                BlockDirections::Inbound => Interest::READABLE,
                BlockDirections::Outbound => Interest::WRITABLE,
                _ => Interest::READABLE | Interest::WRITABLE,
            };
            (
                direction,
                Box::pin(async move { socket.ready(interest).await }),
            )
        });
        let result = future.as_mut().poll(cx);
        if result.is_ready() {
            *wait = None;
        }
        drop(wait);
        self.check_transport()?;
        result
    }
    fn try_native<T, E: From<Error>>(
        &self,
        mut func: impl FnMut() -> Result<T, E>,
        would_block: impl Fn(&E) -> bool,
    ) -> (Result<T, E>, BlockDirections) {
        if self.check_transport().is_err() {
            return (
                Err(Error::new(
                    crate::consts::ERROR_BAD_SOCKET,
                    "native transport was replaced",
                )
                .into()),
                BlockDirections::None,
            );
        }
        let mut call = || {
            let _io = self.io.lock().unwrap();
            let result = func();
            let direction = if result.as_ref().is_err_and(&would_block) {
                self.block_directions()
            } else {
                BlockDirections::None
            };
            (result, direction)
        };
        let direction = self.block_directions();
        let interests: &[Interest] = match direction {
            BlockDirections::Outbound => &[Interest::WRITABLE],
            BlockDirections::Both => &[Interest::READABLE, Interest::WRITABLE],
            _ => &[Interest::READABLE],
        };
        for &interest in interests {
            let mut result = None;
            self.socket.try_io(interest, || {
                let (attempt, direction) = call();
                let blocked = attempt.as_ref().is_err_and(&would_block);
                let matches = direction == BlockDirections::Both
                    || direction
                        == if interest.is_readable() {
                            BlockDirections::Inbound
                        } else {
                            BlockDirections::Outbound
                        };
                result = Some((attempt, direction));
                if blocked && matches {
                    Err(io::ErrorKind::WouldBlock.into())
                } else {
                    Ok(())
                }
            });
            if let Some(result) = result {
                return result;
            }
        }
        // Native SSH/SFTP buffers may already contain the result even when
        // Tokio has no cached socket readiness. Always allow that first try.
        call()
    }
}

impl RuntimeContext for TokioContext {
    type Transport = TokioTransport;

    fn new(mut session: ssh2::Session, transport: Self::Transport) -> Result<Self, Error> {
        let socket = Arc::new(transport);
        session.set_tcp_stream(SharedTransport(socket.clone()));
        Ok(Self {
            poll_wait: Mutex::new(None),
            io: Arc::new(Mutex::new(())),
            socket,
            session,
        })
    }

    fn block_directions(&self) -> BlockDirections {
        self.session.block_directions()
    }

    async fn wait_ready(&self) -> io::Result<()> {
        self.wait_for(self.block_directions()).await
    }

    async fn with_async<T, F>(&self, mut func: F) -> Result<T, Error>
    where
        T: Send,
        F: FnMut() -> Result<T, Error> + Send,
    {
        loop {
            let (attempt, direction) = self.try_native(&mut func, super::would_block_ssh);
            match attempt {
                Err(error) if super::would_block_ssh(&error) => {
                    self.wait_for(direction).await.map_err(|_| {
                        Error::new(crate::consts::ERROR_BAD_SOCKET, "socket wait failed")
                    })?;
                }
                result => return result,
            }
        }
    }

    fn with_poll<T, F>(&self, cx: &mut Context<'_>, mut func: F) -> Poll<io::Result<T>>
    where
        F: FnMut() -> io::Result<T>,
    {
        loop {
            let (attempt, direction) = self.try_native(&mut func, super::would_block_io);
            match attempt {
                Err(error) if super::would_block_io(&error) => {
                    std::task::ready!(self.poll_for(cx, direction))?;
                }
                result => {
                    *self.poll_wait.lock().unwrap() = None;
                    return Poll::Ready(result);
                }
            }
        }
    }

    fn poll_ready(&self, cx: &mut Context) -> Poll<io::Result<()>> {
        self.poll_for(cx, self.block_directions())
    }
}

impl Clone for TokioContext {
    fn clone(&self) -> Self {
        Self {
            poll_wait: Mutex::new(None),
            io: self.io.clone(),
            socket: self.socket.clone(),
            session: self.session.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::os::unix::net::UnixStream;

    use super::*;

    #[tokio::test]
    async fn native_session_keeps_the_owned_registration() {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        stream.set_nonblocking(true).unwrap();
        peer.set_nonblocking(true).unwrap();
        let native = ssh2::Session::new().unwrap();
        let ctx = TokioContext::new(native.clone(), stream.try_into().unwrap()).unwrap();
        let socket = Arc::downgrade(&ctx.socket);
        let clone = ctx.clone();
        drop(ctx);
        drop(clone);
        assert!(socket.upgrade().is_some());
        assert_eq!(
            peer.read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(native);
        assert!(socket.upgrade().is_none());
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    }

    #[tokio::test]
    async fn replaced_transport_is_rejected() {
        let (stream, _peer) = UnixStream::pair().unwrap();
        stream.set_nonblocking(true).unwrap();
        let mut native = ssh2::Session::new().unwrap();
        let ctx = TokioContext::new(native.clone(), stream.try_into().unwrap()).unwrap();
        let (replacement, _peer) = UnixStream::pair().unwrap();
        native.set_tcp_stream(replacement);
        assert_eq!(
            ctx.wait_ready().await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(
            ctx.poll_ready(&mut Context::from_waker(std::task::Waker::noop()))
                .is_ready()
        );
    }
}

#[cfg(test)]
mod waiter_tests {
    use std::io;
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Waker;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn independent_poll_waiters_complete_on_read_event() {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut native = ssh2::Session::new().unwrap();
        native.set_blocking(false);
        let ctx = TokioContext::new(native.clone(), stream.try_into().unwrap()).unwrap();
        assert!(native.handshake().is_err());
        assert_eq!(native.block_directions(), BlockDirections::Inbound);
        let ready = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let ctx = ctx.clone();
            let ready = ready.clone();
            let (started, waiting) = futures::channel::oneshot::channel();
            tasks.push(tokio::spawn(async move {
                let mut started = Some(started);
                futures::future::poll_fn(|cx| {
                    ctx.with_poll(cx, || {
                        if let Some(started) = started.take() {
                            let _ = started.send(());
                        }
                        if ready.load(Ordering::SeqCst) {
                            Ok(())
                        } else {
                            Err(io::ErrorKind::WouldBlock.into())
                        }
                    })
                })
                .await
            }));
            waiting.await.unwrap();
        }
        ready.store(true, Ordering::SeqCst);
        peer.write_all(b"SSH-2.0-test\r\n").unwrap();
        for mut task in tasks {
            let result = tokio::time::timeout(Duration::from_millis(200), &mut task).await;
            task.abort();
            result
                .expect("read event lost a poll waiter")
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn no_direction_schedules_a_retry() {
        use std::sync::atomic::AtomicUsize;
        use std::task::Wake;
        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (stream, _peer) = UnixStream::pair().unwrap();
        let ctx =
            TokioContext::new(ssh2::Session::new().unwrap(), stream.try_into().unwrap()).unwrap();
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let result = ctx.poll_ready(&mut Context::from_waker(&Waker::from(count.clone())));
        assert!(result.is_ready() || count.0.load(Ordering::SeqCst) > 0);
    }
}

#[cfg(test)]
mod readiness_tests {
    use std::cell::Cell;
    use std::io::Write;
    use std::os::unix::net::UnixStream;

    use super::*;

    #[tokio::test]
    async fn waiting_preserves_readiness_until_native_io() {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut native = ssh2::Session::new().unwrap();
        native.set_blocking(false);
        let ctx = TokioContext::new(native.clone(), stream.try_into().unwrap()).unwrap();
        assert!(native.handshake().is_err());
        peer.write_all(b"SSH-2.0-test\r\n").unwrap();
        ctx.wait_ready().await.unwrap();
        let called = Cell::new(false);
        let TokioTransport::Unix(stream) = &*ctx.socket else {
            unreachable!()
        };
        let _ = stream.try_io(Interest::READABLE, || {
            called.set(true);
            native.handshake().map_err(io::Error::from)
        });
        assert!(
            called.get(),
            "readiness was cleared before the native I/O attempt"
        );
    }
}

#[cfg(test)]
mod idle_tests {
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn idle_native_operations_do_not_keep_retrying() {
        let (stream, _peer) = UnixStream::pair().unwrap();
        let native = ssh2::Session::new().unwrap();
        native.set_blocking(false);
        let ctx = TokioContext::new(native.clone(), stream.try_into().unwrap()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let ctx = ctx.clone();
            let calls = calls.clone();
            let mut native = native.clone();
            tasks.push(tokio::spawn(async move {
                ctx.with_async(|| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    native.handshake()
                })
                .await
            }));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        let settled = calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(75)).await;
        let after = calls.load(Ordering::SeqCst);
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        assert!(settled >= 16);
        assert_eq!(settled, after, "idle native operations kept retrying");
    }
}

#[cfg(test)]
mod waiter_lifetime_tests {
    use std::os::unix::net::UnixStream;
    use std::task::Wake;
    use std::task::Waker;

    use super::*;

    #[tokio::test]
    async fn completed_poll_releases_its_previous_waker() {
        struct IdleWake;
        impl Wake for IdleWake {
            fn wake(self: Arc<Self>) {}
        }
        let (stream, _peer) = UnixStream::pair().unwrap();
        let mut native = ssh2::Session::new().unwrap();
        native.set_blocking(false);
        let ctx = TokioContext::new(native.clone(), stream.try_into().unwrap()).unwrap();
        let owner = Arc::new(IdleWake);
        let weak = Arc::downgrade(&owner);
        let waker = Waker::from(owner);
        assert!(
            ctx.with_poll(&mut Context::from_waker(&waker), || native
                .handshake()
                .map_err(io::Error::from))
                .is_pending()
        );
        drop(waker);
        assert!(weak.upgrade().is_some());
        // Another operation may have filled libssh2's buffers while this
        // waiter was pending, so a native retry can finish before readiness.
        assert!(
            ctx.with_poll(&mut Context::from_waker(Waker::noop()), || Ok(()))
                .is_ready()
        );
        assert!(
            weak.upgrade().is_none(),
            "completed I/O retained its old task waker"
        );
    }
}
