use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

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
        let ready = match self {
            Self::Tcp(stream) => stream.ready(interest).await?,
            Self::Unix(stream) => stream.ready(interest).await?,
        };
        if ready.is_readable() || ready.is_read_closed() {
            self.clear_ready(Interest::READABLE);
        }
        if ready.is_writable() || ready.is_write_closed() {
            self.clear_ready(Interest::WRITABLE);
        }
        Ok(())
    }

    fn clear_ready(&self, interest: Interest) {
        // libssh2 performs the I/O itself. Clear the event consumed by its
        // retry without performing any additional socket I/O here.
        let clear = || Err::<(), _>(io::ErrorKind::WouldBlock.into());
        let _ = match self {
            Self::Tcp(stream) => stream.try_io(interest, clear),
            Self::Unix(stream) => stream.try_io(interest, clear),
        };
    }

    fn poll_ready(&self, cx: &mut Context<'_>, interest: Interest) -> Poll<io::Result<()>> {
        let result = match (self, interest.is_readable()) {
            (Self::Tcp(stream), true) => stream.poll_read_ready(cx),
            (Self::Tcp(stream), false) => stream.poll_write_ready(cx),
            (Self::Unix(stream), true) => stream.poll_read_ready(cx),
            (Self::Unix(stream), false) => stream.poll_write_ready(cx),
        };
        result.map_ok(|()| self.clear_ready(interest))
    }
}

/// Tokio runtime context sharing one owned socket with the native session.
///
/// Clones share the existing Tokio registration. No descriptor is duplicated
/// or registered through `AsyncFd`.
#[derive(Clone)]
pub struct TokioContext {
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
}

impl RuntimeContext for TokioContext {
    type Transport = TokioTransport;

    fn new(mut session: ssh2::Session, transport: Self::Transport) -> Result<Self, Error> {
        let socket = Arc::new(transport);
        session.set_tcp_stream(SharedTransport(socket.clone()));
        Ok(Self { socket, session })
    }

    fn block_directions(&self) -> BlockDirections {
        self.session.block_directions()
    }

    async fn wait_ready(&self) -> io::Result<()> {
        self.check_transport()?;
        match self.block_directions() {
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

    fn poll_ready(&self, cx: &mut Context) -> Poll<io::Result<()>> {
        self.check_transport()?;
        match self.block_directions() {
            BlockDirections::None => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            BlockDirections::Inbound => self.socket.poll_ready(cx, Interest::READABLE),
            BlockDirections::Outbound => self.socket.poll_ready(cx, Interest::WRITABLE),
            BlockDirections::Both => {
                if let ready @ Poll::Ready(_) = self.socket.poll_ready(cx, Interest::READABLE) {
                    return ready;
                }
                self.socket.poll_ready(cx, Interest::WRITABLE)
            }
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
