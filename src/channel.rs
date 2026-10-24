use std::io;
use std::io::Read;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use futures::io::AsyncRead;
use futures::io::AsyncWrite;
use ssh2::Error;
use ssh2::ExitSignal;
use ssh2::ExtendedData;
use ssh2::PtyModes;
use ssh2::ReadWindow;
use ssh2::WriteWindow;

use crate::RuntimeContext;
use crate::write::WriteRequest;

/// Async wrapper for [`ssh2::Channel`].
///
/// Writes report native progress immediately. An unfinished native write is
/// retained across polls; flush it before extracting or using native handles.
/// Reads do not flush outgoing data.
pub struct Channel<C: RuntimeContext> {
    inner: ssh2::Channel,
    ctx: C,
    read_ctx: C,
    writes: Arc<Mutex<ChannelWrite>>,
    requests: Arc<futures::lock::Mutex<bool>>,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Channel<C> {
    pub(crate) fn new(
        inner: ssh2::Channel,
        ctx: C,
        requests: Arc<futures::lock::Mutex<bool>>,
    ) -> Self {
        Self {
            inner,
            requests,
            writes: Arc::new(Mutex::new(ChannelWrite::default())),
            read_ctx: ctx.clone(),
            ctx,
        }
    }

    /// Returns the wrapped inner.
    pub fn into_inner(self) -> ssh2::Channel {
        self.inner
    }

    /// Returns a reference to the wrapped inner.
    pub const fn as_inner(&self) -> &ssh2::Channel {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped inner.
    pub const fn as_inner_mut(&mut self) -> &mut ssh2::Channel {
        &mut self.inner
    }
}

/// Async wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> Channel<C> {
    async fn flush_pending(&self) -> Result<(), Error> {
        futures::future::poll_fn(|cx| {
            self.ctx
                .with_poll(cx, || self.writes.lock().unwrap().flush())
        })
        .await
        .map_err(|_| {
            Error::new(
                crate::consts::ERROR_BAD_SOCKET,
                "failed completing pending channel write",
            )
        })
    }

    async fn with_request<T: Send>(
        &mut self,
        mut func: impl FnMut(&mut ssh2::Channel) -> Result<T, Error> + Send,
    ) -> Result<T, Error> {
        crate::runtime::request::with_request(&self.ctx, &self.requests, || func(&mut self.inner))
            .await
    }

    pub async fn setenv(&mut self, var: &str, val: &str) -> Result<(), Error> {
        self.with_request(|channel| channel.setenv(var, val)).await
    }

    pub async fn request_pty(
        &mut self,
        term: &str,
        mode: Option<PtyModes>,
        dim: Option<(u32, u32, u32, u32)>,
    ) -> Result<(), Error> {
        self.with_request(|channel| channel.request_pty(term, mode.clone(), dim))
            .await
    }

    pub async fn request_pty_size(
        &mut self,
        width: u32,
        height: u32,
        width_px: Option<u32>,
        height_px: Option<u32>,
    ) -> Result<(), Error> {
        self.with_request(|channel| channel.request_pty_size(width, height, width_px, height_px))
            .await
    }

    pub async fn request_auth_agent_forwarding(&mut self) -> Result<(), Error> {
        self.with_request(ssh2::Channel::request_auth_agent_forwarding)
            .await
    }

    pub async fn exec(&mut self, command: &str) -> Result<(), Error> {
        self.with_request(|channel| channel.exec(command)).await
    }

    pub async fn shell(&mut self) -> Result<(), Error> {
        self.with_request(ssh2::Channel::shell).await
    }

    pub async fn subsystem(&mut self, system: &str) -> Result<(), Error> {
        self.with_request(|channel| channel.subsystem(system)).await
    }

    pub async fn process_startup(
        &mut self,
        request: &str,
        message: Option<&str>,
    ) -> Result<(), Error> {
        self.with_request(|channel| channel.process_startup(request, message))
            .await
    }

    pub async fn handle_extended_data(&mut self, mode: ExtendedData) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.handle_extended_data(mode))
            .await
    }

    pub async fn adjust_receive_window(&mut self, adjust: u64, force: bool) -> Result<u64, Error> {
        self.ctx
            .with_async(|| self.inner.adjust_receive_window(adjust, force))
            .await
    }

    pub async fn send_eof(&mut self) -> Result<(), Error> {
        self.flush_pending().await?;
        self.ctx.with_async(|| self.inner.send_eof()).await
    }

    pub async fn wait_eof(&mut self) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.wait_eof()).await
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.flush_pending().await?;
        self.ctx.with_async(|| self.inner.close()).await
    }

    pub async fn wait_close(&mut self) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.wait_close()).await
    }
}

/// Sync wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> Channel<C> {
    pub fn stderr(&self) -> Stream<C> {
        self.stream(1)
    }

    pub fn stream(&self, stream_id: i32) -> Stream<C> {
        Stream::new(
            self.inner.stream(stream_id),
            self.ctx.clone(),
            self.inner.clone(),
            stream_id,
            self.writes.clone(),
        )
    }

    pub fn exit_status(&self) -> Result<i32, Error> {
        self.inner.exit_status()
    }

    pub fn exit_signal(&self) -> Result<ExitSignal, Error> {
        self.inner.exit_signal()
    }

    pub fn read_window(&self) -> ReadWindow {
        self.inner.read_window()
    }

    pub fn write_window(&self) -> WriteWindow {
        self.inner.write_window()
    }

    pub fn eof(&self) -> bool {
        self.inner.eof()
    }
}

impl<C: RuntimeContext + Unpin> AsyncRead for Channel<C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.read_ctx
            .with_poll(cx, || this.inner.stream(0).read(buf))
    }
}

impl<C: RuntimeContext + Unpin> AsyncWrite for Channel<C> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.ctx.with_poll(cx, || {
            this.writes
                .lock()
                .unwrap()
                .write(0, this.inner.stream(0), buf)
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.ctx
            .with_poll(cx, || this.writes.lock().unwrap().flush())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(
            this.ctx
                .with_poll(cx, || this.writes.lock().unwrap().flush())
        )?;
        this.ctx
            .with_poll(cx, || this.inner.close().map_err(io::Error::from))
    }
}

/// Async wrapper for [`ssh2::Stream`].
///
/// Pending writes are shared with the channel and its other streams. Flush
/// before extracting or using native handles.
pub struct Stream<C: RuntimeContext> {
    inner: ssh2::Stream,
    channel: ssh2::Channel,
    ctx: C,
    read_ctx: C,
    id: i32,
    writes: Arc<Mutex<ChannelWrite>>,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Stream<C> {
    fn new(
        inner: ssh2::Stream,
        ctx: C,
        channel: ssh2::Channel,
        stream_id: i32,
        writes: Arc<Mutex<ChannelWrite>>,
    ) -> Self {
        Self {
            inner,
            channel,
            id: stream_id,
            writes,
            read_ctx: ctx.clone(),
            ctx,
        }
    }

    /// Returns the wrapped inner.
    pub fn into_inner(self) -> ssh2::Stream {
        self.inner
    }

    /// Returns a reference to the wrapped inner.
    pub const fn as_inner(&self) -> &ssh2::Stream {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped inner.
    pub const fn as_inner_mut(&mut self) -> &mut ssh2::Stream {
        &mut self.inner
    }
}

impl<C: RuntimeContext + Unpin> AsyncRead for Stream<C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.read_ctx.with_poll(cx, || this.inner.read(buf))
    }
}

impl<C: RuntimeContext + Unpin> AsyncWrite for Stream<C> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.ctx.with_poll(cx, || {
            this.writes
                .lock()
                .unwrap()
                .write(this.id, this.inner.clone(), buf)
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.ctx
            .with_poll(cx, || this.writes.lock().unwrap().flush())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(
            this.ctx
                .with_poll(cx, || this.writes.lock().unwrap().flush())
        )?;
        this.ctx
            .with_poll(cx, || this.channel.send_eof().map_err(io::Error::from))
    }
}

#[derive(Default)]
struct ChannelWrite {
    stream: Option<(i32, ssh2::Stream)>,
    request: WriteRequest,
}

impl ChannelWrite {
    fn flush(&mut self) -> io::Result<()> {
        if let Some((_, stream)) = &mut self.stream {
            self.request.flush(|bytes| stream.write(bytes))?;
        }
        self.stream = None;
        Ok(())
    }

    fn write(&mut self, id: i32, stream: ssh2::Stream, bytes: &[u8]) -> io::Result<usize> {
        if self
            .stream
            .as_ref()
            .is_some_and(|(previous, _)| *previous != id)
        {
            self.flush()?;
        }
        let (_, stream) = self.stream.get_or_insert((id, stream));
        let result = self
            .request
            .write(bytes, 32_700, |bytes| stream.write(bytes));
        if !self.request.is_pending() {
            self.stream = None;
        }
        result
    }
}
