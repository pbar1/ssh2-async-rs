use std::io;
use std::io::Read;
use std::io::Write;
use std::pin::Pin;
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

/// Async wrapper for [`ssh2::Channel`].
pub struct Channel<C: RuntimeContext> {
    inner: ssh2::Channel,
    ctx: C,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Channel<C> {
    pub(crate) const fn new(inner: ssh2::Channel, ctx: C) -> Self {
        Self { inner, ctx }
    }
}

/// Async wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> Channel<C> {
    pub async fn setenv(&mut self, var: &str, val: &str) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.setenv(var, val)).await
    }

    pub async fn request_pty(
        &mut self,
        term: &str,
        mode: Option<PtyModes>,
        dim: Option<(u32, u32, u32, u32)>,
    ) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.request_pty(term, mode.clone(), dim))
            .await
    }

    pub async fn request_pty_size(
        &mut self,
        width: u32,
        height: u32,
        width_px: Option<u32>,
        height_px: Option<u32>,
    ) -> Result<(), Error> {
        self.ctx
            .with_async(|| {
                self.inner
                    .request_pty_size(width, height, width_px, height_px)
            })
            .await
    }

    pub async fn request_auth_agent_forwarding(&mut self) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.request_auth_agent_forwarding())
            .await
    }

    pub async fn exec(&mut self, command: &str) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.exec(command)).await
    }

    pub async fn shell(&mut self) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.shell()).await
    }

    pub async fn subsystem(&mut self, system: &str) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.subsystem(system)).await
    }

    pub async fn process_startup(
        &mut self,
        request: &str,
        message: Option<&str>,
    ) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.process_startup(request, message))
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
        self.ctx.with_async(|| self.inner.send_eof()).await
    }

    pub async fn wait_eof(&mut self) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.wait_eof()).await
    }

    pub async fn close(&mut self) -> Result<(), Error> {
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
        Stream::new(self.inner.stderr(), self.ctx.clone())
    }

    pub fn stream(&self, stream_id: i32) -> Stream<C> {
        Stream::new(self.inner.stream(stream_id), self.ctx.clone())
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
        this.ctx.with_poll(cx, || this.inner.stream(0).read(buf))
    }
}

impl<C: RuntimeContext + Unpin> AsyncWrite for Channel<C> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.ctx.with_poll(cx, || this.inner.stream(0).write(buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.ctx.with_poll(cx, || this.inner.stream(0).flush())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.ctx
            .with_poll(cx, || this.inner.close().map_err(io::Error::other))
    }
}

/// Async wrapper for [`ssh2::Stream`].
pub struct Stream<C: RuntimeContext> {
    inner: ssh2::Stream,
    ctx: C,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Stream<C> {
    pub(crate) const fn new(inner: ssh2::Stream, ctx: C) -> Self {
        Self { inner, ctx }
    }
}

impl<C: RuntimeContext + Unpin> AsyncRead for Stream<C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.ctx.with_poll(cx, || this.inner.read(buf))
    }
}

impl<C: RuntimeContext + Unpin> AsyncWrite for Stream<C> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.ctx.with_poll(cx, || this.inner.write(buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.ctx.with_poll(cx, || this.inner.flush())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
