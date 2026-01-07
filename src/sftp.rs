use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::io::AsyncRead;
use futures::io::AsyncSeek;
use futures::io::AsyncWrite;
use ssh2::Error;
use ssh2::FileStat;
use ssh2::OpenFlags;
use ssh2::OpenType;
use ssh2::RenameFlags;

use crate::RuntimeContext;

/// Async wrapper for [`ssh2::Sftp`].
pub struct Sftp<C: RuntimeContext> {
    inner: ssh2::Sftp,
    ctx: C,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Sftp<C> {
    pub(crate) const fn new(inner: ssh2::Sftp, ctx: C) -> Self {
        Self { inner, ctx }
    }
}

/// Async wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> Sftp<C> {
    pub async fn open_mode<T: AsRef<Path> + Clone + Send>(
        &self,
        filename: T,
        flags: OpenFlags,
        mode: i32,
        open_type: OpenType,
    ) -> Result<File<C>, Error> {
        let filename = filename.as_ref();
        let file = self
            .ctx
            .with_async(|| self.inner.open_mode(filename, flags, mode, open_type))
            .await?;
        Ok(File::new(file, self.ctx.clone()))
    }

    pub async fn open<T: AsRef<Path>>(&self, filename: T) -> Result<File<C>, Error> {
        let filename = filename.as_ref();
        let file = self.ctx.with_async(|| self.inner.open(filename)).await?;
        Ok(File::new(file, self.ctx.clone()))
    }

    pub async fn create(&self, filename: &Path) -> Result<File<C>, Error> {
        let file = self.ctx.with_async(|| self.inner.create(filename)).await?;
        Ok(File::new(file, self.ctx.clone()))
    }

    pub async fn opendir<T: AsRef<Path>>(&self, dirname: T) -> Result<File<C>, Error> {
        let dirname = dirname.as_ref();
        let file = self.ctx.with_async(|| self.inner.opendir(dirname)).await?;
        Ok(File::new(file, self.ctx.clone()))
    }

    pub async fn readdir<T: AsRef<Path> + Send>(
        &self,
        dirname: T,
    ) -> Result<Vec<(PathBuf, FileStat)>, Error> {
        let dirname = dirname.as_ref();
        self.ctx.with_async(|| self.inner.readdir(dirname)).await
    }

    pub async fn mkdir(&self, filename: &Path, mode: i32) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.mkdir(filename, mode))
            .await
    }

    pub async fn rmdir(&self, filename: &Path) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.rmdir(filename)).await
    }

    pub async fn stat(&self, filename: &Path) -> Result<FileStat, Error> {
        self.ctx.with_async(|| self.inner.stat(filename)).await
    }

    pub async fn lstat(&self, filename: &Path) -> Result<FileStat, Error> {
        self.ctx.with_async(|| self.inner.lstat(filename)).await
    }

    pub async fn setstat(&self, filename: &Path, stat: FileStat) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.setstat(filename, stat.clone()))
            .await
    }

    pub async fn symlink(&self, path: &Path, target: &Path) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.symlink(path, target))
            .await
    }

    pub async fn readlink(&self, path: &Path) -> Result<PathBuf, Error> {
        self.ctx.with_async(|| self.inner.readlink(path)).await
    }

    pub async fn realpath(&self, path: &Path) -> Result<PathBuf, Error> {
        self.ctx.with_async(|| self.inner.realpath(path)).await
    }

    pub async fn rename(
        &self,
        src: &Path,
        dst: &Path,
        flags: Option<RenameFlags>,
    ) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.rename(src, dst, flags))
            .await
    }

    pub async fn unlink(&self, file: &Path) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.unlink(file)).await
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.shutdown()).await
    }
}

/// Async wrapper for [`ssh2::File`].
pub struct File<C: RuntimeContext> {
    inner: ssh2::File,
    ctx: C,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> File<C> {
    pub(crate) const fn new(inner: ssh2::File, ctx: C) -> Self {
        Self { inner, ctx }
    }
}

/// Async wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> File<C> {
    pub async fn setstat(&mut self, stat: FileStat) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.setstat(stat.clone()))
            .await
    }

    pub async fn stat(&mut self) -> Result<FileStat, Error> {
        self.ctx.with_async(|| self.inner.stat()).await
    }

    pub async fn readdir(&mut self) -> Result<(PathBuf, FileStat), Error> {
        self.ctx.with_async(|| self.inner.readdir()).await
    }

    pub async fn fsync(&mut self) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.fsync()).await
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.close()).await
    }
}

impl<C: RuntimeContext + Unpin> AsyncRead for File<C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.ctx.with_poll(cx, || this.inner.read(buf))
    }
}

impl<C: RuntimeContext + Unpin> AsyncWrite for File<C> {
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

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.ctx
            .with_poll(cx, || this.inner.close().map_err(io::Error::other))
    }
}

impl<C: RuntimeContext + Unpin> AsyncSeek for File<C> {
    fn poll_seek(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        // Seek is a local operation in libssh2, there is no network IO
        let this = self.get_mut();
        Poll::Ready(this.inner.seek(pos))
    }
}
