use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
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
use crate::write::WriteRequest;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Read,
    Write,
    Seek,
    Close,
    Request,
}

#[derive(Default)]
struct Requests {
    pending: Mutex<Option<(Arc<()>, Operation)>>,
}

impl Requests {
    fn run<T, E: From<Error>>(
        &self,
        owner: &Arc<()>,
        operation: Operation,
        func: impl FnOnce() -> Result<T, E>,
        would_block: impl FnOnce(&E) -> bool,
    ) -> Result<T, E> {
        let mut pending = self.pending.lock().unwrap();
        if pending
            .as_ref()
            .is_some_and(|(previous, kind)| !Arc::ptr_eq(previous, owner) || *kind != operation)
        {
            return Err(Error::new(
                ssh2::ErrorCode::Session(-39),
                "unfinished SFTP operation; resume it before starting another",
            )
            .into());
        }
        let result = func();
        *pending = if result.as_ref().is_err_and(would_block) {
            Some((owner.clone(), operation))
        } else {
            None
        };
        drop(pending);
        result
    }
}

async fn with_request<C: RuntimeContext, T: Send>(
    ctx: &C,
    requests: &Requests,
    mut func: impl FnMut() -> Result<T, Error> + Send,
) -> Result<T, Error> {
    let owner = Arc::new(());
    ctx.with_async(|| {
        requests.run(&owner, Operation::Request, &mut func, |error| {
            error.code() == crate::consts::ERROR_EAGAIN
        })
    })
    .await
}

fn would_block(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
}

/// Async wrapper for [`ssh2::Sftp`].
///
/// libssh2 shares nonblocking request state across SFTP handles. Only one
/// operation may be pending at a time; conflicting calls return an error
/// rather than waiting for an abandoned caller. Finish that operation before
/// retrying. If a metadata/open future is cancelled, recreate this SFTP object;
/// other SSH channels remain usable.
pub struct Sftp<C: RuntimeContext> {
    inner: ssh2::Sftp,
    ctx: C,
    requests: Arc<Requests>,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Sftp<C> {
    pub(crate) fn new(inner: ssh2::Sftp, ctx: C) -> Self {
        Self {
            inner,
            ctx,
            requests: Arc::new(Requests::default()),
        }
    }

    /// Returns the wrapped inner.
    pub fn into_inner(self) -> ssh2::Sftp {
        self.inner
    }

    /// Returns a reference to the wrapped inner.
    pub const fn as_inner(&self) -> &ssh2::Sftp {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped inner.
    pub const fn as_inner_mut(&mut self) -> &mut ssh2::Sftp {
        &mut self.inner
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
        let file = with_request(&self.ctx, &self.requests, || {
            self.inner.open_mode(filename, flags, mode, open_type)
        })
        .await?;
        Ok(File::new(file, self.ctx.clone(), self.requests.clone()))
    }

    pub async fn open<T: AsRef<Path>>(&self, filename: T) -> Result<File<C>, Error> {
        let filename = filename.as_ref();
        let file = with_request(&self.ctx, &self.requests, || self.inner.open(filename)).await?;
        Ok(File::new(file, self.ctx.clone(), self.requests.clone()))
    }

    pub async fn create(&self, filename: &Path) -> Result<File<C>, Error> {
        let file = with_request(&self.ctx, &self.requests, || self.inner.create(filename)).await?;
        Ok(File::new(file, self.ctx.clone(), self.requests.clone()))
    }

    pub async fn opendir<T: AsRef<Path>>(&self, dirname: T) -> Result<File<C>, Error> {
        let dirname = dirname.as_ref();
        let file = with_request(&self.ctx, &self.requests, || self.inner.opendir(dirname)).await?;
        Ok(File::new(file, self.ctx.clone(), self.requests.clone()))
    }

    pub async fn readdir<T: AsRef<Path> + Send>(
        &self,
        dirname: T,
    ) -> Result<Vec<(PathBuf, FileStat)>, Error> {
        let dirname = dirname.as_ref();
        let mut directory = self.opendir(dirname).await?;
        let mut entries = Vec::new();
        let result = loop {
            match directory.readdir().await {
                Ok((name, stat)) if name != Path::new(".") && name != Path::new("..") => {
                    entries.push((dirname.join(name), stat));
                }
                Ok(_) => {}
                Err(error) if error.code() == ssh2::ErrorCode::Session(-16) => break Ok(entries),
                Err(error) => break Err(error),
            }
        };
        let closed = directory.close().await;
        let entries = result?;
        closed?;
        Ok(entries)
    }

    pub async fn mkdir(&self, filename: &Path, mode: i32) -> Result<(), Error> {
        with_request(&self.ctx, &self.requests, || {
            self.inner.mkdir(filename, mode)
        })
        .await
    }

    pub async fn rmdir(&self, filename: &Path) -> Result<(), Error> {
        with_request(&self.ctx, &self.requests, || self.inner.rmdir(filename)).await
    }

    pub async fn stat(&self, filename: &Path) -> Result<FileStat, Error> {
        with_request(&self.ctx, &self.requests, || self.inner.stat(filename)).await
    }

    pub async fn lstat(&self, filename: &Path) -> Result<FileStat, Error> {
        with_request(&self.ctx, &self.requests, || self.inner.lstat(filename)).await
    }

    pub async fn setstat(&self, filename: &Path, stat: FileStat) -> Result<(), Error> {
        with_request(&self.ctx, &self.requests, || {
            self.inner.setstat(filename, stat.clone())
        })
        .await
    }

    pub async fn symlink(&self, path: &Path, target: &Path) -> Result<(), Error> {
        with_request(&self.ctx, &self.requests, || {
            self.inner.symlink(path, target)
        })
        .await
    }

    pub async fn readlink(&self, path: &Path) -> Result<PathBuf, Error> {
        with_request(&self.ctx, &self.requests, || self.inner.readlink(path)).await
    }

    pub async fn realpath(&self, path: &Path) -> Result<PathBuf, Error> {
        with_request(&self.ctx, &self.requests, || self.inner.realpath(path)).await
    }

    pub async fn rename(
        &self,
        src: &Path,
        dst: &Path,
        flags: Option<RenameFlags>,
    ) -> Result<(), Error> {
        with_request(&self.ctx, &self.requests, || {
            self.inner.rename(src, dst, flags)
        })
        .await
    }

    pub async fn unlink(&self, file: &Path) -> Result<(), Error> {
        with_request(&self.ctx, &self.requests, || self.inner.unlink(file)).await
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        with_request(&self.ctx, &self.requests, || self.inner.shutdown()).await
    }
}

/// Async wrapper for [`ssh2::File`].
///
/// Unfinished native writes retain their payload across polls. Flush before
/// extracting or using native handles. File reads, seeks, metadata requests,
/// and close complete that file's pending write first.
///
/// A pending read must be resumed on the same file before another operation on
/// this SFTP object. Dropping a borrowing read future does not cancel libssh2's
/// request. Finish pending operations before extracting native handles.
pub struct File<C: RuntimeContext> {
    inner: ssh2::File,
    ctx: C,
    read_ctx: C,
    writes: WriteRequest,
    requests: Arc<Requests>,
    owner: Arc<()>,
    seek_target: Option<SeekFrom>,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> File<C> {
    fn new(inner: ssh2::File, ctx: C, requests: Arc<Requests>) -> Self {
        Self {
            inner,
            read_ctx: ctx.clone(),
            ctx,
            writes: WriteRequest::default(),
            requests,
            owner: Arc::new(()),
            seek_target: None,
        }
    }

    /// Returns the wrapped inner.
    pub fn into_inner(self) -> ssh2::File {
        self.inner
    }

    /// Returns a reference to the wrapped inner.
    pub const fn as_inner(&self) -> &ssh2::File {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped inner.
    pub const fn as_inner_mut(&mut self) -> &mut ssh2::File {
        &mut self.inner
    }
}

/// Async wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> File<C> {
    fn poll_io<T>(
        &mut self,
        cx: &mut Context<'_>,
        operation: Operation,
        mut func: impl FnMut(&mut ssh2::File, &mut WriteRequest) -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        let ctx = if operation == Operation::Read {
            &self.read_ctx
        } else {
            &self.ctx
        };
        ctx.with_poll(cx, || {
            self.requests.run(
                &self.owner,
                operation,
                || func(&mut self.inner, &mut self.writes),
                would_block,
            )
        })
    }

    fn poll_flush_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.writes.is_pending() {
            return Poll::Ready(Ok(()));
        }
        self.poll_io(cx, Operation::Write, |file, writes| {
            writes.flush(|bytes| file.write(bytes))
        })
    }

    async fn flush_pending(&mut self) -> Result<(), Error> {
        futures::future::poll_fn(|cx| self.poll_flush_pending(cx))
            .await
            .map_err(|_| {
                Error::new(
                    crate::consts::ERROR_BAD_SOCKET,
                    "failed completing pending file write",
                )
            })
    }

    pub async fn setstat(&mut self, stat: FileStat) -> Result<(), Error> {
        self.flush_pending().await?;
        with_request(&self.ctx, &self.requests, || {
            self.inner.setstat(stat.clone())
        })
        .await
    }

    pub async fn stat(&mut self) -> Result<FileStat, Error> {
        self.flush_pending().await?;
        with_request(&self.ctx, &self.requests, || self.inner.stat()).await
    }

    pub async fn readdir(&mut self) -> Result<(PathBuf, FileStat), Error> {
        self.flush_pending().await?;
        with_request(&self.ctx, &self.requests, || self.inner.readdir()).await
    }

    pub async fn fsync(&mut self) -> Result<(), Error> {
        self.flush_pending().await?;
        with_request(&self.ctx, &self.requests, || self.inner.fsync()).await
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.flush_pending().await?;
        self.ctx
            .with_async(|| {
                self.requests.run(
                    &self.owner,
                    Operation::Close,
                    || self.inner.close(),
                    |error| error.code() == crate::consts::ERROR_EAGAIN,
                )
            })
            .await
    }
}

impl<C: RuntimeContext + Unpin> AsyncRead for File<C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_flush_pending(cx))?;
        this.poll_io(cx, Operation::Read, |file, _| file.read(buf))
    }
}

impl<C: RuntimeContext + Unpin> AsyncWrite for File<C> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // Stay within one native SFTP packet, avoiding unreported pipeline data.
        self.get_mut()
            .poll_io(cx, Operation::Write, |file, writes| {
                writes.write(buf, 16 * 1024, |bytes| file.write(bytes))
            })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_flush_pending(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_flush_pending(cx))?;
        this.poll_io(cx, Operation::Close, |file, _| {
            file.close().map_err(io::Error::from)
        })
    }
}

impl<C: RuntimeContext + Unpin> AsyncSeek for File<C> {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_flush_pending(cx))?;
        if this.seek_target.is_some_and(|target| target != pos) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resume the pending seek before changing its target",
            )));
        }
        let (base, offset) = match pos {
            SeekFrom::Start(position) => {
                return this.poll_io(cx, Operation::Seek, |file, _| {
                    file.seek(SeekFrom::Start(position))
                });
            }
            SeekFrom::Current(offset) => (
                std::task::ready!(
                    this.poll_io(cx, Operation::Seek, |file, _| file.stream_position())
                )?,
                offset,
            ),
            SeekFrom::End(offset) => {
                this.seek_target = Some(pos);
                let result = std::task::ready!(this.poll_io(cx, Operation::Seek, |file, _| {
                    file.stat().map_err(io::Error::from)
                }));
                this.seek_target = None;
                let Some(size) = result?.size else {
                    return Poll::Ready(Err(io::Error::other("file size unavailable")));
                };
                (size, offset)
            }
        };
        let Some(position) = base.checked_add_signed(offset) else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek offset",
            )));
        };
        this.poll_io(cx, Operation::Seek, |file, _| {
            file.seek(SeekFrom::Start(position))
        })
    }
}
