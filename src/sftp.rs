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
    session: ssh2::Session,
    ctx: C,
}

/// Async wrapper for [`ssh2::File`].
pub struct File<C: RuntimeContext> {
    inner: ssh2::File,
    session: ssh2::Session,
    ctx: C,
}
