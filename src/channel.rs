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
    session: ssh2::Session,
    ctx: C,
}

/// Async wrapper for [`ssh2::Stream`].
pub struct Stream<C: RuntimeContext> {
    inner: ssh2::Stream,
    session: ssh2::Session,
    ctx: C,
}
