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
use crate::session;

/// Async wrapper for [`ssh2::Channel`].
pub struct Channel<C: RuntimeContext> {
    inner: ssh2::Channel,
    session: ssh2::Session,
    ctx: C,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Channel<C> {
    pub(crate) const fn new(inner: ssh2::Channel, session: ssh2::Session, ctx: C) -> Self {
        Self {
            inner,
            session,
            ctx,
        }
    }
}

/// Async wrappers
impl<C: RuntimeContext> Channel<C> {
    // setenv
    // request_pty
    // request_pty_size
    // request_auth_agent_forwarding
    // exec
    // shell
    // subsystem
    // process_startup
    // handle_extended_data
    // adjust_receive_window
    // send_eof
    // wait_eof
    // close
    // wait_close
}

/// Sync wrappers
impl<C: RuntimeContext> Channel<C> {
    // stderr
    // stream
    // exit_status
    // exit_signal
    // read_window
    // write_window
    // eof
}

// TODO: AsyncRead + AsyncWrite

/// Async wrapper for [`ssh2::Stream`].
pub struct Stream<C: RuntimeContext> {
    inner: ssh2::Stream,
    session: ssh2::Session,
    ctx: C,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Stream<C> {
    pub(crate) const fn new(inner: ssh2::Stream, session: ssh2::Session, ctx: C) -> Self {
        Self {
            inner,
            session,
            ctx,
        }
    }
}

// TODO: AsyncRead + AsyncWrite
