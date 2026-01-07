use std::path::Path;

use ssh2::BlockDirections;
use ssh2::DisconnectCode;
use ssh2::Error;
use ssh2::MethodType;
use ssh2::ScpFileStat;
use ssh2::TraceFlags;

use crate::Agent;
use crate::Channel;
use crate::Listener;
use crate::RuntimeContext;
use crate::Sftp;

/// Async wrapper for [`ssh2::Session`].
pub struct Session<C: RuntimeContext> {
    inner: ssh2::Session,
    ctx: C,
}
