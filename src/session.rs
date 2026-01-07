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

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Session<C> {
    /// Wraps an [`ssh2::Session`] with async bindings.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime context cannot be created from blocking
    /// session. This can happen if the transport has not yet been set with
    /// [`ssh2::Session::set_tcp_stream`].
    pub fn from_blocking(inner: ssh2::Session) -> Result<Self, Error> {
        inner.set_blocking(false);
        let ctx = C::new(&inner)?;
        Ok(Self { inner, ctx })
    }

    /// Creates an async session from the given transport stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying `libssh2` library has not been
    /// initialized (unlikely, as [`ssh2`] does this automatically).
    #[cfg(unix)]
    pub fn from_stream(stream: impl std::os::fd::AsRawFd + 'static) -> Result<Self, Error> {
        let mut inner = ssh2::Session::new()?;
        inner.set_tcp_stream(stream);
        Self::from_blocking(inner)
    }
}

/// Async wrappers
impl<C: RuntimeContext> Session<C> {
    // handshake
    // userauth_password
    // userauth_pubkey_file
    // userauth_pubkey_memory
    // userauth_hostbased_file
    // userauth_keyboard_interactive
    // userauth_agent
    // auth_methods
    // method_pref
    // supported_algs
    // channel_session
    // channel_direct_tcpip
    // channel_direct_streamlocal
    // channel_forward_listen
    // channel_open
    // scp_recv
    // scp_send
    // sftp
    // keepalive_send
    // disconnect
}

/// Sync wrappers
impl<C: RuntimeContext> Session<C> {
    // agent
    // known_hosts
    // host_key
    // host_key_hash
    // authenticated
    // is_blocking
    // banner
    // banner_bytes
    // timeout
    // methods
    // block_directions
    // set_keepalive
    // trace
    // userauth_banner
}
