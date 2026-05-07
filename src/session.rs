use std::path::Path;

use ssh2::BlockDirections;
use ssh2::DisconnectCode;
use ssh2::Error;
use ssh2::HashType;
use ssh2::HostKeyType;
use ssh2::KeyboardInteractivePrompt;
use ssh2::KnownHosts;
use ssh2::MethodType;
use ssh2::ScpFileStat;
use ssh2::TraceFlags;

use crate::Agent;
use crate::Channel;
use crate::Listener;
use crate::RuntimeContext;
use crate::Sftp;
#[cfg(feature = "tokio")]
use crate::TokioContext;
use crate::consts::ERROR_BAD_SOCKET;
use crate::consts::ERROR_INVAL;

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
        let ctx = C::new(inner.clone())?;
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

    /// Returns the wrapped inner.
    pub fn into_inner(self) -> ssh2::Session {
        self.inner
    }

    /// Returns a reference to the wrapped inner.
    pub const fn as_inner(&self) -> &ssh2::Session {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped inner.
    pub const fn as_inner_mut(&mut self) -> &mut ssh2::Session {
        &mut self.inner
    }
}

#[cfg(all(feature = "tokio", unix))]
impl TryFrom<tokio::net::TcpStream> for Session<TokioContext> {
    type Error = Error;

    fn try_from(stream: tokio::net::TcpStream) -> Result<Self, Self::Error> {
        let stream = stream
            .into_std()
            .map_err(|_| Error::new(ERROR_BAD_SOCKET, "failed converting Tokio TCP stream"))?;
        Self::from_stream(stream)
    }
}

/// Async wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> Session<C> {
    pub async fn handshake(&mut self) -> Result<(), Error> {
        self.ctx.with_async(|| self.inner.handshake()).await
    }

    pub async fn userauth_password(&self, username: &str, password: &str) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.userauth_password(username, password))
            .await
    }

    pub async fn userauth_pubkey_file(
        &self,
        username: &str,
        pubkey: Option<&Path>,
        privatekey: &Path,
        passphrase: Option<&str>,
    ) -> Result<(), Error> {
        self.ctx
            .with_async(|| {
                self.inner
                    .userauth_pubkey_file(username, pubkey, privatekey, passphrase)
            })
            .await
    }

    pub async fn userauth_pubkey_memory(
        &self,
        username: &str,
        pubkeydata: Option<&str>,
        privatekeydata: &str,
        passphrase: Option<&str>,
    ) -> Result<(), Error> {
        self.ctx
            .with_async(|| {
                self.inner
                    .userauth_pubkey_memory(username, pubkeydata, privatekeydata, passphrase)
            })
            .await
    }

    pub async fn userauth_hostbased_file(
        &self,
        username: &str,
        publickey: &Path,
        privatekey: &Path,
        passphrase: Option<&str>,
        hostname: &str,
        local_username: Option<&str>,
    ) -> Result<(), Error> {
        self.ctx
            .with_async(|| {
                self.inner.userauth_hostbased_file(
                    username,
                    publickey,
                    privatekey,
                    passphrase,
                    hostname,
                    local_username,
                )
            })
            .await
    }

    pub async fn userauth_keyboard_interactive<P: KeyboardInteractivePrompt + Send>(
        &self,
        username: &str,
        prompter: &mut P,
    ) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.userauth_keyboard_interactive(username, prompter))
            .await
    }

    // It is unsound to simply wrap `ssh2::Session::userauth_agent` with async
    // like the rest of the methods since that would recreate the agent
    // connection if called with retries, which is not reentrant safe. Instead
    // we mimic the implementation in terms of our own async methods and avoid
    // duplicating the agent connection.
    pub async fn userauth_agent(&self, username: &str) -> Result<(), Error> {
        let mut agent = self.agent()?;
        agent.connect()?;
        agent.list_identities()?;
        let identities = agent.identities()?;
        let Some(identity) = identities.first() else {
            return Err(Error::new(
                ERROR_INVAL,
                "no identities found in the ssh agent",
            ));
        };
        agent.userauth(username, identity).await
    }

    pub async fn auth_methods(&self, username: &str) -> Result<&str, Error> {
        self.ctx
            .with_async(|| self.inner.auth_methods(username))
            .await
    }

    pub async fn method_pref(&self, method_type: MethodType, prefs: &str) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.method_pref(method_type, prefs))
            .await
    }

    pub async fn supported_algs(
        &self,
        method_type: MethodType,
    ) -> Result<Vec<&'static str>, Error> {
        self.ctx
            .with_async(|| self.inner.supported_algs(method_type))
            .await
    }

    pub async fn channel_session(&self) -> Result<Channel<C>, Error> {
        let channel = self.ctx.with_async(|| self.inner.channel_session()).await?;
        Ok(Channel::new(channel, self.ctx.clone()))
    }

    pub async fn channel_direct_tcpip(
        &self,
        host: &str,
        port: u16,
        src: Option<(&str, u16)>,
    ) -> Result<Channel<C>, Error> {
        let channel = self
            .ctx
            .with_async(|| self.inner.channel_direct_tcpip(host, port, src))
            .await?;
        Ok(Channel::new(channel, self.ctx.clone()))
    }

    pub async fn channel_direct_streamlocal(
        &self,
        socket_path: &str,
        src: Option<(&str, u16)>,
    ) -> Result<Channel<C>, Error> {
        let channel = self
            .ctx
            .with_async(|| self.inner.channel_direct_streamlocal(socket_path, src))
            .await?;
        Ok(Channel::new(channel, self.ctx.clone()))
    }

    pub async fn channel_forward_listen(
        &self,
        remote_port: u16,
        host: Option<&str>,
        queue_maxsize: Option<u32>,
    ) -> Result<(Listener<C>, u16), Error> {
        let (listener, port) = self
            .ctx
            .with_async(|| {
                self.inner
                    .channel_forward_listen(remote_port, host, queue_maxsize)
            })
            .await?;
        Ok((Listener::new(listener, self.ctx.clone()), port))
    }

    pub async fn channel_open(
        &self,
        channel_type: &str,
        window_size: u32,
        packet_size: u32,
        message: Option<&str>,
    ) -> Result<Channel<C>, Error> {
        let channel = self
            .ctx
            .with_async(|| {
                self.inner
                    .channel_open(channel_type, window_size, packet_size, message)
            })
            .await?;
        Ok(Channel::new(channel, self.ctx.clone()))
    }

    pub async fn scp_recv(&self, path: &Path) -> Result<(Channel<C>, ScpFileStat), Error> {
        let (channel, stat) = self.ctx.with_async(|| self.inner.scp_recv(path)).await?;
        Ok((Channel::new(channel, self.ctx.clone()), stat))
    }

    pub async fn scp_send(
        &self,
        remote_path: &Path,
        mode: i32,
        size: u64,
        times: Option<(u64, u64)>,
    ) -> Result<Channel<C>, Error> {
        let channel = self
            .ctx
            .with_async(|| self.inner.scp_send(remote_path, mode, size, times))
            .await?;
        Ok(Channel::new(channel, self.ctx.clone()))
    }

    pub async fn sftp(&self) -> Result<Sftp<C>, Error> {
        let sftp = self.ctx.with_async(|| self.inner.sftp()).await?;
        Ok(Sftp::new(sftp, self.ctx.clone()))
    }

    pub async fn keepalive_send(&self) -> Result<u32, Error> {
        self.ctx.with_async(|| self.inner.keepalive_send()).await
    }

    pub async fn disconnect(
        &self,
        reason: Option<DisconnectCode>,
        description: &str,
        lang: Option<&str>,
    ) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.disconnect(reason, description, lang))
            .await
    }
}

/// Sync wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> Session<C> {
    pub fn agent(&self) -> Result<Agent<C>, Error> {
        let agent = self.inner.agent()?;
        Ok(Agent::new(agent, self.ctx.clone()))
    }

    pub fn known_hosts(&self) -> Result<KnownHosts, Error> {
        self.inner.known_hosts()
    }

    pub fn host_key(&self) -> Option<(&[u8], HostKeyType)> {
        self.inner.host_key()
    }

    pub fn host_key_hash(&self, hash: HashType) -> Option<&[u8]> {
        self.inner.host_key_hash(hash)
    }

    pub fn authenticated(&self) -> bool {
        self.inner.authenticated()
    }

    pub fn is_blocking(&self) -> bool {
        self.inner.is_blocking()
    }

    pub fn banner(&self) -> Option<&str> {
        self.inner.banner()
    }

    pub fn banner_bytes(&self) -> Option<&[u8]> {
        self.inner.banner_bytes()
    }

    pub fn timeout(&self) -> u32 {
        self.inner.timeout()
    }

    pub fn methods(&self, method_type: MethodType) -> Option<&str> {
        self.inner.methods(method_type)
    }

    pub fn block_directions(&self) -> BlockDirections {
        self.inner.block_directions()
    }

    #[allow(clippy::semicolon_if_nothing_returned)]
    pub fn set_keepalive(&self, want_reply: bool, interval: u32) {
        self.inner.set_keepalive(want_reply, interval)
    }

    #[allow(clippy::semicolon_if_nothing_returned)]
    pub fn trace(&self, bitmask: TraceFlags) {
        self.inner.trace(bitmask)
    }

    pub fn userauth_banner(&self) -> Result<Option<&str>, Error> {
        self.inner.userauth_banner()
    }
}
