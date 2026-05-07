use ssh2::Error;
use ssh2::PublicKey;

use crate::RuntimeContext;

/// Async wrapper for [`ssh2::Agent`].
pub struct Agent<C: RuntimeContext> {
    inner: ssh2::Agent,
    ctx: C,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Agent<C> {
    pub(crate) const fn new(inner: ssh2::Agent, ctx: C) -> Self {
        Self { inner, ctx }
    }

    /// Returns the wrapped inner.
    pub fn into_inner(self) -> ssh2::Agent {
        self.inner
    }

    /// Returns a reference to the wrapped inner.
    pub const fn as_inner(&self) -> &ssh2::Agent {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped inner.
    pub const fn as_inner_mut(&mut self) -> &mut ssh2::Agent {
        &mut self.inner
    }
}

/// Async wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> Agent<C> {
    /// See [`ssh2::Agent::userauth`]
    pub async fn userauth(&self, username: &str, identity: &PublicKey) -> Result<(), Error> {
        self.ctx
            .with_async(|| self.inner.userauth(username, identity))
            .await
    }
}

/// Sync wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> Agent<C> {
    // FIXME: Async
    /// See [`ssh2::Agent::connect`]
    pub fn connect(&mut self) -> Result<(), Error> {
        self.inner.connect()
    }

    // FIXME: Async
    /// See [`ssh2::Agent::disconnect`]
    pub fn disconnect(&mut self) -> Result<(), Error> {
        self.inner.disconnect()
    }

    // FIXME: Async
    /// See [`ssh2::Agent::list_identities`]
    pub fn list_identities(&mut self) -> Result<(), Error> {
        self.inner.list_identities()
    }

    /// See [`ssh2::Agent::identities`]
    pub fn identities(&self) -> Result<Vec<PublicKey>, Error> {
        self.inner.identities()
    }
}
