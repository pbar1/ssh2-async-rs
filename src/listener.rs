use ssh2::Error;

use crate::Channel;
use crate::RuntimeContext;

/// Async wrapper for [`ssh2::Listener`].
pub struct Listener<C: RuntimeContext> {
    inner: ssh2::Listener,
    ctx: C,
}

/// Constructors and non-wrapping functions
impl<C: RuntimeContext> Listener<C> {
    pub(crate) const fn new(inner: ssh2::Listener, ctx: C) -> Self {
        Self { inner, ctx }
    }

    /// Returns the wrapped inner.
    pub fn into_inner(self) -> ssh2::Listener {
        self.inner
    }

    /// Returns a reference to the wrapped inner.
    pub const fn as_inner(&self) -> &ssh2::Listener {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped inner.
    pub const fn as_inner_mut(&mut self) -> &mut ssh2::Listener {
        &mut self.inner
    }
}

/// Async wrappers
#[allow(clippy::missing_errors_doc)]
impl<C: RuntimeContext> Listener<C> {
    /// See [`ssh2::Listener::accept`]
    pub async fn accept(&mut self) -> Result<Channel<C>, Error> {
        let channel = self.ctx.with_async(|| self.inner.accept()).await?;
        Ok(Channel::new(channel, self.ctx.clone()))
    }
}
