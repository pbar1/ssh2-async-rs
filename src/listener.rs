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
