use ssh2::Error;

use crate::runtime::Runtime;

/// Async wrapper for [`ssh2::Session`].
pub struct Session<R: Runtime> {
    inner: ssh2::Session,
    ctx: R::Context,
}

impl<R: Runtime> Session<R> {
    pub async fn handshake(&mut self) -> Result<(), Error> {
        R::with_async_mut(&self.ctx, &mut self.inner, ssh2::Session::handshake).await
    }

    pub async fn userauth_password(&self, username: &str, password: &str) -> Result<(), Error> {
        R::with_async(&self.ctx, &self.inner, |session| {
            session.userauth_password(username, password)
        })
        .await
    }
}
