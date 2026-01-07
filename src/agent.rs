use ssh2::Error;
use ssh2::PublicKey;

use crate::RuntimeContext;

/// Async wrapper for [`ssh2::Agent`].
pub struct Agent<C: RuntimeContext> {
    inner: ssh2::Agent,
    session: ssh2::Session,
    ctx: C,
}
