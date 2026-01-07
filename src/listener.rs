use ssh2::Error;

use crate::Channel;
use crate::RuntimeContext;

/// Async wrapper for [`ssh2::Listener`].
pub struct Listener<C: RuntimeContext> {
    inner: ssh2::Listener,
    session: ssh2::Session,
    ctx: C,
}
