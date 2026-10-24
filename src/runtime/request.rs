use futures::lock::Mutex;
use ssh2::Error;

use super::RuntimeContext;
use crate::consts::ERROR_EAGAIN;

// libssh2's reply waiter can fail on another channel's reply of the same type.
// Keep requesting controls serial across the session, without gating data I/O,
// EOF observers, or close. A cancelled request leaves its native state pending.
pub async fn with_request<C: RuntimeContext, T: Send>(
    ctx: &C,
    requests: &Mutex<bool>,
    mut func: impl FnMut() -> Result<T, Error> + Send,
) -> Result<T, Error> {
    let mut pending = requests.lock().await;
    if *pending {
        return Err(Error::new(
            ssh2::ErrorCode::Session(-39),
            "previous control request was cancelled; recreate the session",
        ));
    }
    ctx.with_async(|| {
        let result = func();
        *pending = matches!(&result, Err(error) if error.code() == ERROR_EAGAIN);
        result
    })
    .await
}
