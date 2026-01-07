//! Runtime-agnostic async wrapper for [`ssh2`].

#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::missing_errors_doc)]

mod agent;
mod channel;
mod runtime;
mod session;
mod sftp;

pub use ssh2;

pub use self::runtime::RuntimeContext;
#[cfg(feature = "tokio")]
pub use self::runtime::TokioContext;

/// Copy constants to avoid depending directly on `libssh2-sys`, as `ssh2`
/// already does - doing so would make the dependency solver more likely to
/// fail.
mod consts {
    use ssh2::ErrorCode;

    pub const ERROR_EAGAIN: ErrorCode = ErrorCode::Session(-37);
    pub const ERROR_BAD_SOCKET: ErrorCode = ErrorCode::Session(-45);
}
