use ssh2::ErrorCode;

// Copy constants to avoid depending directly on `libssh2-sys`, as `ssh2`
// already does - doing so would make the dependency solver more likely to fail.

pub const ERROR_EAGAIN: ErrorCode = ErrorCode::Session(-37);
pub const ERROR_SOCKET_SEND: ErrorCode = ErrorCode::Session(-7);
pub const ERROR_SOCKET_RECV: ErrorCode = ErrorCode::Session(-43);
pub const ERROR_BAD_SOCKET: ErrorCode = ErrorCode::Session(-45);
