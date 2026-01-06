use ssh2::ErrorCode;

pub const ERROR_EAGAIN: ErrorCode = ErrorCode::Session(-37);
pub const ERROR_SOCKET_SEND: ErrorCode = ErrorCode::Session(-7);
pub const ERROR_SOCKET_RECV: ErrorCode = ErrorCode::Session(-43);
pub const ERROR_BAD_SOCKET: ErrorCode = ErrorCode::Session(-45);
