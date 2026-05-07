//! Runtime-agnostic async wrapper for [`ssh2`].
//!
//! # Examples
//!
//! ## Authenticating with a password
//!
//! ```
//! # mod testing { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/testing.rs")); }
//! use ssh2_async::Session;
//! use tokio::net::TcpStream;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # testing::with_server(|server_addr| async move {
//! // Connect to the SSH server
//! let tcp = TcpStream::connect(server_addr).await?;
//! let mut sess = Session::try_from(tcp)?;
//! sess.handshake().await?;
//!
//! sess.userauth_password("username", "password").await?;
//! assert!(sess.authenticated());
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! # }).await??;
//! # Ok(())
//! # }
//! ```
//!
//! ## Run a command
//!
//! ```
//! # mod testing { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/testing.rs")); }
//! use futures::AsyncReadExt;
//! use ssh2_async::Session;
//! use tokio::net::TcpStream;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # testing::server()
//! #     .exec("ls", testing::ExecResponse::stdout("remote\n"))
//! #     .with_server(|server_addr| async move {
//! // Connect to the SSH server
//! let tcp = TcpStream::connect(server_addr).await?;
//! let mut sess = Session::try_from(tcp)?;
//! sess.handshake().await?;
//!
//! sess.userauth_password("username", "password").await?;
//!
//! let mut channel = sess.channel_session().await?;
//! channel.exec("ls").await?;
//! let mut s = String::new();
//! channel.read_to_string(&mut s).await?;
//! println!("{s}");
//! channel.wait_close().await?;
//! println!("{}", channel.exit_status()?);
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! # }).await??;
//! # Ok(())
//! # }
//! ```
//!
//! ## Upload a file
//!
//! ```
//! # mod testing { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/testing.rs")); }
//! use futures::AsyncWriteExt;
//! use ssh2_async::Session;
//! use std::path::Path;
//! use tokio::net::TcpStream;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # testing::with_server(|server_addr| async move {
//! // Connect to the SSH server
//! let tcp = TcpStream::connect(server_addr).await?;
//! let mut sess = Session::try_from(tcp)?;
//! sess.handshake().await?;
//! sess.userauth_password("username", "password").await?;
//!
//! // Write the file
//! let mut remote_file = sess
//!     .scp_send(Path::new("remote"), 0o644, 10, None)
//!     .await?;
//! remote_file.write_all(b"1234567890").await?;
//! // Close the channel and wait for the whole content to be transferred
//! remote_file.send_eof().await?;
//! remote_file.wait_eof().await?;
//! remote_file.close().await?;
//! remote_file.wait_close().await?;
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! # }).await??;
//! # Ok(())
//! # }
//! ```
//!
//! ## Download a file
//!
//! ```
//! # mod testing { include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/testing.rs")); }
//! use futures::AsyncReadExt;
//! use ssh2_async::Session;
//! use std::path::Path;
//! use tokio::net::TcpStream;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # testing::server()
//! #     .file("remote", b"1234567890")
//! #     .with_server(|server_addr| async move {
//! // Connect to the SSH server
//! let tcp = TcpStream::connect(server_addr).await?;
//! let mut sess = Session::try_from(tcp)?;
//! sess.handshake().await?;
//! sess.userauth_password("username", "password").await?;
//!
//! let (mut remote_file, stat) = sess.scp_recv(Path::new("remote")).await?;
//! println!("remote file size: {}", stat.size());
//! let mut contents = Vec::new();
//! remote_file.read_to_end(&mut contents).await?;
//!
//! // Close the channel and wait for the whole content to be transferred
//! remote_file.send_eof().await?;
//! remote_file.wait_eof().await?;
//! remote_file.close().await?;
//! remote_file.wait_close().await?;
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! # }).await??;
//! # Ok(())
//! # }
//! ```

#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod agent;
mod channel;
mod listener;
mod runtime;
mod session;
mod sftp;
#[cfg(test)]
#[allow(dead_code)]
mod testing;

pub use ssh2;

pub use self::agent::Agent;
pub use self::channel::Channel;
pub use self::channel::Stream;
pub use self::listener::Listener;
pub use self::runtime::RuntimeContext;
#[cfg(feature = "tokio")]
pub use self::runtime::TokioContext;
pub use self::session::Session;
pub use self::sftp::File;
pub use self::sftp::Sftp;

/// Copy constants to avoid depending directly on `libssh2-sys`, as `ssh2`
/// already does - doing so would make the dependency solver more likely to
/// fail.
mod consts {
    use ssh2::ErrorCode;

    pub const ERROR_EAGAIN: ErrorCode = ErrorCode::Session(-37);
    pub const ERROR_BAD_SOCKET: ErrorCode = ErrorCode::Session(-45);
}
