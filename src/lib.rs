//! Runtime-agnostic async wrapper for [`ssh2`].

#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod agent;
mod channel;
mod listener;
mod runtime;
mod session;
mod sftp;

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

/// Tests are adapted from the [`ssh2`] crate examples.
#[cfg(test)]
mod tests {
    use std::path::Path;

    use futures::AsyncReadExt;
    use futures::AsyncWriteExt;
    use tokio::net::TcpStream;

    use super::*;

    const ADDR: &str = "localhost:22";

    #[tokio::test]
    async fn run_command() {
        // Connect to the SSH server
        let tcp = TcpStream::connect(ADDR).await.unwrap().into_std().unwrap();
        let mut sess = Session::<TokioContext>::from_stream(tcp).unwrap();
        sess.handshake().await.unwrap();

        // Authenticate
        // TODO:

        // Run the command
        let mut channel = sess.channel_session().await.unwrap();
        channel.exec("ls").await.unwrap();
        let mut s = String::new();
        channel.read_to_string(&mut s).await.unwrap();
        println!("{s}");
        channel.wait_close().await.unwrap();
        println!("{}", channel.exit_status().unwrap());
    }

    #[tokio::test]
    async fn upload_file() {
        // Connect to the SSH server
        let tcp = TcpStream::connect(ADDR).await.unwrap().into_std().unwrap();
        let mut sess = Session::<TokioContext>::from_stream(tcp).unwrap();
        sess.handshake().await.unwrap();

        // Authenticate
        // TODO:

        // Write the file
        let mut remote_file = sess
            .scp_send(Path::new("remote"), 0o644, 10, None)
            .await
            .unwrap();
        remote_file.write_all(b"1234567890").await.unwrap();
        // Close the channel and wait for the whole content to be transferred
        remote_file.send_eof().await.unwrap();
        remote_file.wait_eof().await.unwrap();
        remote_file.close().await.unwrap();
        remote_file.wait_close().await.unwrap();
    }

    #[tokio::test]
    async fn download_file() {
        // Connect to the SSH server
        let tcp = TcpStream::connect(ADDR).await.unwrap().into_std().unwrap();
        let mut sess = Session::<TokioContext>::from_stream(tcp).unwrap();
        sess.handshake().await.unwrap();

        // Authenticate
        // TODO:

        let (mut remote_file, stat) = sess.scp_recv(Path::new("remote")).await.unwrap();
        println!("remote file size: {}", stat.size());
        let mut contents = Vec::new();
        remote_file.read_to_end(&mut contents).await.unwrap();

        // Close the channel and wait for the whole content to be transferred
        remote_file.send_eof().await.unwrap();
        remote_file.wait_eof().await.unwrap();
        remote_file.close().await.unwrap();
        remote_file.wait_close().await.unwrap();
    }
}
