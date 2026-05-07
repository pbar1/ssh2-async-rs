//! Runtime-agnostic async wrapper for [`ssh2`].

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

/// Tests are adapted from the [`ssh2`] crate examples.
#[cfg(test)]
mod tests {
    use std::path::Path;

    use futures::AsyncReadExt;
    use futures::AsyncWriteExt;
    use tokio::net::TcpStream;

    use super::Session;
    use super::testing;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn inspecting_ssh_agent() -> Result<()> {
        let _agent = testing::agent().await?;

        testing::with_server(|server_addr| async move {
            // Almost all APIs require a `Session` to be available
            let tcp = TcpStream::connect(server_addr).await?;
            let sess = Session::try_from(tcp)?;
            let mut agent = sess.agent()?;

            // Connect the agent and request a list of identities
            agent.connect()?;
            agent.list_identities()?;

            assert_eq!(agent.identities()?.len(), 1);
            for identity in agent.identities()? {
                assert_eq!(identity.comment(), "");
                let pubkey = identity.blob();
                assert_eq!(
                    testing::public_key_openssh(pubkey)?,
                    testing::USER_PUBLIC_KEY
                );
            }

            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn authenticating_with_ssh_agent() -> Result<()> {
        let _agent = testing::agent().await?;

        testing::with_server(|server_addr| async move {
            // Connect to the SSH server
            let tcp = TcpStream::connect(server_addr).await?;
            let mut sess = Session::try_from(tcp)?;
            sess.handshake().await?;

            // Try to authenticate with the first identity in the agent.
            let mut agent = sess.agent()?;
            agent.connect()?;
            agent.list_identities()?;
            let identity = agent.identities()?.remove(0);
            agent.userauth("username", &identity).await?;

            // Make sure we succeeded
            assert!(sess.authenticated());

            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn authenticate_with_password() -> Result<()> {
        testing::with_server(|server_addr| async move {
            // Connect to the SSH server
            let tcp = TcpStream::connect(server_addr).await?;
            let mut sess = Session::try_from(tcp)?;
            sess.handshake().await?;

            sess.userauth_password("username", "password").await?;
            assert!(sess.authenticated());

            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn run_command() -> Result<()> {
        testing::server()
            .exec("ls", testing::ExecResponse::stdout("remote\n"))
            .with_server(|server_addr| async move {
                // Connect to the SSH server
                let tcp = TcpStream::connect(server_addr).await?;
                let mut sess = Session::try_from(tcp)?;
                sess.handshake().await?;

                sess.userauth_password("username", "password").await?;

                let mut channel = sess.channel_session().await?;
                channel.exec("ls").await?;
                let mut s = String::new();
                channel.read_to_string(&mut s).await?;
                assert_eq!(s, "remote\n");
                channel.wait_close().await?;
                assert_eq!(channel.exit_status()?, 0);

                Ok(())
            })
            .await?
    }

    #[tokio::test]
    async fn upload_file() -> Result<()> {
        testing::with_server(|server_addr| async move {
            // Connect to the SSH server
            let tcp = TcpStream::connect(server_addr).await?;
            let mut sess = Session::try_from(tcp)?;
            sess.handshake().await?;
            sess.userauth_password("username", "password").await?;

            // Write the file
            let mut remote_file = sess.scp_send(Path::new("remote"), 0o644, 10, None).await?;
            remote_file.write_all(b"1234567890").await?;
            // Close the channel and wait for the whole content to be transferred
            remote_file.send_eof().await?;
            remote_file.wait_eof().await?;
            remote_file.close().await?;
            remote_file.wait_close().await?;

            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn download_file() -> Result<()> {
        testing::server()
            .file("remote", b"1234567890")
            .with_server(|server_addr| async move {
                // Connect to the SSH server
                let tcp = TcpStream::connect(server_addr).await?;
                let mut sess = Session::try_from(tcp)?;
                sess.handshake().await?;
                sess.userauth_password("username", "password").await?;

                let (mut remote_file, stat) = sess.scp_recv(Path::new("remote")).await?;
                println!("remote file size: {}", stat.size());
                let mut contents = Vec::new();
                remote_file.read_to_end(&mut contents).await?;

                // Close the channel and wait for the whole content to be transferred
                remote_file.send_eof().await?;
                remote_file.wait_eof().await?;
                remote_file.close().await?;
                remote_file.wait_close().await?;

                Ok(())
            })
            .await?
    }
}
