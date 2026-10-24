use std::io;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use futures::AsyncWriteExt;
use ssh2::BlockDirections;
use ssh2::Error;

use crate::RuntimeContext;
use crate::Session;
use crate::testing;

// A blocking test context makes unexpected readiness calls fail. It exercises
// actual adapter I/O and the with_poll override without the Tokio backend.
#[derive(Clone)]
struct TestContext {
    polls: Arc<AtomicUsize>,
}

impl RuntimeContext for TestContext {
    type Transport = (TcpStream, Arc<AtomicUsize>);

    fn new(mut session: ssh2::Session, (stream, polls): Self::Transport) -> Result<Self, Error> {
        session.set_tcp_stream(stream);
        session.set_blocking(true);
        session.set_timeout(1000);
        Ok(Self { polls })
    }

    fn block_directions(&self) -> BlockDirections {
        BlockDirections::None
    }

    async fn wait_ready(&self) -> io::Result<()> {
        unreachable!("blocking test I/O must complete without readiness")
    }

    fn poll_ready(&self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        unreachable!("adapters must use the runtime's with_poll override")
    }

    fn with_poll<T, F>(&self, _: &mut Context<'_>, mut func: F) -> Poll<io::Result<T>>
    where
        F: FnMut() -> io::Result<T>,
    {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(func())
    }
}

#[test]
fn custom_context_runs_real_channel_and_file_io() -> Result<(), Box<dyn std::error::Error>> {
    futures::executor::block_on(testing::with_sftp_server(|addr| async move {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut session =
            Session::<TestContext>::from_stream((TcpStream::connect(addr)?, polls.clone()))?;
        session.handshake().await?;
        session
            .userauth_password(testing::USERNAME, testing::PASSWORD)
            .await?;
        let mut channel = session.channel_session().await?;
        channel.exec("missing").await?;
        let mut output = String::new();
        channel.read_to_string(&mut output).await?;
        assert!(output.is_empty());
        channel.stderr().read_to_string(&mut output).await?;
        assert_eq!(output, "unknown command: missing\n");
        let sftp = session.sftp().await?;
        let mut file = sftp.create(std::path::Path::new("file")).await?;
        file.write_all(b"runtime").await?;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        file.close().await?;
        sftp.into_inner().shutdown()?;
        assert_eq!(bytes, b"runtime");
        assert!(polls.load(Ordering::SeqCst) >= 3);
        Ok::<_, Box<dyn std::error::Error>>(())
    }))??;
    Ok(())
}
