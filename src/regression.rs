use std::net::SocketAddr;

use futures::AsyncReadExt;
use futures::AsyncWriteExt;

use crate::Session;
use crate::TokioContext;
use crate::testing;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn connect(addr: SocketAddr) -> Result<Session<TokioContext>> {
    let mut session = Session::try_from(tokio::net::TcpStream::connect(addr).await?)?;
    session.handshake().await?;
    session
        .userauth_password(testing::USERNAME, testing::PASSWORD)
        .await?;
    Ok(session)
}

#[tokio::test]
async fn flush_preserves_channel_input() -> Result {
    testing::server()
        .exec("data", testing::ExecResponse::stdout("abcdef"))
        .with_server(|addr| async move {
            let session = connect(addr).await?;
            let mut channel = session.channel_session().await?;
            channel.exec("data").await?;
            channel.read_exact(&mut [0]).await?;
            channel.flush().await?;
            channel.stream(0).flush().await?;
            let mut rest = Vec::new();
            channel.read_to_end(&mut rest).await?;
            assert_eq!(rest, b"bcdef");
            Ok(())
        })
        .await?
}
