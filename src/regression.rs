use std::net::SocketAddr;

use futures::AsyncReadExt;
use futures::AsyncWriteExt;

use crate::Session;
use crate::TokioContext;
use crate::testing;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn connect(addr: SocketAddr) -> Result<Session<TokioContext>> {
    let mut session = Session::try_from(tokio::net::TcpStream::connect(addr).await?)?;
    session.as_inner().set_timeout(200);
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

#[tokio::test]
async fn close_waits_for_channel_and_file_replies() -> Result {
    testing::with_sftp_server(|addr| async move {
        let session = connect(addr).await?;
        let mut channel = session.channel_session().await?;
        let channel_result = AsyncWriteExt::close(&mut channel).await;
        let sftp = session.sftp().await?;
        let mut file = sftp.open("file").await?;
        let file_result = AsyncWriteExt::close(&mut file).await;
        // Drop before asserting so upstream's blocking cleanup has a live peer.
        drop(file);
        drop(sftp);
        assert!(channel_result.is_ok(), "{channel_result:?}");
        assert!(file_result.is_ok(), "{file_result:?}");
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn closing_stream_sends_eof() -> Result {
    testing::with_server(|addr| async move {
        let session = connect(addr).await?;
        let channel = session.channel_session().await?;
        let mut stream = channel.stream(0);
        stream.close().await?;
        assert!(stream.write(b"x").await.is_err());
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn seek_uses_remote_size_and_rejects_negative_offsets() -> Result {
    use std::io::SeekFrom;

    use futures::AsyncSeekExt;

    testing::with_sftp_server(|addr| async move {
        let session = connect(addr).await?;
        let sftp = session.sftp().await?;
        let mut file = sftp.open("file").await?;
        let negative = file.seek(SeekFrom::Current(-1)).await;
        file.seek(SeekFrom::Start(0)).await?;
        let end = file.seek(SeekFrom::End(-2)).await;
        file.close().await?;
        assert!(negative.is_err(), "{negative:?}");
        assert_eq!(end?, 10);
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn readdir_yields_while_waiting_for_entries() -> Result {
    testing::with_server(|addr| async move {
        let session = connect(addr).await?;
        let sftp = session.sftp().await?;
        let entries = sftp.readdir(".").await;
        tokio::task::spawn_blocking(move || drop(sftp)).await?;
        assert!(entries?.is_empty());
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn outgoing_window_does_not_block_channel_or_stream_reads() -> Result {
    for stream in [false, true] {
        testing::server()
            .window_size(1)
            .exec(
                "duplex",
                testing::ExecResponse {
                    keep_open: true,
                    ..testing::ExecResponse::stdout("ready")
                },
            )
            .with_server(|addr| async move {
                let session = connect(addr).await?;
                let mut channel = session.channel_session().await?;
                channel.exec("duplex").await?;
                assert_eq!(channel.write(b"12").await?, 1);
                let mut input = [0; 5];
                let read = async {
                    if stream {
                        channel.stream(0).read_exact(&mut input).await
                    } else {
                        channel.read_exact(&mut input).await
                    }
                };
                tokio::time::timeout(std::time::Duration::from_millis(200), read).await??;
                assert_eq!(&input, b"ready");
                Result::Ok(())
            })
            .await??;
    }
    Ok(())
}

#[tokio::test]
async fn cancelling_eof_wait_keeps_other_channels_usable() -> Result {
    testing::with_server(|addr| async move {
        let session = connect(addr).await?;
        let mut channel = session.channel_session().await?;
        let mut wait = Box::pin(channel.wait_eof());
        assert!(futures::poll!(&mut wait).is_pending());
        drop(wait);
        session.channel_session().await?;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn both_close_interfaces_can_be_resumed_after_cancellation() -> Result {
    testing::with_server(|addr| async move {
        let session = connect(addr).await?;
        for inherent in [true, false] {
            let mut channel = session.channel_session().await?;
            {
                let mut close = Box::pin(async {
                    if inherent {
                        channel.close().await.map_err(std::io::Error::from)
                    } else {
                        AsyncWriteExt::close(&mut channel).await
                    }
                });
                assert!(futures::poll!(&mut close).is_pending());
            }
            if inherent {
                channel.close().await?;
            } else {
                AsyncWriteExt::close(&mut channel).await?;
            }
            session.channel_session().await?;
        }
        Ok(())
    })
    .await?
}
