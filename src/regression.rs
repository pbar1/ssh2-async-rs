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

#[tokio::test]
async fn cancelled_file_write_keeps_each_payload_and_count() -> Result {
    use std::io::SeekFrom;

    use futures::AsyncSeekExt;
    testing::with_server(|addr| async move {
        let session = connect(addr).await?;
        let sftp = session.sftp().await?;
        let mut file = sftp.create(std::path::Path::new("file")).await?;
        let mut first = Box::pin(file.write(b"first"));
        assert!(futures::poll!(&mut first).is_pending());
        drop(first);
        let count = file.write(b"z").await?;
        let payload: Vec<_> = (0_u32..100_000)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect();
        file.write_all(&payload).await?;
        file.seek(SeekFrom::Start(0)).await?;
        let mut actual = Vec::new();
        file.read_to_end(&mut actual).await?;
        file.close().await?;
        tokio::task::spawn_blocking(move || drop(sftp)).await?;
        assert_eq!(count, 1);
        let mut expected = b"firstz".to_vec();
        expected.extend(payload);
        assert_eq!(actual, expected);
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn changed_channel_write_keeps_exact_payload_under_backpressure() -> Result {
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use futures::AsyncWrite;
    testing::server()
        .exec(
            "echo",
            testing::ExecResponse {
                keep_open: true,
                echo: true,
                ..testing::ExecResponse::default()
            },
        )
        .with_server(|addr| async move {
            let tcp = tokio::net::TcpStream::connect(addr).await?;
            socket2::SockRef::from(&tcp).set_send_buffer_size(1024)?;
            let mut session = Session::try_from(tcp)?;
            session.handshake().await?;
            session
                .userauth_password(testing::USERNAME, testing::PASSWORD)
                .await?;
            let mut channel = session.channel_session().await?;
            channel.exec("echo").await?;
            let mut accepted = 0;
            let mut pending = false;
            for _ in 0..4096 {
                match Pin::new(&mut channel)
                    .poll_write(&mut Context::from_waker(Waker::noop()), &[b'a'; 1024])
                {
                    Poll::Ready(result) => accepted += result?,
                    Poll::Pending => {
                        pending = true;
                        break;
                    }
                }
            }
            assert!(pending, "test did not reach native backpressure");
            assert!(matches!(
                session.block_directions(),
                ssh2::BlockDirections::Outbound | ssh2::BlockDirections::Both
            ));
            assert!(channel.write_window().remaining >= 1024);
            assert_eq!(channel.write(b"z").await?, 1);
            channel.send_eof().await?;
            let mut actual = Vec::new();
            channel.read_to_end(&mut actual).await?;
            let mut expected = vec![b'a'; accepted + 1024];
            expected.push(b'z');
            assert_eq!(actual, expected);
            Ok(())
        })
        .await?
}

#[test]
fn handles_remain_send_and_sync() {
    fn check<T: Send + Sync>() {}
    check::<crate::Channel<TokioContext>>();
    check::<crate::Stream<TokioContext>>();
    check::<crate::File<TokioContext>>();
}

#[tokio::test]
async fn concurrent_session_opens_keep_their_requests() -> Result {
    testing::with_server(|addr| async move {
        let session = connect(addr).await?;
        let mut valid = Box::pin(session.channel_session());
        assert!(futures::poll!(&mut valid).is_pending());
        let (invalid, valid) = futures::join!(
            session.channel_open("invalid-channel-type", 65536, 32768, None),
            valid,
        );
        assert!(valid.is_ok());
        assert!(invalid.is_err());
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn cancelled_session_request_does_not_poison_existing_channels() -> Result {
    testing::server()
        .exec("data", testing::ExecResponse::stdout("ready"))
        .with_server(|addr| async move {
            let session = connect(addr).await?;
            let mut channel = session.channel_session().await?;
            let mut open = Box::pin(session.channel_session());
            assert!(futures::poll!(&mut open).is_pending());
            drop(open);
            let next = session.channel_session().await;
            let rejected = next.as_ref().err().map(ssh2::Error::code);
            channel.exec("data").await?;
            let mut actual = Vec::new();
            channel.read_to_end(&mut actual).await?;
            assert_eq!(rejected, Some(ssh2::ErrorCode::Session(-39)));
            assert_eq!(actual, b"ready");
            Ok(())
        })
        .await?
}

#[tokio::test]
async fn abandoned_sftp_read_rejects_conflicts_without_stranding_handles() -> Result {
    testing::server()
        .file("a", "A")
        .file("b", "B")
        .with_server(|addr| async move {
            let session = connect(addr).await?;
            let sftp = session.sftp().await?;
            let mut a = sftp.open("a").await?;
            let mut b = sftp.open("b").await?;
            let mut a_byte = [0];
            let mut reading = Box::pin(a.read(&mut a_byte));
            assert!(futures::poll!(&mut reading).is_pending());
            drop(reading);
            let conflict =
                tokio::time::timeout(std::time::Duration::from_millis(200), b.read(&mut [0])).await;
            let a_result = a.read_exact(&mut a_byte).await;
            let mut b_byte = [0];
            let b_result = b.read_exact(&mut b_byte).await;
            a.close().await?;
            b.close().await?;
            tokio::task::spawn_blocking(move || drop(sftp)).await?;
            assert!(
                matches!(conflict, Ok(Err(_))),
                "conflicting read was reused or left waiting: {conflict:?}"
            );
            a_result?;
            b_result?;
            assert_eq!(a_byte, *b"A");
            assert_eq!(b_byte, *b"B");
            Ok(())
        })
        .await?
}

#[tokio::test]
async fn cancelled_sftp_open_does_not_invalidate_the_session() -> Result {
    testing::with_server(|addr| async move {
        let session = connect(addr).await?;
        let sftp = session.sftp().await?;
        let mut first = Box::pin(sftp.open("first"));
        assert!(futures::poll!(&mut first).is_pending());
        drop(first);
        let next = sftp.open("second").await;
        let code = next.as_ref().err().map(ssh2::Error::code);
        session.channel_session().await?;
        tokio::task::spawn_blocking(move || {
            drop(next);
            drop(sftp);
        })
        .await?;
        assert_eq!(code, Some(ssh2::ErrorCode::Session(-39)));
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn seek_cannot_replace_an_unfinished_file_read() -> Result {
    use std::io::SeekFrom;

    use futures::AsyncSeekExt;
    testing::server()
        .file("file", "AB")
        .with_server(|addr| async move {
            let session = connect(addr).await?;
            let sftp = session.sftp().await?;
            let mut file = sftp.open("file").await?;
            let mut byte = [0];
            let mut read = Box::pin(file.read(&mut byte));
            assert!(futures::poll!(&mut read).is_pending());
            drop(read);
            let rejected = file.seek(SeekFrom::Start(1)).await.is_err();
            file.read_exact(&mut byte).await?;
            assert_eq!(byte, *b"A");
            file.seek(SeekFrom::Start(1)).await?;
            file.read_exact(&mut byte).await?;
            file.close().await?;
            tokio::task::spawn_blocking(move || drop(sftp)).await?;
            assert!(rejected);
            assert_eq!(byte, *b"B");
            Ok(())
        })
        .await?
}
