# ssh2-async

Runtime-agnostic async wrappers for [`ssh2`][ssh2].

Includes pluggable runtime support through the `RuntimeContext` trait to
intelligently support async without resorting to busy-waiting. [Tokio][tokio]
support is implemented already and enabled by default.

Also implements `futures::io::{AsyncRead, AsyncWrite}` for things that
implement `std::io::{Read, Write}` in `ssh2`, such as channels and files.

## Example

```rust
use futures::AsyncReadExt;
use ssh2_async::{Session, TokioContext};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), ssh2::Error> {
    let tcp = TcpStream::connect("127.0.0.1:22")
        .await
        .unwrap()
        .into_std()
        .unwrap();

    let mut session = Session::<TokioContext>::from_stream(tcp)?;
    session.handshake().await?;
    session.userauth_password("user", "password").await?;

    let mut channel = session.channel_session().await?;
    channel.exec("uname -a").await?;

    let mut output = String::new();
    channel.read_to_string(&mut output).await.unwrap();
    channel.wait_close().await?;

    println!("{output}");
    Ok(())
}
```

[ssh2]: https://github.com/alexcrichton/ssh2-rs
[tokio]: https://tokio.rs/
