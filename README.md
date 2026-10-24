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

Runtime contexts receive an owned transport through `RuntimeContext::Transport`
and `RuntimeContext::new(session, transport)`. The Tokio backend shares the
socket's existing registration with the native session; it neither duplicates
the descriptor nor registers a borrowed descriptor. `from_blocking` accepts an
unconnected native session and the transport as separate arguments. Established
native sessions cannot supply their socket ownership through ssh2's API.

libssh2 retains native state across `EAGAIN`. Keep control-request futures alive
until they finish. Cancelling a session request prevents later session requests;
cancelling a requesting channel control prevents later controls sharing that
reply gate. Data I/O, EOF waits, and close remain available. SFTP allows one
pending operation at a time and rejects conflicting calls promptly. Resume a
pending read on the same file; recreate the SFTP object after cancelling an
open or metadata request.

Flush a pending write before starting unrelated requests or using native
handles. A cancelled write can still transmit its original bytes. libssh2 keeps
a partially sent SSH packet in connection-wide transport state, so abandoning
its originating operation can stall other channels. This upstream limitation
requires completing that operation or recreating the session; the wrapper does
not provide an autonomous connection driver.

Upstream cleanup contracts still apply. In particular, ssh2's final channel
destructor does not acquire the session mutex, and native cleanup can require
network work or return `EAGAIN`. Explicit async close and coordinated ownership
help, but native aliases and final destruction remain upstream concerns.
