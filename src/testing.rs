use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use russh::Channel;
use russh::ChannelId;
use russh::keys::PrivateKey;
use russh::keys::PublicKeyBase64;
use russh::keys::agent::client::AgentClient;
use russh::server::Auth;
use russh::server::Config;
use russh::server::Handler;
use russh::server::Msg;
use russh::server::Session;
use tokio::net::TcpListener;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::sync::OnceCell;
use tokio_stream::wrappers::UnixListenerStream;

type Result<T> = std::result::Result<T, russh::Error>;

pub const USERNAME: &str = "username";
pub const PASSWORD: &str = "password";

const HOST_KEY: &str = r"
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACCzPq7zfqLffKoBDe/eo04kH2XxtSmk9D7RQyf1xUqrYgAAAJgAIAxdACAM
XQAAAAtzc2gtZWQyNTUxOQAAACCzPq7zfqLffKoBDe/eo04kH2XxtSmk9D7RQyf1xUqrYg
AAAEC2BsIi0QwW2uFscKTUUXNHLsYX4FxlaSDSblbAj7WR7bM+rvN+ot98qgEN796jTiQf
ZfG1KaT0PtFDJ/XFSqtiAAAAEHVzZXJAZXhhbXBsZS5jb20BAgMEBQ==
-----END OPENSSH PRIVATE KEY-----
";

const USER_KEY: &str = r"
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACA+/q51/y/PfVFNDP2Z0j9j0829+sI3wHybdibXPzuqsQAAAJh/FAkdfxQJ
HQAAAAtzc2gtZWQyNTUxOQAAACA+/q51/y/PfVFNDP2Z0j9j0829+sI3wHybdibXPzuqsQ
AAAEBf9AF7cqXX+8dFnoAefD88l2TISDiuY7c2KLzhBPvZLj7+rnX/L899UU0M/ZnSP2PT
zb36wjfAfJt2Jtc/O6qxAAAAFHNzaDItYXN5bmMtdGVzdC11c2VyAQ==
-----END OPENSSH PRIVATE KEY-----
";
pub const USER_PUBLIC_KEY: &str =
    r"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAID7+rnX/L899UU0M/ZnSP2PTzb36wjfAfJt2Jtc/O6qx";

static AGENT: OnceCell<AgentServer> = OnceCell::const_new();

pub fn public_key_openssh(blob: &[u8]) -> Result<String> {
    let key = russh::keys::key::parse_public_key(blob)?;
    Ok(format!("{} {}", key.algorithm(), key.public_key_base64()))
}

pub fn server() -> Builder {
    Builder::default()
}

pub async fn agent() -> Result<&'static AgentServer> {
    AGENT.get_or_try_init(AgentServer::spawn).await
}

pub async fn with_server<F, Fut, T>(f: F) -> Result<T>
where
    F: FnOnce(SocketAddr) -> Fut,
    Fut: Future<Output = T>,
{
    server().with_server(f).await
}

pub struct AgentServer {
    socket_path: PathBuf,
}

impl AgentServer {
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    async fn spawn() -> Result<Self> {
        let socket_path = std::env::temp_dir().join(format!(
            "ssh2-async-testing-agent-{}.sock",
            std::process::id()
        ));
        match std::fs::remove_file(&socket_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let (ready_tx, ready_rx) = mpsc::channel();
        let agent_socket_path = socket_path.clone();
        let _agent = std::thread::spawn(move || {
            let startup_tx = ready_tx.clone();
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .and_then(|runtime| {
                    runtime.block_on(async move {
                        let listener = UnixListener::bind(agent_socket_path)?;
                        let _ = startup_tx.send(Ok(()));
                        let stream = UnixListenerStream::new(listener);
                        let _ = russh::keys::agent::server::serve(stream, ()).await;
                        Ok(())
                    })
                });

            if let Err(error) = result {
                let _ = ready_tx.send(Err(error));
            }
        });
        ready_rx
            .recv()
            .map_err(|_| std::io::Error::other("test SSH agent thread stopped during startup"))??;

        let key = PrivateKey::from_openssh(USER_KEY)?;
        let stream = UnixStream::connect(&socket_path).await?;
        let mut client = AgentClient::connect(stream);
        client.add_identity(&key, &[]).await?;
        if client.request_identities().await?.is_empty() {
            return Err(russh::keys::Error::AgentFailure.into());
        }

        // Tests use a stable singleton agent, so setting this once is safe for
        // agent-related tests in this process.
        unsafe {
            std::env::set_var("SSH_AUTH_SOCK", &socket_path);
        }

        Ok(Self { socket_path })
    }
}

#[derive(Default)]
pub struct Builder {
    exec: HashMap<String, ExecResponse>,
    files: HashMap<String, Vec<u8>>,
}

impl Builder {
    #[must_use]
    pub fn exec(mut self, command: impl Into<String>, response: ExecResponse) -> Self {
        self.exec.insert(command.into(), response);
        self
    }

    #[must_use]
    pub fn file(mut self, path: impl Into<String>, contents: impl AsRef<[u8]>) -> Self {
        self.files.insert(path.into(), contents.as_ref().to_vec());
        self
    }

    pub async fn bind(self) -> Result<Server> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let config = Arc::new(Config {
            auth_rejection_time: Duration::ZERO,
            auth_rejection_time_initial: Some(Duration::ZERO),
            inactivity_timeout: Some(Duration::from_secs(30)),
            keys: vec![PrivateKey::from_openssh(HOST_KEY)?],
            ..Config::default()
        });
        let state = Arc::new(State {
            exec: self.exec,
            files: Mutex::new(self.files),
        });

        Ok(Server {
            listener,
            config,
            state,
        })
    }

    pub async fn with_server<F, Fut, T>(self, f: F) -> Result<T>
    where
        F: FnOnce(SocketAddr) -> Fut,
        Fut: Future<Output = T>,
    {
        let server = self.bind().await?;
        let client = f(server.addr());
        tokio::pin!(client);
        let serve = server.serve_one();
        tokio::pin!(serve);

        tokio::select! {
            output = &mut client => Ok(output),
            result = &mut serve => {
                result?;
                Err(russh::Error::Disconnect)
            }
        }
    }
}

pub struct Server {
    listener: TcpListener,
    config: Arc<Config>,
    state: Arc<State>,
}

impl Server {
    pub fn addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("test SSH listener must have a local address")
    }

    pub async fn serve_one(&self) -> Result<()> {
        let (stream, _) = self.listener.accept().await?;
        let session = russh::server::run_stream(
            Arc::clone(&self.config),
            stream,
            TestHandler::new(Arc::clone(&self.state)),
        )
        .await?;

        session.await
    }
}

#[derive(Clone, Default)]
pub struct ExecResponse {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub status: u32,
}

impl ExecResponse {
    pub fn stdout(stdout: impl AsRef<[u8]>) -> Self {
        Self {
            stdout: stdout.as_ref().to_vec(),
            stderr: Vec::new(),
            status: 0,
        }
    }
}

struct State {
    exec: HashMap<String, ExecResponse>,
    files: Mutex<HashMap<String, Vec<u8>>>,
}

struct TestHandler {
    state: Arc<State>,
    scp: HashMap<ChannelId, ScpState>,
}

impl TestHandler {
    fn new(state: Arc<State>) -> Self {
        Self {
            state,
            scp: HashMap::new(),
        }
    }

    fn write_exec_response(
        channel: ChannelId,
        response: &ExecResponse,
        session: &mut Session,
    ) -> Result<()> {
        session.channel_success(channel)?;
        if !response.stdout.is_empty() {
            session.data(channel, response.stdout.clone())?;
        }
        if !response.stderr.is_empty() {
            session.extended_data(channel, 1, response.stderr.clone())?;
        }
        finish_channel(channel, response.status, session)
    }

    fn start_scp_sink(
        &mut self,
        channel: ChannelId,
        path: String,
        session: &mut Session,
    ) -> Result<()> {
        self.scp.insert(channel, ScpState::Sink(ScpSink::new(path)));
        session.channel_success(channel)?;
        session.data(channel, vec![0])
    }

    fn start_scp_source(
        &mut self,
        channel: ChannelId,
        path: String,
        session: &mut Session,
    ) -> Result<()> {
        self.scp
            .insert(channel, ScpState::Source(ScpSource::new(path)));
        session.channel_success(channel)
    }

    fn handle_scp_data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<()> {
        let Some(state) = self.scp.remove(&channel) else {
            return Ok(());
        };

        match state {
            ScpState::Sink(mut sink) => {
                let finished = sink.read(data, &self.state, session, channel)?;
                if !finished {
                    self.scp.insert(channel, ScpState::Sink(sink));
                }
            }
            ScpState::Source(mut source) => {
                let finished = source.write(data, &self.state, session, channel)?;
                if !finished {
                    self.scp.insert(channel, ScpState::Source(source));
                }
            }
        }

        Ok(())
    }
}

impl Handler for TestHandler {
    type Error = russh::Error;

    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth> {
        if user == USERNAME && password == PASSWORD {
            Ok(Auth::Accept)
        } else {
            Ok(Auth::reject())
        }
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        _public_key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<Auth> {
        if user == USERNAME {
            Ok(Auth::Accept)
        } else {
            Ok(Auth::reject())
        }
    }

    async fn channel_open_session(
        &mut self,
        _channel: Channel<Msg>,
        _session: &mut Session,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<()> {
        let command = String::from_utf8_lossy(data);

        if let Some(path) = scp_path(&command, "-t") {
            return self.start_scp_sink(channel, path, session);
        }
        if let Some(path) = scp_path(&command, "-f") {
            return self.start_scp_source(channel, path, session);
        }

        let response = self
            .state
            .exec
            .get(command.as_ref())
            .cloned()
            .unwrap_or_else(|| unknown_command(&command));
        Self::write_exec_response(channel, &response, session)
    }

    async fn data(&mut self, channel: ChannelId, data: &[u8], session: &mut Session) -> Result<()> {
        self.handle_scp_data(channel, data, session)
    }
}

enum ScpState {
    Sink(ScpSink),
    Source(ScpSource),
}

struct ScpSink {
    path: String,
    size: Option<usize>,
    buffer: Vec<u8>,
}

impl ScpSink {
    fn new(path: String) -> Self {
        Self {
            path,
            size: None,
            buffer: Vec::new(),
        }
    }

    fn read(
        &mut self,
        data: &[u8],
        state: &State,
        session: &mut Session,
        channel: ChannelId,
    ) -> Result<bool> {
        self.buffer.extend_from_slice(data);

        if self.size.is_none() {
            let Some(end) = self.buffer.iter().position(|byte| *byte == b'\n') else {
                return Ok(false);
            };
            let header = String::from_utf8_lossy(&self.buffer[..end]);
            self.size = parse_scp_size(&header);
            self.buffer.drain(..=end);
            session.data(channel, vec![0])?;
        }

        let Some(size) = self.size else {
            return Ok(false);
        };
        if self.buffer.len() < size {
            return Ok(false);
        }

        let contents = self.buffer[..size].to_vec();
        let mut files = state.files.lock().map_err(|_| russh::Error::Disconnect)?;
        files.insert(self.path.clone(), contents);
        drop(files);

        session.data(channel, vec![0])?;
        finish_channel(channel, 0, session)?;
        Ok(true)
    }
}

struct ScpSource {
    path: String,
    phase: ScpSourcePhase,
}

impl ScpSource {
    fn new(path: String) -> Self {
        Self {
            path,
            phase: ScpSourcePhase::Header,
        }
    }

    fn write(
        &mut self,
        data: &[u8],
        state: &State,
        session: &mut Session,
        channel: ChannelId,
    ) -> Result<bool> {
        if !data.contains(&0) {
            return Ok(false);
        }

        match self.phase {
            ScpSourcePhase::Header => {
                let files = state.files.lock().map_err(|_| russh::Error::Disconnect)?;
                let contents = files.get(&self.path).cloned().unwrap_or_default();
                drop(files);

                let header = format!("C0644 {} {}\n", contents.len(), file_name(&self.path));
                session.data(channel, header.into_bytes())?;
                self.phase = ScpSourcePhase::Body(contents);
                Ok(false)
            }
            ScpSourcePhase::Body(ref contents) => {
                let mut data = contents.clone();
                data.push(0);
                session.data(channel, data)?;
                self.phase = ScpSourcePhase::Close;
                Ok(false)
            }
            ScpSourcePhase::Close => {
                finish_channel(channel, 0, session)?;
                Ok(true)
            }
        }
    }
}

enum ScpSourcePhase {
    Header,
    Body(Vec<u8>),
    Close,
}

fn finish_channel(channel: ChannelId, status: u32, session: &mut Session) -> Result<()> {
    session.exit_status_request(channel, status)?;
    session.eof(channel)?;
    session.close(channel)
}

fn parse_scp_size(header: &str) -> Option<usize> {
    let mut parts = header.split_whitespace();
    parts.next()?;
    parts.next()?.parse().ok()
}

fn scp_path(command: &str, flag: &str) -> Option<String> {
    let mut saw_scp = false;
    let mut saw_flag = false;

    for part in command.split_whitespace() {
        if part == "scp" {
            saw_scp = true;
        } else if part == flag {
            saw_flag = true;
        } else if saw_scp && saw_flag && !part.starts_with('-') {
            return Some(part.trim_matches('\'').to_owned());
        }
    }

    None
}

fn file_name(path: &str) -> &str {
    path.rsplit('/')
        .find(|part| !part.is_empty())
        .unwrap_or("file")
}

fn unknown_command(command: &str) -> ExecResponse {
    ExecResponse {
        stdout: Vec::new(),
        stderr: format!("unknown command: {command}\n").into_bytes(),
        status: 127,
    }
}
