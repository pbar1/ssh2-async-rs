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
use russh::keys::Algorithm;
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
    public_key: String,
}

impl AgentServer {
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn public_key(&self) -> &str {
        &self.public_key
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

        let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;
        let public_key = key.public_key().to_openssh()?;
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

        Ok(Self {
            socket_path,
            public_key,
        })
    }
}

#[derive(Default)]
pub struct Builder {
    exec: HashMap<String, ExecResponse>,
    files: HashMap<String, Vec<u8>>,
    window_size: Option<u32>,
}

impl Builder {
    pub const fn window_size(mut self, size: u32) -> Self {
        self.window_size = Some(size);
        self
    }

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
            window_size: self
                .window_size
                .unwrap_or_else(|| Config::default().window_size),
            auth_rejection_time: Duration::ZERO,
            auth_rejection_time_initial: Some(Duration::ZERO),
            inactivity_timeout: Some(Duration::from_secs(30)),
            keys: vec![PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?],
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
    pub keep_open: bool,
    pub echo: bool,
}

impl ExecResponse {
    pub fn stdout(stdout: impl AsRef<[u8]>) -> Self {
        Self {
            stdout: stdout.as_ref().to_vec(),
            stderr: Vec::new(),
            status: 0,
            keep_open: false,
            echo: false,
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
    sftp: HashMap<ChannelId, Vec<u8>>,
    handles: HashMap<u8, String>,
    next_handle: u8,
    echo_channels: std::collections::HashSet<ChannelId>,
    listed_dirs: std::collections::HashSet<u8>,
}

impl TestHandler {
    fn new(state: Arc<State>) -> Self {
        Self {
            state,
            scp: HashMap::new(),
            sftp: HashMap::new(),
            handles: HashMap::new(),
            next_handle: 0,
            echo_channels: std::collections::HashSet::new(),
            listed_dirs: std::collections::HashSet::new(),
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
        if response.keep_open {
            Ok(())
        } else {
            finish_channel(channel, response.status, session)
        }
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
        if response.echo {
            self.echo_channels.insert(channel);
        }
        Self::write_exec_response(channel, &response, session)
    }

    async fn channel_eof(&mut self, channel: ChannelId, session: &mut Session) -> Result<()> {
        session.eof(channel)?;
        session.close(channel)
    }

    async fn channel_close(&mut self, channel: ChannelId, session: &mut Session) -> Result<()> {
        session.close(channel)
    }

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<()> {
        if name == "sftp" {
            self.sftp.insert(channel, Vec::new());
            session.channel_success(channel)?;
        }
        Ok(())
    }

    async fn data(&mut self, channel: ChannelId, data: &[u8], session: &mut Session) -> Result<()> {
        if self.echo_channels.contains(&channel) {
            return session.data(channel, data.to_vec());
        }
        let Some(buffer) = self.sftp.get_mut(&channel) else {
            return self.handle_scp_data(channel, data, session);
        };
        buffer.extend_from_slice(data);
        while buffer.len() >= 4 {
            let len = u32::from_be_bytes(buffer[..4].try_into().unwrap()) as usize;
            if buffer.len() < len + 4 {
                break;
            }
            let packet: Vec<_> = buffer.drain(..len + 4).skip(4).collect();
            let mut reply = Vec::new();
            match packet[0] {
                1 => {
                    reply.push(2);
                    reply.extend(3_u32.to_be_bytes());
                }
                3 | 11 => {
                    let size = u32::from_be_bytes(packet[5..9].try_into().unwrap()) as usize;
                    let path = String::from_utf8(packet[9..9 + size].to_vec()).unwrap();
                    self.next_handle = self.next_handle.checked_add(1).unwrap();
                    self.handles.insert(self.next_handle, path);
                    reply.push(102);
                    reply.extend(&packet[1..5]);
                    reply.extend(1_u32.to_be_bytes());
                    reply.push(self.next_handle);
                }
                6 => {
                    let offset =
                        usize::try_from(u64::from_be_bytes(packet[10..18].try_into().unwrap()))
                            .unwrap();
                    let data = &packet[22..];
                    let mut files = self.state.files.lock().unwrap();
                    let file = files.entry(self.handles[&packet[9]].clone()).or_default();
                    file.resize(file.len().max(offset + data.len()), 0);
                    file[offset..offset + data.len()].copy_from_slice(data);
                    drop(files);
                    reply.push(101);
                    reply.extend(&packet[1..5]);
                    reply.extend([0; 12]);
                }
                5 => {
                    let offset =
                        usize::try_from(u64::from_be_bytes(packet[10..18].try_into().unwrap()))
                            .unwrap();
                    let count = u32::from_be_bytes(packet[18..22].try_into().unwrap()) as usize;
                    let files = self.state.files.lock().unwrap();
                    let file = files
                        .get(&self.handles[&packet[9]])
                        .map_or(&[][..], Vec::as_slice);
                    if offset >= file.len() {
                        reply.push(101);
                        reply.extend(&packet[1..5]);
                        reply.extend(1_u32.to_be_bytes());
                        reply.extend([0; 8]);
                    } else {
                        let data = &file[offset..file.len().min(offset + count)];
                        reply.push(103);
                        reply.extend(&packet[1..5]);
                        reply.extend(u32::try_from(data.len()).unwrap().to_be_bytes());
                        reply.extend(data);
                    }
                    drop(files);
                }
                12 if self.listed_dirs.insert(packet[9]) => {
                    let prefix = format!("{}/", self.handles[&packet[9]].trim_end_matches('/'));
                    let files = self.state.files.lock().unwrap();
                    let mut names: Vec<_> = files
                        .keys()
                        .filter_map(|path| path.strip_prefix(&prefix))
                        .filter(|name| !name.contains('/'))
                        .map(str::to_owned)
                        .collect();
                    drop(files);
                    names.sort();
                    names.extend([".".into(), "..".into()]);
                    reply.push(104);
                    reply.extend(&packet[1..5]);
                    reply.extend(u32::try_from(names.len()).unwrap().to_be_bytes());
                    for name in names {
                        for _ in 0..2 {
                            reply.extend(u32::try_from(name.len()).unwrap().to_be_bytes());
                            reply.extend(name.as_bytes());
                        }
                        reply.extend(0_u32.to_be_bytes());
                    }
                }
                8 => {
                    reply.push(105);
                    reply.extend(&packet[1..5]);
                    reply.extend(1_u32.to_be_bytes());
                    reply.extend(12_u64.to_be_bytes());
                }
                _ => {
                    reply.push(101);
                    reply.extend(&packet[1..5]);
                    reply.extend(u32::from(packet[0] == 12).to_be_bytes());
                    reply.extend([0; 8]);
                }
            }
            let mut framed = u32::try_from(reply.len()).unwrap().to_be_bytes().to_vec();
            framed.extend(reply);
            session.data(channel, framed)?;
        }
        Ok(())
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
    const fn new(path: String) -> Self {
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
    const fn new(path: String) -> Self {
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
        keep_open: false,
        echo: false,
    }
}

// A separate reactor lets tests finish upstream's blocking SFTP destructors.
pub async fn with_sftp_server<F, Fut, T>(client: F) -> Result<T>
where
    F: FnOnce(SocketAddr) -> Fut,
    Fut: Future<Output = T>,
{
    let (ready_tx, ready_rx) = futures::channel::oneshot::channel();
    let (stop_tx, stop_rx) = futures::channel::oneshot::channel::<()>();
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let server = server().bind().await.unwrap();
                ready_tx.send(server.addr()).unwrap();
                tokio::select! { _ = server.serve_one() => {}, _ = stop_rx => {} }
            });
    });
    let result = client(ready_rx.await.unwrap()).await;
    let _ = stop_tx.send(());
    worker.join().unwrap();
    Ok(result)
}
