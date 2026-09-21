use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};

/// Convert a filesystem path to a Windows named pipe path.
///
/// e.g. `/run/user/1000/jcode.sock` -> `\\.\pipe\jcode`
/// e.g. `/run/user/1000/jcode/myserver.sock` -> `\\.\pipe\jcode-myserver`
fn path_to_pipe_name(path: &Path) -> String {
    let stem: String = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("jcode")
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        .take(32)
        .collect();
    let stem = if stem.is_empty() { "jcode" } else { &stem };
    let normalized = path
        .to_string_lossy()
        .replace('\\', "/")
        .to_ascii_lowercase();
    let digest = Sha256::digest(normalized.as_bytes());
    let hash = hex::encode(digest);
    format!(r"\\.\pipe\{}-{}", stem, &hash[..16])
}

/// Listener wraps a Windows named pipe server, providing an accept loop
/// that matches the UnixListener interface.
pub struct Listener {
    pipe_name: String,
    current_server: NamedPipeServer,
}

impl Listener {
    pub fn bind(path: &Path) -> io::Result<Self> {
        let pipe_name = path_to_pipe_name(path);
        match ServerOptions::new()
            .first_pipe_instance(true)
            .create(&pipe_name)
        {
            Ok(server) => Ok(Self {
                pipe_name,
                current_server: server,
            }),
            Err(e)
                if e.raw_os_error()
                    == Some(windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED as i32) =>
            {
                eprintln!(
                    "[windows] Named pipe {} busy (access denied), retrying without first_pipe_instance",
                    pipe_name
                );
                std::thread::sleep(std::time::Duration::from_millis(200));
                let server = ServerOptions::new().create(&pipe_name)?;
                Ok(Self {
                    pipe_name,
                    current_server: server,
                })
            }
            Err(e) => Err(e),
        }
    }

    pub async fn accept(&mut self) -> io::Result<(Stream, PipeAddr)> {
        self.current_server.connect().await?;

        let connected = std::mem::replace(
            &mut self.current_server,
            ServerOptions::new().create(&self.pipe_name)?,
        );

        Ok((Stream::Server(connected), PipeAddr))
    }
}

/// Placeholder for the "address" of a named pipe connection.
pub struct PipeAddr;

/// Stream wraps either a NamedPipeServer (accepted connection) or
/// NamedPipeClient (outgoing connection).
pub enum Stream {
    Server(NamedPipeServer),
    Client(NamedPipeClient),
}

impl Stream {
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let pipe_name = path_to_pipe_name(path.as_ref());
        loop {
            match ClientOptions::new().open(&pipe_name) {
                Ok(client) => return Ok(Stream::Client(client)),
                Err(e)
                    if e.raw_os_error()
                        == Some(windows_sys::Win32::Foundation::ERROR_PIPE_BUSY as i32) =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn into_split(self) -> (ReadHalf, WriteHalf) {
        let (read, write) = tokio::io::split(self);
        (ReadHalf { inner: read }, WriteHalf { inner: write })
    }

    pub fn split(&mut self) -> (SplitReadRef<'_>, SplitWriteRef<'_>) {
        let (read, write) = tokio::io::split(self);
        (SplitReadRef { inner: read }, SplitWriteRef { inner: write })
    }

    pub fn pair() -> io::Result<(Self, Self)> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static PAIR_COUNTER: AtomicU64 = AtomicU64::new(0);
        let counter = PAIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pipe_name = format!(r"\\.\pipe\jcode-pair-{}-{}", std::process::id(), counter);
        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&pipe_name)?;
        let client = ClientOptions::new().open(&pipe_name)?;

        // The client connected when we opened it above, but the server must
        // call connect() to transition into the connected state.  For an
        // already-connected client this returns immediately.
        //
        // We use a short-lived runtime-free poll: since the client already
        // connected synchronously, the server's connect future will resolve
        // on the first poll.
        use std::future::Future;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        fn dummy_raw_waker() -> RawWaker {
            fn no_op(_: *const ()) {}
            fn clone(p: *const ()) -> RawWaker {
                RawWaker::new(p, &VTABLE)
            }
            const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        let waker = unsafe { Waker::from_raw(dummy_raw_waker()) };
        let mut cx = Context::from_waker(&waker);
        let mut fut = server.connect();
        let pinned = unsafe { std::pin::Pin::new_unchecked(&mut fut) };
        match pinned.poll(&mut cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Err(e),
            Poll::Pending => {
                // Should not happen since the client already connected.
                // Drop the future and proceed - the pipe is still usable.
            }
        }
        drop(fut);

        Ok((Stream::Server(server), Stream::Client(client)))
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Server(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Stream::Client(c) => std::pin::Pin::new(c).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Server(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Stream::Client(c) => std::pin::Pin::new(c).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Server(s) => std::pin::Pin::new(s).poll_flush(cx),
            Stream::Client(c) => std::pin::Pin::new(c).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Server(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Stream::Client(c) => std::pin::Pin::new(c).poll_shutdown(cx),
        }
    }
}

/// Owned read half of a Stream, created by `into_split()`.
pub struct ReadHalf {
    inner: tokio::io::ReadHalf<Stream>,
}

impl AsyncRead for ReadHalf {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

/// Owned write half of a Stream, created by `into_split()`.
pub struct WriteHalf {
    inner: tokio::io::WriteHalf<Stream>,
}

impl AsyncWrite for WriteHalf {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Borrowed read reference for `stream.split()`.
pub struct SplitReadRef<'a> {
    inner: tokio::io::ReadHalf<&'a mut Stream>,
}

impl<'a> AsyncRead for SplitReadRef<'a> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

/// Borrowed write reference for `stream.split()`.
pub struct SplitWriteRef<'a> {
    inner: tokio::io::WriteHalf<&'a mut Stream>,
}

impl<'a> AsyncWrite for SplitWriteRef<'a> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Synchronous named pipe stream for blocking IPC.
///
/// A blocking client reads on one thread while another writes, so `try_clone`
/// must produce halves that make progress independently. Two `std::fs::File`
/// handles on one synchronous pipe do not: Windows serializes their blocking
/// I/O, so a reader parked before the first request keeps the writer's bytes
/// from ever reaching the server. The SDK starts exactly that way, which left
/// the desktop app connected but unable to fetch any session history.
///
/// Keep one native pipe and hand the halves out through a private runtime,
/// which gives genuinely independent read and write paths.
pub struct SyncStream {
    shared: std::sync::Arc<SyncStreamShared>,
}

struct SyncStreamShared {
    runtime: tokio::runtime::Runtime,
    reader: std::sync::Mutex<ReadHalf>,
    writer: std::sync::Mutex<WriteHalf>,
}

impl SyncStream {
    pub fn connect(path: &Path) -> io::Result<Self> {
        let pipe_name = path_to_pipe_name(path);
        // One worker thread is enough: reads and writes are independent
        // operations on the same pipe, never nested inside one another.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let stream = {
            let _guard = runtime.enter();
            Stream::Client(ClientOptions::new().open(&pipe_name)?)
        };
        let (reader, writer) = stream.into_split();
        Ok(Self {
            shared: std::sync::Arc::new(SyncStreamShared {
                runtime,
                reader: std::sync::Mutex::new(reader),
                writer: std::sync::Mutex::new(writer),
            }),
        })
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            shared: std::sync::Arc::clone(&self.shared),
        })
    }

    pub fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
        let _ = timeout;
        // Named pipes do not expose socket-style read timeouts. The communicate
        // tool only uses this to avoid hanging forever; on Windows we rely on
        // the server side to respond promptly.
        Ok(())
    }
}

impl io::Read for SyncStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut reader = self
            .shared
            .reader
            .lock()
            .map_err(|_| io::Error::other("named-pipe reader lock poisoned"))?;
        self.shared
            .runtime
            .block_on(tokio::io::AsyncReadExt::read(&mut *reader, buf))
    }
}

impl io::Write for SyncStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut writer = self
            .shared
            .writer
            .lock()
            .map_err(|_| io::Error::other("named-pipe writer lock poisoned"))?;
        self.shared
            .runtime
            .block_on(tokio::io::AsyncWriteExt::write(&mut *writer, buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut writer = self
            .shared
            .writer
            .lock()
            .map_err(|_| io::Error::other("named-pipe writer lock poisoned"))?;
        self.shared
            .runtime
            .block_on(tokio::io::AsyncWriteExt::flush(&mut *writer))
    }
}

pub fn is_socket_path(path: &Path) -> bool {
    let pipe_name = path_to_pipe_name(path);
    // Probe with a plain `CreateFile`. `ClientOptions::open` registers the
    // handle with the Tokio IO driver as soon as the pipe exists, so probing a
    // live endpoint from an ordinary thread (the SDK's `ensure_runtime`, called
    // from Jcode Desktop's startup thread) panicked with "there is no reactor
    // running" exactly when the runtime was already up.
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&pipe_name)
    {
        Ok(_) => true,
        Err(error)
            if error.raw_os_error()
                == Some(windows_sys::Win32::Foundation::ERROR_PIPE_BUSY as i32) =>
        {
            // Every published instance being occupied proves that a server is
            // listening. Do not report the endpoint as absent and race a second
            // daemon against it merely because another client connected first.
            true
        }
        Err(_) => false,
    }
}

pub fn remove_socket(path: &Path) {
    let pipe_name = path_to_pipe_name(path);
    // Same reactor-free probe: this also runs outside an async context.
    if std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&pipe_name)
        .is_ok()
    {
        eprintln!(
            "[windows] Named pipe {} still open, will be replaced by new server",
            pipe_name
        );
    }
}

pub fn stream_pair() -> io::Result<(Stream, Stream)> {
    Stream::pair()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    static BUSY_PIPE_TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// The TypeScript SDK derives this name independently, so the two must
    /// agree exactly or a Windows client dials a pipe nobody is listening on.
    /// Pinning literal values here gives that duplicate a fixed contract to
    /// match, and `sockets.test.ts` asserts the same strings.
    #[test]
    fn pipe_name_matches_the_typescript_sdk() {
        for (path, expected) in [
            (
                r"C:\Users\jeremy\AppData\Local\jcode\run\jcode-api.sock",
                r"\\.\pipe\jcode-api-5e00c01702e8cfe4",
            ),
            (r"C:\a\b\jcode.sock", r"\\.\pipe\jcode-52dfdb00b2f35a71"),
        ] {
            assert_eq!(
                path_to_pipe_name(Path::new(path)),
                expected,
                "pipe name for {path} drifted from the SDK's derivation"
            );
        }
    }

    #[test]
    fn pipe_name_is_stable_and_normalizes_case_and_separators() {
        let a = path_to_pipe_name(Path::new(r"C:\Temp\Jcode\server.sock"));
        let b = path_to_pipe_name(Path::new("c:/temp/jcode/server.sock"));
        assert_eq!(a, b, "pipe names should be normalized consistently");
        assert!(
            a.starts_with(r"\\.\pipe\server-"),
            "unexpected pipe name: {}",
            a
        );
    }

    #[test]
    fn pipe_name_falls_back_when_stem_is_empty() {
        let name = path_to_pipe_name(Path::new("..."));
        assert!(
            name.starts_with(r"\\.\pipe\jcode-"),
            "unexpected pipe name: {}",
            name
        );
    }

    #[tokio::test]
    async fn stream_pair_round_trips_bytes() {
        let (mut a, mut b) = stream_pair().expect("create stream pair");
        a.write_all(b"ping").await.expect("write to pipe");
        a.flush().await.expect("flush pipe");

        let mut buf = [0u8; 4];
        b.read_exact(&mut buf).await.expect("read from pipe");
        assert_eq!(&buf, b"ping");
    }

    #[tokio::test]
    async fn split_stream_supports_concurrent_read_and_write() {
        let (a, mut b) = stream_pair().expect("create stream pair");
        let (mut read, mut write) = a.into_split();

        let peer = tokio::spawn(async move {
            let mut request = [0u8; 4];
            b.read_exact(&mut request).await.expect("read request");
            assert_eq!(&request, b"ping");
            b.write_all(b"pong").await.expect("write response");
            b.flush().await.expect("flush response");
        });

        let exchange = async {
            write.write_all(b"ping").await.expect("write request");
            write.flush().await.expect("flush request");
            let mut response = [0u8; 4];
            read.read_exact(&mut response).await.expect("read response");
            assert_eq!(&response, b"pong");
        };

        tokio::time::timeout(std::time::Duration::from_secs(5), exchange)
            .await
            .expect("split stream exchange should not stall");
        peer.await.expect("peer task");
    }

    #[tokio::test]
    async fn busy_pipe_is_reported_as_a_live_socket_path() {
        let path = std::env::temp_dir().join(format!(
            "jcode-busy-pipe-probe-{}-{}.sock",
            std::process::id(),
            BUSY_PIPE_TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _listener = Listener::bind(&path).expect("bind named pipe");
        let _client = Stream::connect(&path).await.expect("occupy named pipe");

        assert!(
            is_socket_path(&path),
            "a busy named pipe must still be recognized as a live server endpoint"
        );
    }

    /// Deliberately not a `#[tokio::test]`: the SDK probes the socket from an
    /// ordinary thread, which is exactly where the Tokio-based probe panicked.
    #[test]
    fn socket_path_probe_works_without_a_tokio_runtime() {
        let path = std::env::temp_dir().join(format!(
            "jcode-probe-no-reactor-{}-{}.sock",
            std::process::id(),
            BUSY_PIPE_TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        assert!(
            !is_socket_path(&path),
            "an absent pipe must report false rather than panic"
        );

        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
        let (stop_tx, stop_rx) = std::sync::mpsc::sync_channel(0);
        let server_path = path.clone();
        let server = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build named-pipe runtime");
            let _guard = runtime.enter();
            let _listener = Listener::bind(&server_path).expect("bind named pipe");
            ready_tx.send(()).expect("announce named pipe");
            stop_rx.recv().expect("wait for probe");
        });

        ready_rx.recv().expect("wait for named pipe");
        assert!(
            is_socket_path(&path),
            "a live pipe must be recognized without a Tokio runtime"
        );
        stop_tx.send(()).expect("stop named-pipe server");
        server.join().expect("join named-pipe server");
    }

    /// The SDK parks a reader thread on the connection before it writes its
    /// first request, exactly as `JcodeClient` does during its handshake. A
    /// clone whose blocking reads serialize against the writer strands that
    /// request forever, which is how the desktop app ended up connected yet
    /// unable to load any session history.
    #[test]
    fn a_parked_reader_clone_does_not_block_the_writer() {
        use std::io::{Read, Write};

        let path = std::env::temp_dir().join(format!(
            "jcode-duplex-pipe-probe-{}-{}.sock",
            std::process::id(),
            BUSY_PIPE_TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let server_path = path.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
        let server = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build server runtime");
            runtime.block_on(async move {
                let mut listener = Listener::bind(&server_path).expect("bind named pipe");
                ready_tx.send(()).expect("announce named pipe");
                let (mut stream, _) = listener.accept().await.expect("accept client");
                let mut request = [0u8; 4];
                tokio::io::AsyncReadExt::read_exact(&mut stream, &mut request)
                    .await
                    .expect("read request");
                assert_eq!(&request, b"ping");
                tokio::io::AsyncWriteExt::write_all(&mut stream, b"pong")
                    .await
                    .expect("write reply");
                tokio::io::AsyncWriteExt::flush(&mut stream)
                    .await
                    .expect("flush reply");
            });
        });

        ready_rx.recv().expect("wait for named pipe");
        let mut reader = SyncStream::connect(&path).expect("connect");
        let mut writer = reader.try_clone().expect("clone connection");
        let (parked_tx, parked_rx) = std::sync::mpsc::sync_channel(0);
        let read = std::thread::spawn(move || {
            parked_tx.send(()).expect("announce parked reader");
            let mut reply = [0u8; 4];
            reader.read_exact(&mut reply).expect("read reply");
            reply
        });

        parked_rx.recv().expect("reader parked");
        std::thread::sleep(std::time::Duration::from_millis(100));
        writer.write_all(b"ping").expect("write request");
        writer.flush().expect("flush request");
        assert_eq!(
            &read.join().expect("join reader"),
            b"pong",
            "a request written while a clone is parked on read must still reach the server"
        );
        server.join().expect("join server");
    }
}
