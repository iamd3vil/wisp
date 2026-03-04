// src/server.rs
use crate::command::ServerCommand; // Import ServerCommand
use crate::error::{ServerError, ServerResult};
use crate::handler::NatsServerHandler;
use crate::protocol::{self, ConnectOptions, ServerInfo}; // Add ServerCommand
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

lazy_static::lazy_static! {
    static ref NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
}

fn generate_client_id() -> u64 {
    NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

/// Append a server command directly into the contiguous write buffer.
/// Returns (should_shutdown, should_flush_now).
#[inline]
fn append_command_to_buf(buf: &mut Vec<u8>, cmd: ServerCommand) -> (bool, bool) {
    match cmd {
        ServerCommand::Shutdown => (true, false),
        ServerCommand::SendImmediate(bytes) => {
            buf.extend_from_slice(&bytes);
            (false, true)
        }
        ServerCommand::Send(bytes) => {
            buf.extend_from_slice(&bytes);
            (false, false)
        }
        ServerCommand::SendMessage {
            header_prefix,
            reply_to,
            payload_len,
            payload,
        } => {
            buf.extend_from_slice(&header_prefix);
            if let Some(ref reply) = reply_to {
                buf.extend_from_slice(reply);
                buf.push(b' ');
            }
            let mut size_buf = itoa::Buffer::new();
            buf.extend_from_slice(size_buf.format(payload_len).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(&payload);
            buf.extend_from_slice(b"\r\n");
            (false, false)
        }
    }
}

// Main server structure (remains mostly the same)
pub struct NatsServer<H: NatsServerHandler> {
    listener: TcpListener,
    handler: Arc<H>,
    server_info: Arc<ServerInfo>,
}

impl<H: NatsServerHandler + Clone> NatsServer<H> {
    pub async fn new(addr: &str, handler: H, server_info: ServerInfo) -> ServerResult<Self> {
        let listener = TcpListener::bind(addr).await?;
        info!("NATS server listening on {}", addr);
        Ok(NatsServer {
            listener,
            handler: Arc::new(handler),
            server_info: Arc::new(server_info),
        })
    }

    pub async fn run(self) -> ServerResult<()> {
        // Channel buffer size - adjust as needed
        const CHANNEL_BUF_SIZE: usize = 10000;

        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Accepted connection from: {}", addr);
                    let handler_clone = Arc::clone(&self.handler);
                    let server_info_clone = Arc::clone(&self.server_info);

                    tokio::spawn(async move {
                        let client_id = generate_client_id();

                        let (read_stream, write_stream) = stream.into_split();

                        // Spawn a dedicated writer task
                        let (write_task_tx, mut write_task_rx) =
                            mpsc::channel::<ServerCommand>(CHANNEL_BUF_SIZE);

                        tokio::spawn(async move {
                            let flush_threshold_bytes =
                                env_usize("WISP_WRITEV_FLUSH_THRESHOLD_BYTES", 64 * 1024);
                            let flush_idle_us = env_u64("WISP_WRITEV_FLUSH_IDLE_US", 1_000);

                            let mut writer = write_stream;
                            // Contiguous write buffer — all message parts are copied here,
                            // then flushed with a single write_all. This eliminates the
                            // overhead of writev scatter-gather with many small IoSlices.
                            let mut write_buf: Vec<u8> =
                                Vec::with_capacity(flush_threshold_bytes + 4096);
                            let mut flush_deadline: Option<Instant> = None;

                            loop {
                                let maybe_cmd = match flush_deadline {
                                    Some(deadline) => {
                                        tokio::select! {
                                            received = write_task_rx.recv() => received,
                                            _ = tokio::time::sleep_until(deadline) => {
                                                if !write_buf.is_empty() {
                                                    if let Err(e) = writer.write_all(&write_buf).await {
                                                        error!("[Client {} Writer] Error flushing on timer: {}", client_id, e);
                                                        break;
                                                    }
                                                    write_buf.clear();
                                                }
                                                flush_deadline = None;
                                                continue;
                                            }
                                        }
                                    }
                                    None => write_task_rx.recv().await,
                                };

                                let Some(cmd) = maybe_cmd else {
                                    // Channel closed — final flush
                                    if !write_buf.is_empty() {
                                        let _ = writer.write_all(&write_buf).await;
                                    }
                                    break;
                                };

                                let (mut should_shutdown, mut should_flush_now) =
                                    append_command_to_buf(&mut write_buf, cmd);

                                // Drain all immediately-available commands
                                while let Ok(additional_cmd) = write_task_rx.try_recv() {
                                    let (shutdown, flush_now) =
                                        append_command_to_buf(&mut write_buf, additional_cmd);
                                    should_flush_now = should_flush_now || flush_now;
                                    if shutdown {
                                        should_shutdown = true;
                                        break;
                                    }
                                }

                                if write_buf.len() >= flush_threshold_bytes
                                    || should_shutdown
                                    || should_flush_now
                                {
                                    if let Err(e) = writer.write_all(&write_buf).await {
                                        error!(
                                            "[Client {} Writer] Error flushing pending writes: {}",
                                            client_id, e
                                        );
                                        break;
                                    }
                                    write_buf.clear();
                                    flush_deadline = None;
                                } else if !write_buf.is_empty() {
                                    flush_deadline =
                                        Some(Instant::now() + Duration::from_micros(flush_idle_us));
                                }

                                if should_shutdown {
                                    debug!(
                                        "[Client {} Writer] Shutdown command received.",
                                        client_id
                                    );
                                    break;
                                }
                            }

                            let _ = writer.flush().await;
                            debug!("[Client {} Writer] Writer task finished.", client_id);
                        });

                        // --- Reader task logic ---
                        let reader = BufReader::new(read_stream);
                        let mut connection_logic = ClientConnectionLogic {
                            id: client_id,
                            reader,
                            payload_buffer: BytesMut::with_capacity(1024),
                            handler: handler_clone,
                            server_info: server_info_clone,
                            connect_options: None,
                            sender_to_writer: write_task_tx,
                        };

                        if let Err(e) = connection_logic.process_incoming().await {
                            match e {
                                ServerError::Io(ref io_err)
                                    if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                                {
                                    debug!(
                                        "[Client {} Reader] Connection closed by peer.",
                                        connection_logic.id
                                    );
                                }
                                ServerError::ClientDisconnected => {
                                    debug!(
                                        "[Client {} Reader] Client disconnected expectedly.",
                                        connection_logic.id
                                    );
                                }
                                _ => error!(
                                    "[Client {} Reader] Error processing connection: {}",
                                    connection_logic.id, e
                                ),
                            }
                        }
                        // Ensure disconnect handler is called (and tell writer task to shut down)
                        let _ = connection_logic
                            .sender_to_writer
                            .send(ServerCommand::Shutdown)
                            .await; // Ignore error if writer task already died
                        connection_logic
                            .handler
                            .handle_disconnect(connection_logic.id)
                            .await;
                        debug!(
                            "[Client {} Reader] Reader task finished.",
                            connection_logic.id
                        );
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

// Extracted logic for processing incoming commands for clarity
struct ClientConnectionLogic<H: NatsServerHandler> {
    id: u64,
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    payload_buffer: BytesMut,
    handler: Arc<H>,
    server_info: Arc<ServerInfo>,
    connect_options: Option<ConnectOptions>,
    sender_to_writer: mpsc::Sender<ServerCommand>,
}

impl<H: NatsServerHandler> ClientConnectionLogic<H> {
    /// Fast-path inline PUB parser. Parses "PUB <subject> [reply-to] <size>"
    /// directly from bytes without going through the generic parse pipeline.
    /// Returns (subject, reply_to, size) or None if the line is not a PUB command.
    #[inline]
    fn try_parse_pub_fast(line: &[u8]) -> Option<Result<(&str, Option<&str>, usize), ServerError>> {
        // Check for "PUB " or "pub " prefix (4 bytes)
        if line.len() < 5 {
            return None;
        }
        let cmd = &line[..3];
        if !((cmd[0] == b'P' || cmd[0] == b'p')
            && (cmd[1] == b'U' || cmd[1] == b'u')
            && (cmd[2] == b'B' || cmd[2] == b'b')
            && line[3] == b' ')
        {
            return None;
        }

        let args = &line[4..];
        if !args.is_ascii() {
            return Some(Err(ServerError::InvalidProtocol(
                "Non-ASCII PUB arguments".to_string(),
            )));
        }
        // SAFETY: validated ASCII above
        let args_str = unsafe { std::str::from_utf8_unchecked(args) };

        // Parse: <subject> [reply-to] <size>
        // Find first space → end of subject
        let subject_end = match memchr::memchr(b' ', args) {
            Some(i) => i,
            None => {
                return Some(Err(ServerError::InvalidCommand(
                    "PUB missing size argument".to_string(),
                )));
            }
        };
        let subject = &args_str[..subject_end];

        // Check for wildcards in subject
        if memchr::memchr(b'*', subject.as_bytes()).is_some()
            || memchr::memchr(b'>', subject.as_bytes()).is_some()
        {
            return Some(Err(ServerError::InvalidArgument {
                command: "PUB".to_string(),
                argument: format!("wildcard subjects are not allowed: '{}'", subject),
            }));
        }

        // Skip spaces after subject
        let mut pos = subject_end + 1;
        while pos < args.len() && args[pos] == b' ' {
            pos += 1;
        }
        if pos >= args.len() {
            return Some(Err(ServerError::InvalidCommand(
                "PUB missing size argument".to_string(),
            )));
        }

        // Find next space (if any) → determines if there's a reply-to
        let rest = &args_str[pos..];
        match memchr::memchr(b' ', rest.as_bytes()) {
            Some(i) => {
                // Three tokens: subject reply-to size
                let reply_to = &rest[..i];
                let size_str = rest[i + 1..].trim_start();
                match size_str.parse::<usize>() {
                    Ok(size) => Some(Ok((subject, Some(reply_to), size))),
                    Err(_) => Some(Err(ServerError::InvalidArgument {
                        command: "PUB".to_string(),
                        argument: format!("Invalid size: {}", size_str),
                    })),
                }
            }
            None => {
                // Two tokens: subject size
                match rest.parse::<usize>() {
                    Ok(size) => Some(Ok((subject, None, size))),
                    Err(_) => Some(Err(ServerError::InvalidArgument {
                        command: "PUB".to_string(),
                        argument: format!("Invalid size: {}", rest),
                    })),
                }
            }
        }
    }

    /// Read payload + CRLF, validate, return payload as Bytes.
    #[inline]
    async fn read_pub_payload(&mut self, size: usize) -> ServerResult<Bytes> {
        let payload_with_crlf = size + 2;
        self.payload_buffer.clear();
        self.payload_buffer.reserve(payload_with_crlf);
        self.payload_buffer.resize(payload_with_crlf, 0);
        self.reader.read_exact(&mut self.payload_buffer).await?;

        if &self.payload_buffer[size..] != b"\r\n" {
            return Err(ServerError::InvalidProtocol(
                "PUB payload not followed by CRLF".to_string(),
            ));
        }

        let payload = self.payload_buffer.split_to(size).freeze();
        self.payload_buffer.clear();
        Ok(payload)
    }

    /// Processes incoming commands from the client's reader half.
    async fn process_incoming(&mut self) -> ServerResult<()> {
        let info_wire_string = self.server_info.to_wire_string()?;
        let info_bytes = Bytes::from(info_wire_string);

        if self
            .sender_to_writer
            .send(ServerCommand::Send(info_bytes))
            .await
            .is_err()
        {
            return Err(ServerError::ClientDisconnected);
        }
        debug!("[Client {}] Queued INFO for sending", self.id);

        let mut line_buffer = Vec::with_capacity(1024);

        // Main command processing loop
        loop {
            line_buffer.clear();

            let read_result = self.reader.read_until(b'\n', &mut line_buffer).await;

            match read_result {
                Ok(0) => {
                    info!("[Client {}] Read stream closed by peer.", self.id);
                    return Err(ServerError::ClientDisconnected);
                }
                Ok(_) => {
                    // Trim trailing \r\n or \n
                    let mut line_bytes = &line_buffer[..];
                    if line_bytes.ends_with(b"\r\n") {
                        line_bytes = &line_bytes[..line_bytes.len() - 2];
                    } else if line_bytes.ends_with(b"\n") {
                        line_bytes = &line_bytes[..line_bytes.len() - 1];
                    }

                    if line_bytes.is_empty() {
                        continue;
                    }

                    // === Fast path: PUB command (most frequent in benchmarks) ===
                    if let Some(pub_result) = Self::try_parse_pub_fast(line_bytes) {
                        let handler_result = match pub_result {
                            Ok((subject, reply_to, size)) => {
                                let payload = self.read_pub_payload(size).await?;
                                self.handler
                                    .handle_pub(
                                        self.id,
                                        subject,
                                        reply_to,
                                        payload,
                                        &self.sender_to_writer,
                                    )
                                    .await
                            }
                            Err(e) => Err(e),
                        };

                        if let Err(e) = handler_result {
                            error!("[Client {}] Error handling PUB: {}", self.id, e);
                            let err_msg = protocol::format_err(&e.to_string());
                            if self
                                .sender_to_writer
                                .send(ServerCommand::SendImmediate(err_msg))
                                .await
                                .is_err()
                            {
                                return Err(ServerError::ClientDisconnected);
                            }
                        }
                        continue;
                    }

                    // === Slow path: all other commands ===
                    let parse_result = protocol::parse_command_line_bytes(line_bytes);

                    match parse_result {
                        Ok((command_bytes, args_bytes)) => {
                            let handler_result =
                                if protocol::command_matches(command_bytes, b"CONNECT") {
                                    let args_str = std::str::from_utf8(args_bytes).map_err(|_| {
                                        ServerError::InvalidProtocol(
                                            "Invalid UTF-8 in command arguments".to_string(),
                                        )
                                    });

                                    match args_str {
                                        Ok(args_str) => {
                                            self.handle_command_bytes(command_bytes, args_str).await
                                        }
                                        Err(e) => Err(e),
                                    }
                                } else {
                                    if !args_bytes.is_ascii() {
                                        Err(ServerError::InvalidProtocol(
                                            "Non-ASCII command arguments".to_string(),
                                        ))
                                    } else {
                                        let args_str =
                                            unsafe { std::str::from_utf8_unchecked(args_bytes) };
                                        self.handle_command_bytes(command_bytes, args_str).await
                                    }
                                };

                            if let Err(e) = handler_result {
                                let command_str = String::from_utf8_lossy(command_bytes);
                                error!(
                                    "[Client {}] Error handling command '{}': {}",
                                    self.id, command_str, e
                                );
                                let err_msg = protocol::format_err(&e.to_string());
                                if self
                                    .sender_to_writer
                                    .send(ServerCommand::SendImmediate(err_msg))
                                    .await
                                    .is_err()
                                {
                                    return Err(ServerError::ClientDisconnected);
                                }
                            }
                        }
                        Err(e) => {
                            let line_str = String::from_utf8_lossy(line_bytes);
                            warn!(
                                "[Client {}] Invalid protocol line '{}': {}",
                                self.id, line_str, e
                            );
                            let err_msg = protocol::format_err("Unknown Protocol Operation");
                            if self
                                .sender_to_writer
                                .send(ServerCommand::SendImmediate(err_msg))
                                .await
                                .is_err()
                            {
                                return Err(ServerError::ClientDisconnected);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("[Client {}] Error reading from stream: {}", self.id, e);
                    return Err(ServerError::Io(e));
                }
            }
        }
    }

    /// Zero-allocation version of handle_command that works with byte slices
    async fn handle_command_bytes(
        &mut self,
        command_bytes: &[u8],
        args_str: &str,
    ) -> ServerResult<()> {
        // Use case-insensitive byte comparison instead of string conversion
        if protocol::command_matches(command_bytes, b"CONNECT") {
            let options = protocol::parse_connect_args(args_str)?;
            // Pass the sender clone to the handler
            self.handler
                .handle_connect(self.id, &options, &self.sender_to_writer)
                .await?;
            self.connect_options = Some(options); // Store options after successful handling
        } else if protocol::command_matches(command_bytes, b"PUB") {
            // Fallback PUB path (fast path in process_incoming handles most PUBs)
            let (subject, reply_to, size) = protocol::parse_pub_args(args_str)?;
            let payload = self.read_pub_payload(size).await?;
            self.handler
                .handle_pub(self.id, subject, reply_to, payload, &self.sender_to_writer)
                .await?;
        } else if protocol::command_matches(command_bytes, b"SUB") {
            let (subject, queue_group, sid) = protocol::parse_sub_args(args_str)?;
            self.handler
                .handle_sub(self.id, subject, queue_group, sid, &self.sender_to_writer)
                .await?;
        } else if protocol::command_matches(command_bytes, b"UNSUB") {
            let (sid, max_msgs) = protocol::parse_unsub_args(args_str)?;
            self.handler
                .handle_unsub(self.id, sid, max_msgs, &self.sender_to_writer)
                .await?;
        } else if protocol::command_matches(command_bytes, b"PING") {
            if !args_str.is_empty() {
                warn!(
                    "[Client {}] PING received with unexpected arguments: '{}'",
                    self.id, args_str
                );
            }
            self.handler
                .handle_ping(self.id, &self.sender_to_writer)
                .await?;
            // Respond with PONG unless echo is disabled
            let should_send_pong = self.connect_options.as_ref().map_or(true, |opts| opts.echo);
            if should_send_pong {
                let pong_bytes = Bytes::from_static(b"PONG\r\n");
                // Queue PONG via sender
                if self
                    .sender_to_writer
                    .send(ServerCommand::SendImmediate(pong_bytes))
                    .await
                    .is_err()
                {
                    warn!(
                        "[Client {}] Failed to queue PONG response: channel closed.",
                        self.id
                    );
                    return Err(ServerError::ClientDisconnected);
                }
                debug!("[Client {}] Queued PONG for sending", self.id);
            } else {
                debug!("[Client {}] Suppressing PONG due to echo: false", self.id);
            }
        } else if protocol::command_matches(command_bytes, b"PONG") {
            if !args_str.is_empty() {
                warn!(
                    "[Client {}] PONG received with unexpected arguments: '{}'",
                    self.id, args_str
                );
            }
            self.handler
                .handle_pong(self.id, &self.sender_to_writer)
                .await?;
        } else {
            let command_str = String::from_utf8_lossy(command_bytes);
            warn!(
                "[Client {}] Received unknown command: {}",
                self.id, command_str
            );
            return Err(ServerError::InvalidCommand(format!(
                "Unknown command: {}",
                command_str
            ))); // Return error to send -ERR
        }
        Ok(()) // Command handled successfully (at least by the parser/dispatcher)
    }
}
