// src/server.rs
use crate::command::ServerCommand; // Import ServerCommand
use crate::error::{ServerError, ServerResult};
use crate::handler::NatsServerHandler;
use crate::protocol::{self, ConnectOptions, ServerInfo}; // Add ServerCommand
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
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
                            const WRITER_BUF_CAPACITY: usize = 64 * 1024;
                            const FLUSH_THRESHOLD_BYTES: usize = 32 * 1024;
                            const FLUSH_IDLE_MS: u64 = 1;
                            const CRLF: &[u8] = b"\r\n";

                            let mut writer =
                                BufWriter::with_capacity(WRITER_BUF_CAPACITY, write_stream);
                            let mut pending_bytes = 0usize;
                            let mut flush_deadline: Option<Instant> = None;

                            async fn write_command(
                                writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
                                cmd: ServerCommand,
                            ) -> std::io::Result<usize> {
                                match cmd {
                                    ServerCommand::Send(bytes) => {
                                        writer.write_all(&bytes).await?;
                                        Ok(bytes.len())
                                    }
                                    ServerCommand::SendMessage { header, payload } => {
                                        writer.write_all(&header).await?;
                                        writer.write_all(&payload).await?;
                                        writer.write_all(CRLF).await?;
                                        Ok(header.len() + payload.len() + CRLF.len())
                                    }
                                    ServerCommand::Shutdown => Ok(0),
                                }
                            }

                            async fn flush_pending(
                                writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
                                pending_bytes: &mut usize,
                            ) -> std::io::Result<()> {
                                if *pending_bytes == 0 {
                                    return Ok(());
                                }

                                writer.flush().await?;
                                *pending_bytes = 0;
                                Ok(())
                            }

                            loop {
                                let cmd = match flush_deadline {
                                    Some(deadline) => {
                                        tokio::select! {
                                            maybe_cmd = write_task_rx.recv() => maybe_cmd,
                                            _ = tokio::time::sleep_until(deadline) => {
                                                if let Err(e) = flush_pending(&mut writer, &mut pending_bytes).await {
                                                    error!("[Client {} Writer] Error flushing on timer: {}", client_id, e);
                                                    break;
                                                }
                                                flush_deadline = None;
                                                continue;
                                            }
                                        }
                                    }
                                    None => write_task_rx.recv().await,
                                };

                                let Some(cmd) = cmd else {
                                    if let Err(e) =
                                        flush_pending(&mut writer, &mut pending_bytes).await
                                    {
                                        error!(
                                            "[Client {} Writer] Error flushing on channel close: {}",
                                            client_id, e
                                        );
                                    }
                                    break;
                                };

                                if matches!(cmd, ServerCommand::Shutdown) {
                                    debug!(
                                        "[Client {} Writer] Shutdown command received.",
                                        client_id
                                    );
                                    if let Err(e) =
                                        flush_pending(&mut writer, &mut pending_bytes).await
                                    {
                                        error!(
                                            "[Client {} Writer] Error flushing on shutdown: {}",
                                            client_id, e
                                        );
                                    }
                                    break;
                                }

                                match write_command(&mut writer, cmd).await {
                                    Ok(bytes_written) => {
                                        pending_bytes += bytes_written;
                                    }
                                    Err(e) => {
                                        error!(
                                            "[Client {} Writer] Error writing: {}",
                                            client_id, e
                                        );
                                        break;
                                    }
                                }

                                while let Ok(additional_cmd) = write_task_rx.try_recv() {
                                    if matches!(additional_cmd, ServerCommand::Shutdown) {
                                        debug!(
                                            "[Client {} Writer] Shutdown command received.",
                                            client_id
                                        );
                                        if let Err(e) =
                                            flush_pending(&mut writer, &mut pending_bytes).await
                                        {
                                            error!(
                                                "[Client {} Writer] Error flushing on shutdown: {}",
                                                client_id, e
                                            );
                                        }
                                        return;
                                    }

                                    match write_command(&mut writer, additional_cmd).await {
                                        Ok(bytes_written) => {
                                            pending_bytes += bytes_written;
                                        }
                                        Err(e) => {
                                            error!(
                                                "[Client {} Writer] Error writing pending: {}",
                                                client_id, e
                                            );
                                            return;
                                        }
                                    }
                                }

                                if pending_bytes >= FLUSH_THRESHOLD_BYTES {
                                    if let Err(e) =
                                        flush_pending(&mut writer, &mut pending_bytes).await
                                    {
                                        error!(
                                            "[Client {} Writer] Error flushing on threshold: {}",
                                            client_id, e
                                        );
                                        break;
                                    }
                                    flush_deadline = None;
                                    continue;
                                }

                                flush_deadline =
                                    Some(Instant::now() + Duration::from_millis(FLUSH_IDLE_MS));
                            }

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
    /// Processes incoming commands from the client's reader half.
    async fn process_incoming(&mut self) -> ServerResult<()> {
        // 1. Send INFO immediately using the sender
        // Correct way to send INFO:
        let info_wire_string = self.server_info.to_wire_string()?;
        let info_bytes = Bytes::from(info_wire_string);

        if self
            .sender_to_writer
            .send(ServerCommand::Send(info_bytes))
            .await
            .is_err()
        {
            return Err(ServerError::ClientDisconnected); // Channel closed, writer task likely died
        }
        debug!("[Client {}] Queued INFO for sending", self.id);

        let mut line_buffer = Vec::with_capacity(1024);

        // Main command processing loop
        loop {
            line_buffer.clear();

            // Use Vec<u8> for more efficient line reading - avoids String allocations
            let read_result = self.reader.read_until(b'\n', &mut line_buffer).await;

            match read_result {
                Ok(0) => {
                    // Connection closed by client
                    info!("[Client {}] Read stream closed by peer.", self.id);
                    return Err(ServerError::ClientDisconnected);
                }
                Ok(_) => {
                    // Process the line we just read - remove trailing \r\n
                    let mut line_bytes = &line_buffer[..];
                    if line_bytes.ends_with(b"\r\n") {
                        line_bytes = &line_bytes[..line_bytes.len() - 2];
                    } else if line_bytes.ends_with(b"\n") {
                        line_bytes = &line_bytes[..line_bytes.len() - 1];
                    }

                    debug!(
                        "[Client {}] Received Raw: '{}'",
                        self.id,
                        String::from_utf8_lossy(line_bytes)
                    );

                    if line_bytes.is_empty() {
                        continue;
                    }

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
                                            debug!(
                                                "[Client {}] Parsed Command: '{}', Args Str: '{}'",
                                                self.id,
                                                String::from_utf8_lossy(command_bytes),
                                                args_str
                                            );
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
                                        // SAFETY: Non-CONNECT command arguments are validated as ASCII above.
                                        let args_str =
                                            unsafe { std::str::from_utf8_unchecked(args_bytes) };
                                        debug!(
                                            "[Client {}] Parsed Command: '{}', Args Str: '{}'",
                                            self.id,
                                            String::from_utf8_lossy(command_bytes),
                                            args_str
                                        );
                                        self.handle_command_bytes(command_bytes, args_str).await
                                    }
                                };

                            if let Err(e) = handler_result {
                                let command_str = String::from_utf8_lossy(command_bytes);
                                error!(
                                    "[Client {}] Error handling command '{}': {}",
                                    self.id, command_str, e
                                );
                                // Send -ERR back to client
                                let err_msg = protocol::format_err(&e.to_string());
                                if self
                                    .sender_to_writer
                                    .send(ServerCommand::Send(err_msg))
                                    .await
                                    .is_err()
                                {
                                    warn!(
                                        "[Client {}] Failed to queue error response: channel closed.",
                                        self.id
                                    );
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
                                .send(ServerCommand::Send(err_msg))
                                .await
                                .is_err()
                            {
                                warn!(
                                    "[Client {}] Failed to queue protocol error response: channel closed.",
                                    self.id
                                );
                                return Err(ServerError::ClientDisconnected);
                            }
                        }
                    }
                }
                Err(e) => {
                    // IO error reading line
                    error!("[Client {}] Error reading from stream: {}", self.id, e);
                    return Err(ServerError::Io(e));
                }
            }
        } // end loop
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
            let (subject, reply_to, size) = protocol::parse_pub_args(args_str)?;

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
                    .send(ServerCommand::Send(pong_bytes))
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
