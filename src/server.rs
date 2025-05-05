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
use tracing::{debug, error, info, warn}; // For potential timeouts

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
        const CHANNEL_BUF_SIZE: usize = 128;

        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Accepted connection from: {}", addr);
                    let handler_clone = Arc::clone(&self.handler);
                    let server_info_clone = Arc::clone(&self.server_info);

                    tokio::spawn(async move {
                        let client_id = generate_client_id();

                        // Split the stream into independent read/write halves
                        let (read_stream, write_stream) = stream.into_split();
                        let writer =
                            Arc::new(tokio::sync::Mutex::new(BufWriter::new(write_stream)));

                        // Spawn a dedicated writer task
                        let writer_clone = Arc::clone(&writer);
                        let (write_task_tx, mut write_task_rx) =
                            mpsc::channel::<ServerCommand>(CHANNEL_BUF_SIZE);

                        tokio::spawn(async move {
                            while let Some(cmd) = write_task_rx.recv().await {
                                match cmd {
                                    ServerCommand::Send(bytes) => {
                                        let mut w = writer_clone.lock().await;
                                        if let Err(e) = w.write_all(&bytes).await {
                                            error!(
                                                "[Client {} Writer] Error writing bytes: {}",
                                                client_id, e
                                            );
                                            break; // Stop writing on error
                                        }
                                        if let Err(e) = w.flush().await {
                                            error!(
                                                "[Client {} Writer] Error flushing writer: {}",
                                                client_id, e
                                            );
                                            break; // Stop writing on error
                                        }
                                    }
                                    ServerCommand::Shutdown => {
                                        debug!(
                                            "[Client {} Writer] Shutdown command received.",
                                            client_id
                                        );
                                        // Try a final flush
                                        let mut w = writer_clone.lock().await;
                                        let _ = w.flush().await;
                                        break;
                                    }
                                }
                            }
                            debug!("[Client {} Writer] Writer task finished.", client_id);
                        });

                        // --- Reader task logic ---
                        let reader = BufReader::new(read_stream);
                        let mut connection_logic = ClientConnectionLogic {
                            id: client_id,
                            reader,
                            // writer: writer, // Writer is handled by the separate task now
                            handler: handler_clone,
                            server_info: server_info_clone,
                            connect_options: None,
                            sender_to_writer: write_task_tx, // Use this sender to talk to the writer task
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
    // writer: Arc<tokio::sync::Mutex<BufWriter<tokio::net::tcp::WriteHalf<'static>>>>, // No longer needed here
    handler: Arc<H>,
    server_info: Arc<ServerInfo>,
    connect_options: Option<ConnectOptions>,
    sender_to_writer: mpsc::Sender<ServerCommand>, // Sender to communicate with the writer task
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

        let mut line_buffer = String::new();

        // Main command processing loop
        loop {
            line_buffer.clear();

            // Use select! to potentially handle timeouts or other events later
            // For now, just read the line
            let read_result = self.reader.read_line(&mut line_buffer).await;

            match read_result {
                Ok(0) => {
                    // Connection closed by client
                    info!("[Client {}] Read stream closed by peer.", self.id);
                    return Err(ServerError::ClientDisconnected);
                }
                Ok(_) => {
                    // Bytes read, process line
                    let raw_line = line_buffer.trim_end();
                    debug!("[Client {}] Received Raw: '{}'", self.id, raw_line);

                    if raw_line.is_empty() {
                        continue;
                    }

                    let parse_result = protocol::parse_command_line(raw_line);

                    match parse_result {
                        Ok((command_upper, args_str)) => {
                            debug!(
                                "[Client {}] Parsed Command: '{}', Args Str: '{}'",
                                self.id, command_upper, args_str
                            );
                            let handler_result =
                                self.handle_command(&command_upper, args_str).await;

                            if let Err(e) = handler_result {
                                error!(
                                    "[Client {}] Error handling command '{}': {}",
                                    self.id, command_upper, e
                                );
                                // Send -ERR back to client
                                let err_msg = protocol::format_err(&e.to_string());
                                // Use try_send for immediate errors if buffer full is acceptable, or await send
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
                                    return Err(ServerError::ClientDisconnected); // Assume disconnect if can't send error
                                }
                                // Decide if the error is fatal for the connection
                                // if is_fatal(&e) { return Err(e); }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "[Client {}] Invalid protocol line '{}': {}",
                                self.id, raw_line, e
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
                            // Continue processing? Or disconnect on protocol errors? Let's continue for now.
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

    /// Helper to dispatch to the correct handler method
    async fn handle_command(&mut self, command: &str, args_str: &str) -> ServerResult<()> {
        match command {
            "CONNECT" => {
                let options = protocol::parse_connect_args(args_str)?;
                // Pass the sender clone to the handler
                self.handler
                    .handle_connect(self.id, &options, &self.sender_to_writer)
                    .await?;
                self.connect_options = Some(options); // Store options after successful handling
            }
            "PUB" => {
                let (subject, reply_to, size) = protocol::parse_pub_args(args_str)?;
                // Read payload
                let mut payload_buffer = BytesMut::with_capacity(size + 2);
                payload_buffer.resize(size + 2, 0);
                self.reader.read_exact(&mut payload_buffer).await?; // Read from reader half

                if &payload_buffer[size..] != b"\r\n" {
                    return Err(ServerError::InvalidProtocol(
                        "PUB payload not followed by CRLF".to_string(),
                    ));
                }
                let payload = payload_buffer.split_to(size).freeze();
                // Pass sender clone to handler
                self.handler
                    .handle_pub(
                        self.id,
                        &subject,
                        reply_to.as_deref(),
                        payload,
                        &self.sender_to_writer,
                    )
                    .await?;
            }
            "SUB" => {
                let (subject, queue_group, sid) = protocol::parse_sub_args(args_str)?;
                self.handler
                    .handle_sub(
                        self.id,
                        &subject,
                        queue_group.as_deref(),
                        &sid,
                        &self.sender_to_writer,
                    )
                    .await?;
            }
            "UNSUB" => {
                let (sid, max_msgs) = protocol::parse_unsub_args(args_str)?;
                self.handler
                    .handle_unsub(self.id, &sid, max_msgs, &self.sender_to_writer)
                    .await?;
            }
            "PING" => {
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
            }
            "PONG" => {
                if !args_str.is_empty() {
                    warn!(
                        "[Client {}] PONG received with unexpected arguments: '{}'",
                        self.id, args_str
                    );
                }
                self.handler
                    .handle_pong(self.id, &self.sender_to_writer)
                    .await?;
            }
            _ => {
                warn!("[Client {}] Received unknown command: {}", self.id, command);
                return Err(ServerError::InvalidCommand(format!(
                    "Unknown command: {}",
                    command
                ))); // Return error to send -ERR
            }
        }
        Ok(()) // Command handled successfully (at least by the parser/dispatcher)
    }
}
