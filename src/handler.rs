use std::sync::Arc;

use crate::command::ServerCommand;
use crate::error::ServerResult;
use crate::protocol::{self, ConnectOptions};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{RwLock, mpsc};

/// Trait defining the callbacks for handling NATS client commands.
/// Implement this trait to define the server's behavior.
/// Trait defining the callbacks for handling NATS client commands.
/// Implement this trait to define the server's behavior.
#[async_trait]
pub trait NatsServerHandler: Send + Sync + 'static {
    /// Called when a client issues a CONNECT command.
    /// `sender` can be used to queue messages back to the client (e.g., errors).
    async fn handle_connect(
        &self,
        client_id: u64,
        options: &ConnectOptions,
        sender: &mpsc::Sender<ServerCommand>, // <-- Add sender
    ) -> ServerResult<()>;

    /// Called when a client issues a PUB command.
    /// `sender` can be used to queue messages back to the client (e.g., errors, or later for dispatching MSG).
    async fn handle_pub(
        &self,
        client_id: u64,
        subject: &str,
        reply_to: Option<&str>,
        payload: Bytes,
        sender: &mpsc::Sender<ServerCommand>, // <-- Add sender
    ) -> ServerResult<()>;

    /// Called when a client issues a SUB command.
    /// `sender` can be used to queue messages back to the client (e.g., acknowledgements, errors).
    async fn handle_sub(
        &self,
        client_id: u64,
        subject: &str,
        queue_group: Option<&str>,
        sid: &str,
        sender: &mpsc::Sender<ServerCommand>, // <-- Add sender
    ) -> ServerResult<()>;

    /// Called when a client issues an UNSUB command.
    async fn handle_unsub(
        &self,
        client_id: u64,
        sid: &str,
        max_msgs: Option<u64>,
        sender: &mpsc::Sender<ServerCommand>, // <-- Add sender
    ) -> ServerResult<()>;

    /// Called when a client issues a PING command.
    /// The server automatically sends PONG via the connection task unless ConnectOptions.echo was true.
    /// This handler might be used for custom logic or error reporting via sender.
    async fn handle_ping(
        &self,
        client_id: u64,
        sender: &mpsc::Sender<ServerCommand>, // <-- Add sender
    ) -> ServerResult<()>;

    /// Called when a client issues a PONG command.
    async fn handle_pong(
        &self,
        client_id: u64,
        sender: &mpsc::Sender<ServerCommand>, // <-- Add sender
    ) -> ServerResult<()>;

    /// Called when a client connection is closed (normally or due to error).
    /// The sender might be invalid here, so it's not passed.
    async fn handle_disconnect(&self, client_id: u64);
}

// --- Example Handler Implementation ---

/// A simple handler that just prints received commands.
#[derive(Debug, Clone)]
pub struct ClientHandler {
    // Client IDs - using DashMap for lock-free concurrent access
    // Now stores direct connection to client writer tasks
    clients: Arc<DashMap<u64, mpsc::Sender<ServerCommand>>>,

    // Single subscription map - can't shard due to wildcard matching requirements
    subscriptions: Arc<RwLock<submap::SubMap<u64>>>,

    // Map from (client_id, subject) to SID for proper message formatting
    sid_map: Arc<DashMap<(u64, String), String>>,
}

impl ClientHandler {
    /// Creates a new instance of `ClientHandler`.
    pub fn new() -> Self {
        ClientHandler {
            clients: Arc::new(DashMap::new()),
            subscriptions: Arc::new(RwLock::new(
                submap::SubMap::new().separator('.').wildcard("*"),
            )),
            sid_map: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl NatsServerHandler for ClientHandler {
    async fn handle_connect(
        &self,
        client_id: u64,
        options: &ConnectOptions,
        _sender: &mpsc::Sender<ServerCommand>,
    ) -> ServerResult<()> {
        println!("[Client {}] CONNECT: {:?}", client_id, options);
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.register_client(&client_id);
        Ok(())
    }

    async fn handle_pub(
        &self,
        _client_id: u64,
        subject: &str,
        reply_to: Option<&str>,
        payload: Bytes,
        _sender: &mpsc::Sender<ServerCommand>,
    ) -> ServerResult<()> {
        // let payload_str = String::from_utf8_lossy(&payload);
        // println!(
        //     "[Client {}] PUB Subject: '{}', ReplyTo: {:?}, Payload: '{}'",
        //     client_id, subject, reply_to, payload_str
        // );

        // Find out the clients subscribed to this subject
        let subscriptions = self.subscriptions.read().await;
        let client_ids = subscriptions.get_subscribers(subject);
        
        
        // Collect clients and their senders first to minimize lock time
        let mut clients_to_send = Vec::with_capacity(client_ids.len());
        for client_id in client_ids {
            if let Some(tx_ref) = self.clients.get(&client_id) {
                let tx = tx_ref.value().clone();
                clients_to_send.push((client_id, tx));
            }
        }
        drop(subscriptions); // Release the read lock early
        
        // Process each client
        for (client_id, tx) in clients_to_send {
            // Look up the SID for this client and subject
            let sid = self
                .sid_map
                .get(&(client_id, subject.to_string()))
                .map(|entry| entry.value().clone())
                .unwrap_or_else(|| "1".to_string()); // Fallback SID

            // Format the MSG protocol message for this subscriber
            let msg = protocol::format_msg(subject, &sid, reply_to, &payload);

            // Send the formatted message directly to the client's writer task
            // Try non-blocking first, fall back to blocking if channel is full
            match tx.try_send(ServerCommand::Send(msg)) {
                Ok(_) => {
                    // Message sent successfully
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Client disconnected, remove from map
                    self.clients.remove(&client_id);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Channel full, use blocking send to apply back-pressure
                    let msg_copy = protocol::format_msg(subject, &sid, reply_to, &payload);
                    if let Err(_) = tx.send(ServerCommand::Send(msg_copy)).await {
                        // Client likely disconnected, remove from map
                        self.clients.remove(&client_id);
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_sub(
        &self,
        client_id: u64,
        subject: &str,
        queue_group: Option<&str>,
        sid: &str,
        sender: &mpsc::Sender<ServerCommand>,
    ) -> ServerResult<()> {
        println!(
            "[Client {}] SUB Subject: '{}', QueueGroup: {:?}, SID: '{}'",
            client_id, subject, queue_group, sid
        );

        // Store the client's writer channel directly (no per-subscription channels)
        self.clients.insert(client_id, sender.clone());

        // Register the client's interest in this subject
        self.subscriptions
            .write()
            .await
            .subscribe(subject, &client_id);

        // Store the SID mapping for this subscription
        self.sid_map
            .insert((client_id, subject.to_string()), sid.to_string());

        Ok(())
    }

    async fn handle_unsub(
        &self,
        client_id: u64,
        sid: &str,
        max_msgs: Option<u64>,
        _sender: &mpsc::Sender<ServerCommand>,
    ) -> ServerResult<()> {
        println!(
            "[Client {}] UNSUB SID: '{}', MaxMsgs: {:?}",
            client_id, sid, max_msgs
        );
        Ok(())
    }

    async fn handle_ping(
        &self,
        client_id: u64,
        _sender: &mpsc::Sender<ServerCommand>,
    ) -> ServerResult<()> {
        println!("[Client {}] PING", client_id);
        Ok(())
    }

    async fn handle_pong(
        &self,
        client_id: u64,
        _sender: &mpsc::Sender<ServerCommand>,
    ) -> ServerResult<()> {
        println!("[Client {}] PONG", client_id);
        Ok(())
    }

    async fn handle_disconnect(&self, client_id: u64) {
        // Remove the client from the clients map
        self.clients.remove(&client_id);

        // Remove all SID mappings for this client
        self.sid_map.retain(|(cid, _), _| *cid != client_id);

        // Unregister client from subscriptions
        self.subscriptions
            .write()
            .await
            .unregister_client(&client_id);
        println!("[Client {}] Disconnected", client_id);
    }
}
