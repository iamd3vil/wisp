use std::collections::HashMap;
use std::sync::Arc;

use crate::command::ServerCommand;
use crate::protocol::{self, ConnectOptions};
use crate::{error::ServerResult, protocol::NatsCommand};
use async_trait::async_trait;
use bytes::Bytes;
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
    // Client IDs.
    clients: Arc<RwLock<HashMap<u64, mpsc::Sender<NatsCommand>>>>, // Map of client IDs to their command channels

    // submap
    subscriptions: Arc<RwLock<submap::SubMap<u64>>>,
}

impl ClientHandler {
    /// Creates a new instance of `PrintHandler`.
    pub fn new() -> Self {
        ClientHandler {
            clients: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(
                submap::SubMap::new().separator('.').wildcard("*"),
            )),
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

        // Find out the clients subscribed to this subject.
        let subscriptions = self.subscriptions.read().await;
        let client_ids = subscriptions.get_subscribers(subject);
        for client_id in client_ids {
            // Get sender from the clients map.
            if let Some(tx) = self.clients.read().await.get(&client_id) {
                let res = tx
                    .send(NatsCommand::Pub {
                        subject: subject.to_string(),
                        payload: payload.clone(),
                        reply_to: reply_to.map(|s| s.to_string()),
                    })
                    .await;

                if let Err(e) = res {
                    println!(
                        "[Client {}] Error sending message to client {}: {:?}",
                        client_id, client_id, e
                    );
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
        // Create a mpsc channel for the subscription.
        let (tx, mut rx) = mpsc::channel::<NatsCommand>(1000000);
        self.clients.write().await.insert(client_id, tx);

        self.subscriptions
            .write()
            .await
            .subscribe(subject, &client_id);

        // Spawn a task to handle incoming messages for this subscription.
        let sid_c = sid.to_string();
        let sender = sender.clone();
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    NatsCommand::Pub {
                        subject,
                        payload,
                        reply_to,
                    } => {
                        // Write the message to the Tcp stream.
                        let msg =
                            protocol::format_msg(&subject, &sid_c, reply_to.as_deref(), &payload);
                        // Send the message to the client. Use try_send to avoid blocking.
                        if let Err(e) = sender.try_send(ServerCommand::Send(msg)) {
                            match e {
                                mpsc::error::TrySendError::Closed(_) => {
                                    // println!("[Client {}] Channel closed", client_id);
                                }
                                mpsc::error::TrySendError::Full(_) => {
                                    println!(
                                        "[Client {}] Channel full, dropping messages",
                                        client_id
                                    );
                                }
                            }
                        }
                    }
                    NatsCommand::Disconnect => {
                        // Handle disconnect logic if needed.
                        rx.close();
                        println!("[Client {}] Disconnecting", client_id);
                        break;
                    }
                }
            }
        });

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
        // Close the channel for this client.
        // This will cause the any subscription tasks to exit.
        if let Some(tx) = self.clients.write().await.remove(&client_id) {
            // Close the channel to signal that the client is disconnected.
            let _ = tx.send(NatsCommand::Disconnect);
        }

        self.subscriptions
            .write()
            .await
            .unregister_client(&client_id);
        println!("[Client {}] Disconnected", client_id);
    }
}
