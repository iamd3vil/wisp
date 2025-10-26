use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::command::ServerCommand;
use crate::error::ServerResult;
use crate::protocol::{self, ConnectOptions};
use ahash::AHasher;
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

#[derive(Debug, Clone)]
struct SubscriberDispatch {
    client_id: u64,
    sender: mpsc::Sender<ServerCommand>,
    sid: Arc<str>,
}

// --- Example Handler Implementation ---

/// A simple handler that just prints received commands.
#[derive(Debug, Clone)]
pub struct ClientHandler {
    // Client IDs - using DashMap for lock-free concurrent access
    // Now stores direct connection to client writer tasks
    clients: Arc<DashMap<u64, mpsc::Sender<ServerCommand>>>,

    // Sharded subscription maps to reduce contention on hot paths
    subscriptions: Arc<Vec<RwLock<submap::SubMap<u64>>>>,
    // Dedicated shard for wildcard-first subscriptions (e.g., "*.foo", ">")
    wildcard_subscriptions: Arc<RwLock<submap::SubMap<u64>>>,
    wildcard_has_subscribers: Arc<AtomicBool>,

    // Map from client_id to subject -> SID for proper message formatting
    sid_map: Arc<DashMap<u64, DashMap<String, String>>>,

    // Cache resolved subscriber lists per publish subject
    dispatch_cache: Arc<DashMap<String, Arc<Vec<SubscriberDispatch>>>>,
}

impl ClientHandler {
    const NUM_SUB_SHARDS: usize = 64;

    /// Creates a new instance of `ClientHandler`.
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(Self::NUM_SUB_SHARDS);
        for _ in 0..Self::NUM_SUB_SHARDS {
            shards.push(RwLock::new(Self::create_submap()));
        }

        ClientHandler {
            clients: Arc::new(DashMap::new()),
            subscriptions: Arc::new(shards),
            wildcard_subscriptions: Arc::new(RwLock::new(Self::create_submap())),
            wildcard_has_subscribers: Arc::new(AtomicBool::new(false)),
            sid_map: Arc::new(DashMap::new()),
            dispatch_cache: Arc::new(DashMap::new()),
        }
    }

    fn create_submap() -> submap::SubMap<u64> {
        submap::SubMap::new().separator('.').wildcard("*")
    }

    fn subject_prefix(subject: &str) -> &str {
        subject.split('.').next().unwrap_or("")
    }

    fn is_wildcard_prefix(prefix: &str) -> bool {
        prefix.is_empty() || prefix == "*" || prefix == ">"
    }

    fn shard_index(prefix: &str) -> usize {
        let mut hasher = AHasher::default();
        prefix.hash(&mut hasher);
        (hasher.finish() as usize) % Self::NUM_SUB_SHARDS
    }

    fn invalidate_dispatch_cache(&self) {
        self.dispatch_cache.clear();
    }

    fn resolve_sid_for_subject(&self, client_id: u64, subject: &str) -> Option<Arc<str>> {
        let subject_map = self.sid_map.get(&client_id)?;

        if let Some(sid_ref) = subject_map.get(subject) {
            return Some(Arc::<str>::from(sid_ref.value().as_str()));
        }

        for entry in subject_map.value().iter() {
            if Self::subject_matches(entry.key(), subject) {
                return Some(Arc::<str>::from(entry.value().as_str()));
            }
        }

        None
    }

    fn subject_matches(pattern: &str, subject: &str) -> bool {
        if pattern == subject {
            return true;
        }

        let mut pattern_tokens = pattern.split('.');
        let mut subject_tokens = subject.split('.');

        loop {
            match (pattern_tokens.next(), subject_tokens.next()) {
                (Some(">"), _) => return true,
                (Some(p), Some(s)) => {
                    if p == "*" || p == s {
                        continue;
                    }
                    return false;
                }
                (Some(p), None) => {
                    if p == ">" && pattern_tokens.next().is_none() {
                        return true;
                    }
                    return false;
                }
                (None, Some(_)) => return false,
                (None, None) => return true,
            }
        }
    }

    async fn build_dispatches(&self, subject: &str) -> Vec<SubscriberDispatch> {
        let prefix = Self::subject_prefix(subject);
        let mut client_ids: Vec<u64> = Vec::new();

        if !Self::is_wildcard_prefix(prefix) {
            let idx = Self::shard_index(prefix);
            let targeted = {
                let shard = self.subscriptions[idx].read().await;
                shard.get_subscribers(subject)
            };
            client_ids.extend(targeted.into_iter());
        }

        if self.wildcard_has_subscribers.load(Ordering::Relaxed) {
            let wildcard_clients = {
                let shard = self.wildcard_subscriptions.read().await;
                shard.get_subscribers(subject)
            };
            client_ids.extend(wildcard_clients.into_iter());
        }

        if client_ids.is_empty() {
            return Vec::new();
        }

        client_ids.sort_unstable();
        client_ids.dedup();

        let mut dispatches = Vec::with_capacity(client_ids.len());

        for client_id in client_ids {
            if let Some(sender_ref) = self.clients.get(&client_id) {
                let sender = sender_ref.value().clone();
                drop(sender_ref);

                let sid = self
                    .resolve_sid_for_subject(client_id, subject)
                    .unwrap_or_else(|| Arc::<str>::from("1"));

                dispatches.push(SubscriberDispatch {
                    client_id,
                    sender,
                    sid,
                });
            }
        }

        dispatches
    }

    async fn get_or_build_dispatches(&self, subject: &str) -> Arc<Vec<SubscriberDispatch>> {
        if let Some(entry) = self.dispatch_cache.get(subject) {
            return Arc::clone(entry.value());
        }

        let dispatches = self.build_dispatches(subject).await;
        let arc = Arc::new(dispatches);
        self.dispatch_cache
            .insert(subject.to_string(), Arc::clone(&arc));
        arc
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
        let dispatches = self.get_or_build_dispatches(subject).await;

        if dispatches.is_empty() {
            return Ok(());
        }

        for dispatch in dispatches.iter() {
            let msg = ServerCommand::Send(protocol::format_msg(
                subject,
                dispatch.sid.as_ref(),
                reply_to,
                &payload,
            ));

            match dispatch.sender.try_send(msg) {
                Ok(_) => {}
                Err(mpsc::error::TrySendError::Closed(_cmd)) => {
                    self.clients.remove(&dispatch.client_id);
                    self.sid_map.remove(&dispatch.client_id);
                    self.invalidate_dispatch_cache();
                }
                Err(mpsc::error::TrySendError::Full(cmd)) => {
                    if dispatch.sender.send(cmd).await.is_err() {
                        self.clients.remove(&dispatch.client_id);
                        self.sid_map.remove(&dispatch.client_id);
                        self.invalidate_dispatch_cache();
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

        // Register the client's interest in this subject on shard that matches the prefix
        let prefix = Self::subject_prefix(subject);

        if Self::is_wildcard_prefix(prefix) {
            let mut shard = self.wildcard_subscriptions.write().await;
            if !shard.subscribe(subject, &client_id) {
                shard.register_client(&client_id);
                shard.subscribe(subject, &client_id);
            }
            self.wildcard_has_subscribers.store(true, Ordering::Relaxed);
        } else {
            let idx = Self::shard_index(prefix);
            let mut shard = self.subscriptions[idx].write().await;
            if !shard.subscribe(subject, &client_id) {
                shard.register_client(&client_id);
                shard.subscribe(subject, &client_id);
            }
        }

        // Store the SID mapping for this subscription
        let subject_map = self.sid_map.entry(client_id).or_insert_with(DashMap::new);
        subject_map.insert(subject.to_string(), sid.to_string());

        self.invalidate_dispatch_cache();

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
        self.invalidate_dispatch_cache();
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
        self.sid_map.remove(&client_id);

        // Unregister client from subscriptions
        for shard in self.subscriptions.iter() {
            shard.write().await.unregister_client(&client_id);
        }
        {
            let mut shard = self.wildcard_subscriptions.write().await;
            shard.unregister_client(&client_id);
            if self.wildcard_has_subscribers.load(Ordering::Relaxed) && shard.is_empty() {
                self.wildcard_has_subscribers
                    .store(false, Ordering::Relaxed);
            }
        }
        println!("[Client {}] Disconnected", client_id);

        self.invalidate_dispatch_cache();
    }
}
