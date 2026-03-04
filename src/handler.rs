use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::str::Split;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::command::ServerCommand;
use crate::error::ServerResult;
use crate::protocol::{self, ConnectOptions};
use ahash::AHasher;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{RwLock, mpsc};

/// Trait defining the callbacks for handling NATS client commands.
/// Implement this trait to define the server's behavior.
///
/// Uses native async fn in traits (Rust 2024 edition) to avoid
/// the per-call Box allocation imposed by #[async_trait].
pub trait NatsServerHandler: Send + Sync + 'static {
    /// Called when a client issues a CONNECT command.
    fn handle_connect(
        &self,
        client_id: u64,
        options: &ConnectOptions,
        sender: &mpsc::Sender<ServerCommand>,
    ) -> impl Future<Output = ServerResult<()>> + Send;

    /// Called when a client issues a PUB command.
    fn handle_pub(
        &self,
        client_id: u64,
        subject: &str,
        reply_to: Option<&str>,
        payload: Bytes,
        sender: &mpsc::Sender<ServerCommand>,
    ) -> impl Future<Output = ServerResult<()>> + Send;

    /// Called when a client issues a SUB command.
    fn handle_sub(
        &self,
        client_id: u64,
        subject: &str,
        queue_group: Option<&str>,
        sid: &str,
        sender: &mpsc::Sender<ServerCommand>,
    ) -> impl Future<Output = ServerResult<()>> + Send;

    /// Called when a client issues an UNSUB command.
    fn handle_unsub(
        &self,
        client_id: u64,
        sid: &str,
        max_msgs: Option<u64>,
        sender: &mpsc::Sender<ServerCommand>,
    ) -> impl Future<Output = ServerResult<()>> + Send;

    /// Called when a client issues a PING command.
    fn handle_ping(
        &self,
        client_id: u64,
        sender: &mpsc::Sender<ServerCommand>,
    ) -> impl Future<Output = ServerResult<()>> + Send;

    /// Called when a client issues a PONG command.
    fn handle_pong(
        &self,
        client_id: u64,
        sender: &mpsc::Sender<ServerCommand>,
    ) -> impl Future<Output = ServerResult<()>> + Send;

    /// Called when a client connection is closed (normally or due to error).
    fn handle_disconnect(&self, client_id: u64) -> impl Future<Output = ()> + Send;
}

#[derive(Debug, Default)]
struct WildcardSidNode {
    sid: Option<Arc<str>>,
    multi_level_sid: Option<Arc<str>>,
    star_child: Option<Box<WildcardSidNode>>,
    token_children: HashMap<String, WildcardSidNode>,
}

impl WildcardSidNode {
    fn is_empty(&self) -> bool {
        self.sid.is_none()
            && self.multi_level_sid.is_none()
            && self.star_child.is_none()
            && self.token_children.is_empty()
    }
}

#[derive(Debug, Default)]
struct WildcardSidIndex {
    root: WildcardSidNode,
}

impl WildcardSidIndex {
    fn insert(&mut self, pattern: &str, sid: &str) {
        let sid = Arc::<str>::from(sid);
        Self::insert_recursive(&mut self.root, pattern.split('.'), &sid);
    }

    fn insert_recursive(node: &mut WildcardSidNode, mut tokens: Split<'_, char>, sid: &Arc<str>) {
        let Some(token) = tokens.next() else {
            node.sid = Some(Arc::clone(sid));
            return;
        };

        if token == ">" {
            node.multi_level_sid = Some(Arc::clone(sid));
            return;
        }

        if token == "*" {
            let child = node
                .star_child
                .get_or_insert_with(|| Box::new(WildcardSidNode::default()));
            Self::insert_recursive(child.as_mut(), tokens, sid);
            return;
        }

        let child = node.token_children.entry(token.to_string()).or_default();
        Self::insert_recursive(child, tokens, sid);
    }

    fn remove(&mut self, pattern: &str, sid: &str) {
        let _ = Self::remove_recursive(&mut self.root, pattern.split('.'), sid);
    }

    fn remove_recursive(
        node: &mut WildcardSidNode,
        mut tokens: Split<'_, char>,
        sid: &str,
    ) -> bool {
        let Some(token) = tokens.next() else {
            if node.sid.as_deref() == Some(sid) {
                node.sid = None;
            }
            return node.is_empty();
        };

        if token == ">" {
            if node.multi_level_sid.as_deref() == Some(sid) {
                node.multi_level_sid = None;
            }
            return node.is_empty();
        }

        if token == "*" {
            let remove_star_child = if let Some(star_child) = node.star_child.as_mut() {
                Self::remove_recursive(star_child.as_mut(), tokens, sid)
            } else {
                false
            };

            if remove_star_child {
                node.star_child = None;
            }

            return node.is_empty();
        }

        let remove_token_child = if let Some(token_child) = node.token_children.get_mut(token) {
            Self::remove_recursive(token_child, tokens, sid)
        } else {
            false
        };

        if remove_token_child {
            node.token_children.remove(token);
        }

        node.is_empty()
    }

    fn resolve(&self, subject: &str) -> Option<Arc<str>> {
        Self::resolve_recursive(&self.root, subject.split('.'))
    }

    fn resolve_recursive(node: &WildcardSidNode, mut tokens: Split<'_, char>) -> Option<Arc<str>> {
        let Some(token) = tokens.next() else {
            if let Some(sid) = node.sid.as_ref() {
                return Some(Arc::clone(sid));
            }
            return node.multi_level_sid.as_ref().map(Arc::clone);
        };

        let remaining = tokens;

        if let Some(token_child) = node.token_children.get(token) {
            if let Some(sid) = Self::resolve_recursive(token_child, remaining.clone()) {
                return Some(sid);
            }
        }

        if let Some(star_child) = node.star_child.as_ref() {
            if let Some(sid) = Self::resolve_recursive(star_child.as_ref(), remaining.clone()) {
                return Some(sid);
            }
        }

        node.multi_level_sid.as_ref().map(Arc::clone)
    }

    fn is_empty(&self) -> bool {
        self.root.is_empty()
    }
}

#[derive(Debug, Clone)]
struct SubscriberDispatch {
    client_id: u64,
    sender: mpsc::Sender<ServerCommand>,
    msg_header_prefix: Bytes,
}

#[derive(Debug)]
struct CachedDispatch {
    dispatches: Arc<Vec<SubscriberDispatch>>,
    last_access_tick: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct ClientHandler {
    clients: Arc<DashMap<u64, mpsc::Sender<ServerCommand>>>,

    // Sharded subscription maps to reduce contention on hot paths
    subscriptions: Arc<Vec<RwLock<submap::SubMap<u64>>>>,
    // Dedicated shard for wildcard-first subscriptions (e.g., "*.foo", ">")
    wildcard_subscriptions: Arc<RwLock<submap::SubMap<u64>>>,
    wildcard_has_subscribers: Arc<AtomicBool>,

    // Map from client_id to subject -> SID for exact SID lookup and UNSUB cleanup.
    sid_map: Arc<DashMap<u64, DashMap<String, String>>>,

    // Per-client reverse wildcard index to avoid linear wildcard SID scans.
    wildcard_sid_index: Arc<DashMap<u64, WildcardSidIndex>>,

    // Cache resolved subscriber lists per publish subject (uses ahash for faster hashing)
    dispatch_cache: Arc<DashMap<String, Arc<CachedDispatch>, ahash::RandomState>>,
    dispatch_cache_capacity: usize,
    dispatch_cache_tick: Arc<AtomicU64>,
    dispatch_cache_evictions: Arc<AtomicU64>,
}

impl ClientHandler {
    const NUM_SUB_SHARDS: usize = 64;
    const DEFAULT_DISPATCH_CACHE_CAPACITY: usize = 100_000;

    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(Self::NUM_SUB_SHARDS);
        for _ in 0..Self::NUM_SUB_SHARDS {
            shards.push(RwLock::new(Self::create_submap()));
        }

        let dispatch_cache_capacity = std::env::var("WISP_DISPATCH_CACHE_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(Self::DEFAULT_DISPATCH_CACHE_CAPACITY);

        ClientHandler {
            clients: Arc::new(DashMap::new()),
            subscriptions: Arc::new(shards),
            wildcard_subscriptions: Arc::new(RwLock::new(Self::create_submap())),
            wildcard_has_subscribers: Arc::new(AtomicBool::new(false)),
            sid_map: Arc::new(DashMap::new()),
            wildcard_sid_index: Arc::new(DashMap::new()),
            dispatch_cache: Arc::new(DashMap::with_hasher(ahash::RandomState::new())),
            dispatch_cache_capacity,
            dispatch_cache_tick: Arc::new(AtomicU64::new(0)),
            dispatch_cache_evictions: Arc::new(AtomicU64::new(0)),
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

    fn next_dispatch_cache_tick(&self) -> u64 {
        self.dispatch_cache_tick.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn maybe_evict_dispatch_cache_entries(&self) {
        while self.dispatch_cache.len() > self.dispatch_cache_capacity {
            let mut lru_key: Option<String> = None;
            let mut lru_tick = u64::MAX;

            for entry in self.dispatch_cache.iter() {
                let access_tick = entry.value().last_access_tick.load(Ordering::Relaxed);
                if access_tick < lru_tick {
                    lru_tick = access_tick;
                    lru_key = Some(entry.key().clone());
                }
            }

            let Some(key) = lru_key else {
                break;
            };

            if self.dispatch_cache.remove(&key).is_none() {
                break;
            }

            self.dispatch_cache_evictions
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn invalidate_dispatch_cache_for_subscription(&self, subscription: &str) {
        if !protocol::subject_contains_wildcard(subscription) {
            self.dispatch_cache.remove(subscription);
            return;
        }

        let keys_to_remove: Vec<String> = self
            .dispatch_cache
            .iter()
            .filter_map(|entry| {
                if Self::subject_matches(subscription, entry.key()) {
                    return Some(entry.key().clone());
                }
                None
            })
            .collect();

        for key in keys_to_remove {
            self.dispatch_cache.remove(&key);
        }
    }

    fn invalidate_dispatch_cache_for_subscriptions(&self, subscriptions: &[String]) {
        for subscription in subscriptions {
            self.invalidate_dispatch_cache_for_subscription(subscription);
        }
    }

    fn client_subjects(&self, client_id: u64) -> Vec<String> {
        let Some(subject_map) = self.sid_map.get(&client_id) else {
            return Vec::new();
        };

        subject_map
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    fn find_subject_for_sid(&self, client_id: u64, sid: &str) -> Option<String> {
        let subject_map = self.sid_map.get(&client_id)?;

        for entry in subject_map.iter() {
            if entry.value().as_str() == sid {
                return Some(entry.key().clone());
            }
        }

        None
    }

    fn upsert_wildcard_sid_index(&self, client_id: u64, subject: &str, sid: &str) {
        let mut index = self
            .wildcard_sid_index
            .entry(client_id)
            .or_insert_with(WildcardSidIndex::default);
        index.insert(subject, sid);
    }

    fn remove_wildcard_sid_index(&self, client_id: u64, subject: &str, sid: &str) {
        let mut should_remove_client_entry = false;

        if let Some(mut index) = self.wildcard_sid_index.get_mut(&client_id) {
            index.remove(subject, sid);
            should_remove_client_entry = index.is_empty();
        }

        if should_remove_client_entry {
            self.wildcard_sid_index.remove(&client_id);
        }
    }

    async fn remove_subscription_from_shard(&self, client_id: u64, subject: &str) {
        let prefix = Self::subject_prefix(subject);

        if Self::is_wildcard_prefix(prefix) {
            let mut shard = self.wildcard_subscriptions.write().await;
            shard.unsubscribe(subject, &client_id);

            if shard.list_topics(&client_id).is_empty() {
                shard.unregister_client(&client_id);
            }

            self.wildcard_has_subscribers
                .store(shard.subscription_count() > 0, Ordering::Relaxed);
            return;
        }

        let idx = Self::shard_index(prefix);
        let mut shard = self.subscriptions[idx].write().await;
        shard.unsubscribe(subject, &client_id);

        if shard.list_topics(&client_id).is_empty() {
            shard.unregister_client(&client_id);
        }
    }

    async fn unregister_client_from_all_shards(&self, client_id: u64) {
        for shard in self.subscriptions.iter() {
            shard.write().await.unregister_client(&client_id);
        }

        let mut wildcard_shard = self.wildcard_subscriptions.write().await;
        wildcard_shard.unregister_client(&client_id);
        self.wildcard_has_subscribers
            .store(wildcard_shard.subscription_count() > 0, Ordering::Relaxed);
    }

    fn resolve_sid_for_subject(&self, client_id: u64, subject: &str) -> Option<Arc<str>> {
        if let Some(subject_map) = self.sid_map.get(&client_id) {
            if let Some(sid_ref) = subject_map.get(subject) {
                return Some(Arc::<str>::from(sid_ref.value().as_str()));
            }
        }

        let index = self.wildcard_sid_index.get(&client_id)?;
        index.resolve(subject)
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

        let targeted_clients: Vec<u64> = if !Self::is_wildcard_prefix(prefix) {
            let idx = Self::shard_index(prefix);
            let targeted = {
                let shard = self.subscriptions[idx].read().await;
                shard.get_subscribers(subject)
            };
            targeted.into_iter().collect()
        } else {
            Vec::new()
        };

        let wildcard_clients: Vec<u64> = if self.wildcard_has_subscribers.load(Ordering::Relaxed) {
            let wildcard = {
                let shard = self.wildcard_subscriptions.read().await;
                shard.get_subscribers(subject)
            };
            wildcard.into_iter().collect()
        } else {
            Vec::new()
        };

        if targeted_clients.is_empty() && wildcard_clients.is_empty() {
            return Vec::new();
        }

        let mut client_ids = Vec::with_capacity(targeted_clients.len() + wildcard_clients.len());
        let mut targeted_iter = targeted_clients.into_iter().peekable();
        let mut wildcard_iter = wildcard_clients.into_iter().peekable();

        while let (Some(&targeted_id), Some(&wildcard_id)) =
            (targeted_iter.peek(), wildcard_iter.peek())
        {
            if targeted_id < wildcard_id {
                client_ids.push(targeted_iter.next().expect("peek guaranteed next"));
                continue;
            }

            if wildcard_id < targeted_id {
                client_ids.push(wildcard_iter.next().expect("peek guaranteed next"));
                continue;
            }

            client_ids.push(targeted_iter.next().expect("peek guaranteed next"));
            let _ = wildcard_iter.next();
        }

        client_ids.extend(targeted_iter);
        client_ids.extend(wildcard_iter);

        let mut dispatches = Vec::with_capacity(client_ids.len());

        for client_id in client_ids {
            if let Some(sender_ref) = self.clients.get(&client_id) {
                let sender = sender_ref.value().clone();
                drop(sender_ref);

                let sid = self
                    .resolve_sid_for_subject(client_id, subject)
                    .unwrap_or_else(|| Arc::<str>::from("1"));
                let msg_header_prefix = protocol::format_msg_header_prefix(subject, sid.as_ref());

                dispatches.push(SubscriberDispatch {
                    client_id,
                    sender,
                    msg_header_prefix,
                });
            }
        }

        dispatches
    }

    async fn get_or_build_dispatches(&self, subject: &str) -> Arc<Vec<SubscriberDispatch>> {
        let access_tick = self.next_dispatch_cache_tick();

        if let Some(entry) = self.dispatch_cache.get(subject) {
            entry
                .value()
                .last_access_tick
                .store(access_tick, Ordering::Relaxed);
            return Arc::clone(&entry.value().dispatches);
        }

        let dispatches = Arc::new(self.build_dispatches(subject).await);
        self.dispatch_cache.insert(
            subject.to_string(),
            Arc::new(CachedDispatch {
                dispatches: Arc::clone(&dispatches),
                last_access_tick: AtomicU64::new(access_tick),
            }),
        );
        self.maybe_evict_dispatch_cache_entries();
        dispatches
    }
}

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

        let payload_len = payload.len();
        let reply_to = reply_to.map(|value| Bytes::copy_from_slice(value.as_bytes()));

        for dispatch in dispatches.iter() {
            let msg = ServerCommand::SendMessage {
                header_prefix: dispatch.msg_header_prefix.clone(),
                reply_to: reply_to.clone(),
                payload_len,
                payload: payload.clone(),
            };

            match dispatch.sender.try_send(msg) {
                Ok(_) => {}
                Err(mpsc::error::TrySendError::Closed(_cmd)) => {
                    self.handle_disconnect(dispatch.client_id).await;
                }
                Err(mpsc::error::TrySendError::Full(cmd)) => {
                    if dispatch.sender.send(cmd).await.is_err() {
                        self.handle_disconnect(dispatch.client_id).await;
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

        self.clients.insert(client_id, sender.clone());

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

        let subject_map = self.sid_map.entry(client_id).or_insert_with(DashMap::new);
        subject_map.insert(subject.to_string(), sid.to_string());

        if protocol::subject_contains_wildcard(subject) {
            self.upsert_wildcard_sid_index(client_id, subject, sid);
        }

        self.invalidate_dispatch_cache_for_subscription(subject);

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

        if let Some(limit) = max_msgs {
            if limit > 0 {
                println!(
                    "[Client {}] UNSUB with max_msgs={} is not implemented yet for SID '{}'",
                    client_id, limit, sid
                );
                return Ok(());
            }
        }

        let Some(subject) = self.find_subject_for_sid(client_id, sid) else {
            return Ok(());
        };

        self.remove_subscription_from_shard(client_id, &subject)
            .await;

        if protocol::subject_contains_wildcard(&subject) {
            self.remove_wildcard_sid_index(client_id, &subject, sid);
        }

        let mut remove_sid_entry = false;
        if let Some(subject_map) = self.sid_map.get(&client_id) {
            subject_map.remove(&subject);
            remove_sid_entry = subject_map.is_empty();
        }

        if remove_sid_entry {
            self.sid_map.remove(&client_id);
            self.wildcard_sid_index.remove(&client_id);
            self.unregister_client_from_all_shards(client_id).await;
        }

        self.invalidate_dispatch_cache_for_subscription(&subject);

        Ok(())
    }

    async fn handle_ping(
        &self,
        _client_id: u64,
        _sender: &mpsc::Sender<ServerCommand>,
    ) -> ServerResult<()> {
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
        let subjects = self.client_subjects(client_id);

        self.clients.remove(&client_id);
        self.sid_map.remove(&client_id);
        self.wildcard_sid_index.remove(&client_id);
        self.unregister_client_from_all_shards(client_id).await;

        println!("[Client {}] Disconnected", client_id);

        self.invalidate_dispatch_cache_for_subscriptions(&subjects);
    }
}

#[cfg(test)]
mod tests {
    use super::WildcardSidIndex;

    #[test]
    fn wildcard_sid_index_matches_specific_before_multi_level() {
        let mut index = WildcardSidIndex::default();
        index.insert("orders.>", "sid_multi");
        index.insert("orders.*", "sid_single");

        let sid = index.resolve("orders.NSE").expect("sid should resolve");
        assert_eq!(sid.as_ref(), "sid_single");
    }

    #[test]
    fn wildcard_sid_index_matches_multi_level_fallback() {
        let mut index = WildcardSidIndex::default();
        index.insert("orders.>", "sid_multi");

        let sid = index
            .resolve("orders.NSE.RELIANCE")
            .expect("sid should resolve");
        assert_eq!(sid.as_ref(), "sid_multi");
    }

    #[test]
    fn wildcard_sid_index_remove_prunes_paths() {
        let mut index = WildcardSidIndex::default();
        index.insert("orders.*", "sid_single");
        index.insert("orders.>", "sid_multi");

        index.remove("orders.*", "sid_single");
        let sid = index.resolve("orders.NSE").expect("sid should resolve");
        assert_eq!(sid.as_ref(), "sid_multi");

        index.remove("orders.>", "sid_multi");
        assert!(index.resolve("orders.NSE").is_none());
        assert!(index.is_empty());
    }
}
