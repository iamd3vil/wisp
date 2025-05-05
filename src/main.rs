mod command;
mod error;
mod handler;
mod protocol;
mod server;

use crate::error::ServerResult;
use crate::handler::ClientHandler;
use crate::protocol::ServerInfo;
use crate::server::NatsServer;
use tracing::Level;

// Basic Server Configuration
const SERVER_NAME: &str = "Tans";
const SERVER_VERSION: &str = "0.1.0";
const DEFAULT_PORT: u16 = 4222;
const MAX_PAYLOAD: usize = 1024 * 1024; // 1MB example

#[tokio::main]
async fn main() -> ServerResult<()> {
    // Setup tracing/logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO) // Log DEBUG and above
        .init();

    let listen_addr = format!("0.0.0.0:{}", DEFAULT_PORT);

    // Create the handler instance
    let handler = ClientHandler::new();

    // Define Server Info
    let server_info = ServerInfo {
        server_id: format!("{}_{}", SERVER_NAME, uuid::Uuid::new_v4().to_string()), // Unique ID
        server_name: SERVER_NAME.to_string(),
        version: SERVER_VERSION.to_string(),
        go: "rustc".to_string(),     // Mimic Go client field name convention
        host: "0.0.0.0".to_string(), // Listen host
        port: DEFAULT_PORT,
        headers: true, // Assuming we'll support headers eventually
        max_payload: MAX_PAYLOAD,
        proto: 1,            // Protocol level 1
        auth_required: None, // No auth for now
        tls_required: None,
        tls_verify: None,
        connect_urls: None, // Add cluster discovery URLs if needed
        ws_connect_urls: None,
        ldm: None,
        git_commit: None,
        jetstream: None, // No JetStream support yet
        // These can be filled per-client if needed, but are often part of the general INFO
        client_id: None,
        ip: None,
        client_ip: None,
        nonce: None,
        cluster: None,
        domain: None,
    };

    // Create and run the server
    let nats_server = NatsServer::new(&listen_addr, handler, server_info).await?;
    nats_server.run().await?;

    Ok(()) // Although run() is an infinite loop typically
}
