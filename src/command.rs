// Add near the top of src/protocol.rs (or in a new file)
use bytes::Bytes; // Make sure Bytes is imported

/// Commands sent internally from handler/server logic to the client connection task.
#[derive(Debug)]
pub enum ServerCommand {
    /// Send these specific bytes to the client.
    Send(Bytes),
    /// Send a message with separate header and shared payload (zero-copy optimization).
    /// Header contains: MSG <subject> <sid> [reply-to] <size>\r\n
    /// Payload is shared across all subscribers via Bytes refcount.
    SendMessage { header: Bytes, payload: Bytes },
    /// Instruct the client connection task to shut down gracefully.
    Shutdown,
}

// pub fn format_err(message: &str) -> Bytes {
//     // Basic single quote escaping: replace ' with \' (though spec doesn't strictly define escaping)
//     // A simpler approach is just to ensure the message doesn't contain single quotes or handle it carefully.
//     // For now, let's just wrap. Ensure your error messages don't contain '.
//     // TODO: Implement robust single-quote escaping if needed.
//     let formatted_msg = format!("-ERR '{}'\r\n", message.replace('\'', "")); // Simple replace
//     Bytes::from(formatted_msg)
// }
