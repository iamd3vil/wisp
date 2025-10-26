// src/protocol.rs
use crate::error::{ServerError, ServerResult};
use bytes::{Bytes, BytesMut}; // Added BufMut
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct ConnectOptions {
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub pedantic: bool,
    #[serde(default)]
    pub tls_required: bool,
    pub auth_token: Option<String>,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub name: Option<String>,
    pub lang: Option<String>,
    pub version: Option<String>,
    pub protocol: Option<i32>,
    #[serde(default)]
    pub echo: bool,
    #[serde(default)]
    pub sig: Option<String>,
    #[serde(default)]
    pub jwt: Option<String>,
    #[serde(default)]
    pub no_responders: Option<bool>,
    #[serde(default)]
    pub headers: Option<bool>,
    #[serde(default)]
    pub nkey: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ServerInfo {
    pub server_id: String,
    pub server_name: String,
    pub version: String,
    pub go: String,
    pub host: String,
    pub port: u16,
    pub headers: bool,
    pub max_payload: usize,
    pub proto: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_verify: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_urls: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ws_connect_urls: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ldm: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jetstream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
}

impl ServerInfo {
    /// Returns the INFO line ready to be sent.
    /// Optimization Note: Still allocates String twice (serde_json + format!).
    /// Returning Bytes would be more efficient but changes the API.
    pub fn to_wire_string(&self) -> ServerResult<String> {
        let json = serde_json::to_string(self)?;
        // Using Ok(...) here to avoid redundant format! if not needed,
        // but if performance is critical, consider returning Bytes or writing directly to buffer.
        Ok(format!("INFO {}\r\n", json))
    }
}

/// Zero-allocation version of parse_command_line that works with bytes.
/// Returns (command_bytes, args_bytes) without any string allocations.
/// The caller is responsible for converting to strings if needed.
pub fn parse_command_line_bytes(line: &[u8]) -> ServerResult<(&[u8], &[u8])> {
    // Skip leading whitespace
    let mut start = 0;
    while start < line.len() && line[start].is_ascii_whitespace() {
        start += 1;
    }

    if start >= line.len() {
        return Err(ServerError::InvalidProtocol(
            "Empty line received".to_string(),
        ));
    }

    let trimmed_line = &line[start..];

    // Find first whitespace character
    for (i, &byte) in trimmed_line.iter().enumerate() {
        if byte.is_ascii_whitespace() {
            let command = &trimmed_line[..i];

            // Skip whitespace to find start of args
            let mut args_start = i;
            while args_start < trimmed_line.len() && trimmed_line[args_start].is_ascii_whitespace()
            {
                args_start += 1;
            }

            let args = if args_start < trimmed_line.len() {
                &trimmed_line[args_start..]
            } else {
                &[]
            };

            return Ok((command, args));
        }
    }

    // No whitespace found, entire line is the command
    Ok((trimmed_line, &[]))
}

/// Helper function to check if byte slice matches a command (case-insensitive ASCII)
pub fn command_matches(command_bytes: &[u8], target: &[u8]) -> bool {
    if command_bytes.len() != target.len() {
        return false;
    }

    command_bytes
        .iter()
        .zip(target.iter())
        .all(|(&a, &b)| a.to_ascii_uppercase() == b.to_ascii_uppercase())
}

/// Returns true if the subject contains wildcard tokens (`*` or `>`)
pub fn subject_contains_wildcard(subject: &str) -> bool {
    subject.split('.').any(|token| token == "*" || token == ">")
}

/// Helper function to parse PUB command arguments from the arguments string.
/// Returns (subject, reply_to, size)
/// Optimized: Avoids intermediate Vec allocation.
pub fn parse_pub_args(args_str: &str) -> ServerResult<(String, Option<String>, usize)> {
    // Use split_ascii_whitespace for potentially better performance on ASCII text.
    let mut parts = args_str.split_ascii_whitespace();

    let subject = parts.next();
    let part2 = parts.next(); // Could be reply_to or size
    let part3 = parts.next(); // Could be size or None
    let trailing = parts.next(); // Should be None

    match (subject, part2, part3, trailing) {
        (Some(subj), Some(size_str), None, None) => {
            // PUB <subject> <size>
            if subject_contains_wildcard(subj) {
                return Err(ServerError::InvalidArgument {
                    command: "PUB".to_string(),
                    argument: format!("wildcard subjects are not allowed: '{}'", subj),
                });
            }
            let size = size_str
                .parse::<usize>()
                .map_err(|_| ServerError::InvalidArgument {
                    command: "PUB".to_string(),
                    argument: format!("Invalid size: {}", size_str),
                })?;
            Ok((subj.to_string(), None, size)) // Allocate final Strings here
        }
        (Some(subj), Some(reply), Some(size_str), None) => {
            // PUB <subject> <reply-to> <size>
            if subject_contains_wildcard(subj) {
                return Err(ServerError::InvalidArgument {
                    command: "PUB".to_string(),
                    argument: format!("wildcard subjects are not allowed: '{}'", subj),
                });
            }
            let size = size_str
                .parse::<usize>()
                .map_err(|_| ServerError::InvalidArgument {
                    command: "PUB".to_string(),
                    argument: format!("Invalid size: {}", size_str),
                })?;
            Ok((subj.to_string(), Some(reply.to_string()), size)) // Allocate final Strings here
        }
        _ => Err(ServerError::InvalidCommand(format!(
            "Incorrect number/format of arguments for PUB: '{}'",
            args_str
        ))),
    }
}

/// Helper function to parse SUB command arguments from the arguments string.
/// Returns (subject, queue_group, sid)
/// Optimized: Avoids intermediate Vec allocation.
pub fn parse_sub_args(args_str: &str) -> ServerResult<(String, Option<String>, String)> {
    let mut parts = args_str.split_ascii_whitespace();

    let subject = parts.next();
    let part2 = parts.next(); // Could be queue_group or sid
    let part3 = parts.next(); // Could be sid or None
    let trailing = parts.next(); // Should be None

    match (subject, part2, part3, trailing) {
        (Some(subj), Some(sid_str), None, None) => {
            // SUB <subject> <sid>
            Ok((subj.to_string(), None, sid_str.to_string())) // Allocate final Strings
        }
        (Some(subj), Some(qg), Some(sid_str), None) => {
            // SUB <subject> <queue_group> <sid>
            Ok((subj.to_string(), Some(qg.to_string()), sid_str.to_string())) // Allocate final Strings
        }
        _ => Err(ServerError::InvalidCommand(format!(
            "Incorrect number/format of arguments for SUB: '{}'",
            args_str
        ))),
    }
}

/// Helper function to parse UNSUB command arguments from the arguments string.
/// Returns (sid, max_msgs)
/// Optimized: Avoids intermediate Vec allocation.
pub fn parse_unsub_args(args_str: &str) -> ServerResult<(String, Option<u64>)> {
    let mut parts = args_str.split_ascii_whitespace();

    let sid_str = parts.next();
    let max_msgs_str = parts.next();
    let trailing = parts.next(); // Should be None

    match (sid_str, max_msgs_str, trailing) {
        (Some(sid), None, None) => {
            // UNSUB <sid>
            Ok((sid.to_string(), None)) // Allocate final String
        }
        (Some(sid), Some(max_str), None) => {
            // UNSUB <sid> <max_msgs>
            let max_msgs = max_str
                .parse::<u64>()
                .map_err(|_| ServerError::InvalidArgument {
                    command: "UNSUB".to_string(),
                    argument: format!("Invalid max_msgs: {}", max_str),
                })?;
            Ok((sid.to_string(), Some(max_msgs))) // Allocate final String
        }
        _ => Err(ServerError::InvalidCommand(format!(
            "Incorrect number/format of arguments for UNSUB: '{}'",
            args_str
        ))),
    }
}

/// Helper function to parse CONNECT command arguments from the arguments string.
/// The entire arguments string is treated as the JSON payload.
pub fn parse_connect_args(args_str: &str) -> ServerResult<ConnectOptions> {
    // args_str should contain the JSON object. It might be empty if client sends "CONNECT\r\n"
    // NATS spec seems to imply the JSON object *must* be present, even if empty '{}'
    if args_str.is_empty() {
        // NATS server accepts CONNECT without args as default options.
        // If strict adherence is needed, return Err here. Let's allow empty.
        // Ok(ConnectOptions::default())
        // Original stricter version:
        return Err(ServerError::MissingArgument(
            "CONNECT requires JSON options argument".to_string(),
        ));
    }
    // Using from_slice might be marginally faster if args_str originates from bytes anyway,
    // but from_str is fine. serde_json performance dominates here.
    serde_json::from_str(args_str).map_err(ServerError::JsonError)
}

/// Formats a message payload for sending to a client according to the NATS MSG protocol.
/// Optimized: Uses `itoa` for efficient integer formatting.
///
/// # Arguments
/// * `subject` - The target subject.
/// * `sid` - The client's subscription ID for this subject.
/// * `reply_to` - Optional reply-to subject. **Changed to `Option<&str>` for efficiency**.
/// * `payload` - The message payload.
///
/// # Returns
/// A `Bytes` object containing the fully formatted `MSG` command ready for sending.
pub fn format_msg(subject: &str, sid: &str, reply_to: Option<&str>, payload: &Bytes) -> Bytes {
    let payload_len = payload.len();
    let reply_len = reply_to.map_or(0, |rt| rt.len() + 1); // +1 for space

    // More precise estimate, avoids excessive overallocation
    // MSG + space + subject + space + sid + [space + reply] + space + size_digits + \r\n + payload + \r\n
    let size_digits_estimate = 10; // Max for u64/usize usually safe enough
    let initial_len_estimate = 4
        + subject.len()
        + 1
        + sid.len()
        + reply_len
        + 1
        + size_digits_estimate
        + 2
        + payload_len
        + 2;

    let mut buf = BytesMut::with_capacity(initial_len_estimate);

    // Build the control line: MSG <subject> <sid> [reply-to] <size>\r\n
    buf.extend_from_slice(b"MSG ");
    buf.extend_from_slice(subject.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(sid.as_bytes());

    if let Some(rt) = reply_to {
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(rt.as_bytes());
    }

    buf.extend_from_slice(b" ");
    // Use itoa for efficient integer writing
    let mut itoa_buf = itoa::Buffer::new();
    buf.extend_from_slice(itoa_buf.format(payload_len).as_bytes());

    buf.extend_from_slice(b"\r\n");

    // Append payload & final CRLF
    // Using extend_from_slice is fine, but put() can sometimes be slightly faster
    // if the compiler optimizes it well, especially for larger payloads.
    // buf.put(payload.as_ref()); // Alternative
    buf.extend_from_slice(payload);
    buf.extend_from_slice(b"\r\n");

    buf.freeze() // Convert BytesMut to immutable Bytes efficiently
}

// --- Optional: For Header Support ---

/// Formats a message payload with headers for sending according to the NATS HMSG protocol.
/// `HMSG <subject> <sid> [reply-to] <#header_bytes> <#total_bytes>\r\n<headers>\r\n<payload>\r\n`
/// Note: Assumes headers are already pre-formatted according to NATS spec
/// (e.g., "NATS/1.0\r\nKey1: Value1\r\n\r\n") including the *final* double CRLF.
/// Optimized: Uses `itoa` for efficient integer formatting.
// pub fn format_hmsg(
//     subject: &str,
//     sid: &str,
//     reply_to: Option<&str>, // Changed to &str
//     headers: &Bytes,        // Pre-formatted headers including final CRLF
//     payload: &Bytes,
// ) -> Bytes {
//     let headers_len = headers.len();
//     let payload_len = payload.len();
//     let total_len = headers_len + payload_len;
//     let reply_len = reply_to.map_or(0, |rt| rt.len() + 1); // +1 for space

//     let size_digits_estimate = 10; // Max for usize
//     // HMSG + space + subject + space + sid + [space + reply] + space + hdr_len + space + total_len + \r\n + headers + payload + \r\n
//     let initial_len_estimate = 5
//         + subject.len()
//         + 1
//         + sid.len()
//         + reply_len
//         + 1
//         + size_digits_estimate
//         + 1
//         + size_digits_estimate
//         + 2
//         + headers_len // Assumes headers *includes* its trailing CRLF
//         + payload_len
//         + 2; // Final CRLF for HMSG

//     let mut buf = BytesMut::with_capacity(initial_len_estimate);
//     let mut itoa_buf = itoa::Buffer::new(); // Create one buffer for reuse

//     // Build control line: HMSG <subject> <sid> [reply-to] <#hdr_bytes> <#total_bytes>\r\n
//     buf.extend_from_slice(b"HMSG ");
//     buf.extend_from_slice(subject.as_bytes());
//     buf.extend_from_slice(b" ");
//     buf.extend_from_slice(sid.as_bytes());

//     if let Some(rt) = reply_to {
//         buf.extend_from_slice(b" ");
//         buf.extend_from_slice(rt.as_bytes());
//     }

//     buf.extend_from_slice(b" ");
//     buf.extend_from_slice(itoa_buf.format(headers_len).as_bytes()); // Header size
//     buf.extend_from_slice(b" ");
//     buf.extend_from_slice(itoa_buf.format(total_len).as_bytes()); // Total size
//     buf.extend_from_slice(b"\r\n");

//     // Append headers (assuming they contain their own trailing \r\n or \r\n\r\n)
//     buf.extend_from_slice(headers);

//     // Append payload
//     buf.extend_from_slice(payload);

//     // Append final CRLF for the whole HMSG command
//     buf.extend_from_slice(b"\r\n");

//     buf.freeze()
// }

/// Formats an error message according to NATS protocol.
/// Optimized: Avoids intermediate String allocation.
pub fn format_err(message: &str) -> Bytes {
    // Estimate length: -ERR ' + message + '\r\n
    let cap = 6 + message.len() + 3;
    let mut buf = BytesMut::with_capacity(cap);

    buf.extend_from_slice(b"-ERR '");
    buf.extend_from_slice(message.as_bytes());
    buf.extend_from_slice(b"'\r\n");

    buf.freeze()
}

///// Formats a HashMap into the NATS header wire format.
///// Includes the leading `NATS/1.0\r\n` and trailing `\r\n`.
///// Note: This function itself performs allocations for the BytesMut,
///// but avoids intermediate Strings for the lines.
// pub fn format_headers_to_wire(headers_map: &HashMap<String, String>) -> Bytes {
//     if headers_map.is_empty() {
//         // NATS/1.0\r\n\r\n (version line + empty line for end of headers)
//         return Bytes::from_static(b"NATS/1.0\r\n\r\n");
//     }

//     // Estimate capacity: Version line + (key + : + value + \r\n) * num_headers + final \r\n
//     let mut estimated_cap = 10; // "NATS/1.0\r\n" + "\r\n"
//     for (key, value) in headers_map {
//         estimated_cap += key.len() + 2 + value.len() + 2; // ": " + "\r\n"
//     }

//     let mut buf = BytesMut::with_capacity(estimated_cap);
//     buf.extend_from_slice(b"NATS/1.0\r\n"); // Version line is mandatory

//     for (key, value) in headers_map {
//         // Basic validation could be added here (e.g., no ':' in key, no CRLF in key/value)
//         // if needed, but keep it lean for performance.
//         buf.extend_from_slice(key.as_bytes());
//         buf.extend_from_slice(b": "); // Use space after colon as per common practice
//         buf.extend_from_slice(value.as_bytes());
//         buf.extend_from_slice(b"\r\n");
//     }

//     // Final empty line signifies end of headers
//     buf.extend_from_slice(b"\r\n");

//     buf.freeze()
// }
