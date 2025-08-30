/*!
 * # Volatix - High-Performance In-Memory Cache Server
 *
 * Volatix is a Redis-compatible cache server built in Rust that provides:
 * - Thread-safe concurrent access
 * - Multiple data types (Int, Float, Bool, Text, Bytes, List, Map)
 * - TTL support with automatic expiration
 * - Configurable eviction policies (LRU, LFU, Oldest, Size-aware)
 * - Disk persistence with background snapshots
 * - RESP3 protocol compatibility
 * - Optional compression for large values
 *
 * ## Core Modules
 *
 * ### Storage (`storage`)
 * The heart of the cache system, providing thread-safe data storage with:
 * - `LockedStorage`: Main storage interface with RwLock protection
 * - `StorageEntry`: Individual cache entries with metadata
 * - `StorageValue`: Enum supporting multiple data types
 * - `StorageOptions`: Configuration for TTL, capacity, eviction, compression
 * - `EvictionPolicy`: Different strategies for removing entries when full
 *
 * ### Protocol (`resp3`)
 * RESP3 protocol implementation for Redis compatibility:
 * - `RequestType`: All supported RESP3 data types
 * - `parse_request()`: Converts bytes to structured requests
 * - Response functions: Convert internal data to RESP3 format
 *
 * ### Processing (`process`)
 * Command routing and execution layer:
 * - `process_request()`: Main entry point for handling client requests
 * - Command handlers for GET, SET, DELETE, etc.
 * - Batch operations for efficiency
 * - Configuration management commands
 *
 */

// Re-export all public APIs from the modules
pub mod error;
pub mod process;
pub mod resp3;
pub mod storage;

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
// Make common types available at the crate root
pub use error::*;
pub use process::*;
pub use resp3::*;
pub use storage::*;
use tokio::sync::broadcast::Receiver;

pub fn ascii_art() -> &'static str {
    "
░██    ░██            ░██               ░██    ░██           
░██    ░██            ░██               ░██                  
░██    ░██  ░███████  ░██  ░██████   ░████████ ░██░██    ░██ 
░██    ░██ ░██    ░██ ░██       ░██     ░██    ░██ ░██  ░██  
 ░██  ░██  ░██    ░██ ░██  ░███████     ░██    ░██  ░█████   
  ░██░██   ░██    ░██ ░██ ░██   ░██     ░██    ░██ ░██  ░██  
   ░███     ░███████  ░██  ░█████░██     ░████ ░██░██    ░██ 
"
}

/// Represents different types of program information and messages
#[derive(Debug, Clone)]
pub enum Message {
    /// Any useful information
    Info(String),
    /// Error Information
    Error(String),
    /// Debug Information
    Debug(String),
    /// Signal to the message handler to quit
    Break,
}

pub async fn handle_messages(
    log_file: &Path,
    mut handler: Receiver<Message>,
) -> anyhow::Result<()> {
    let log_file = File::options()
        .create(true)
        .append(true)
        .open(log_file)
        .context("Open log file for writing")?;
    let mut f = BufWriter::new(log_file);

    while let Ok(msg) = handler.recv().await {
        let now = SystemTime::now();
        match msg {
            Message::Info(m) => {
                let m = format!(
                    "{now} INFO {m}",
                    now = now.duration_since(UNIX_EPOCH).unwrap().as_secs()
                );
                let _ = writeln!(&mut f, "{m}");
            }
            Message::Error(m) => {
                let m = format!(
                    "{now} ERROR {m}",
                    now = now.duration_since(UNIX_EPOCH).unwrap().as_secs()
                );
                let _ = writeln!(&mut f, "{m}");
            }
            Message::Debug(m) => {
                let m = format!(
                    "{now} DEBUG {m}",
                    now = now.duration_since(UNIX_EPOCH).unwrap().as_secs()
                );
                let _ = writeln!(&mut f, "{m}");
            }
            Message::Break => break,
        }
    }

    Ok(())
}
