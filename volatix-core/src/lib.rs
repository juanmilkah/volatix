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
pub mod resp3;
pub mod storage;

// Make common types available at the crate root
pub use error::*;
pub use resp3::*;
pub use storage::*;

/// Ascii representation of the word `Volatix`.
pub fn volatix_ascii_art() -> &'static str {
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
