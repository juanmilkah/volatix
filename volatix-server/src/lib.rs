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
 * ## Example Usage
 *
 * ```rust
 * # use libvolatix::{LockedStorage, StorageOptions, StorageValue, EvictionPolicy,
*     Compression};
 * # use std::time::Duration;
 *
 * // Create storage with custom options
 * let options = StorageOptions::new(
 *     Duration::from_secs(3600),  // 1 hour TTL
 *     10000,                      // 10k entries max
 *     &EvictionPolicy::LRU,       // Use LRU eviction
 *     &Compression::Enabled,      // Enable compression
 *     1024                        // Compress values > 1KB
 * );
 * let mut storage = LockedStorage::new(options);
 *
 * // Store different data types
 * storage.insert_entry("counter".to_string(), StorageValue::Int(42)).unwrap();
 * storage.insert_entry("price".to_string(), StorageValue::Float(19.99)).unwrap();
 * storage.insert_entry("active".to_string(), StorageValue::Bool(true)).unwrap();
 * storage.insert_entry("name".to_string(), StorageValue::Text("John".to_string())).unwrap();
 *
 * // Retrieve values
 * if let Some(entry) = storage.get_entry("counter") {
 *     println!("Counter: {}", entry.value);
 *     println!("Accessed {} times", entry.access_count);
 * }
 *
 * // Batch operations
 * let keys = vec!["counter".to_string(), "name".to_string()];
 * let results = storage.get_entries(&keys);
 *
 * // TTL management
 * storage.extend_ttl("name", 1800).unwrap(); // Add 30 minutes
 *
 * // Save/load from disk
 * storage.save_to_disk("cache.db").unwrap();
 * storage.load_from_disk("cache.db").unwrap();
 * ```
 */

// Re-export all public APIs from the modules
pub mod process;
pub mod resp3;
pub mod storage;

// Make common types available at the crate root
pub use process::*;
pub use resp3::*;
pub use storage::*;
