use std::{
    collections::{BinaryHeap, HashMap},
    fmt::Display,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use anyhow::Context;
use flate2::bufread::{ZlibDecoder, ZlibEncoder};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Thread-safe storage wrapper with atomic statistics and configuration options.
/// This is the main interface for cache operations.
///
/// # Example
/// ```rust
/// use std::time::Duration;
/// use libvolatix::{LockedStorage, StorageValue};
///
/// let mut storage = LockedStorage::default();
/// storage.insert_entry("key".to_string(), StorageValue::Text("value".to_string())).unwrap();
/// let entry = storage.get_entry("key");
/// assert!(entry.is_some());
/// ```
#[derive(Default)]
pub struct LockedStorage {
    /// Thread-safe HashMap containing all cache entries
    pub store: Arc<RwLock<HashMap<String, StorageEntry>>>,
    /// Configuration options for the cache
    pub options: StorageOptions,
    /// Atomic statistics for thread-safe performance tracking
    pub stats: StorageStats,
    /// A flag for any unsynched changes to disk
    pub is_dirty: bool,
}

/// Serializable version of storage for disk persistence.
/// Used internally during save/load operations.
#[derive(Serialize, Deserialize)]
struct UnlockedStorage {
    store: HashMap<String, StorageEntry>,
    options: StorageOptions,
    stats: NonAtomicStats,
}

/// Represents all possible value types that can be stored in the cache.
/// Supports Redis-like data structures with automatic size calculation.
///
/// # Examples
/// ```rust
/// use libvolatix::StorageValue;
///
/// let int_val = StorageValue::Int(42);
/// let text_val = StorageValue::Text("hello world".to_string());
/// let list_val = StorageValue::List(vec![
///     StorageValue::Int(1),
///     StorageValue::Text("item".to_string())
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum StorageValue {
    /// A Null value
    Null,
    /// 64-bit signed integer
    Int(i64),
    /// 64-bit floating point number
    Float(f64),
    /// Boolean value
    Bool(bool),
    /// UTF-8 string
    Text(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// Ordered list of storage values
    List(Vec<StorageValue>),
    /// Key-value pairs (similar to JSON object)
    Map(Vec<(String, StorageValue)>),
}

impl Display for StorageValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageValue::Int(i) => write!(f, "{i}"),
            StorageValue::Float(fl) => write!(f, "{fl}"),
            StorageValue::Bool(b) => write!(f, "{b}"),
            StorageValue::Text(t) => write!(f, "{t}"),
            StorageValue::Bytes(b) => write!(f, "{b:?}"),
            StorageValue::List(storage_values) => write!(f, "{storage_values:?}"),
            StorageValue::Map(items) => write!(f, "{items:?}"),
            StorageValue::Null => write!(f, "null"),
        }
    }
}

impl StorageValue {
    /// Calculates the approximate memory usage of this value in bytes.
    /// Used for eviction policies and memory management.
    fn size_in_bytes(&self) -> usize {
        match self {
            StorageValue::Int(_) => size_of_val(self),
            StorageValue::Float(_) => size_of_val(self),
            StorageValue::Bool(_) => size_of_val(self),
            StorageValue::Text(t) => size_of_val(self) + t.capacity(),
            StorageValue::Bytes(b) => b.len(),
            StorageValue::List(storage_values) => {
                storage_values.iter().map(|s| s.size_in_bytes()).sum()
            }
            StorageValue::Map(items) => items
                .iter()
                .map(|(k, v)| k.capacity() + v.size_in_bytes())
                .sum(),
            StorageValue::Null => 0,
        }
    }
}

/// Individual cache entry with metadata for TTL, access tracking, and compression.
/// Each entry contains not just the value, but also metadata needed for
/// cache management and eviction policies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageEntry {
    /// The actual stored value
    pub value: StorageValue,
    /// When this entry was first created
    pub created_at: SystemTime,
    /// When this entry was last accessed (for LRU)
    pub last_accessed: SystemTime,
    /// Number of times this entry has been accessed (for LFU)
    pub access_count: usize,
    /// Size of the entry in bytes (for size-aware eviction)
    pub entry_size: usize,
    /// Time-to-live for this entry
    pub ttl: Duration,
    /// Whether this entry's value is compressed
    pub compressed: bool,
}

impl Display for StorageEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Value:{}\r\nCreated_at:{:?}\r\nLastaccessed:{:?}\r\nAccessCount:{}\r\nEntrysize:{}\r\nTtl:{}\r\nCompressed:{}",
            self.value,
            self.created_at,
            self.last_accessed,
            self.access_count,
            self.entry_size,
            self.ttl.as_secs(),
            self.compressed,
        )
    }
}

impl StorageEntry {
    /// Checks if this entry has expired based on its TTL and creation time.
    ///
    /// # Returns
    /// `true` if the entry has exceeded its TTL, `false` otherwise.
    fn is_expired(&self) -> bool {
        SystemTime::now()
            .duration_since(self.created_at)
            .expect("clock gone backwards")
            > self.ttl
    }

    /// Decompresses the entry value if it was compressed.
    /// This is called automatically when retrieving compressed entries.
    ///
    /// # Errors
    /// Returns `io::Error` if decompression fails.
    fn decompress(&mut self) -> io::Result<()> {
        let mut output = String::new();
        let input = match &self.value {
            StorageValue::Bytes(bytes) => bytes,
            _ => return Ok(()), // Not compressed or not bytes
        };
        let mut z = ZlibDecoder::new(&input[..]);
        z.read_to_string(&mut output)?;

        self.value = StorageValue::Text(output);
        Ok(())
    }
}

/// Configuration options for the storage engine.
/// These settings control behavior like TTL, capacity limits, and compression.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct StorageOptions {
    /// Default TTL for new entries
    pub ttl: Duration,
    /// Maximum number of entries allowed
    pub max_capacity: u64,
    /// Strategy for removing entries when at capacity
    pub eviction_policy: EvictionPolicy,
    /// Whether to enable automatic compression
    pub compression: bool,
    /// Minimum size in bytes before compression is applied
    pub compression_threshold: usize,
}

/// Helper enum for more readable compression settings.
#[derive(Debug)]
pub enum Compression {
    Enabled,
    Disabled,
}

impl Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::Enabled => write!(f, "enabled"),
            Compression::Disabled => write!(f, "disabled"),
        }
    }
}

impl From<bool> for Compression {
    fn from(value: bool) -> Self {
        if value {
            Compression::Enabled
        } else {
            Compression::Disabled
        }
    }
}

impl StorageOptions {
    /// Creates new storage options with the specified parameters.
    ///
    /// # Example
    /// ```rust
    /// use std::time::Duration;
    /// use libvolatix::{Compression, EvictionPolicy, StorageOptions};
    ///
    /// let options = StorageOptions::new(
    ///     Duration::from_secs(3600),  // 1 hour TTL
    ///     10000,                      // 10k entries max
    ///     &EvictionPolicy::LRU,       // Use LRU eviction
    ///     &Compression::Enabled,      // Enable compression
    ///     1024                        // Compress values > 1KB
    /// );
    /// ```
    pub fn new(
        ttl: Duration,
        max_cap: u64,
        evict_policy: &EvictionPolicy,
        compression: &Compression,
        compression_threshold: usize,
    ) -> Self {
        let compression = match compression {
            Compression::Enabled => true,
            Compression::Disabled => false,
        };
        Self {
            ttl,
            max_capacity: max_cap,
            eviction_policy: *evict_policy,
            compression,
            compression_threshold,
        }
    }
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(60 * 60 * 6), // 6 hours
            max_capacity: (1000 * 1000),           // 1 million entries
            eviction_policy: EvictionPolicy::default(),
            compression: false,
            compression_threshold: 1024 * 4, // 4KB
        }
    }
}

impl Display for StorageOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GLOBALTTL: {}, MAXCAP: {}, EVICTPOLICY: {}, COMPRESSION: {}, COMPRESSIONTHRESHOLD: {}",
            self.ttl.as_secs(),
            self.max_capacity,
            self.eviction_policy,
            self.compression,
            self.compression_threshold,
        )
    }
}

/// Non-atomic version of statistics for serialization.
/// Used when saving/loading storage state to/from disk.
#[derive(Debug, Default, Serialize, Deserialize)]
struct NonAtomicStats {
    total_entries: usize,
    hits: usize,
    misses: usize,
    evictions: usize,
    expired_removals: usize,
}

/// Thread-safe statistics tracking for cache performance monitoring.
/// All fields use atomic operations for safe concurrent access.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StorageStats {
    /// Total number of entries currently in cache
    pub total_entries: AtomicUsize,
    /// Number of successful cache hits
    pub hits: AtomicUsize,
    /// Number of cache misses
    pub misses: AtomicUsize,
    /// Number of entries removed by eviction policies
    pub evictions: AtomicUsize,
    /// Number of entries removed due to TTL expiration
    pub expired_removals: AtomicUsize,
}

impl Display for StorageStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Total Entries: {}, Hits: {}, Misses: {}, Evictions: {}, Expired Removals: {}",
            self.total_entries.load(Ordering::Relaxed),
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
            self.expired_removals.load(Ordering::Relaxed),
        )
    }
}

/// Strategies for removing entries when the cache reaches capacity.
/// Each policy optimizes for different use cases and access patterns.
#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// Remove the oldest entries first (by creation time)
    #[default]
    Oldest,
    /// Remove least recently used entries (by last access time)
    LRU,
    /// Remove least frequently used entries (by access count)
    LFU,
    /// Remove largest entries first (by size)
    SizeAware,
}

impl Display for EvictionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionPolicy::Oldest => write!(f, "Oldest"),
            EvictionPolicy::LRU => write!(f, "LRU"),
            EvictionPolicy::LFU => write!(f, "LFU"),
            EvictionPolicy::SizeAware => write!(f, "SizeAware"),
        }
    }
}

/// Configuration entries that can be modified at runtime.
/// Used by the CONFSET and CONFGET commands.
#[derive(Debug)]
pub enum ConfigEntry {
    EvictPolicy(EvictionPolicy),
    GlobalTtl(u64),
    MaxCapacity(u64),
    Compression(Compression),
    CompressionThreshold(usize),
}

impl Display for ConfigEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigEntry::EvictPolicy(e) => write!(f, "EVICTPOLICY: {e}"),
            ConfigEntry::GlobalTtl(t) => write!(f, "GLOBALTTL: {t}"),
            ConfigEntry::MaxCapacity(c) => write!(f, "MAXCAP: {c}"),
            ConfigEntry::Compression(b) => write!(f, "COMPRESSION: {b}"),
            ConfigEntry::CompressionThreshold(s) => write!(f, "COMPRESSIONTHRESHOLD: {s}"),
        }
    }
}

/// Compresses a string using zlib compression.
/// Used automatically for large text values when compression is enabled.
///
/// # Arguments
/// * `data` - The string to compress
///
/// # Returns
/// Compressed bytes or error message
fn compress(data: &str) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    let input = data.as_bytes();
    let mut z = ZlibEncoder::new(input, flate2::Compression::fast());
    match z.read_to_end(&mut output) {
        Ok(_) => (),
        Err(e) => return Err(e.to_string()),
    }

    Ok(output)
}

impl LockedStorage {
    /// Creates a new storage instance with the given options.
    ///
    /// # Example
    /// ```rust
    /// use libvolatix::{LockedStorage, StorageOptions};
    ///
    /// let options = StorageOptions::default();
    /// let storage = LockedStorage::new(options);
    /// ```
    pub fn new(options: StorageOptions) -> Self {
        LockedStorage {
            store: Arc::new(RwLock::new(HashMap::with_capacity(1000))),
            options,
            stats: StorageStats::default(),
            is_dirty: false,
        }
    }

    /// Clears all entries from the cache.
    /// Configuration options are retained.
    /// To reset config options try `reset_options`
    pub fn flush(&mut self) {
        let old_config = self.options;
        let _old_storage = std::mem::take(self);
        self.options = old_config;
        self.is_dirty = true;
    }

    /// Retrieves an entry by key, updating access statistics and metadata.
    /// Automatically handles decompression and TTL expiration.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// `Some(StorageEntry)` if found and not expired, `None` otherwise
    ///
    /// # Example
    /// ```rust
    /// use libvolatix::{LockedStorage, StorageOptions};
    ///
    /// let storage = LockedStorage::new(StorageOptions::default());
    ///
    /// if let Some(entry) = storage.get_entry("user:123") {
    ///     println!("Value: {}", entry.value);
    /// }
    /// ```
    pub fn get_entry(&self, key: &str) -> Option<StorageEntry> {
        // First, try to get the entry and update its access metadata
        let mut entry = if let Some(entry) = self.store.write().get_mut(key)
            && !entry.is_expired()
        {
            // Update access tracking for LRU/LFU eviction policies
            entry.access_count += 1;
            entry.last_accessed = SystemTime::now();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            entry.clone()
        } else {
            // Entry not found or expired
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };

        // Handle decompression outside the lock to minimize lock time.
        // We don't want to hold an exclusive lock while doing CPU-intensive work.
        #[allow(clippy::collapsible_if)]
        if entry.compressed {
            if entry.decompress().is_err() {
                eprintln!("Entry decompression on get_entry");
                return None;
            }
        }

        Some(entry)
    }

    /// Checks if a key exists in the cache without updating access metadata.
    ///
    /// # Arguments
    /// * `key` - The key to check
    ///
    /// # Returns
    /// `true` if the key exists and is not expired, `false` otherwise
    pub fn key_exists(&self, key: &str) -> bool {
        self.get_entry(key).is_some()
    }

    /// Returns a list of all keys currently in the cache.
    /// Note: This creates a snapshot at the time of call.
    ///
    /// # Returns
    /// Vector of all keys in the cache
    pub fn get_keys(&self) -> Vec<String> {
        self.store.read().keys().cloned().collect()
    }

    /// Retrieves multiple entries in a single operation (batch get).
    /// More efficient than individual gets for multiple keys.
    ///
    /// # Arguments
    /// * `keys` - Slice of keys to retrieve
    ///
    /// # Returns
    /// Vector of tuples containing (key, `Option<StorageEntry>`)
    ///
    /// # Example
    /// ```rust
    /// use libvolatix::{LockedStorage, StorageOptions};
    ///
    /// let storage = LockedStorage::new(StorageOptions::default());
    ///
    /// let keys = vec!["user:1".to_string(), "user:2".to_string()];
    /// let results = storage.get_entries(&keys);
    /// for (key, entry) in results {
    ///     match entry {
    ///         Some(e) => println!("{}: {}", key, e.value),
    ///         None => println!("{}: not found", key),
    ///     }
    /// }
    /// ```
    pub fn get_entries(&self, keys: &[String]) -> Vec<(String, Option<StorageEntry>)> {
        keys.iter()
            .map(|k| (k.to_string(), self.get_entry(k)))
            .collect()
    }

    /// Inserts multiple entries in a single operation (batch set).
    /// Uses the default TTL for all entries.
    ///
    /// # Arguments
    /// * `entries` - HashMap of key-value pairs to insert
    ///
    /// # Returns
    /// `Ok(())` on success, `Err(String)` on failure
    pub fn insert_entries(&mut self, entries: HashMap<String, StorageValue>) -> Result<(), String> {
        for (key, value) in entries {
            self.insert_entry(key, value)?;
        }
        Ok(())
    }

    /// Removes multiple entries in a single operation (batch delete).
    ///
    /// # Arguments
    /// * `keys` - Slice of keys to remove
    pub fn remove_entries(&mut self, keys: &[String]) {
        for key in keys {
            self.remove_entry(key);
        }
    }

    /// Inserts a single entry with the default TTL.
    /// Convenience method that uses the global TTL setting.
    ///
    /// # Arguments
    /// * `key` - The key to insert
    /// * `value` - The value to store
    ///
    /// # Returns
    /// `Ok(())` on success, `Err(String)` on failure
    pub fn insert_entry(&mut self, key: String, value: StorageValue) -> Result<(), String> {
        self.insert_with_ttl(key, value, self.options.ttl)
    }

    /// Increments an integer value by 1.
    /// Only works if the existing value is an integer.
    /// Updates access statistics on success.
    ///
    /// # Arguments
    /// * `key` - The key containing an integer value
    pub fn increment_entry(&mut self, key: &str) {
        if let Some(entry) = self.store.write().get_mut(key)
            && let StorageValue::Int(n) = entry.value
        {
            entry.value = StorageValue::Int(n + 1);
            entry.last_accessed = SystemTime::now();
            entry.access_count += 1;
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements an integer value by 1.
    /// Only works if the existing value is an integer.
    /// Updates access statistics on success.
    ///
    /// # Arguments
    /// * `key` - The key containing an integer value
    pub fn decrement_entry(&mut self, key: &str) {
        if let Some(entry) = self.store.write().get_mut(key)
            && let StorageValue::Int(n) = entry.value
        {
            entry.value = StorageValue::Int(n - 1);
            entry.last_accessed = SystemTime::now();
            entry.access_count += 1;
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return;
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Removes a single entry from the cache.
    /// Updates the total entry count if an entry was actually removed.
    ///
    /// # Arguments
    /// * `key` - The key to remove
    pub fn remove_entry(&mut self, key: &str) {
        if self.store.write().remove_entry(key).is_some() {
            self.stats.total_entries.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Inserts an entry with a specific TTL.
    /// This is the core insertion method that handles compression and eviction.
    ///
    /// # Arguments
    /// * `key` - The key to insert
    /// * `value` - The value to store
    /// * `ttl` - Time-to-live for this specific entry
    ///
    /// # Returns
    /// `Ok(())` on success, `Err(String)` on failure
    ///
    /// # Example
    /// ```rust
    /// use std::time::Duration;
    /// use libvolatix::{LockedStorage, StorageValue, StorageOptions};
    ///
    /// let mut storage = LockedStorage::new(StorageOptions::default());
    ///
    /// storage.insert_with_ttl(
    ///     "session:abc123".to_string(),
    ///     StorageValue::Text("user_data".to_string()),
    ///     Duration::from_secs(3600) // 1 hour TTL
    /// ).unwrap();
    /// ```
    pub fn insert_with_ttl(
        &mut self,
        key: String,
        value: StorageValue,
        ttl: Duration,
    ) -> Result<(), String> {
        // Check if we need to make room for this entry
        if self.is_full() {
            // Evict 10% of the entries
            self.evict_entries(0);
        }

        let entry_size = value.size_in_bytes();
        let now = SystemTime::now();

        // Handle automatic compression for large text values
        let (value, compressed) = {
            if self.options.compression
                && let StorageValue::Text(text) = &value
                && entry_size >= self.options.compression_threshold
            {
                let bytes = compress(text).map_err(|err| format!("Compression error: {err}"))?;
                (StorageValue::Bytes(bytes), true)
            } else {
                (value, false)
            }
        };

        // Create the storage entry with all metadata
        let entry = StorageEntry {
            value,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            entry_size,
            ttl,
            compressed,
        };

        // Insert the entry and update statistics
        self.store.write().insert(key, entry);
        self.stats.total_entries.fetch_add(1, Ordering::Relaxed);
        self.is_dirty = true;
        Ok(())
    }

    /// Extends or reduces the TTL of an existing entry.
    /// Can add or subtract time from the current TTL.
    ///
    /// # Arguments
    /// * `key` - The key to modify
    /// * `additional_time` - Seconds to add (positive) or subtract (negative)
    ///
    /// # Returns
    /// `Ok(())` on success, `Err("UNALTERED")` if the operation would make TTL negative
    ///
    /// # Example
    /// ```rust
    /// use libvolatix::{LockedStorage, StorageOptions};
    ///
    /// let mut storage = LockedStorage::new(StorageOptions::default());
    ///
    /// // Add 30 minutes
    /// storage.extend_ttl("session:123", 1800).unwrap();
    ///
    /// // Subtract 10 minutes (but don't go negative)
    /// storage.extend_ttl("session:123", -600).unwrap();
    /// ```
    pub fn extend_ttl(&mut self, key: &str, additional_time: i64) -> Result<(), String> {
        if let Some(entry) = self.store.write().get_mut(key) {
            if additional_time < 0 {
                // Prevent TTL from going negative
                if additional_time.unsigned_abs() > entry.ttl.as_secs() {
                    return Err("UNALTERED".to_string());
                }

                entry.ttl -= Duration::from_secs(additional_time.unsigned_abs());
            } else {
                entry.ttl += Duration::from_secs(additional_time as u64);
            }
            // Update access metadata
            entry.access_count += 1;
            entry.last_accessed = SystemTime::now();
        }

        Ok(())
    }

    /// Gets the remaining TTL for a key.
    ///
    /// # Arguments
    /// * `key` - The key to check
    ///
    /// # Returns
    /// `Some(Duration)` if the key exists, `None` otherwise
    pub fn time_to_live(&self, key: &str) -> Option<Duration> {
        self.get_entry(key).map(|e| e.ttl)
    }

    /// Checks if the cache is at capacity.
    /// Used internally to decide when eviction is needed.
    pub fn is_full(&self) -> bool {
        self.store.read().len() as u64 >= self.options.max_capacity
    }

    /// Checks whether storage has any unsyched changes to disk
    pub fn should_flush(&self) -> bool {
        self.is_dirty
    }

    /// Flip the state of the is_dirty storage flag
    pub fn toggle_dirty_flag(&mut self) {
        self.is_dirty = !self.is_dirty
    }

    /// Checks the current maximum capacity of the storage
    pub fn max_capacity(&self) -> u64 {
        self.options.max_capacity
    }

    /// Check the total number of entries currently in the store.
    /// Valid and invalid values inclusive
    pub fn entry_count(&self) -> usize {
        self.store.read().len()
    }

    /// Removes all expired entries from the cache.
    /// Called automatically during eviction and can be called manually.
    /// Updates statistics to track expired removals.
    pub fn remove_expired(&mut self) {
        let prev_count = self.store.read().len();
        let now = SystemTime::now();
        {
            // Remove entries that have exceeded their TTL
            self.store.write().retain(|_, value| {
                now.duration_since(value.created_at)
                    .expect("clock gone backwards")
                    < value.ttl
            });
        }
        let current_count = self.store.read().len();
        let removed = prev_count - current_count;

        // Update statistics
        self.stats
            .expired_removals
            .fetch_add(removed, Ordering::Release);
        self.stats
            .total_entries
            .store(current_count, Ordering::Release);
        self.is_dirty = true;
    }

    /// Performs eviction based on the configured eviction policy.
    /// First removes expired entries, then applies the eviction policy.
    /// If the specified count is greater than or equal to the
    /// [`Self::entry_count`] the storage is flushed.
    /// If count is zero, 10% of total entries is evicted.
    pub fn evict_entries(&mut self, count: usize) {
        let s = self.entry_count();
        if count >= s {
            self.flush();
            return;
        }

        self.remove_expired();

        let count = if count == 0 {
            (10.0 / 100.0 * s as f64).ceil() as usize
        } else {
            count
        };

        let oldest_metric = |_k: &String, v: &StorageEntry| v.created_at;
        let lru_metric = |_k: &String, v: &StorageEntry| v.last_accessed;
        let lfu_metric = |_k: &String, v: &StorageEntry| v.access_count;
        let largest_metric = |_k: &String, v: &StorageEntry| -(v.entry_size as i64); // Invert the metric

        match self.options.eviction_policy {
            EvictionPolicy::Oldest => self.remove_n_entries(count, oldest_metric),
            EvictionPolicy::LRU => self.remove_n_entries(count, lru_metric),
            EvictionPolicy::LFU => self.remove_n_entries(count, lfu_metric),
            EvictionPolicy::SizeAware => self.remove_n_entries(count, largest_metric),
        }
    }

    /// Removes the least n entries by a metric.
    /// Removes the least recently used n entries (LRU eviction policy).
    /// Removes the least frequently used n entries (LFU eviction policy).
    /// Removes the oldest n entries by their creation time (Oldest eviction policy).
    /// Removes the largest n entries by their size (Size-aware eviction policy).
    pub fn remove_n_entries<F, M>(&mut self, n: usize, mut metric: F)
    where
        M: Ord + Copy,
        F: FnMut(&String, &StorageEntry) -> M,
    {
        let mut heap: BinaryHeap<(M, String)> = BinaryHeap::new();
        {
            let store = self.store.read();
            for (k, v) in store.iter() {
                let m = metric(k, v);
                if heap.len() < n {
                    heap.push((m, k.clone()));
                } else if let Some((top_m, _)) = heap.peek()
                    && m < *top_m
                // Keep smallest by metric
                {
                    heap.pop();
                    heap.push((m, k.clone()));
                }
            }
        }
        let keys = heap.into_iter().map(|(_, k)| k).collect::<Vec<String>>();
        self.remove_entries(&keys);
        self.stats.evictions.fetch_add(n, Ordering::Relaxed);
    }

    /// Renames an existing key to a new name.
    /// The old key is removed and a new entry is created with the same value.
    /// Updates access statistics.
    ///
    /// # Arguments
    /// * `old_key` - The current key name
    /// * `new_key` - The new key name
    ///
    /// # Example
    /// ```rust
    /// use libvolatix::{StorageOptions, LockedStorage};
    ///
    /// let mut storage = LockedStorage::new(StorageOptions::default());
    /// storage.rename_entry("old_name", "new_name");
    /// ```
    pub fn rename_entry(&mut self, old_key: &str, new_key: &str) {
        if let Some((_, mut entry)) = self.store.write().remove_entry(old_key) {
            // Update access metadata
            entry.access_count += 1;
            entry.last_accessed = SystemTime::now();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);

            // Insert with new key
            self.store.write().insert(new_key.to_string(), entry);
            return;
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Gets a snapshot of the current statistics.
    /// Returns non-atomic copies of the atomic values.
    ///
    /// # Returns
    /// `StorageStats` with current values
    pub fn get_stats(&self) -> StorageStats {
        StorageStats {
            total_entries: self.stats.total_entries.load(Ordering::Relaxed).into(),
            hits: self.stats.hits.load(Ordering::Relaxed).into(),
            misses: self.stats.misses.load(Ordering::Relaxed).into(),
            evictions: self.stats.evictions.load(Ordering::Relaxed).into(),
            expired_removals: self.stats.expired_removals.load(Ordering::Relaxed).into(),
        }
    }

    /// Resets all statistics to zero.
    /// Used by the RESETSTATS command.
    pub fn reset_stats(&mut self) {
        let _old_stats = std::mem::take(&mut self.stats);
    }

    /// Retrieves a configuration entry by key name.
    /// Used by the CONFGET command.
    ///
    /// # Arguments
    /// * `key` - Configuration key name (case insensitive)
    ///
    /// # Returns
    /// `Some(ConfigEntry)` if the key exists, `None` otherwise
    pub fn get_config_entry(&self, key: &str) -> Option<ConfigEntry> {
        match key.to_uppercase().as_str() {
            "EVICTPOLICY" => Some(ConfigEntry::EvictPolicy(self.options.eviction_policy)),
            "MAXCAP" => Some(ConfigEntry::MaxCapacity(self.options.max_capacity)),
            "GLOBALTTL" => Some(ConfigEntry::GlobalTtl(self.options.ttl.as_secs())),
            "COMPRESSION" => Some(ConfigEntry::Compression(self.options.compression.into())),
            "COMPRESSIONTHRESHOLD" => Some(ConfigEntry::CompressionThreshold(
                self.options.compression_threshold,
            )),
            _ => None,
        }
    }

    /// Updates a configuration setting.
    /// Used by the CONFSET command.
    ///
    /// # Arguments
    /// * `entry` - The configuration entry to update
    pub fn set_config_entry(&mut self, entry: &ConfigEntry) {
        match entry {
            ConfigEntry::EvictPolicy(p) => self.options.eviction_policy = *p,
            ConfigEntry::GlobalTtl(t) => self.options.ttl = Duration::from_secs(*t),
            ConfigEntry::MaxCapacity(c) => self.options.max_capacity = *c,
            ConfigEntry::Compression(b) => {
                self.options.compression = match b {
                    Compression::Enabled => true,
                    Compression::Disabled => false,
                }
            }
            ConfigEntry::CompressionThreshold(s) => self.options.compression_threshold = *s,
        }
    }

    /// Gets a copy of the current storage options.
    ///
    /// # Returns
    /// Current `StorageOptions` configuration
    pub fn get_options(&self) -> StorageOptions {
        self.options
    }

    /// Resets the current storage options to the default value
    /// provided by `StorageOptions::default()`
    pub fn reset_options(&mut self) {
        std::mem::take(&mut self.options);
    }

    /// Loads storage data from disk.
    /// Used during startup to restore cache state from previous runs.
    ///
    /// # Arguments
    /// * `path` - Path to the storage file
    ///
    /// # Returns
    /// `anyhow::Result<()>` - Success or error details
    ///
    /// # Example
    /// ```rust
    /// use libvolatix::{StorageOptions, LockedStorage};
    /// use std::path::Path;
    ///
    /// let mut storage = LockedStorage::new(StorageOptions::default());
    ///
    /// let db_path = Path::new("cache.bin").to_path_buf();
    /// storage.load_from_disk(&db_path).expect("Failed to load cache");
    /// ```
    pub fn load_from_disk(&mut self, path: &PathBuf) -> anyhow::Result<()> {
        let path = Path::new(path);
        if !path.exists() {
            return Ok(()); // No existing data to load
        }

        let file = File::open(path).context("open db")?;
        let mut reader = BufReader::new(file);

        // Deserialize the storage data
        let unlocked_storage: UnlockedStorage =
            bincode2::deserialize_from(&mut reader).context("deserialize from buffer")?;

        // Convert non-atomic stats back to atomic
        let stats = StorageStats {
            total_entries: AtomicUsize::new(unlocked_storage.stats.total_entries),
            hits: AtomicUsize::new(unlocked_storage.stats.hits),
            misses: AtomicUsize::new(unlocked_storage.stats.misses),
            evictions: AtomicUsize::new(unlocked_storage.stats.evictions),
            expired_removals: AtomicUsize::new(unlocked_storage.stats.expired_removals),
        };

        // Replace current storage with loaded data
        *self = LockedStorage {
            store: Arc::new(RwLock::new(unlocked_storage.store)),
            options: unlocked_storage.options,
            stats,
            is_dirty: false,
        };

        Ok(())
    }

    /// Saves storage data to disk.
    /// Used for persistence during shutdown and periodic snapshots.
    ///
    /// # Arguments
    /// * `path` - Path where to save the storage file
    ///
    /// # Returns
    /// `anyhow::Result<()>` - Success or error details
    ///
    /// # Example
    /// ```rust
    /// use libvolatix::{LockedStorage, StorageOptions};
    /// use std::path::Path;
    ///
    /// let storage = LockedStorage::new(StorageOptions::default());
    ///
    /// let db_path = Path::new("/tmp/cache.bin").to_path_buf();
    /// storage.save_to_disk(&db_path).expect("Failed to save cache");
    /// ```
    pub fn save_to_disk(&self, path: &PathBuf) -> anyhow::Result<()> {
        let path = Path::new(path);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .context("open db for writing")?;

        // Convert atomic stats to non-atomic for serialization
        let stats = NonAtomicStats {
            total_entries: self.stats.total_entries.load(Ordering::Relaxed),
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            expired_removals: self.stats.expired_removals.load(Ordering::Relaxed),
        };

        // Create serializable version
        let unlocked_storage = UnlockedStorage {
            // FIX: Find a way to go around this clone
            store: self.store.read().clone(),
            options: self.options,
            stats,
        };

        // Serialize to disk
        let mut writer = BufWriter::new(file);
        bincode2::serialize_into(&mut writer, &unlocked_storage).context("serialize into db")?;
        writer.flush().context("flush writer")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::atomic::Ordering, thread, time::Duration};

    use super::*;

    // Test default configuration
    #[test]
    fn test_default_options() {
        let storage = LockedStorage::default();
        assert_eq!(storage.store.read().len(), 0);
        assert_eq!(storage.options.ttl, Duration::from_secs(60 * 60 * 6));
        assert_eq!(storage.options.max_capacity, 1000 * 1000);
    }

    // Test creating storage with custom options
    #[test]
    fn test_custom_options() {
        let ttl = Duration::from_secs(30);
        let max_capacity = 50;
        let options = StorageOptions::new(
            ttl,
            max_capacity,
            &EvictionPolicy::Oldest,
            &Compression::Disabled,
            0,
        );
        let storage = LockedStorage::new(options);

        assert_eq!(storage.options.ttl, Duration::from_secs(30));
        assert_eq!(storage.options.max_capacity, 50);
        assert_eq!(storage.store.read().len(), 0);
    }

    // Test inserting and retrieving keys
    #[test]
    fn test_insert_and_get_key() {
        let mut storage = LockedStorage::default();
        let v = StorageValue::Text("test value".to_string());
        storage
            .insert_entry("test_key".to_string(), v.clone())
            .unwrap();

        let entry = storage.get_entry("test_key");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, v);
    }

    // Test removing keys
    #[test]
    fn test_remove_entry() {
        let mut storage = LockedStorage::default();
        let v = StorageValue::Text("test value".to_string());
        storage.insert_entry("test_key".to_string(), v).unwrap();

        assert!(storage.get_entry("test_key").is_some());

        storage.remove_entry("test_key");
        assert!(storage.get_entry("test_key").is_none());
    }

    // Test removing non-existent key
    #[test]
    fn test_remove_nonexistent_key() {
        let mut storage = LockedStorage::default();
        storage.remove_entry("nonexistent_key");
        // Should not panic
    }

    // Test capacity enforcement
    #[test]
    fn test_capacity_enforcement() {
        let options = StorageOptions::new(
            Duration::from_secs(3600),
            3,
            &EvictionPolicy::Oldest,
            &Compression::Disabled,
            0,
        );
        let mut storage = LockedStorage::new(options);
        let v1 = StorageValue::Text("value1".to_string());
        let v2 = StorageValue::Text("value2".to_string());
        let v3 = StorageValue::Text("value3".to_string());
        let v4 = StorageValue::Text("value4".to_string());

        // Insert up to capacity
        storage.insert_entry("key1".to_string(), v1).unwrap();
        storage.insert_entry("key2".to_string(), v2).unwrap();
        storage.insert_entry("key3".to_string(), v3).unwrap();

        assert_eq!(storage.store.read().len(), 3);
        assert!(storage.is_full());

        // Insert one more - oldest should be removed
        storage.insert_entry("key4".to_string(), v4).unwrap();

        assert_eq!(storage.store.read().len(), 3);
        assert!(storage.get_entry("key1").is_none());
        assert!(storage.get_entry("key2").is_some());
        assert!(storage.get_entry("key3").is_some());
        assert!(storage.get_entry("key4").is_some());
    }

    // Test TTL expiration
    #[test]
    fn test_ttl_expiration() {
        let options = StorageOptions::new(
            Duration::from_millis(100),
            10,
            &EvictionPolicy::Oldest,
            &Compression::Disabled,
            0,
        );
        let mut storage = LockedStorage::new(options);
        let v1 = StorageValue::Text("value1".to_string());
        let v2 = StorageValue::Text("value2".to_string());

        storage.insert_entry("key1".to_string(), v1).unwrap();

        // Verify key exists
        assert!(storage.get_entry("key1").is_some());

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(150));

        // Insert another key to trigger remove_expired()
        storage.insert_entry("key2".to_string(), v2).unwrap();

        // Verify expired key is gone
        assert!(storage.get_entry("key1").is_none());
        assert!(storage.get_entry("key2").is_some());
    }

    // Test the existing remove_expired function directly
    #[test]
    fn test_remove_expired_directly() {
        let options = StorageOptions::new(
            Duration::from_millis(100),
            5,
            &EvictionPolicy::Oldest,
            &Compression::Disabled,
            0,
        );
        let mut storage = LockedStorage::new(options);
        let v1 = StorageValue::Text("value1".to_string());
        let v2 = StorageValue::Text("value2".to_string());

        storage.insert_entry("key1".to_string(), v1).unwrap();
        storage.insert_entry("key2".to_string(), v2).unwrap();

        assert_eq!(storage.store.read().len(), 2);

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(150));

        // Call remove_expired directly
        storage.remove_expired();

        assert_eq!(storage.store.read().len(), 0);
    }

    // Test that oldest entries are removed when capacity is reached
    #[test]
    fn test_remove_oldest_key() {
        let options = StorageOptions::new(
            Duration::from_secs(3600),
            3,
            &EvictionPolicy::Oldest,
            &Compression::Disabled,
            0,
        );
        let mut storage = LockedStorage::new(options);
        let v1 = StorageValue::Text("value1".to_string());
        let v2 = StorageValue::Text("value2".to_string());
        let v3 = StorageValue::Text("value3".to_string());
        let v4 = StorageValue::Text("value4".to_string());

        storage.insert_entry("key1".to_string(), v1).unwrap();

        thread::sleep(Duration::from_millis(10)); // Ensure different timestamps
        storage.insert_entry("key2".to_string(), v2).unwrap();
        thread::sleep(Duration::from_millis(10));
        storage.insert_entry("key3".to_string(), v3).unwrap();

        assert_eq!(storage.store.read().len(), 3);

        // Insert a new key - should remove the oldest (key1)
        thread::sleep(Duration::from_millis(10));
        storage.insert_entry("key4".to_string(), v4).unwrap();

        assert_eq!(storage.store.read().len(), 3);
        assert!(storage.get_entry("key1").is_none());
        assert!(storage.get_entry("key2").is_some());
        assert!(storage.get_entry("key3").is_some());
        assert!(storage.get_entry("key4").is_some());
    }

    // Test updating an existing key
    #[test]
    fn test_update_existing_key() {
        let mut storage = LockedStorage::default();

        let v1 = StorageValue::Text("value1".to_string());
        let v2 = StorageValue::Text("updated value".to_string());
        storage.insert_entry("key1".to_string(), v1).unwrap();
        let original_entry = storage.get_entry("key1").unwrap();

        // Wait a moment to ensure timestamp is different
        thread::sleep(Duration::from_millis(10));

        // Update the key
        storage.insert_entry("key1".to_string(), v2).unwrap();
        let updated_entry = storage.get_entry("key1").unwrap();

        assert_eq!(storage.store.read().len(), 1);
        assert_eq!(
            updated_entry.value,
            StorageValue::Text("updated value".to_string())
        );
        assert!(updated_entry.created_at > original_entry.created_at);
    }

    #[test]
    fn test_insert_with_custom_ttl() {
        let mut storage = LockedStorage::default();
        let ttl = Duration::from_secs(5);
        let v1 = StorageValue::Text("custom val".to_string());
        storage
            .insert_with_ttl("custom_key".to_string(), v1, ttl)
            .unwrap();

        let entry = storage.get_entry("custom_key").unwrap();
        assert_eq!(entry.value, StorageValue::Text("custom val".to_string()));
        assert_eq!(entry.ttl, ttl);
    }

    #[test]
    fn test_extend_ttl() {
        let mut storage = LockedStorage::default();
        let original_ttl = Duration::from_secs(5);
        let extension = 10;
        let v1 = StorageValue::Text("val".to_string());
        storage
            .insert_with_ttl("key".to_string(), v1, original_ttl)
            .unwrap();

        let result = storage.extend_ttl("key", extension);
        assert!(result.is_ok());
        let new_ttl = storage.time_to_live("key").unwrap();
        assert_eq!(
            new_ttl,
            Duration::from_secs(extension as u64) + original_ttl
        );
    }

    #[test]
    fn test_extend_negative_ttl() {
        let mut storage = LockedStorage::default();
        let original_ttl = Duration::from_secs(5);
        let extension = -10; // would overflow
        let v1 = StorageValue::Text("val".to_string());
        storage
            .insert_with_ttl("key".to_string(), v1, original_ttl)
            .unwrap();

        let result = storage.extend_ttl("key", extension);
        assert!(result.is_err());
        let new_ttl = storage.time_to_live("key").unwrap();
        assert_eq!(new_ttl, original_ttl);
    }

    #[test]
    fn test_time_to_live_none_for_missing_key() {
        let storage = LockedStorage::default();
        assert!(storage.time_to_live("missing").is_none());
    }

    #[test]
    fn test_get_entries_batch() {
        let mut storage = LockedStorage::default();
        let v1 = StorageValue::Text("value1".to_string());
        let v2 = StorageValue::Text("value2".to_string());

        storage.insert_entry("key1".to_string(), v1).unwrap();
        storage.insert_entry("key2".to_string(), v2).unwrap();

        let result = storage.get_entries(&[
            "key1".to_string(),
            "key2".to_string(),
            "missing".to_string(),
        ]);
        assert_eq!(
            result[0].1.as_ref().unwrap().value,
            StorageValue::Text("value1".to_string())
        );
        assert_eq!(
            result[1].1.as_ref().unwrap().value,
            StorageValue::Text("value2".to_string())
        );
        assert!(result[2].1.is_none())
    }

    #[test]
    fn test_insert_and_remove_entries_batch() {
        let mut storage = LockedStorage::default();
        let mut batch = HashMap::new();
        let v1 = StorageValue::Text("value1".to_string());
        let v2 = StorageValue::Text("value2".to_string());
        batch.insert("k1".to_string(), v1);
        batch.insert("k2".to_string(), v2);

        storage.insert_entries(batch).unwrap();
        assert!(storage.get_entry("k1").is_some());
        assert!(storage.get_entry("k2").is_some());

        storage.remove_entries(&["k1".to_string(), "k2".to_string()]);
        assert!(storage.get_entry("k1").is_none());
        assert!(storage.get_entry("k2").is_none());
    }

    #[test]
    fn test_eviction_policy_lru() {
        let options = StorageOptions::new(
            Duration::from_secs(10),
            2,
            &EvictionPolicy::Oldest,
            &Compression::Disabled,
            0,
        );
        let mut storage = LockedStorage::new(options);
        storage.set_config_entry(&ConfigEntry::EvictPolicy(EvictionPolicy::LRU));
        let v1 = StorageValue::Text("value1".to_string());
        let v2 = StorageValue::Text("value2".to_string());
        let v3 = StorageValue::Text("value3".to_string());

        storage.insert_entry("key1".to_string(), v1).unwrap();
        storage.insert_entry("key2".to_string(), v2).unwrap();

        storage.get_entry("k1"); // Make k1 recently used
        storage.insert_entry("key3".to_string(), v3).unwrap();

        assert!(storage.get_entry("k2").is_none()); // Least recently used
    }

    #[test]
    fn test_eviction_policy_lfu() {
        let options = StorageOptions::new(
            Duration::from_secs(10),
            2,
            &EvictionPolicy::Oldest,
            &Compression::Disabled,
            0,
        );
        let mut storage = LockedStorage::new(options);
        storage.set_config_entry(&ConfigEntry::EvictPolicy(EvictionPolicy::LFU));

        let v1 = StorageValue::Text("value1".to_string());
        let v2 = StorageValue::Text("value2".to_string());
        let v3 = StorageValue::Text("value3".to_string());

        storage.insert_entry("key1".to_string(), v1).unwrap();
        storage.insert_entry("key2".to_string(), v2).unwrap();
        storage.get_entry("k1"); // Increase access count for k1
        storage.get_entry("k1");
        storage.insert_entry("key3".to_string(), v3).unwrap();

        assert!(storage.get_entry("k2").is_none()); // Least frequently used
    }

    #[test]
    fn test_eviction_policy_size_aware() {
        let options = StorageOptions::new(
            Duration::from_secs(10),
            2,
            &EvictionPolicy::Oldest,
            &Compression::Disabled,
            0,
        );
        let mut storage = LockedStorage::new(options);
        storage.set_config_entry(&ConfigEntry::EvictPolicy(EvictionPolicy::SizeAware));
        let v1 = StorageValue::Text("v".to_string());
        let v2 = StorageValue::Text("valuevalue".to_string());
        let v3 = StorageValue::Text("x".to_string());

        storage.insert_entry("small".to_string(), v1).unwrap();
        storage.insert_entry("large".to_string(), v2).unwrap();
        storage.insert_entry("extra".to_string(), v3).unwrap();

        // Largest value ("v") should be evicted
        assert!(storage.get_entry("large").is_none());
    }

    #[test]
    fn test_reset_stats() {
        let mut storage = LockedStorage::default();
        let v1 = StorageValue::Text("value1".to_string());
        storage.insert_entry("k1".to_string(), v1).unwrap();
        storage.get_entry("k1");
        storage.get_entry("missing");

        let stats = storage.get_stats();
        assert_eq!(stats.hits.load(Ordering::Relaxed), 1);
        assert_eq!(stats.misses.load(Ordering::Relaxed), 1);

        storage.reset_stats();
        let stats = storage.get_stats();
        assert_eq!(stats.hits.load(Ordering::Relaxed), 0);
        assert_eq!(stats.misses.load(Ordering::Relaxed), 0);
        assert_eq!(stats.evictions.load(Ordering::Relaxed), 0);
        assert_eq!(stats.expired_removals.load(Ordering::Relaxed), 0);
    }
}
