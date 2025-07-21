use std::{
    collections::HashMap,
    fmt::Display,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
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

#[derive(Default)]
pub struct LockedStorage {
    pub store: Arc<RwLock<HashMap<String, StorageEntry>>>,
    pub options: StorageOptions,
    pub stats: StorageStats,
}

#[derive(Serialize, Deserialize)]
struct UnlockedStorage {
    store: HashMap<String, StorageEntry>,
    options: StorageOptions,
    stats: NonAtomicStats,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum StorageValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
}

impl Display for StorageValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageValue::Int(i) => write!(f, "{i}"),
            StorageValue::Float(fl) => write!(f, "{fl}"),
            StorageValue::Bool(b) => write!(f, "{b}"),
            StorageValue::Text(t) => write!(f, "{t}"),
            StorageValue::Bytes(b) => write!(f, "{b:?}"),
        }
    }
}

impl StorageValue {
    fn size_in_bytes(&self) -> usize {
        match self {
            StorageValue::Int(_) => size_of_val(self),
            StorageValue::Float(_) => size_of_val(self),
            StorageValue::Bool(_) => size_of_val(self),
            // size_of_val(self) only captures the size of the
            // String struct (usually 24 bytes on 64-bit systems).
            // The actual bytes on the heap are in s.capacity()
            StorageValue::Text(t) => size_of_val(self) + t.capacity(),
            StorageValue::Bytes(b) => b.len(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageEntry {
    pub value: StorageValue,
    pub created_at: SystemTime,
    pub last_accessed: SystemTime,
    pub access_count: usize,
    pub entry_size: usize,
    pub ttl: Duration,
    pub compressed: bool,
}

impl Display for StorageEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Value: {}, Created_at: {:?}, Last accessed: {:?}, Access Count: {}, Entry size: {}, Ttl: {}, Compressed: {}",
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
    fn is_expired(&self) -> bool {
        SystemTime::now()
            .duration_since(self.created_at)
            .expect("clock gone backwards")
            > self.ttl
    }

    fn decompress(&mut self) -> io::Result<()> {
        let mut output = String::new();
        let input = match &self.value {
            StorageValue::Bytes(bytes) => bytes,
            _ => return Ok(()),
        };
        let mut z = ZlibDecoder::new(&input[..]);
        z.read_to_string(&mut output)?;

        self.value = StorageValue::Text(output);
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct StorageOptions {
    pub ttl: Duration,
    pub max_capacity: u64,
    pub eviction_policy: EvictionPolicy,
    pub compression: bool,
    pub compression_threshold: usize,
}

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
            ttl: Duration::from_secs(60 * 60 * 6), // 6 hrs
            max_capacity: (1000 * 1000),           // 1 Million
            eviction_policy: EvictionPolicy::default(),
            compression: false,
            compression_threshold: 1024 * 4, // 4Kb
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

#[derive(Debug, Default, Serialize, Deserialize)]
struct NonAtomicStats {
    total_entries: usize,
    hits: usize,
    misses: usize,
    evictions: usize,
    expired_removals: usize,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StorageStats {
    pub total_entries: AtomicUsize,
    pub hits: AtomicUsize,
    pub misses: AtomicUsize,
    pub evictions: AtomicUsize,
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

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EvictionPolicy {
    #[default]
    Oldest, // Oldest created
    LRU,       // Least recently  accessed
    LFU,       // Least frequently used
    SizeAware, // Largest items
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
    pub fn new(options: StorageOptions) -> Self {
        LockedStorage {
            store: Arc::new(RwLock::new(HashMap::with_capacity(1000))),
            options,
            stats: StorageStats::default(),
        }
    }

    pub fn flush(&mut self) {
        let _old_storage = std::mem::take(self);
    }

    pub fn get_entry(&self, key: &str) -> Option<StorageEntry> {
        let mut entry = if let Some(entry) = self.store.write().get_mut(key)
            && !entry.is_expired()
        {
            entry.access_count += 1;
            entry.last_accessed = SystemTime::now();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            entry.clone()
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };

        // You don't wanna move this into the if block;
        // Tryna hold exclusive lock to store for as
        // shortest time as possible
        #[allow(clippy::collapsible_if)]
        if entry.compressed {
            if entry.decompress().is_err() {
                eprintln!("Entry decompression on get_entry");
                return None;
            }
        }

        Some(entry.clone())
    }

    pub fn key_exists(&self, key: &str) -> bool {
        self.get_entry(key).is_some()
    }

    pub fn get_keys(&self) -> Vec<String> {
        self.store.read().keys().cloned().collect()
    }

    // batch operations
    pub fn get_entries(&self, keys: &[String]) -> Vec<(String, Option<StorageEntry>)> {
        let mut result = Vec::new();
        for key in keys {
            result.push((key.to_string(), self.get_entry(key)));
        }

        result
    }

    pub fn insert_entries(&mut self, entries: HashMap<String, StorageValue>) -> Result<(), String> {
        for (key, value) in entries {
            self.insert_entry(key, value)?;
        }

        Ok(())
    }

    pub fn remove_entries(&mut self, keys: &[String]) {
        for key in keys {
            self.remove_entry(key);
        }
    }

    pub fn insert_entry(&mut self, key: String, value: StorageValue) -> Result<(), String> {
        self.insert_with_ttl(key, value, self.options.ttl)?;
        Ok(())
    }

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

    pub fn remove_entry(&mut self, key: &str) {
        if self.store.write().remove_entry(key).is_some() {
            self.stats.total_entries.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn insert_with_ttl(
        &mut self,
        key: String,
        value: StorageValue,
        ttl: Duration,
    ) -> Result<(), String> {
        if self.is_full() {
            self.evict_entries();
        }
        let entry_size = value.size_in_bytes();
        let now = SystemTime::now();

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

        let entry = StorageEntry {
            value,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            entry_size,
            ttl,
            compressed,
        };

        self.store.write().insert(key, entry);
        self.stats.total_entries.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn extend_ttl(&mut self, key: &str, additional_time: i64) -> Result<(), String> {
        if let Some(entry) = self.store.write().get_mut(key) {
            if additional_time < 0 {
                if additional_time.unsigned_abs() > entry.ttl.as_secs() {
                    return Err("UNALTERED".to_string());
                }

                entry.ttl -= Duration::from_secs(additional_time.unsigned_abs());
            } else {
                entry.ttl += Duration::from_secs(additional_time as u64);
            }
            entry.access_count += 1;
            entry.last_accessed = SystemTime::now();
        }

        Ok(())
    }

    pub fn time_to_live(&self, key: &str) -> Option<Duration> {
        self.get_entry(key).map(|e| e.ttl)
    }

    fn is_full(&self) -> bool {
        self.store.read().len() as u64 >= self.options.max_capacity
    }

    fn remove_expired(&mut self) {
        let prev_count = self.store.read().len();
        let now = SystemTime::now();
        {
            self.store.write().retain(|_, value| {
                now.duration_since(value.created_at)
                    .expect("clock gone backwards")
                    < value.ttl
            });
        }
        let current_count = self.store.read().len();
        let removed = prev_count - current_count;
        self.stats
            .expired_removals
            .fetch_add(removed, Ordering::Release);
        self.stats
            .total_entries
            .store(current_count, Ordering::Release);
    }

    pub fn evict_entries(&mut self) {
        self.remove_expired();
        // TODO! Not sure whether we should return here
        if !self.is_full() {
            return;
        }

        match self.options.eviction_policy {
            EvictionPolicy::Oldest => self.remove_oldest_entry(),
            EvictionPolicy::LRU => self.remove_lru_entry(),
            EvictionPolicy::LFU => self.remove_lfu_entry(),
            EvictionPolicy::SizeAware => self.remove_largest_entry(),
        }
    }

    // least recently used/ accessed
    pub fn remove_lru_entry(&mut self) {
        let key = self
            .store
            .read()
            .iter()
            .min_by(|(_, v1), (_, v2)| v1.last_accessed.cmp(&v2.last_accessed))
            .map(|(k, _)| k.clone());

        if let Some(key) = key {
            self.remove_entry(&key);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    // least frequently used/accessed
    pub fn remove_lfu_entry(&mut self) {
        let key = self
            .store
            .read()
            .iter()
            .min_by(|(_, v1), (_, v2)| v1.access_count.cmp(&v2.access_count))
            .map(|(k, _)| k.clone());

        if let Some(key) = key {
            self.remove_entry(&key);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn remove_largest_entry(&mut self) {
        let key = self
            .store
            .read()
            .iter()
            .min_by(|(_, v1), (_, v2)| v1.entry_size.cmp(&v2.entry_size))
            .map(|(k, _)| k.clone());

        if let Some(key) = key {
            self.remove_entry(&key);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn remove_oldest_entry(&mut self) {
        let oldest_key = self
            .store
            .read()
            .iter()
            .min_by(|(_k1, v1), (_k2, v2)| v1.created_at.cmp(&v2.created_at))
            .map(|(k, _v)| k.clone());

        if let Some(key) = oldest_key {
            self.remove_entry(&key);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn rename_entry(&mut self, old_key: &str, new_key: &str) {
        if let Some((_, mut entry)) = self.store.write().remove_entry(old_key) {
            entry.access_count += 1;
            entry.last_accessed = SystemTime::now();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            self.store.write().insert(new_key.to_string(), entry);
            return;
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> StorageStats {
        StorageStats {
            total_entries: self.stats.total_entries.load(Ordering::Relaxed).into(),
            hits: self.stats.hits.load(Ordering::Relaxed).into(),
            misses: self.stats.misses.load(Ordering::Relaxed).into(),
            evictions: self.stats.evictions.load(Ordering::Relaxed).into(),
            expired_removals: self.stats.expired_removals.load(Ordering::Relaxed).into(),
        }
    }

    pub fn reset_stats(&mut self) {
        let _old_stats = std::mem::take(&mut self.stats);
    }

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

    pub fn get_options(&self) -> StorageOptions {
        self.options
    }

    pub fn load_from_disk(&mut self, path: &str) -> anyhow::Result<()> {
        let path = Path::new(path);
        if !path.exists() {
            return Ok(());
        }

        let file = File::open(path).context("open db")?;
        let mut reader = BufReader::new(file);

        let unlocked_storage: UnlockedStorage =
            bincode2::deserialize_from(&mut reader).context("deserialize from buffer")?;
        let stats = StorageStats {
            total_entries: AtomicUsize::new(unlocked_storage.stats.total_entries),
            hits: AtomicUsize::new(unlocked_storage.stats.hits),
            misses: AtomicUsize::new(unlocked_storage.stats.misses),
            evictions: AtomicUsize::new(unlocked_storage.stats.evictions),
            expired_removals: AtomicUsize::new(unlocked_storage.stats.expired_removals),
        };

        *self = LockedStorage {
            store: Arc::new(RwLock::new(unlocked_storage.store)),
            options: unlocked_storage.options,
            stats,
        };

        Ok(())
    }

    pub fn save_to_disk(&self, path: &str) -> anyhow::Result<()> {
        let path = Path::new(path);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .context("open db for writing")?;

        let stats = NonAtomicStats {
            total_entries: self.stats.total_entries.load(Ordering::Relaxed),
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            expired_removals: self.stats.expired_removals.load(Ordering::Relaxed),
        };
        let unlocked_storage = UnlockedStorage {
            store: self.store.read().clone(),
            options: self.options,
            stats,
        };
        let mut writer = BufWriter::new(file);
        bincode2::serialize_into(&mut writer, &unlocked_storage).context("serialize into db")?;
        writer.flush().context("flush writer")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::atomic::Ordering, thread, time::Duration};

    use crate::{Compression, ConfigEntry, EvictionPolicy, StorageValue};

    use super::{LockedStorage, StorageOptions};

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

        // Smallest value ("v") should be evicted
        assert!(storage.get_entry("small").is_none());
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
