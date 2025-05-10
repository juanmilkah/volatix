use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

// Cons of BTreeMap compared to HashMap
// Slower lookups: O(log n) time complexity for lookups vs O(1) average case for HashMap.
// Slower insertions: O(log n) vs O(1) average for HashMap.
// Slower deletions: O(log n) vs O(1) average for HashMap.
#[derive(Default)]
pub struct Storage {
    pub store: HashMap<String, StorageEntry>,
    pub options: StorageOptions,
    pub stats: StorageStats,
    pub eviction_policy: EvictionPolicy,
}

#[derive(Clone)]
pub struct StorageEntry {
    pub value: String,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub access_count: usize,
    pub entry_size: usize,
    pub ttl: Duration,
}

#[derive(Debug)]
pub struct StorageOptions {
    pub ttl: Duration,
    pub max_capacity: usize,
}

impl StorageOptions {
    pub fn new(ttl: Duration, max_cap: usize) -> Self {
        Self {
            ttl,
            max_capacity: max_cap,
        }
    }
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(60 * 60 * 24), // 24 hrs
            max_capacity: 1000,
        }
    }
}

#[derive(Default)]
pub struct StorageStats {
    pub hits: usize,
    pub misses: usize,
    pub evictions: usize,
    pub expired_removals: usize,
    // pub avg_lookup_time: Duration,
}

#[derive(Default, Debug, Clone)]
pub enum EvictionPolicy {
    #[default]
    Oldest, // Oldest created
    LRU,       // Least recently  accessed
    LFU,       // Least frequently used
    SizeAware, // Largest items
}

impl Storage {
    pub fn new(options: StorageOptions) -> Self {
        Storage {
            store: HashMap::new(),
            options,
            stats: StorageStats::default(),
            eviction_policy: EvictionPolicy::default(),
        }
    }

    pub fn get_key(&mut self, key: &str) -> Option<StorageEntry> {
        if let Some(entry) = self.store.get_mut(key) {
            self.stats.hits += 1;
            entry.access_count += 1;
            entry.last_accessed = Instant::now();
            return Some(entry.clone());
        }
        self.stats.misses += 1;
        None
    }

    // batch operations
    pub fn get_entries(&mut self, keys: &[String]) -> Vec<(String, Option<StorageEntry>)> {
        let mut result = Vec::new();
        for key in keys {
            result.push((key.to_string(), self.get_key(key)));
        }

        result
    }

    pub fn insert_entries(&mut self, entries: HashMap<String, String>) {
        for (key, value) in entries {
            self.insert_entry(key, value);
        }
    }

    pub fn remove_entries(&mut self, keys: &[String]) {
        for key in keys {
            self.remove_entry(key);
        }
    }

    pub fn insert_entry(&mut self, key: String, value: String) {
        if self.is_full() {
            self.evict_entries();
        }

        let timestamp = Instant::now();
        let entry_size = value.len();
        let entry = StorageEntry {
            value,
            created_at: timestamp,
            last_accessed: timestamp,
            access_count: 0,
            entry_size,
            ttl: self.options.ttl,
        };
        self.store.insert(key, entry);
        self.remove_expired();
    }

    pub fn remove_entry(&mut self, key: &str) {
        self.store.remove(key);
    }

    pub fn insert_with_ttl(&mut self, key: String, value: String, ttl: Duration) {
        let entry_size = value.len();

        let entry = StorageEntry {
            value,
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 0,
            entry_size,
            ttl,
        };

        self.store.insert(key, entry);
        self.remove_expired();
    }

    pub fn extend_ttl(&mut self, key: &str, additional_time: Duration) {
        if let Some(entry) = self.store.get_mut(key) {
            entry.ttl += additional_time;
            entry.access_count += 1;
            entry.last_accessed = Instant::now();
        }
    }

    pub fn time_to_live(&mut self, key: &str) -> Option<Duration> {
        if let Some(entry) = self.get_key(key) {
            return Some(entry.ttl);
        }
        None
    }

    fn is_full(&self) -> bool {
        self.store.len() >= self.options.max_capacity
    }

    fn remove_oldest_entry(&mut self) {
        let oldest_key = self
            .store
            .iter()
            .min_by(|(_k1, v1), (_k2, v2)| v1.created_at.cmp(&v2.created_at))
            .map(|(k, _v)| k.clone());

        if let Some(key) = oldest_key {
            self.remove_entry(&key);
            self.stats.evictions += 1;
        }
    }

    fn remove_expired(&mut self) {
        let prev_count = self.store.keys().count();
        self.store
            .retain(|_, value| value.created_at.elapsed() < self.options.ttl);
        let current_count = self.store.keys().count();
        let removed = prev_count - current_count;
        self.stats.expired_removals += removed;
    }

    pub fn evict_entries(&mut self) {
        self.remove_expired();
        if self.store.len() < self.options.max_capacity {
            return;
        }

        match self.eviction_policy {
            EvictionPolicy::Oldest => self.remove_oldest_entry(),
            EvictionPolicy::LRU => self.remove_lru_entry(),
            EvictionPolicy::LFU => self.remove_lfu_entry(),
            EvictionPolicy::SizeAware => self.remove_largest_entry(),
        }
    }

    pub fn set_eviction_policy(&mut self, new_policy: &EvictionPolicy) {
        self.eviction_policy = new_policy.clone();
    }

    // least recently used/ accessed
    pub fn remove_lru_entry(&mut self) {
        let key = self
            .store
            .iter()
            .min_by(|(_, v1), (_, v2)| v1.last_accessed.cmp(&v2.last_accessed))
            .map(|(k, _)| k.clone());

        if let Some(key) = key {
            self.store.remove_entry(&key);
            self.stats.evictions += 1;
        }
    }

    // least frequently used/accessed
    pub fn remove_lfu_entry(&mut self) {
        let key = self
            .store
            .iter()
            .min_by(|(_, v1), (_, v2)| v1.access_count.cmp(&v2.access_count))
            .map(|(k, _)| k.clone());

        if let Some(key) = key {
            self.store.remove_entry(&key);
            self.stats.evictions += 1;
        }
    }

    pub fn remove_largest_entry(&mut self) {
        let key = self
            .store
            .iter()
            .min_by(|(_, v1), (_, v2)| v1.entry_size.cmp(&v2.entry_size))
            .map(|(k, _)| k.clone());

        if let Some(key) = key {
            self.store.remove_entry(&key);
            self.stats.evictions += 1;
        }
    }

    pub fn get_stats(&self) -> StorageStats {
        StorageStats {
            hits: self.stats.hits,
            misses: self.stats.misses,
            evictions: self.stats.evictions,
            expired_removals: self.stats.expired_removals,
        }
    }

    pub fn reset_stats(&mut self) {
        self.stats = StorageStats::default();
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, thread, time::Duration};

    use crate::EvictionPolicy;

    use super::{Storage, StorageOptions};

    // Test default configuration
    #[test]
    fn test_default_options() {
        let storage = Storage::default();
        assert_eq!(storage.store.len(), 0);
        assert_eq!(storage.options.ttl, Duration::from_secs(60 * 60 * 24));
        assert_eq!(storage.options.max_capacity, 1000);
    }

    // Test creating storage with custom options
    #[test]
    fn test_custom_options() {
        let ttl = Duration::from_secs(30);
        let max_capacity = 50;
        let options = StorageOptions::new(ttl, max_capacity);
        let storage = Storage::new(options);

        assert_eq!(storage.options.ttl, Duration::from_secs(30));
        assert_eq!(storage.options.max_capacity, 50);
        assert_eq!(storage.store.len(), 0);
    }

    // Test inserting and retrieving keys
    #[test]
    fn test_insert_and_get_key() {
        let mut storage = Storage::default();
        storage.insert_entry("test_key".to_string(), "test_value".to_string());

        let entry = storage.get_key("test_key");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, "test_value");
    }

    // Test removing keys
    #[test]
    fn test_remove_entry() {
        let mut storage = Storage::default();
        storage.insert_entry("test_key".to_string(), "test_value".to_string());

        assert!(storage.get_key("test_key").is_some());

        storage.remove_entry("test_key");
        assert!(storage.get_key("test_key").is_none());
    }

    // Test removing non-existent key
    #[test]
    fn test_remove_nonexistent_key() {
        let mut storage = Storage::default();
        storage.remove_entry("nonexistent_key");
        // Should not panic
    }

    // Test capacity enforcement
    #[test]
    fn test_capacity_enforcement() {
        let options = StorageOptions::new(Duration::from_secs(3600), 3);
        let mut storage = Storage::new(options);

        // Insert up to capacity
        storage.insert_entry("key1".to_string(), "value1".to_string());
        storage.insert_entry("key2".to_string(), "value2".to_string());
        storage.insert_entry("key3".to_string(), "value3".to_string());

        assert_eq!(storage.store.len(), 3);
        assert!(storage.is_full());

        // Insert one more - oldest should be removed
        storage.insert_entry("key4".to_string(), "value4".to_string());

        assert_eq!(storage.store.len(), 3);
        assert!(storage.get_key("key1").is_none());
        assert!(storage.get_key("key2").is_some());
        assert!(storage.get_key("key3").is_some());
        assert!(storage.get_key("key4").is_some());
    }

    // Test TTL expiration
    #[test]
    fn test_ttl_expiration() {
        let options = StorageOptions::new(Duration::from_millis(100), 10);
        let mut storage = Storage::new(options);

        storage.insert_entry("key1".to_string(), "value1".to_string());

        // Verify key exists
        assert!(storage.get_key("key1").is_some());

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(150));

        // Insert another key to trigger remove_expired()
        storage.insert_entry("key2".to_string(), "value2".to_string());

        // Verify expired key is gone
        assert!(storage.get_key("key1").is_none());
        assert!(storage.get_key("key2").is_some());
    }

    // Test that remove_expired correctly handles multiple expired keys
    #[test]
    fn test_multiple_expired_keys() {
        let options = StorageOptions::new(Duration::from_millis(100), 10);
        let mut storage = Storage::new(options);

        storage.insert_entry("key1".to_string(), "value1".to_string());
        storage.insert_entry("key2".to_string(), "value2".to_string());

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(150));

        // Insert a new key to trigger remove_expired()
        storage.insert_entry("key3".to_string(), "value3".to_string());

        // Verify both expired keys are gone
        assert!(storage.get_key("key1").is_none());
        assert!(storage.get_key("key2").is_none());
        assert!(storage.get_key("key3").is_some());
        assert_eq!(storage.store.len(), 1);
    }

    // Test the existing remove_expired function directly
    #[test]
    fn test_remove_expired_directly() {
        let options = StorageOptions::new(Duration::from_millis(100), 5);
        let mut storage = Storage::new(options);

        storage.insert_entry("key1".to_string(), "value1".to_string());
        storage.insert_entry("key2".to_string(), "value2".to_string());

        assert_eq!(storage.store.len(), 2);

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(150));

        // Call remove_expired directly
        storage.remove_expired();

        assert_eq!(storage.store.len(), 0);
    }

    // Test that oldest entries are removed when capacity is reached
    #[test]
    fn test_remove_oldest_key() {
        let options = StorageOptions::new(Duration::from_secs(3600), 3);
        let mut storage = Storage::new(options);

        storage.insert_entry("key1".to_string(), "value1".to_string());
        thread::sleep(Duration::from_millis(10)); // Ensure different timestamps
        storage.insert_entry("key2".to_string(), "value2".to_string());
        thread::sleep(Duration::from_millis(10));
        storage.insert_entry("key3".to_string(), "value3".to_string());

        assert_eq!(storage.store.len(), 3);

        // Insert a new key - should remove the oldest (key1)
        thread::sleep(Duration::from_millis(10));
        storage.insert_entry("key4".to_string(), "value4".to_string());

        assert_eq!(storage.store.len(), 3);
        assert!(storage.get_key("key1").is_none());
        assert!(storage.get_key("key2").is_some());
        assert!(storage.get_key("key3").is_some());
        assert!(storage.get_key("key4").is_some());
    }

    // Test updating an existing key
    #[test]
    fn test_update_existing_key() {
        let mut storage = Storage::default();

        storage.insert_entry("key1".to_string(), "value1".to_string());
        let original_entry = storage.get_key("key1").unwrap();

        // Wait a moment to ensure timestamp is different
        thread::sleep(Duration::from_millis(10));

        // Update the key
        storage.insert_entry("key1".to_string(), "updated_value".to_string());
        let updated_entry = storage.get_key("key1").unwrap();

        assert_eq!(storage.store.len(), 1);
        assert_eq!(updated_entry.value, "updated_value");
        assert!(updated_entry.created_at > original_entry.created_at);
    }

    #[test]
    fn test_insert_with_custom_ttl() {
        let mut storage = Storage::default();
        let ttl = Duration::from_secs(5);
        storage.insert_with_ttl("custom_key".to_string(), "custom_val".to_string(), ttl);

        let entry = storage.get_key("custom_key").unwrap();
        assert_eq!(entry.value, "custom_val");
        assert_eq!(entry.ttl, ttl);
    }

    #[test]
    fn test_extend_ttl() {
        let mut storage = Storage::default();
        let original_ttl = Duration::from_secs(5);
        let extension = Duration::from_secs(10);
        storage.insert_with_ttl("key".to_string(), "val".to_string(), original_ttl);

        storage.extend_ttl("key", extension);
        let new_ttl = storage.time_to_live("key").unwrap();
        assert_eq!(new_ttl, original_ttl + extension);
    }

    #[test]
    fn test_time_to_live_none_for_missing_key() {
        let mut storage = Storage::default();
        assert!(storage.time_to_live("missing").is_none());
    }

    #[test]
    fn test_get_entries_batch() {
        let mut storage = Storage::default();
        storage.insert_entry("key1".to_string(), "val1".to_string());
        storage.insert_entry("key2".to_string(), "val2".to_string());

        let result = storage.get_entries(&[
            "key1".to_string(),
            "key2".to_string(),
            "missing".to_string(),
        ]);
        assert_eq!(result["key1"].as_ref().unwrap().value, "val1");
        assert_eq!(result["key2"].as_ref().unwrap().value, "val2");
        assert!(result["missing"].is_none());
    }

    #[test]
    fn test_insert_and_remove_entries_batch() {
        let mut storage = Storage::default();
        let mut batch = HashMap::new();
        batch.insert("k1".to_string(), "v1".to_string());
        batch.insert("k2".to_string(), "v2".to_string());

        storage.insert_entries(batch);
        assert!(storage.get_key("k1").is_some());
        assert!(storage.get_key("k2").is_some());

        storage.remove_entries(&["k1".to_string(), "k2".to_string()]);
        assert!(storage.get_key("k1").is_none());
        assert!(storage.get_key("k2").is_none());
    }

    #[test]
    fn test_eviction_policy_lru() {
        let options = StorageOptions::new(Duration::from_secs(10), 2);
        let mut storage = Storage::new(options);
        storage.set_eviction_policy(&EvictionPolicy::LRU);

        storage.insert_entry("k1".to_string(), "v1".to_string());
        storage.insert_entry("k2".to_string(), "v2".to_string());
        storage.get_key("k1"); // Make k1 recently used
        storage.insert_entry("k3".to_string(), "v3".to_string());

        assert!(storage.get_key("k2").is_none()); // Least recently used
    }

    #[test]
    fn test_eviction_policy_lfu() {
        let options = StorageOptions::new(Duration::from_secs(10), 2);
        let mut storage = Storage::new(options);
        storage.set_eviction_policy(&EvictionPolicy::LFU);

        storage.insert_entry("k1".to_string(), "v1".to_string());
        storage.insert_entry("k2".to_string(), "v2".to_string());
        storage.get_key("k1"); // Increase access count for k1
        storage.get_key("k1");
        storage.insert_entry("k3".to_string(), "v3".to_string());

        assert!(storage.get_key("k2").is_none()); // Least frequently used
    }

    #[test]
    fn test_eviction_policy_size_aware() {
        let options = StorageOptions::new(Duration::from_secs(10), 2);
        let mut storage = Storage::new(options);
        storage.set_eviction_policy(&EvictionPolicy::SizeAware);

        storage.insert_entry("small".to_string(), "v".to_string());
        storage.insert_entry("large".to_string(), "valuevalue".to_string());
        storage.insert_entry("extra".to_string(), "x".to_string());

        // Smallest value ("v") should be evicted
        assert!(storage.get_key("small").is_none());
    }

    #[test]
    fn test_reset_stats() {
        let mut storage = Storage::default();
        storage.insert_entry("k1".to_string(), "v1".to_string());
        storage.get_key("k1");
        storage.get_key("missing");

        let stats = storage.get_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);

        storage.reset_stats();
        let stats = storage.get_stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.evictions, 0);
        assert_eq!(stats.expired_removals, 0);
    }
}
