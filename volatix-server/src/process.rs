use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    resp3::{
        RequestType, array_response, batch_entries_response, boolean_response, bulk_error_response,
        bulk_string_response, integer_response, null_response,
    },
    storage::{Compression, ConfigEntry, EvictionPolicy, LockedStorage, StorageValue},
};

fn request_type_to_storage_value(req: &RequestType) -> Result<StorageValue, String> {
    match req {
        // All string-like types are converted to StorageValue::Text
        RequestType::SimpleString { data }
        | RequestType::SimpleError { data }
        | RequestType::BulkString { data }
        | RequestType::BulkError { data }
        // FIX: Figure out a better way to handle big numbers
        | RequestType::BigNumber { data } => {
            let text = String::from_utf8_lossy(data).to_string();
            Ok(StorageValue::Text(text))
        }

        RequestType::Null => Ok(StorageValue::Null),

        // Parse integer values
        RequestType::Integer { data } => {
            let int_string = String::from_utf8_lossy(data).to_string();
            let int = int_string.parse::<i64>().map_err(|err| err.to_string())?;
            Ok(StorageValue::Int(int))
        }

        // Boolean values
        RequestType::Boolean { data } => Ok(StorageValue::Bool(*data)),

        // Parse floating point values
        RequestType::Double { data } => {
            let d_string = String::from_utf8_lossy(data).to_string();
            let double = d_string.parse::<f64>().map_err(|err| err.to_string())?;
            Ok(StorageValue::Float(double))
        }

        // Convert arrays to lists by recursively converting each element
        RequestType::Array { children } => {
            let elems = children
                .iter()
                .flat_map(request_type_to_storage_value) // Skip failed conversions
                .collect();
            Ok(StorageValue::List(elems))
        }

        // Convert sets to lists (sets are treated as lists internally)
        RequestType::Set { children } => {
            let elems = children
                .iter()
                .map(|c| {
                    let s = String::from_utf8_lossy(c);
                    get_value_type(&s) // Auto-detect the value type
                })
                .collect();
            Ok(StorageValue::List(elems))
        }

        // Convert maps to our internal map format
        RequestType::Map { children } => {
            let elems = children
                .iter()
                .map(|(k, v)| request_type_to_storage_value(v).map(|v| (k.to_string(), v)))
                .collect();
            match elems {
                Ok(elems) => Ok(StorageValue::Map(elems)),
                Err(e) => Err(e),
            }
        }

        // Unsupported types
        _ => Err("Unsupported storage value".to_string()),
    }
}

/// Enum representing all supported commands in the cache server.
/// This provides a type-safe way to handle different command types.
pub enum Command {
    // Basic operations
    Get,    // Retrieve a value by key
    Exists, // Check if a key exists
    Set,    // Store a key-value pair
    Delete, // Remove a key

    // Batch operations
    SetList,    // Store a list value
    GetList,    // Retrieve multiple keys
    DeleteList, // Remove multiple keys

    // Configuration management
    ConfSet, // Set a configuration option
    ConfGet, // Get a configuration option

    // TTL management
    Expire,  // Modify TTL of an existing key
    SetwTtl, // Set a value with specific TTL
    GetTtl,  // Get remaining TTL for a key

    // Administrative
    Dump,     // Get detailed entry information
    SetMap,   // Store multiple key-value pairs (like Redis MSET)
    Incr,     // Increment an integer value
    Decr,     // Decrement an integer value
    Rename,   // Rename a key
    EvictNow, // Evict entries

    Unknown, // Invalid or unsupported command
}

/// Attempts to automatically detect the type of a string value.
/// Uses heuristics to determine if a string should be parsed as:
/// - Integer (i64)
/// - Float (f64)
/// - Boolean
/// - Text (fallback)
///
/// # Arguments
/// * `value` - The string to analyze
///
/// # Returns
/// The most appropriate `StorageValue` type for the string
fn get_value_type(value: &str) -> StorageValue {
    // Try parsing as integer first (most common)
    if let Ok(n) = value.parse::<i64>() {
        StorageValue::Int(n)
    }
    // Try parsing as float
    else if let Ok(n) = value.parse::<f64>() {
        StorageValue::Float(n)
    }
    // Check for boolean values (case insensitive)
    else if value.to_lowercase().as_str() == "false" {
        StorageValue::Bool(false)
    } else if value.to_lowercase().as_str() == "true" {
        StorageValue::Bool(true)
    }
    // Default to text
    else {
        StorageValue::Text(value.to_string())
    }
}

/// Creates a ConfigEntry from a key-value pair for configuration management.
/// Validates the configuration values and returns appropriate config entries.
///
/// # Arguments
/// * `key` - The configuration key (case insensitive)
/// * `value` - The configuration value to set
///
/// # Returns
/// `Result<ConfigEntry, String>` - The config entry or error message
///
/// # Supported Configuration Keys
/// - `MAXCAP`: Maximum cache capacity (positive integer)
/// - `GLOBALTTL`: Default TTL in seconds (positive integer)
/// - `EVICTPOLICY`: Eviction strategy (OLDEST, LFU, LRU, SIZEAWARE)
/// - `COMPRESSION`: Enable/disable compression (ENABLE/DISABLE)
/// - `COMPTHRESHOLD`: Compression size threshold (positive integer)
fn config_entry(key: &str, value: &StorageValue) -> Result<ConfigEntry, String> {
    match key.to_uppercase().as_str() {
        "MAXCAP" => match value {
            StorageValue::Int(n) => {
                if *n < 0 {
                    return Err("MAXCAP value less than 0".to_string());
                }
                Ok(ConfigEntry::MaxCapacity(*n as u64))
            }
            _ => Err("Invalid MAXCAP value".to_string()),
        },

        "GLOBALTTL" => match value {
            StorageValue::Int(n) => {
                if *n < 0 {
                    return Err("GLOBALTTL value less than 0".to_string());
                }
                Ok(ConfigEntry::GlobalTtl(*n as u64))
            }
            _ => Err("Invalid GLOBALTTL value".to_string()),
        },

        "EVICTPOLICY" => match value {
            StorageValue::Text(t) => match t.to_uppercase().as_str() {
                "OLDEST" => Ok(ConfigEntry::EvictPolicy(EvictionPolicy::Oldest)),
                "LFU" => Ok(ConfigEntry::EvictPolicy(EvictionPolicy::LFU)),
                "LRU" => Ok(ConfigEntry::EvictPolicy(EvictionPolicy::LRU)),
                "SIZEAWARE" => Ok(ConfigEntry::EvictPolicy(EvictionPolicy::SizeAware)),
                _ => Err("Invalid EVICTPOLICY value".to_string()),
            },
            _ => Err("Invalid EVICTPOLICY value".to_string()),
        },

        "COMPRESSION" => match value {
            StorageValue::Text(txt) => match txt.to_uppercase().as_str() {
                "ENABLE" => Ok(ConfigEntry::Compression(Compression::Enabled)),
                "DISABLE" => Ok(ConfigEntry::Compression(Compression::Disabled)),
                _ => Err("Invalid COMPRESSION value".to_string()),
            },
            _ => Err("Invalid COMPRESSION value".to_string()),
        },

        "COMPTHRESHOLD" => match value {
            StorageValue::Int(n) => Ok(ConfigEntry::CompressionThreshold(*n as usize)),
            _ => Err("Invalid COMPRESSION THRESHOLD value".to_string()),
        },

        _ => Err("Invalid CONFSET key".to_string()),
    }
}

/// Main request processing function that routes RESP3 requests to appropriate handlers.
/// This is the entry point for all client requests after RESP3 parsing.
///
/// The client sends requests in three distinct ways:
/// 1. **Single commands** -> a BulkString
///    - `HELLO` -> Handshake command  
///    - `CONFOPTIONS` -> Get all configuration
///
/// 2. **Normal commands** -> an Array of BulkStrings
///    - `[SET, key, value]`
///    - `[GET, key]`
///
/// 3. **List commands** -> A nested Array of Arrays and BulkStrings
///    - `[SETLIST, [key, [value, value]]]`
///    - `[GETLIST, [key, key, key, ..]]`
///    - `[DELETELIST, [key, key, key]]`
///
/// 4. **Maps and JSON** -> An Array of BulkString and RequestType::Maps
///    - `[SETMAP, {key: value, key: value}]`
///
/// # Arguments
/// * `req` - The parsed RESP3 request
/// * `storage` - Thread-safe reference to the storage engine
///
/// # Returns
/// `Vec<u8>` - RESP3-encoded response bytes
pub fn process_request(
    req: &RequestType,
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    match req {
        // Handle single command strings (no arguments)
        RequestType::BulkString { data } => {
            let cmd = String::from_utf8_lossy(data).to_string();
            match cmd.to_uppercase().as_str() {
                // Client handshake - Redis compatibility
                // New RESP connections should begin with HELLO
                "HELLO" => bulk_string_response(Some("HELLO")),

                // Performance monitoring commands
                "GETSTATS" => {
                    let stats = storage.read().get_stats();
                    bulk_string_response(Some(&stats.to_string()))
                }
                "RESETSTATS" => {
                    storage.write().reset_stats();
                    bulk_string_response(Some("SUCCESS"))
                }

                // Configuration management
                "CONFOPTIONS" => {
                    let options = storage.read().get_options();
                    bulk_string_response(Some(&options.to_string()))
                }

                "CONFRESET" => {
                    storage.write().reset_options();
                    bulk_string_response(Some("SUCCESS"))
                }

                // Administrative commands
                "FLUSH" => {
                    storage.write().flush();
                    bulk_string_response(Some("SUCCESS"))
                }

                // List all keys in the cache
                "KEYS" => {
                    let keys = storage.read().get_keys();
                    if keys.is_empty() {
                        return null_response();
                    }
                    array_response(&keys)
                }

                other => bulk_error_response(&format!("Unknown single command: {other}!")),
            }
        }

        // Handle command arrays (commands with arguments)
        RequestType::Array { children } => process_array(children, storage),

        // All other request types are invalid
        _ => bulk_error_response("Unsupported format of commands!"),
    }
}

/// Extracts the command type from a RESP3 request.
/// Used to route array-based requests to the correct handler.
///
/// # Arguments
/// * `req_type` - The first element of a command array
///
/// # Returns
/// The corresponding `Command` enum value
fn get_command(req_type: &RequestType) -> Command {
    match req_type {
        RequestType::BulkString { data } => {
            let cmd = String::from_utf8_lossy(data).to_string();
            match cmd.to_uppercase().as_str() {
                // Basic CRUD operations
                "GET" => Command::Get,
                "SET" => Command::Set,
                "DELETE" => Command::Delete,
                "EXISTS" => Command::Exists,

                // Configuration management
                "CONFGET" => Command::ConfGet,
                "CONFSET" => Command::ConfSet,

                // Administrative and debugging
                "DUMP" => Command::Dump,

                // TTL management
                "GETTTL" => Command::GetTtl,
                "EXPIRE" => Command::Expire,
                "SETWTTL" => Command::SetwTtl,

                // Batch operations
                "DELETELIST" => Command::DeleteList,
                "GETLIST" => Command::GetList,
                "SETLIST" => Command::SetList,
                "SETMAP" => Command::SetMap,

                // Arithmetic operations
                "INCR" => Command::Incr,
                "DECR" => Command::Decr,

                // Key management
                "RENAME" => Command::Rename,
                "EVICTNOW" => Command::EvictNow,

                _ => Command::Unknown,
            }
        }
        _ => Command::Unknown,
    }
}

/// Handles GET command: retrieves a value by key.
/// Format: `GET key`
///
/// # Arguments
/// * `children` - Command arguments (should contain the key)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: bulk string with value or null if not found
///
/// # Example
/// Input: `*2\r\n$3\r\nGET\r\n$4\r\nname\r\n`
/// Output: `$4\r\nJohn\r\n` or `$-1\r\n` (null)
fn handle_get_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return bulk_error_response("Command missing some arguments");
    }

    match &children[0] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            let entry = storage.read().get_entry(&key);

            // respond with the entry's `value` field
            match entry {
                Some(e) => bulk_string_response(Some(&e.value.to_string())),
                None => null_response(),
            }
        }
        _ => bulk_error_response("Invalid request type for GET key"),
    }
}

/// Handles EXISTS command: checks if a key exists.
/// Format: `EXISTS key`
///
/// # Arguments
/// * `children` - Command arguments (should contain the key)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 boolean response: #t or #f
fn handle_exists_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return bulk_error_response("Command missing some arguments");
    }

    match &children[0] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            let exists = storage.read().key_exists(&key);
            boolean_response(exists)
        }
        _ => bulk_error_response("Invalid request type for EXISTS key"),
    }
}

/// Handles SET command: stores a key-value pair.
/// Format: `SET key value`
/// Uses automatic type detection for the value.
///
/// # Arguments
/// * `children` - Command arguments (key and value)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" or error message
///
/// # Example
/// Input: `*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nJohn\r\n`
/// Output: `$7\r\nSUCCESS\r\n`
fn handle_set_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.len() < 2 {
        return bulk_error_response("Command missing some arguments");
    }

    let mut i = 0;
    match &children[i] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            i += 1;
            match &children[i] {
                RequestType::BulkString { data } => {
                    let entry_value = String::from_utf8_lossy(data).to_string();
                    let entry_value = get_value_type(&entry_value); // Auto-detect type
                    match storage.write().insert_entry(key, entry_value) {
                        Ok(()) => bulk_string_response(Some("SUCCESS")),
                        Err(e) => bulk_error_response(&e),
                    }
                }
                _ => bulk_error_response("Invalid request type for SET value"),
            }
        }
        _ => bulk_error_response("Invalid request type for SET key"),
    }
}

/// Handles DELETE command: removes a key from the cache.
/// Format: `DELETE key`
///
/// # Arguments
/// * `children` - Command arguments (should contain the key)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" (always succeeds, even if key doesn't exist)
fn handle_delete_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return bulk_error_response("Command missing some arguments");
    }

    match &children[0] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            storage.write().remove_entry(&key);
            bulk_string_response(Some("SUCCESS"))
        }
        _ => bulk_error_response("Invalid request type for DELETE key"),
    }
}

/// Handles DUMP command: returns detailed information about an entry.
/// Format: `DUMP key`
/// Shows all metadata including TTL, access count, size, etc.
///
/// # Arguments
/// * `children` - Command arguments (should contain the key)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: detailed entry information or null if not found
///
/// # Example Output
/// "Value: John, Created_at: SystemTime, Last accessed: SystemTime, Access Count: 5, Entry size: 128, Ttl: 3600, Compressed: false"
fn handle_dump_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return bulk_error_response("Command missing some arguments");
    }

    match &children[0] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            let entry = storage.read().get_entry(&key);

            match entry {
                Some(e) => bulk_string_response(Some(&e.to_string())),
                None => null_response(),
            }
        }
        _ => bulk_error_response("Invalid request type for Dump key"),
    }
}

/// Handles CONFGET command: retrieves a configuration setting.
/// Format: `CONFGET key`
///
/// # Arguments
/// * `children` - Command arguments (should contain the config key)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: configuration value or null if key doesn't exist
///
/// # Supported Keys
/// - EVICTPOLICY, MAXCAP, GLOBALTTL, COMPRESSION, COMPRESSIONTHRESHOLD
fn handle_confget_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return bulk_error_response("Command missing some arguments");
    }

    match &children[0] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            let entry = storage.read().get_config_entry(&key);

            match entry {
                Some(e) => bulk_string_response(Some(&e.to_string())),
                None => null_response(),
            }
        }
        _ => bulk_error_response("Invalid request type for CONFGET key"),
    }
}

/// Handles CONFSET command: updates a configuration setting.
/// Format: `CONFSET key value`
/// Changes take effect immediately without requiring a restart.
///
/// # Arguments
/// * `children` - Command arguments (config key and new value)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" or detailed error message
///
/// # Example
/// Input: `*3\r\n$7\r\nCONFSET\r\n$6\r\nMAXCAP\r\n$6\r\n500000\r\n`
/// Output: `$7\r\nSUCCESS\r\n`
fn handle_confset_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.len() < 2 {
        return bulk_error_response("Command missing some arguments");
    }

    let mut i = 0;
    match &children[i] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            i += 1;
            match &children[i] {
                RequestType::BulkString { data } => {
                    let entry_value = String::from_utf8_lossy(data).to_string();
                    let entry_value = get_value_type(&entry_value);
                    let conf_entry = match config_entry(&key, &entry_value) {
                        Ok(e) => e,
                        Err(e) => return bulk_error_response(&e),
                    };
                    storage.write().set_config_entry(&conf_entry);

                    bulk_string_response(Some("SUCCESS"))
                }
                _ => bulk_error_response("Invalid request type for SET value"),
            }
        }
        _ => bulk_error_response("Invalid request type for SET key"),
    }
}

/// Handles GETTTL command: retrieves the remaining TTL for a key.
/// Format: `GETTTL key`
///
/// # Arguments
/// * `children` - Command arguments (should contain the key)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 integer response: TTL in seconds, or null if key doesn't exist
fn handle_getttl_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return bulk_error_response("Command missing some arguments");
    }

    match &children[0] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            let ttl = storage.read().time_to_live(&key);
            match ttl {
                Some(ttl) => integer_response(ttl.as_secs() as i64),
                None => null_response(),
            }
        }
        _ => bulk_error_response("Invalid request type for GETTTL key"),
    }
}

/// Handles SETWTTL command: stores a key-value pair with specific TTL.
/// Format: `SETWTTL key value ttl_seconds`
///
/// # Arguments
/// * `children` - Command arguments (key, value, and TTL)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" or error message
///
/// # Example
/// Input: `*4\r\n$8\r\nSETWTTL\r\n$7\r\nsession\r\n$10\r\nsession123\r\n:3600\r\n`
/// Sets "session" = "session123" with 1 hour TTL
fn handle_setwttl_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.len() < 3 {
        return bulk_error_response("Command missing some arguments");
    }

    let mut i = 0;
    match &children[i] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            i += 1;
            match &children[i] {
                RequestType::BulkString { data } => {
                    let entry_value = String::from_utf8_lossy(data).to_string();
                    i += 1;
                    match &children[i] {
                        RequestType::Integer { data } => {
                            let entry_value = get_value_type(&entry_value);
                            let ttl = String::from_utf8_lossy(data).to_string();
                            let ttl = get_value_type(&ttl);
                            let ttl = match ttl {
                                StorageValue::Int(n) => Duration::from_secs(n as u64),
                                _ => {
                                    return bulk_error_response("Invalid SETWTTL ttl");
                                }
                            };
                            match storage.write().insert_with_ttl(key, entry_value, ttl) {
                                Ok(()) => bulk_string_response(Some("SUCCESS")),
                                Err(e) => bulk_error_response(&e),
                            }
                        }
                        _ => bulk_error_response("Invalid SETWTTL ttl"),
                    }
                }
                _ => bulk_error_response("Invalid request type for SETWTTL value"),
            }
        }
        _ => bulk_error_response("Invalid request type for SETWTTL key"),
    }
}

/// Handles EXPIRE command: extends or reduces TTL of an existing key.
/// Format: `EXPIRE key seconds_to_add`
/// Positive values extend TTL, negative values reduce it.
///
/// # Arguments
/// * `children` - Command arguments (key and TTL modification)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" or error message
///
/// # Example
/// `EXPIRE session 1800` adds 30 minutes to the session TTL
/// `EXPIRE session -600` removes 10 minutes from the session TTL
fn handle_expire_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.len() < 2 {
        return bulk_error_response("Command missing some arguments");
    }

    let mut i = 0;
    match &children[i] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data).to_string();
            i += 1;
            match &children[i] {
                RequestType::Integer { data } => {
                    let addition_ttl = String::from_utf8_lossy(data).to_string();
                    let addition_ttl = match get_value_type(&addition_ttl) {
                        StorageValue::Int(n) => n,
                        _ => {
                            return bulk_error_response("Invalid EXPIRE addition_tll");
                        }
                    };

                    match storage.write().extend_ttl(&key, addition_ttl) {
                        Ok(()) => bulk_string_response(Some("SUCCESS")),
                        Err(e) => bulk_error_response(&e),
                    };

                    bulk_string_response(Some("SUCCESS"))
                }
                _ => bulk_error_response("Invalid request type for EXPIRE value"),
            }
        }
        _ => bulk_error_response("Invalid request type for EXPIRE key"),
    }
}

/// Handles DELETELIST command: removes multiple keys in one operation.
/// Format: `DELETELIST key1 key2 key3 ...`
/// More efficient than individual DELETE commands.
///
/// # Arguments
/// * `children` - Command arguments (list of keys to delete)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" (always succeeds)
fn handle_deletelist_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return null_response();
    }

    let mut keys = Vec::new();
    for child in children {
        match child {
            RequestType::BulkString { data } => {
                let key = String::from_utf8_lossy(data).to_string();
                keys.push(key);
            }
            _ => continue, // Skip invalid key types
        }
    }

    storage.write().remove_entries(&keys);
    bulk_string_response(Some("SUCCESS"))
}

/// Handles GETLIST command: retrieves multiple keys in one operation.
/// Format: `GETLIST key1 key2 key3 ...`
/// Returns an array of [key, value] pairs, with null for missing keys.
///
/// # Arguments
/// * `children` - Command arguments (list of keys to retrieve)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 array response: [[key1, value1], [key2, null], [key3, value3]]
fn handle_getlist_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return null_response();
    }

    let mut keys = Vec::new();
    for child in children {
        // skip invalid types
        if let RequestType::BulkString { data } = child {
            let key = String::from_utf8_lossy(data).to_string();
            keys.push(key);
        }
    }

    let entries = storage.read().get_entries(&keys);
    // Entries -> Vec<(String, Option<StorageEntry>)>
    // Response -> [[key, value|null], [key, value|null]]
    batch_entries_response(&entries)
}

/// Converts a RESP3 request type to our internal StorageValue format.
/// This handles the translation between the protocol layer and storage layer.
///
/// # Arguments
/// * `req` - The RESP3 request type to convert
///
/// # Returns
/// `Result<StorageValue, String>` - The converted value or an error message
///
/// # Example
/// Input: `*3\r\n$7\r\nSETLIST\r\n$5\r\nitems\r\n*3\r\n$5\r\napple\r\n$6\r\nbanana\r\n$6\r\norange\r\n`
/// Stores: "items" -> ["apple", "banana", "orange"]
fn handle_setlist_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return null_response();
    }

    let mut key = String::new();
    let mut vals = Vec::new();
    let mut errors = Vec::new();

    for child in children {
        // Expected format: [key, [vals..]]
        match child {
            RequestType::Array { children } => {
                // Array of values: [val, val, val, ..]
                for c in children {
                    match request_type_to_storage_value(c) {
                        Ok(v) => vals.push(v),
                        Err(e) => errors.push(e),
                    }
                }
            }
            RequestType::BulkString { data } => {
                key = String::from_utf8_lossy(data).to_string();
            }
            _ => {
                return bulk_error_response("Not an array list");
            }
        }
    }

    // Store as a list value
    let vals = StorageValue::List(vals);
    if let Err(err) = storage.write().insert_entry(key, vals) {
        return bulk_error_response(&err);
    }

    bulk_string_response(Some("SUCCESS"))
}

/// Handles SETMAP command: stores multiple key-value pairs from a map.
/// Format: `SETMAP {key1: value1, key2: value2, ...}`
/// Similar to Redis MSET but takes a map structure.
///
/// # Arguments
/// * `children` - Command arguments (should contain a RESP3 Map)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" or error message
///
/// # Example
/// Input: `*2\r\n$6\r\nSETMAP\r\n%3\r\n$4\r\nname\r\n$4\r\nJohn\r\n$3\r\nage\r\n:25\r\n$4\r\ncity\r\n$7\r\nSeattle\r\n`
/// Stores: "name" -> "John", "age" -> 25, "city" -> "Seattle"
fn handle_setmap_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return null_response();
    }

    match &children[0] {
        RequestType::Map { children } => {
            let mut entries: HashMap<String, StorageValue> = HashMap::new();

            // Convert each map entry to storage format
            for (child_key, child_value) in children {
                let value = match child_value {
                    RequestType::BulkString { data } => {
                        StorageValue::Text(String::from_utf8_lossy(data).to_string())
                    }
                    RequestType::SimpleString { data } => {
                        StorageValue::Text(String::from_utf8_lossy(data).to_string())
                    }
                    RequestType::Integer { data } => {
                        let int_str = match String::from_utf8(data.to_vec()) {
                            Ok(s) => s,
                            Err(e) => return bulk_error_response(&e.to_string()),
                        };
                        let int = match int_str.parse::<i64>() {
                            Ok(i) => i,
                            Err(e) => return bulk_error_response(&e.to_string()),
                        };
                        StorageValue::Int(int)
                    }
                    RequestType::BigNumber { data } => {
                        let int = String::from_utf8_lossy(data).to_string();
                        let float = match int.parse::<f64>() {
                            Ok(f) => f,
                            Err(_) => {
                                return bulk_error_response("Invalid float value");
                            }
                        };
                        StorageValue::Float(float)
                    }
                    RequestType::Boolean { data } => StorageValue::Bool(*data),
                    _ => {
                        return bulk_error_response("Unsupported json value type");
                    }
                };

                entries.insert(child_key.clone(), value);
            }

            // Insert all entries as a batch operation
            if let Err(e) = storage.write().insert_entries(entries) {
                return bulk_error_response(&e);
            }
            bulk_string_response(Some("SUCCESS"))
        }
        _ => bulk_error_response("Unsuported format for SETMAP"),
    }
}

/// Handles INCR command: increments an integer value by 1.
/// Format: `INCR key`
/// Only works if the existing value is an integer.
///
/// # Arguments
/// * `children` - Command arguments (should contain the key)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" or error if key doesn't exist/isn't an integer
///
/// # Example
/// If "counter" = 5, then `INCR counter` makes it 6
fn handle_incr_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return bulk_error_response("Command missing some arguments");
    }

    match &children[0] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data);
            storage.write().increment_entry(&key);
            bulk_string_response(Some("SUCCESS"))
        }
        _ => bulk_error_response("Invalid INCR key type"),
    }
}

/// Handles DECR command: decrements an integer value by 1.
/// Format: `DECR key`
/// Only works if the existing value is an integer.
///
/// # Arguments
/// * `children` - Command arguments (should contain the key)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" or error if key doesn't exist/isn't an integer
///
/// # Example
/// If "counter" = 5, then `DECR counter` makes it 4
fn handle_decr_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return bulk_error_response("Command missing some arguments");
    }

    match &children[0] {
        RequestType::BulkString { data } => {
            let key = String::from_utf8_lossy(data);
            storage.write().decrement_entry(&key);
            bulk_string_response(Some("SUCCESS"))
        }
        _ => bulk_error_response("Invalid DECR key type"),
    }
}

/// Handles RENAME command: renames an existing key.
/// Format: `RENAME old_key new_key`
/// The old key is removed and the value is stored under the new key.
///
/// # Arguments
/// * `children` - Command arguments (old key and new key)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" (always succeeds, even if old key doesn't exist)
///
/// # Example
/// `RENAME user:123 customer:123` moves the value from user:123 to customer:123
fn handle_rename_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.len() < 2 {
        return bulk_error_response("Command missing some arguments");
    }

    let mut i = 0;
    match &children[i] {
        RequestType::BulkString { data } => {
            let old_key = String::from_utf8_lossy(data);
            i += 1;
            match &children[i] {
                RequestType::BulkString { data } => {
                    let new_key = String::from_utf8_lossy(data);

                    storage.write().rename_entry(&old_key, &new_key);
                    bulk_string_response(Some("SUCCESS"))
                }
                _ => bulk_error_response("Invalid RENAME new_key type"),
            }
        }
        _ => bulk_error_response("Invalid RENAME old_key type"),
    }
}

/// Handles EVICTNOW command: evicts entries based on the current eviction policy
/// This command has an optional integer value that represents the number of entries
/// to evict. The fallback value is 0, if the value is not specified.
/// Format: `EVICTNOW`
///         `EVICTNOW 10`
///
/// # Arguments
/// * `children` - Command arguments (count). Children can be empty
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3 response: "SUCCESS" (always succeeds)
///
fn handle_evictnow_command(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        storage.write().evict_entries(0);
        return bulk_string_response(Some("SUCCESS"));
    }

    match &children[0] {
        RequestType::Integer { data } => {
            let int_str = match String::from_utf8(data.to_vec()) {
                Ok(s) => s,
                Err(e) => return bulk_error_response(&e.to_string()),
            };
            let int = match int_str.parse::<usize>() {
                Ok(i) => i,
                Err(e) => return bulk_error_response(&e.to_string()),
            };
            storage.write().evict_entries(int);
            bulk_string_response(Some("SUCCESS"))
        }
        _ => bulk_error_response("Invalid count type for evictnow command"),
    }
}

/// Processes array-based commands by routing to appropriate handlers.
/// This is called for all commands that come as RESP3 arrays.
///
/// # Arguments
/// * `children` - Array of request elements (first is command, rest are args)
/// * `storage` - Storage engine reference
///
/// # Returns
/// RESP3-encoded response bytes
///
/// # Command Format
/// All commands follow the pattern: [COMMAND, arg1, arg2, ...]
/// - First element: Command name (BulkString)
/// - Remaining elements: Command arguments (various types)
fn process_array(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    let mut i = 0;
    let command = get_command(&children[i]); // Extract command from first element
    i += 1; // Skip to arguments

    // Route to appropriate command handler
    match command {
        Command::Unknown => null_response(),
        Command::Get => handle_get_command(&children[i..], storage),
        Command::Exists => handle_exists_command(&children[i..], storage),
        Command::Set => handle_set_command(&children[i..], storage),
        Command::Delete => handle_delete_command(&children[i..], storage),
        Command::Dump => handle_dump_command(&children[i..], storage),
        Command::ConfGet => handle_confget_command(&children[i..], storage),
        Command::ConfSet => handle_confset_command(&children[i..], storage),
        Command::GetTtl => handle_getttl_command(&children[i..], storage),
        Command::SetwTtl => handle_setwttl_command(&children[i..], storage),
        Command::Expire => handle_expire_command(&children[i..], storage),
        Command::DeleteList => handle_deletelist_command(&children[i..], storage),
        Command::GetList => handle_getlist_command(&children[i..], storage),
        Command::SetList => handle_setlist_command(&children[i..], storage),
        Command::SetMap => handle_setmap_command(&children[i..], storage),
        Command::Incr => handle_incr_command(&children[i..], storage),
        Command::Decr => handle_decr_command(&children[i..], storage),
        Command::Rename => handle_rename_command(&children[i..], storage),
        Command::EvictNow => handle_evictnow_command(&children[i..], storage),
    }
}
