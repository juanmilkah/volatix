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
        RequestType::SimpleString { data }
        | RequestType::SimpleError { data }
        | RequestType::BulkString { data }
        | RequestType::BulkError { data }
        | RequestType::BigNumber { data } => {
            let text = String::from_utf8_lossy(data).to_string();
            Ok(StorageValue::Text(text))
        }
        RequestType::Null => Ok(StorageValue::Text("Null".to_string())),

        RequestType::Integer { data } => {
            let int_string = String::from_utf8_lossy(data).to_string();
            let int = int_string.parse::<i64>().map_err(|err| err.to_string())?;
            Ok(StorageValue::Int(int))
        }
        RequestType::Boolean { data } => Ok(StorageValue::Bool(*data)),
        RequestType::Double { data } => {
            let d_string = String::from_utf8_lossy(data).to_string();
            let double = d_string.parse::<f64>().map_err(|err| err.to_string())?;
            Ok(StorageValue::Float(double))
        }
        RequestType::Array { children } => {
            let elems = children
                .iter()
                .flat_map(request_type_to_storage_value)
                .collect();
            Ok(StorageValue::List(elems))
        }
        RequestType::Set { children } => {
            let elems = children
                .iter()
                .map(|c| {
                    let s = String::from_utf8_lossy(c);
                    get_value_type(&s)
                })
                .collect();
            Ok(StorageValue::List(elems))
        }
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
        _ => Err("Unsupported storage value".to_string()),
    }
}

pub enum Command {
    Get,
    Exists,
    Set,
    Delete,
    SetList,
    GetList,
    DeleteList,
    ConfSet,
    ConfGet,
    Expire,
    SetwTtl,
    GetTtl,
    Dump,
    SetMap,
    Incr,
    Decr,
    Rename,
    Unknown,
}

fn get_value_type(value: &str) -> StorageValue {
    // use brute force
    if let Ok(n) = value.parse::<i64>() {
        StorageValue::Int(n)
    } else if let Ok(n) = value.parse::<f64>() {
        StorageValue::Float(n)
    } else if value.to_lowercase().as_str() == "false" {
        StorageValue::Bool(false)
    } else if value.to_lowercase().as_str() == "true" {
        StorageValue::Bool(true)
    } else {
        StorageValue::Text(value.to_string())
    }
}

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

//
//
// The client sends requests in three distinct ways
// For single commands -> a BulkString
//    HELLO  -> Handshake command
//    CONFOPTIONS
// For normal commands -> an Array of RequestTypes::BulkString
//    [SET key value]
//    [GET key]
// For list commands   -> An nested Array of RequestsTypes::Array && BulkString
//    [SETLIST [key, [value, value]]]
//    [GETLIST [key, key, key, ..]]
//    [DELETELIST [key, key, key]]
// For maps and json  -> An Array of RequestTypes::Bulkstring && RequestType::Maps
//    [SETMAP  {key, value, key, value}]
pub fn process_request(
    req: &RequestType,
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    match req {
        RequestType::BulkString { data } => {
            let cmd = String::from_utf8_lossy(data).to_string();
            match cmd.to_uppercase().as_str() {
                // Client Handshake
                // New RESP connections should begin the
                // session by calling the HELLO command.
                "HELLO" => bulk_string_response(Some("HELLO")),
                "GETSTATS" => {
                    let stats = storage.read().get_stats();
                    bulk_string_response(Some(&stats.to_string()))
                }
                "RESETSTATS" => {
                    storage.write().reset_stats();

                    bulk_string_response(Some("SUCCESS"))
                }
                "CONFOPTIONS" => {
                    let options = storage.read().get_options();

                    bulk_string_response(Some(&options.to_string()))
                }

                "FLUSH" => {
                    storage.write().flush();

                    bulk_string_response(Some("SUCCESS"))
                }

                "EVICTNOW" => {
                    storage.write().evict_entries();

                    bulk_string_response(Some("SUCCESS"))
                }

                "KEYS" => {
                    let keys = storage.read().get_keys();
                    if keys.is_empty() {
                        return null_response();
                    }
                    array_response(&keys)
                }
                _ => null_response(),
            }
        }

        RequestType::Array { children } => process_array(children, storage),
        _ => null_response(),
    }
}

fn get_command(req_type: &RequestType) -> Command {
    match req_type {
        RequestType::BulkString { data } => {
            let cmd = String::from_utf8_lossy(data).to_string();
            match cmd.to_uppercase().as_str() {
                "GET" => Command::Get,
                "SET" => Command::Set,
                "DELETE" => Command::Delete,
                "EXISTS" => Command::Exists,
                "CONFGET" => Command::ConfGet,
                "CONFSET" => Command::ConfSet,
                "DUMP" => Command::Dump,
                "GETTTL" => Command::GetTtl,
                "EXPIRE" => Command::Expire,
                "SETWTTL" => Command::SetwTtl,
                "DELETELIST" => Command::DeleteList,
                "GETLIST" => Command::GetList,
                "SETLIST" => Command::SetList,
                "SETMAP" => Command::SetMap,
                "INCR" => Command::Incr,
                "DECR" => Command::Decr,
                "RENAME" => Command::Rename,
                _ => Command::Unknown,
            }
        }
        _ => Command::Unknown,
    }
}

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

            match entry {
                Some(e) => bulk_string_response(Some(&e.value.to_string())),
                None => null_response(),
            }
        }
        _ => bulk_error_response("Invalid request type for GET key"),
    }
}

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
                    let entry_value = get_value_type(&entry_value);
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
            _ => continue,
        }
    }

    storage.write().remove_entries(&keys);

    bulk_string_response(Some("SUCCESS"))
}

fn handle_getlist_command(
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
            _ => continue,
        }
    }

    let entries = storage.read().get_entries(&keys);
    // Entries -> Vec<(String, Option<StorageEntry>)>
    // Response -> [[key, value|null], [key, value|null]]
    batch_entries_response(&entries)
}

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
        // [key, [vals..]]
        match child {
            RequestType::Array { children } => {
                // [val, val, val, ..]
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

    let vals = StorageValue::List(vals);
    if let Err(err) = storage.write().insert_entry(key, vals) {
        return bulk_error_response(&err);
    }

    bulk_string_response(Some("SUCCESS"))
}

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

            if let Err(e) = storage.write().insert_entries(entries) {
                return bulk_error_response(&e);
            }
            bulk_string_response(Some("SUCCESS"))
        }
        _ => bulk_error_response("Unsuported format for SETMAP"),
    }
}

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

fn process_array(
    children: &[RequestType],
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) -> Vec<u8> {
    if children.is_empty() {
        return null_response();
    }
    let mut i = 0;
    let command = get_command(&children[i]);
    i += 1;

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
    }
}
