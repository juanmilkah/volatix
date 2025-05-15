use std::{
    collections::HashMap,
    env::args,
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    sync::Arc,
    time::Duration,
};

use server_lib::{
    ConfigEntry, EvictionPolicy, RequestType, Storage, StorageOptions, StorageValue, ThreadPool,
    batch_entries_response, boolean_response, bulk_error_response, bulk_string_response,
    integer_response, null_response, parse_request,
};

enum Command {
    Get,
    Exists,
    Set,
    Delete,
    SetList,
    GetList,
    DeleteList,
    ConfSet,
    ConfGet,
    ExtendTtl,
    SetwTtl,
    GetTtl,
    EntryStats,
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
    match key {
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

        _ => Err("Invalid CONFSET key".to_string()),
    }
}

// The client sends requests in three distinct ways
// For single commands -> a BulkString
//    HELLO  -> Handshake command
//    CONFOPTIONS
// For normal commands -> an Array of RequestTypes
//    [SET key value]
//    [GET key]
// For list commands   -> An nested Array of RequestsTypes
//    [SETLIST [key,value], [key, value]]
//    [GETLIST [key, key, key, ..]]
//    [DELETELIST [key, key, key]]
fn process_request(req: &RequestType, storage: Arc<parking_lot::RwLock<Storage>>) -> Vec<u8> {
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
                _ => unreachable!(),
            }
        }

        RequestType::Array { children } => {
            if children.is_empty() {
                return null_response();
            }
            let mut i = 0;

            let command = match &children[i] {
                RequestType::BulkString { data } => {
                    let cmd = String::from_utf8_lossy(data).to_string();
                    match cmd.to_uppercase().as_str() {
                        "GET" => Command::Get,
                        "SET" => Command::Set,
                        "DELETE" => Command::Delete,
                        "EXISTS" => Command::Exists,
                        "CONFGET" => Command::ConfGet,
                        "CONFSET" => Command::ConfSet,
                        "ENTRYSTATS" => Command::EntryStats,
                        "GETTTL" => Command::GetTtl,
                        "EXTENDTTL" => Command::ExtendTtl,
                        "SETWTTL" => Command::SetwTtl,
                        "DELETELIST" => Command::DeleteList,
                        "GETLIST" => Command::GetList,
                        "SETLIST" => Command::SetList,
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            };

            match command {
                Command::Get => {
                    if children.len() < 2 {
                        return bulk_error_response("Command missing some arguments");
                    }

                    i += 1;
                    match &children[i] {
                        RequestType::BulkString { data } => {
                            let key = String::from_utf8_lossy(data).to_string();
                            let entry = storage.write().get_entry(&key);

                            match entry {
                                Some(e) => bulk_string_response(Some(&e.value.to_string())),
                                None => null_response(),
                            }
                        }
                        _ => bulk_error_response("Invalid request type for GET key"),
                    }
                }
                Command::Exists => {
                    if children.len() < 2 {
                        return bulk_error_response("Command missing some arguments");
                    }

                    i += 1;
                    match &children[i] {
                        RequestType::BulkString { data } => {
                            let key = String::from_utf8_lossy(data).to_string();
                            let exists = storage.write().key_exists(&key);
                            boolean_response(exists)
                        }
                        _ => bulk_error_response("Invalid request type for EXISTS key"),
                    }
                }
                Command::Set => {
                    if children.len() < 3 {
                        return bulk_error_response("Command missing some arguments");
                    }
                    i += 1;
                    match &children[i] {
                        RequestType::BulkString { data } => {
                            let key = String::from_utf8_lossy(data).to_string();
                            i += 1;
                            match &children[i] {
                                RequestType::BulkString { data } => {
                                    let entry_value = String::from_utf8_lossy(data).to_string();
                                    let entry_value = get_value_type(&entry_value);
                                    storage.write().insert_entry(key, entry_value);

                                    bulk_string_response(Some("SUCCESS"))
                                }
                                _ => bulk_error_response("Invalid request type for SET value"),
                            }
                        }
                        _ => bulk_error_response("Invalid request type for SET key"),
                    }
                }
                Command::Delete => {
                    if children.len() < 2 {
                        return bulk_error_response("Command missing some arguments");
                    }

                    i += 1;
                    match &children[i] {
                        RequestType::BulkString { data } => {
                            let key = String::from_utf8_lossy(data).to_string();
                            storage.write().remove_entry(&key);
                            bulk_string_response(Some("SUCCESS"))
                        }
                        _ => bulk_error_response("Invalid request type for DELETE key"),
                    }
                }

                Command::EntryStats => {
                    if children.len() < 2 {
                        return bulk_error_response("Command missing some arguments");
                    }

                    i += 1;
                    match &children[i] {
                        RequestType::BulkString { data } => {
                            let key = String::from_utf8_lossy(data).to_string();
                            let entry = storage.write().get_entry(&key);

                            match entry {
                                Some(e) => bulk_string_response(Some(&e.to_string())),
                                None => null_response(),
                            }
                        }
                        _ => bulk_error_response("Invalid request type for ENTRYSTATS key"),
                    }
                }

                Command::ConfGet => {
                    if children.len() < 2 {
                        return bulk_error_response("Command missing some arguments");
                    }

                    i += 1;
                    match &children[i] {
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

                Command::ConfSet => {
                    if children.len() < 3 {
                        return bulk_error_response("Command missing some arguments");
                    }
                    i += 1;
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

                Command::GetTtl => {
                    if children.len() < 2 {
                        return bulk_error_response("Command missing some arguments");
                    }

                    i += 1;
                    match &children[i] {
                        RequestType::BulkString { data } => {
                            let key = String::from_utf8_lossy(data).to_string();
                            let ttl = storage.write().time_to_live(&key);
                            match ttl {
                                Some(ttl) => integer_response(ttl.as_secs() as i64),
                                None => null_response(),
                            }
                        }
                        _ => bulk_error_response("Invalid request type for GETTTL key"),
                    }
                }

                Command::SetwTtl => {
                    if children.len() < 4 {
                        return bulk_error_response("Command missing some arguments");
                    }
                    i += 1;
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
                                                StorageValue::Int(n) => {
                                                    Duration::from_secs(n as u64)
                                                }
                                                _ => {
                                                    return bulk_error_response(
                                                        "Invalid SETWTTL ttl",
                                                    );
                                                }
                                            };
                                            storage.write().insert_with_ttl(key, entry_value, ttl);

                                            bulk_string_response(Some("SUCCESS"))
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

                Command::ExtendTtl => {
                    if children.len() < 3 {
                        return bulk_error_response("Command missing some arguments");
                    }
                    i += 1;
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
                                            return bulk_error_response(
                                                "Invalid EXTENDTTL addition_tll",
                                            );
                                        }
                                    };

                                    match storage.write().extend_ttl(&key, addition_ttl) {
                                        Ok(()) => bulk_string_response(Some("SUCCESS")),
                                        Err(e) => bulk_error_response(&e),
                                    };

                                    bulk_string_response(Some("SUCCESS"))
                                }
                                _ => {
                                    bulk_error_response("Invalid request type for EXTENDTTL value")
                                }
                            }
                        }
                        _ => bulk_error_response("Invalid request type for EXTENDTTL key"),
                    }
                }

                Command::DeleteList => {
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

                Command::GetList => {
                    if children.is_empty() {
                        return null_response();
                    }
                    i += 1;

                    let mut keys = Vec::new();
                    for child in &children[i..] {
                        match child {
                            RequestType::BulkString { data } => {
                                let key = String::from_utf8_lossy(data).to_string();
                                keys.push(key);
                            }
                            _ => continue,
                        }
                    }

                    let entries = storage.write().get_entries(&keys);
                    // Entries -> Vec<(String, Option<StorageEntry>)>
                    // Response -> [[key, value|null], [key, value|null]]
                    batch_entries_response(&entries)
                }

                // [SETLIST  [key, value], [key, value]]
                Command::SetList => {
                    if children.is_empty() {
                        return null_response();
                    }
                    let mut entries = HashMap::with_capacity(children.len());
                    i += 1;
                    for child in &children[i..] {
                        /* [key, value] */
                        match child {
                            RequestType::Array { children } => {
                                for child in children {
                                    match child {
                                        RequestType::Array { children } => {
                                            if children.len() > 2 || children.is_empty() {
                                                return bulk_error_response(
                                                    "Invalid array elems count",
                                                );
                                            }
                                            let key = match &children[0] {
                                                RequestType::BulkString { data } => {
                                                    String::from_utf8_lossy(data).to_string()
                                                }
                                                _ => {
                                                    return bulk_error_response(
                                                        "Invalid SETLIST key entry",
                                                    );
                                                }
                                            };
                                            let value = match &children[1] {
                                                RequestType::BulkString { data } => {
                                                    String::from_utf8_lossy(data).to_string()
                                                }
                                                _ => {
                                                    return bulk_error_response(
                                                        "Invalid SETLIST value entry",
                                                    );
                                                }
                                            };

                                            let storage_value = get_value_type(&value);
                                            entries.insert(key, storage_value);
                                        }
                                        _ => return bulk_error_response("Not an array list"),
                                    }
                                }
                            }
                            _ => return bulk_error_response("Invalid SETLIST args"),
                        }
                    }

                    storage.write().insert_entries(entries);

                    bulk_string_response(Some("SUCCESS"))
                }
            }
        }
        _ => unreachable!("later"),
    }
}

fn handle_client(mut stream: TcpStream, storage: Arc<parking_lot::RwLock<Storage>>) {
    // 1MB
    let mut buffer = [0; 1024 * 1024];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => match parse_request(&buffer[..n]) {
                Ok(req) => {
                    let storage = Arc::clone(&storage);
                    let response = process_request(&req, storage);

                    if let Err(e) = stream.write_all(&response) {
                        eprintln!("ERROR: {e}");
                    }
                }
                Err(err) => {
                    let err = format!("Error parsing request: {err}");
                    if let Err(e) = stream.write_all(err.as_bytes()) {
                        eprintln!("ERROR: {e}");
                    }
                }
            },
            Err(e) => eprintln!("ERROR: {e}"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let args: Vec<_> = args().collect();
    let mut thread_count = 4;

    for i in 1..args.len() {
        if args[i] == "--threads" && i + 1 < args.len() {
            thread_count = args[i + 1].parse().unwrap_or(4);
        }
    }

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 7878));
    let listener = TcpListener::bind(addr)?;
    println!("Server listening on {addr}");

    let options = StorageOptions::default();
    let storage = Arc::new(parking_lot::RwLock::new(Storage::new(options)));
    let persistent_path = "volatix.db";
    storage.write().load_from_disk(persistent_path)?;
    let pool = ThreadPool::new(thread_count); // 4 threads

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let storage = Arc::clone(&storage);
                pool.execute(|| handle_client(stream, storage));
            }
            Err(e) => eprintln!("ERROR: {e}"),
        }
    }

    // TODO: Figure this out
    // println!("Saving data to disk...");
    // storage.write().save_to_disk(persistent_path)?;
    // println!("Data saved successfully. Server shutdown complete.");

    Ok(())
}
