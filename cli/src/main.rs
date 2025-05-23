use std::collections::HashMap;
use std::fmt::Display;
use std::io::{Read, Write, stdin, stdout};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};

use server_lib::{RequestType, parse_request};

#[derive(PartialEq, Debug, Eq)]
enum Command {
    Hello,
    Get {
        key: String,
    },

    Exists {
        key: String,
    },

    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    Help,
    ParseError(String),
    GetList {
        list: Vec<String>,
    },
    SetList {
        list: Vec<String>,
    },
    SetMap {
        map: HashMap<String, String>,
    },
    DeleteList {
        list: Vec<String>,
    },
    GetStats,
    ResetStats,
    // if ttl is negative, the value should be expired on creation
    SetwTtl {
        key: String,
        value: String,
        ttl: u64,
    },

    // allow increment and decrement by using i64
    Expire {
        key: String,
        addition: i64,
    },
    GetTtl {
        key: String,
    },
    // set a config value like global ttl
    ConfSet {
        key: String,
        value: String,
    },
    ConfGet {
        key: String,
    },
    Dump {
        key: String,
    },

    ConfOptions,

    EvictNow,
    Flush,
    Incr {
        key: String,
    },
    Decr {
        key: String,
    },
    Rename {
        old_key: String,
        new_key: String,
    },
    Keys,
}

fn parse_arg(chars: &[char], pointer: &mut usize, arg_name: &str) -> Result<String, String> {
    let l = chars.len();
    while *pointer < l && chars[*pointer].is_whitespace() {
        *pointer += 1;
    }

    if *pointer >= l {
        return Err(format!("Missing {arg_name}"));
    }

    let delimiter = match chars[*pointer] {
        c @ ('"' | '\'') => {
            *pointer += 1;
            Some(c)
        }
        _ => None,
    };

    let mut arg_chars = Vec::new();
    if let Some(delim) = delimiter {
        while *pointer < l && chars[*pointer] != delim {
            arg_chars.push(chars[*pointer]);
            *pointer += 1;
        }

        if *pointer < l && chars[*pointer] == delim {
            *pointer += 1;
        } else {
            return Err(format!("Unclosed quote for {arg_name}"));
        }
    } else {
        if *pointer >= l {
            return Err(format!("Missing {arg_name}"));
        }

        while *pointer < l && chars[*pointer].is_whitespace() {
            *pointer += 1;
        }

        let delimeters = ['[', ']', '{', '}', ','];
        while *pointer < l
            && !chars[*pointer].is_whitespace()
            && !delimeters.contains(&chars[*pointer])
        {
            arg_chars.push(chars[*pointer]);
            *pointer += 1;
        }

        if arg_chars.is_empty() {
            return Err(format!("Missing {arg_name} after whitespace"));
        }
    }
    Ok(String::from_iter(arg_chars))
}

fn parse_list(chars: &[char], pointer: &mut usize) -> Result<Vec<String>, String> {
    let l = chars.len();
    let mut list = Vec::new();

    while *pointer < l && chars[*pointer].is_whitespace() {
        *pointer += 1;
    }

    // LIST {"foo", bar, "baz"}
    // LIST [foo, "bar", "baz"]
    let delimeter = if chars[*pointer] == '[' || chars[*pointer] == '{' {
        *pointer += 1;
        chars[*pointer - 1]
    } else {
        return Err("Missing list_start delimeter".to_string());
    };
    let end_delimeter = match delimeter {
        '[' => ']',
        '{' => '}',
        _ => return Err("invalid end delimeter".to_string()),
    };

    let separator = ',';

    while *pointer < l {
        while *pointer < l && chars[*pointer].is_whitespace() {
            *pointer += 1;
        }
        match parse_arg(chars, pointer, "list_entry") {
            Ok(entry) => list.push(entry),
            Err(e) => return Err(e),
        };

        while *pointer < l && chars[*pointer].is_whitespace() {
            *pointer += 1;
        }

        if chars[*pointer] == separator {
            *pointer += 1;
        }

        if chars[*pointer] == end_delimeter {
            break;
        }
    }

    Ok(list)
}

fn parse_map(data: &[char], pointer: &mut usize) -> Result<HashMap<String, String>, String> {
    let left_delim = '{';
    let right_delim = '}';

    let mut map: HashMap<String, String> = HashMap::new();

    while *pointer < data.len() && data[*pointer].is_whitespace() {
        *pointer += 1;
    }

    if *pointer < data.len() && data[*pointer] == left_delim {
        *pointer += 1;
    } else {
        return Err("Missing left brace".to_string());
    }

    while *pointer < data.len() && data[*pointer] != right_delim {
        while *pointer < data.len() && data[*pointer].is_whitespace() {
            *pointer += 1;
        }
        let key = parse_arg(data, pointer, "key")?;
        while *pointer < data.len() && data[*pointer].is_whitespace() {
            *pointer += 1;
        }

        if *pointer < data.len() && data[*pointer] == ':' {
            *pointer += 1;
        } else {
            return Err("Missing colon separator".to_string());
        }

        let value = parse_arg(data, pointer, "value")?;
        while *pointer < data.len() && data[*pointer].is_whitespace() {
            *pointer += 1;
        }

        map.insert(key, value);

        if *pointer < data.len() && data[*pointer] == ',' {
            *pointer += 1;
        } else {
            break;
        }
    }

    while *pointer < data.len() && data[*pointer].is_whitespace() {
        *pointer += 1;
    }
    if *pointer < data.len() && data[*pointer] == right_delim {
        *pointer += 1;
    } else {
        return Err("Missing right brace".to_string());
    }

    Ok(map)
}

fn parse_line(line: &str) -> Command {
    let chars: Vec<_> = line.trim().chars().collect();
    let mut pointer = 0;
    let l = chars.len();

    let mut cmd_vec = Vec::new();
    while pointer < l && !chars[pointer].is_whitespace() {
        cmd_vec.push(chars[pointer]);
        pointer += 1;
    }

    let cmd_str = String::from_iter(&cmd_vec);
    if cmd_str.is_empty() {
        if cmd_str.trim().is_empty() {
            // line was all spaces
            return Command::Help;
        }
        return Command::ParseError("Empty Command".to_string());
    }

    match cmd_str.to_uppercase().as_str() {
        "GET" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => Command::Get { key },
            Err(e) => Command::ParseError(format!("GET: {e}")),
        },

        "EXISTS" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => Command::Exists { key },
            Err(e) => Command::ParseError(format!("EXISTS: {e}")),
        },

        "SET" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => match parse_arg(&chars, &mut pointer, "value") {
                Ok(value) => Command::Set { key, value },
                Err(e) => Command::ParseError(format!("SET: {e}")),
            },
            Err(e) => Command::ParseError(format!("SET: {e}")),
        },

        "DELETE" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => Command::Delete { key },
            Err(e) => Command::ParseError(format!("DELETE: {e}")),
        },

        "HELP" => Command::Help,

        // GETLIST ["foo", "bar", "baz"]
        "GETLIST" => match parse_list(&chars, &mut pointer) {
            Ok(keys) => Command::GetList { list: keys },
            Err(e) => Command::ParseError(e),
        },

        // SETLIST ["foo", "bar", "foofoo", "barbar"]
        "SETLIST" => match parse_list(&chars, &mut pointer) {
            Ok(l) => {
                if l.len() % 2 != 0 {
                    Command::ParseError("Invalid number of args".to_string())
                } else {
                    Command::SetList { list: l }
                }
            }
            Err(e) => Command::ParseError(e),
        },

        "SETMAP" => match parse_map(&chars, &mut pointer) {
            Ok(map) => Command::SetMap { map },
            Err(e) => Command::ParseError(e),
        },

        "DELETELIST" => match parse_list(&chars, &mut pointer) {
            Ok(keys) => Command::DeleteList { list: keys },
            Err(e) => Command::ParseError(e),
        },

        "GETSTATS" => Command::GetStats,

        "RESETSTATS" => Command::ResetStats,

        "SETWTTL" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => match parse_arg(&chars, &mut pointer, "value") {
                Ok(value) => match parse_arg(&chars, &mut pointer, "ttl") {
                    Ok(ttl) => {
                        let ttl = match ttl.parse::<u64>() {
                            Ok(v) => v,
                            Err(e) => return Command::ParseError(format!("{e:?}")),
                        };
                        Command::SetwTtl { key, value, ttl }
                    }
                    Err(e) => Command::ParseError(e),
                },
                Err(e) => Command::ParseError(e),
            },
            Err(e) => Command::ParseError(e),
        },

        "EXPIRE" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => match parse_arg(&chars, &mut pointer, "ttl") {
                Ok(v) => {
                    let ttl = match v.parse::<i64>() {
                        Ok(v) => v,
                        Err(e) => return Command::ParseError(format!("ERROR: {e}")),
                    };
                    Command::Expire { key, addition: ttl }
                }
                Err(e) => Command::ParseError(e),
            },
            Err(e) => Command::ParseError(e),
        },

        "GETTTL" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => Command::GetTtl { key },
            Err(e) => Command::ParseError(format!("GET: {e}")),
        },

        // set a config value
        "CONFSET" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => match parse_arg(&chars, &mut pointer, "value") {
                Ok(value) => Command::ConfSet { key, value },
                Err(e) => Command::ParseError(e),
            },
            Err(e) => Command::ParseError(e),
        },

        // Get a config value
        "CONFGET" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => Command::ConfGet { key },
            Err(e) => Command::ParseError(e),
        },

        "DUMP" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => Command::Dump { key },
            Err(e) => Command::ParseError(e),
        },

        "CONFOPTIONS" => Command::ConfOptions,

        "EVICTNOW" => Command::EvictNow,

        "FLUSH" => Command::Flush,

        "INCR" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => Command::Incr { key },
            Err(e) => Command::ParseError(e),
        },

        "DECR" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => Command::Decr { key },
            Err(e) => Command::ParseError(e),
        },

        "RENAME" => match parse_arg(&chars, &mut pointer, "old_key") {
            Ok(old_key) => match parse_arg(&chars, &mut pointer, "new_key") {
                Ok(new_key) => Command::Rename { old_key, new_key },
                Err(e) => Command::ParseError(e),
            },
            Err(e) => Command::ParseError(e),
        },

        "KEYS" => Command::Keys,

        _ => Command::ParseError(format!("Unknown command: {cmd_str}")),
    }
}

// $<length>\r\n<data>\r\n
pub fn bstring(s: &str) -> String {
    let mut bstring = String::new();
    let terminator = "\r\n";

    bstring.push('$');
    bstring.push_str(&s.len().to_string());
    bstring.push_str(terminator);
    bstring.push_str(s);
    bstring.push_str(terminator);
    bstring
}

// This type is a CRLF-terminated string that represents a signed,
// base-10, 64-bit integer.
// :[<+|->]<value>\r\n
fn integer(n: i64) -> String {
    let mut s = String::new();
    s.push(':');
    s.push_str(&n.to_string());
    s.push_str("\r\n");
    s
}

// *<number-of-elements>\r\n<element-1>...<element-n>
fn array(elems: &[String]) -> String {
    let mut arr = String::new();
    let terminator = "\r\n";

    arr.push('*');
    arr.push_str(&elems.len().to_string());
    arr.push_str(terminator);

    for s in elems {
        arr.push_str(s);
    }
    arr
}

fn serialize_request(command: &Command) -> Vec<u8> {
    // Clients send commands to the server as an array of bulk strings.
    // The first (and sometimes also the second) bulk string in the array is
    // the command's name. Subsequent elements of the array are the arguments
    // for the command.
    match command {
        Command::Flush => bstring("FLUSH").as_bytes().to_vec(),

        Command::EvictNow => bstring("EVICTNOW").as_bytes().to_vec(),

        Command::Hello => bstring("HELLO").as_bytes().to_vec(),

        Command::Get { key } => {
            let v = [bstring("GET"), bstring(key)];
            let arr = array(&v);

            arr.as_bytes().to_vec()
        }

        Command::Exists { key } => {
            let v = [bstring("EXISTS"), bstring(key)];
            let arr = array(&v);

            arr.as_bytes().to_vec()
        }

        Command::Set { key, value } => {
            let v = [bstring("SET"), bstring(key), bstring(value)];
            let arr = array(&v);

            arr.as_bytes().to_vec()
        }
        Command::Delete { key } => {
            let mut arr = Vec::new();
            let cmd = bstring("DELETE");
            let key = bstring(key);

            arr.push(cmd);
            arr.push(key);
            let arr = array(&arr);

            arr.as_bytes().to_vec()
        }

        Command::DeleteList { list } => {
            let mut arr = Vec::new();
            let cmd = bstring("DELETELIST");
            arr.push(cmd);
            for e in list {
                arr.push(bstring(e));
            }

            let arr = array(&arr);
            arr.as_bytes().to_vec()
        }

        Command::GetList { list } => {
            let mut arr = Vec::new();
            let cmd = bstring("GETLIST");
            arr.push(cmd);
            for e in list {
                arr.push(bstring(e));
            }

            let arr = array(&arr);
            arr.as_bytes().to_vec()
        }

        // [command, [key, value], [key, value]]
        Command::SetList { list } => {
            let mut arr = vec![bstring("SETLIST")];
            let list: Vec<String> = list
                .iter()
                .map(|elem| bstring(elem))
                .collect::<Vec<String>>()
                .chunks(2)
                .map(|pair| array(&[pair[0].to_string(), pair[1].to_string()]))
                .collect();

            arr.extend_from_slice(&list);

            let arr = array(&arr);
            arr.as_bytes().to_vec()
        }

        // [SETMAP {MAP}]
        // %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
        Command::SetMap { map } => {
            let mut arr_str = String::new();
            arr_str.push('*');
            arr_str.push_str(&2.to_string());
            arr_str.push_str("\r\n");
            arr_str.push_str(&bstring("SETMAP"));

            arr_str.push('%');
            arr_str.push_str(&map.len().to_string());
            arr_str.push_str("\r\n");

            for (key, value) in map {
                let key = bstring(key);
                let value = bstring(value);
                arr_str.push_str(&key);
                arr_str.push_str(&value);
            }

            arr_str.as_bytes().to_vec()
        }

        Command::GetStats => {
            let cmd = bstring("GETSTATS");
            cmd.as_bytes().to_vec()
        }

        Command::ResetStats => {
            let cmd = bstring("RESETSTATS");
            cmd.as_bytes().to_vec()
        }

        Command::ConfGet { key } => {
            let v = [bstring("CONFGET"), bstring(key)];
            let arr = array(&v);

            arr.as_bytes().to_vec()
        }

        Command::ConfSet { key, value } => {
            let v = [bstring("CONFSET"), bstring(key), bstring(value)];
            let arr = array(&v);
            arr.as_bytes().to_vec()
        }

        Command::SetwTtl { key, value, ttl } => {
            // we'll have to handle this manually
            // The Integer messes things up
            let terminator = "\r\n";
            let mut arr = String::new();

            arr.push('*');
            arr.push_str(&4.to_string());
            arr.push_str(terminator);
            arr.push_str(&bstring("SETWTTL"));
            arr.push_str(&bstring(key));
            arr.push_str(&bstring(value));
            arr.push_str(&integer(*ttl as i64));

            arr.as_bytes().to_vec()
        }

        Command::Expire { key, addition } => {
            let mut arr = String::new();

            arr.push('*');
            arr.push_str(&3.to_string());
            arr.push_str("\r\n");
            arr.push_str(&bstring("EXPIRE"));
            arr.push_str(&bstring(key));
            arr.push_str(&integer(*addition));

            arr.as_bytes().to_vec()
        }

        Command::GetTtl { key } => {
            let v = [bstring("GETTTL"), bstring(key)];
            let arr = array(&v);

            arr.as_bytes().to_vec()
        }

        Command::Dump { key } => {
            let v = [bstring("DUMP"), bstring(key)];
            let arr = array(&v);
            arr.as_bytes().to_vec()
        }

        Command::ConfOptions => {
            let arr = bstring("CONFOPTIONS");
            arr.as_bytes().to_vec()
        }

        Command::Incr { key } => {
            let v = [bstring("INCR"), bstring(key)];
            let v = array(&v);
            v.as_bytes().to_vec()
        }

        Command::Decr { key } => {
            let v = [bstring("DECR"), bstring(key)];
            let v = array(&v);
            v.as_bytes().to_vec()
        }

        Command::Rename { old_key, new_key } => {
            let v = [bstring("RENAME"), bstring(old_key), bstring(new_key)];
            let v = array(&v);
            v.as_bytes().to_vec()
        }

        Command::Keys => bstring("KEYS").as_bytes().to_vec(),

        _ => Vec::new(),
    }
}

#[derive(Debug)]
enum Response {
    SimpleString { data: String },
    SimpleError { data: String },
    Boolean { data: bool },
    Integer { data: i64 },
    BigNumber { data: f64 },
    Null,
    Array { data: Vec<Response> },
}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SimpleString { data } => write!(f, "{data}"),
            Self::Integer { data } => write!(f, "{data}"),
            Self::Null => write!(f, "NULL"),
            Self::BigNumber { data } => write!(f, "{data}"),
            Self::Array { data } => {
                let mut arr = Vec::new();
                for child in data {
                    arr.push(child.to_string());
                }
                write!(f, "{arr:?}")
            }
            Self::SimpleError { data } => write!(f, "{data}"),
            Self::Boolean { data } => write!(f, "{data}"),
        }
    }
}

// SET foo bar
// GET foo -> "bar"
// GET bar -> NULL
// SETLIST [foo, foofoo, bar, barbar, baz, bazbaz]
// GETLIST [foo, bar,baz] -> [[foo, foofoo], [bar, barbar], [baz, bazbaz]]
fn deserialize_response(resp: &[u8]) -> Result<Response, String> {
    let resp = parse_request(resp)?;

    match &resp {
        RequestType::BulkString { data } => {
            let data = String::from_utf8_lossy(data).to_string();
            Ok(Response::SimpleString { data })
        }
        RequestType::Null => Ok(Response::Null),

        RequestType::Integer { data } => {
            let int = String::from_utf8_lossy(data).to_string();
            let int = match int.parse::<i64>() {
                Ok(i) => i,
                Err(e) => return Err(e.to_string()),
            };

            Ok(Response::Integer { data: int })
        }

        RequestType::BigNumber { data } => {
            let int = String::from_utf8_lossy(data).to_string();
            let float = match int.parse::<f64>() {
                Ok(i) => i,
                Err(e) => return Err(e.to_string()),
            };

            Ok(Response::BigNumber { data: float })
        }

        RequestType::BulkError { data } => Ok(Response::SimpleError {
            data: String::from_utf8_lossy(data).to_string(),
        }),

        RequestType::Boolean { data } => Ok(Response::Boolean { data: *data }),

        RequestType::Array { children } => {
            let mut outer_vec = Vec::new();
            for child in children {
                match child {
                    RequestType::BulkString { data } => {
                        let child = Response::SimpleString {
                            data: String::from_utf8_lossy(data).to_string(),
                        };
                        outer_vec.push(child);
                    }

                    RequestType::Null => outer_vec.push(Response::Null),

                    RequestType::Array { children } => {
                        let mut inner_vec = Vec::new();
                        for child in children {
                            match child {
                                RequestType::BulkString { data } => {
                                    let child = Response::SimpleString {
                                        data: String::from_utf8_lossy(data).to_string(),
                                    };
                                    inner_vec.push(child);
                                }
                                RequestType::Null => inner_vec.push(Response::Null),

                                RequestType::SimpleString { data } => {
                                    let child = Response::SimpleString {
                                        data: String::from_utf8_lossy(data).to_string(),
                                    };
                                    inner_vec.push(child);
                                }
                                _ => return Err("Unreachable".to_string()),
                            }
                        }
                        if !inner_vec.is_empty() {
                            outer_vec.push(Response::Array { data: inner_vec });
                        }
                    }

                    _ => return Err("unreachable!".to_string()),
                }
            }
            Ok(Response::Array { data: outer_vec })
        }
        _ => Err("Unexpected response type".to_string()),
    }
}

fn handshake(mut stream: &TcpStream) -> Result<(), String> {
    let cmd = serialize_request(&Command::Hello);
    match stream.write_all(&cmd) {
        Ok(_) => (),
        Err(e) => return Err(e.to_string()),
    }

    let mut buffer = [0u8; 64];
    match stream.read(&mut buffer) {
        Ok(_) => (),
        Err(e) => return Err(e.to_string()),
    }

    let res = match deserialize_response(&buffer) {
        Ok(r) => r,
        Err(e) => return Err(e.to_string()),
    };

    match res {
        Response::SimpleString { data } => {
            if data == "HELLO" {
                return Ok(());
            }

            Err("Invalid data for handshake".to_string())
        }
        _ => Err("Invalid response type for handshake".to_string()),
    }
}

fn help() {
    println!("USAGE:");
    println!();

    println!("  Basic Operations:");
    println!("    SET <key: string> <value: any>         # Set a single key-value pair");
    println!("    GET <key>                              # Get the value for a key");
    println!("    DELETE <key>                           # Delete a key");
    println!("    EXISTS <key>                           # Check if key exist");
    println!("    FLUSH                                  # Clear the database");
    println!("    INCR <key>                             # Increment an Int value by 1");
    println!("    DECR <key>                             # Decrement an Int value by 1");
    println!("    RENAME <old_key> <new_key>             # Rename key retaining the entry");
    println!();

    println!("  Batch Operations:");
    println!(
        "    SETLIST [key, value, ...]              # Set multiple key-value pairs (array syntax)"
    );
    println!(
        "    SETLIST {{key, value, ...}}              # Set multiple key-value pairs (map syntax)"
    );
    println!("    SETMAP {{\"key\": \"value\"}}                # Set a map of key-value pairs");
    println!("    GETLIST [key, key, ...]                # Get values for multiple keys");
    println!("    DELETELIST [key, key, ...]             # Delete multiple keys");
    println!();

    println!("  Configuration:");
    println!("    CONFOPTIONS                            # List configurable options");
    println!("      MAXCAP <u64>                         # Max entries in DB");
    println!("      GLOBALTTL <u64>                      # Default TTL for entries");
    println!("      COMPRESSION <enable|disable>         # Enable/disable compression");
    println!("      COMPTHRESHOLD <u64>                  # Size threshold for compression");
    println!("      EVICTPOLICY                          # Eviction policy:");
    println!("        RFU                                # Rarely frequently used");
    println!("        LFA                                # Least frequently accessed");
    println!("        OLDEST                             # Oldest entry first");
    println!("        SIZEAWARE                          # Evict largest first");
    println!("    CONFSET <key> <value>                  # Set a config value");
    println!("    CONFGET <key>                          # Get a config value");
    println!();

    println!("  Stats:");
    println!("    GETSTATS                               # Get global stats");
    println!("    RESETSTATS                             # Reset global stats");
    println!("    DUMP <key>                             # Get stats for a specific entry");
    println!();

    println!("  TTL (Time-to-Live):");
    println!("    SETWTTL <key> <ttl: u64>               # Set key with TTL (in seconds)");
    println!("    EXPIRE <key> <delta: i64>              # Extend or reduce TTL");
    println!("    GETTTL <key>                           # Get remaining TTL for key");
    println!(
        "    EVICTNOW <key>                         # Trigger Eviction using current eviction policy"
    );
    println!();
}

fn main() -> Result<(), String> {
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 7878));
    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            eprintln!("Failed to connect to Volatix server! Is it running?");
            return Ok(());
        }
    };

    println!("volatix cli repl");
    println!("If stuck try `HELP`");
    println!("To end session `QUIT` or `EXIT`");
    println!("lowercase is fine too :)");

    let stdin = stdin();
    let mut line = String::new();
    // 1MB
    let mut buffer = [0u8; 1024 * 1024];
    let mut stdout = stdout();
    match handshake(&stream) {
        Ok(()) => (),
        Err(e) => {
            return Err(e);
        }
    }

    loop {
        line.clear();
        let prompt = "volatix> ";
        print!("{prompt}");
        let _ = stdout.flush();
        if let Err(e) = stdin.read_line(&mut line) {
            eprintln!("ERROR: {e}");
        }

        let line = line.trim();
        let line = line.trim_end_matches('\n');
        if line.is_empty() {
            continue;
        }

        if line == "exit" || line == "quit" {
            break;
        }

        let command = parse_line(line);
        match command {
            Command::Help => {
                help();
                continue;
            }
            Command::ParseError(err) => {
                eprintln!("ERROR: {err}");
                continue;
            }
            _ => {
                let req = serialize_request(&command);
                if req.is_empty() {
                    help();
                    continue;
                }
                if stream.write_all(&req).is_err() {
                    eprintln!("Failed writing to tcp stream");
                    continue;
                }
            }
        }

        let read = match stream.read(&mut buffer) {
            Ok(n) => n,
            Err(e) => {
                eprintln!("{e}");
                continue;
            }
        };
        // The server replies with a RESP type.
        // The reply's type is determined by the command's implementation and
        // possibly by the client's protocol version.
        let resp = deserialize_response(&buffer[..read])?;
        match resp {
            Response::SimpleString { data } => println!("{data}"),
            Response::SimpleError { data } => println!("{data}"),
            Response::Integer { data } => println!("{data}"),
            Response::BigNumber { data } => println!("{data}"),
            Response::Null => println!("NULL"),
            Response::Boolean { data } => println!("{data}"),
            Response::Array { data } => {
                // do this the rookie way for now;;
                // fix it if you can :)
                println!("{data:?}");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod cli_parsing {
    use super::*;

    #[test]
    fn test_parse_get_ok() {
        assert_eq!(
            parse_line("GET mykey"),
            Command::Get {
                key: "mykey".to_string()
            }
        );
        assert_eq!(
            parse_line("get MyKey"),
            Command::Get {
                key: "MyKey".to_string()
            }
        );
        assert_eq!(
            parse_line("GET \"my key\""),
            Command::Get {
                key: "my key".to_string()
            }
        );
        assert_eq!(
            parse_line("GET 'another key'"),
            Command::Get {
                key: "another key".to_string()
            }
        );
        assert_eq!(
            parse_line("  GET  spaced_key  "),
            Command::Get {
                key: "spaced_key".to_string()
            }
        );
    }

    #[test]
    fn test_parse_get_error() {
        assert_eq!(
            parse_line("GET"),
            Command::ParseError("GET: Missing key".to_string())
        );
        assert_eq!(
            parse_line("GET "),
            Command::ParseError("GET: Missing key".to_string())
        );
        assert_eq!(
            parse_line("GET \"unclosed key"),
            Command::ParseError("GET: Unclosed quote for key".to_string())
        );
        assert_eq!(
            parse_line("GET \'unclosed key"),
            Command::ParseError("GET: Unclosed quote for key".to_string())
        );
    }

    #[test]
    fn test_parse_set_ok() {
        assert_eq!(
            parse_line("SET mykey myvalue"),
            Command::Set {
                key: "mykey".to_string(),
                value: "myvalue".to_string()
            }
        );
        assert_eq!(
            parse_line("set MyKey YourValue"),
            Command::Set {
                key: "MyKey".to_string(),
                value: "YourValue".to_string()
            }
        );
        assert_eq!(
            parse_line("SET \"my key\" \"my value\""),
            Command::Set {
                key: "my key".to_string(),
                value: "my value".to_string()
            }
        );
        assert_eq!(
            parse_line("SET 'key name' 'value content'"),
            Command::Set {
                key: "key name".to_string(),
                value: "value content".to_string()
            }
        );
        assert_eq!(
            parse_line("SET key1 \"value with spaces\""),
            Command::Set {
                key: "key1".to_string(),
                value: "value with spaces".to_string()
            }
        );
        assert_eq!(
            parse_line("SET \"quoted key\" unquoted_value"),
            Command::Set {
                key: "quoted key".to_string(),
                value: "unquoted_value".to_string()
            }
        );
        assert_eq!(
            parse_line("  SET  key1   value1  "),
            Command::Set {
                key: "key1".to_string(),
                value: "value1".to_string()
            }
        );
    }

    #[test]
    fn test_parse_set_error() {
        assert_eq!(
            parse_line("SET"),
            Command::ParseError("SET: Missing key".to_string())
        );
        assert_eq!(
            parse_line("SET keyonly"),
            Command::ParseError("SET: Missing value".to_string())
        );
        assert_eq!(
            parse_line("SET \"key"),
            Command::ParseError("SET: Unclosed quote for key".to_string())
        );
        assert_eq!(
            parse_line("SET key \"value"),
            Command::ParseError("SET: Unclosed quote for value".to_string())
        );
        assert_eq!(
            parse_line("SET \"key\" "),
            Command::ParseError("SET: Missing value".to_string())
        );
        assert_eq!(
            parse_line("SET \'key\' "),
            Command::ParseError("SET: Missing value".to_string())
        );
    }

    #[test]
    fn test_parse_delete_ok() {
        assert_eq!(
            parse_line("DELETE mykey"),
            Command::Delete {
                key: "mykey".to_string()
            }
        );
        assert_eq!(
            parse_line("DELETE \"my key\""),
            Command::Delete {
                key: "my key".to_string()
            }
        );
    }

    #[test]
    fn test_parse_delete_error() {
        assert_eq!(
            parse_line("DELETE"),
            Command::ParseError("DELETE: Missing key".to_string())
        );
        assert_eq!(
            parse_line("DELETE "),
            Command::ParseError("DELETE: Missing key".to_string())
        );
        assert_eq!(
            parse_line("DELETE \"unclosed"),
            Command::ParseError("DELETE: Unclosed quote for key".to_string())
        );
    }

    #[test]
    fn test_parse_help_and_others() {
        assert_eq!(parse_line("HELP"), Command::Help);
        assert_eq!(parse_line("help"), Command::Help);
        assert_eq!(parse_line(""), Command::Help); // Empty line is Help
        assert_eq!(parse_line("   "), Command::Help); // All whitespace is Help
        assert_eq!(
            parse_line("INVALIDCMD"),
            Command::ParseError("Unknown command: INVALIDCMD".to_string())
        );
        assert_eq!(
            parse_line(" GET"),
            Command::ParseError("GET: Missing key".to_string())
        );
    }

    #[test]
    fn test_empty_string_after_command_extraction() {
        // This test case is to ensure that if `line.chars()` is empty,
        // or if the line is `  `, it's handled.
        assert_eq!(parse_line(""), Command::Help);
        assert_eq!(parse_line(" "), Command::Help);
    }

    #[test]
    fn test_parse_lists() {
        let l: Vec<char> = "['foo', 'bar', 'baz']".chars().collect();
        let mut p = 0;
        let r = parse_list(&l, &mut p).unwrap();
        assert_eq!(r, vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn test_parse_curly_list() {
        let l: Vec<char> = "{'foo',   'bar',  'baz'}".chars().collect();
        let mut p = 0;
        let r = parse_list(&l, &mut p).unwrap();
        assert_eq!(r, vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn test_parse_unquoted_lists() {
        let l: Vec<char> = "[foo, bar, baz]".chars().collect();
        let mut p = 0;
        let r = parse_list(&l, &mut p).unwrap();
        assert_eq!(r, vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn test_parse_json() {
        let input = "{\"hello\" : \"world\"}";
        let data: Vec<_> = input.chars().collect();

        let map = parse_map(&data, &mut 0).unwrap();
        let expected = HashMap::from([("hello".to_string(), "world".to_string())]);
        assert_eq!(map, expected);

        let input2 = "{\"hello\": \"world\", \"foo\":\"bar\"}";
        let data: Vec<_> = input2.chars().collect();

        let map = parse_map(&data, &mut 0).unwrap();
        let expected = HashMap::from([
            ("hello".to_string(), "world".to_string()),
            ("foo".to_string(), "bar".to_string()),
        ]);

        assert_eq!(map, expected);
    }
}
