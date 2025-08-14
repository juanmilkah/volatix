use std::collections::HashMap;

/// Enum representing all supported Volatix database commands
/// Each variant contains the necessary parameters for the command
#[derive(PartialEq, Debug, Eq)]
pub enum Command {
    // Connection and utility commands
    Hello,              // Initial handshake command
    Help,               // Local help display (not sent to server)
    ParseError(String), // Error in command parsing
    Reconnect,          // Disconenct and reconnect to the server

    // Basic key-value operations
    Get {
        key: String,
    }, // Retrieve value for a key
    Set {
        key: String,
        value: String,
    }, // Store key-value pair
    Exists {
        key: String,
    }, // Check if key exists
    Delete {
        key: String,
    }, // Remove a key

    // Numeric operations
    Incr {
        key: String,
    }, // Increment numeric value by 1
    Decr {
        key: String,
    }, // Decrement numeric value by 1

    // Key management
    Rename {
        old_key: String,
        new_key: String,
    }, // Rename a key
    Keys,  // List all keys in database
    Flush, // Clear entire database

    // Batch operations for efficiency
    GetList {
        list: Vec<String>,
    }, // Get multiple keys at once
    SetList {
        key: String,
        list: Vec<String>,
    }, // Set array of values for a key
    SetMap {
        map: HashMap<String, String>,
    }, // Set multiple key-value pairs
    DeleteList {
        list: Vec<String>,
    }, // Delete multiple keys at once

    // Statistics and monitoring
    GetStats,   // Get global database statistics
    ResetStats, // Reset statistics counters
    Dump {
        key: String,
    }, // Get detailed stats for specific key

    // TTL (Time-to-Live) management
    SetwTtl {
        key: String,
        value: String,
        ttl: u64,
    }, // Set key with expiration time
    Expire {
        key: String,
        addition: i64,
    }, // Modify TTL (positive=extend, negative=reduce)
    GetTtl {
        key: String,
    }, // Get remaining TTL for key
    EvictNow, // Trigger immediate eviction using policy

    // Configuration management
    ConfSet {
        key: String,
        value: String,
    }, // Set configuration parameter
    ConfGet {
        key: String,
    }, // Get configuration parameter
    ConfOptions, // List all configurable options
}

/// Parses a single argument from the character stream
/// Handles quoted strings, whitespace, and various delimiters
///
/// # Arguments
/// * `chars` - Array of characters representing the input
/// * `pointer` - Mutable reference to current position in chars
/// * `arg_name` - Name of argument for error messages
///
/// # Returns
/// * `Ok(String)` - Successfully parsed argument
/// * `Err(String)` - Parse error with descriptive message
pub fn parse_arg(chars: &[char], pointer: &mut usize, arg_name: &str) -> Result<String, String> {
    let l = chars.len();

    // Skip leading whitespace
    while *pointer < l && chars[*pointer].is_whitespace() {
        *pointer += 1;
    }

    // Check if we have any characters left
    if *pointer >= l {
        return Err(format!("Missing {arg_name}"));
    }

    // Check for quote delimiters (single or double quotes)
    let delimiter = match chars[*pointer] {
        c @ ('"' | '\'') => {
            *pointer += 1; // Skip opening quote
            Some(c)
        }
        _ => None,
    };

    let mut arg_chars = Vec::new();

    // FIX: Handle escaped quote characters
    if let Some(delim) = delimiter {
        // Handle quoted string - read until matching quote
        while *pointer < l && chars[*pointer] != delim {
            arg_chars.push(chars[*pointer]);
            *pointer += 1;
        }

        // Check for closing quote
        if *pointer < l && chars[*pointer] == delim {
            *pointer += 1; // Skip closing quote
        } else {
            return Err(format!("Unclosed quote for {arg_name}"));
        }
    } else {
        // Handle unquoted string - read until whitespace or delimiter
        if *pointer >= l {
            return Err(format!("Missing {arg_name}"));
        }

        // Skip any remaining whitespace
        while *pointer < l && chars[*pointer].is_whitespace() {
            *pointer += 1;
        }

        // Characters that terminate an unquoted argument
        let delimeters = ['[', ']', '{', '}', ','];

        // Read characters until whitespace or delimiter
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

/// Parses a list/array structure from input
/// Supports both square brackets [item1, item2] and curly braces {item1, item2}
///
/// # Arguments
/// * `chars` - Array of characters representing the input
/// * `pointer` - Mutable reference to current position in chars
///
/// # Returns
/// * `Ok(Vec<String>)` - Successfully parsed list of items
/// * `Err(String)` - Parse error with descriptive message
pub fn parse_list(chars: &[char], pointer: &mut usize) -> Result<Vec<String>, String> {
    let l = chars.len();
    let mut list = Vec::new();

    // Skip leading whitespace
    while *pointer < l && chars[*pointer].is_whitespace() {
        *pointer += 1;
    }

    // Determine list delimiters - support both [] and {}
    // Examples: [foo, bar, "baz"] or {foo, bar, "baz"}
    let delimeter = if chars[*pointer] == '[' || chars[*pointer] == '{' {
        *pointer += 1; // Skip opening delimiter
        chars[*pointer - 1]
    } else {
        return Err("Missing list_start delimeter".to_string());
    };

    // Find matching closing delimiter
    let end_delimeter = match delimeter {
        '[' => ']',
        '{' => '}',
        _ => return Err("invalid end delimeter".to_string()),
    };

    let separator = ',';

    // Parse list items
    while *pointer < l {
        // Skip whitespace before item
        while *pointer < l && chars[*pointer].is_whitespace() {
            *pointer += 1;
        }

        // Parse individual list entry
        match parse_arg(chars, pointer, "list_entry") {
            Ok(entry) => list.push(entry),
            Err(e) => return Err(e),
        };

        // Skip whitespace after item
        while *pointer < l && chars[*pointer].is_whitespace() {
            *pointer += 1;
        }

        // Handle comma separator
        if chars[*pointer] == separator {
            *pointer += 1; // Skip comma
        }

        // Check for end of list
        if chars[*pointer] == end_delimeter {
            *pointer += 1;
            break;
        }
    }

    Ok(list)
}

/// Parses a JSON-like map structure from input
/// Supports format: {"key1": "value1", "key2": "value2"}
///
/// # Arguments
/// * `data` - Array of characters representing the input
/// * `pointer` - Mutable reference to current position in chars
///
/// # Returns
/// * `Ok(HashMap<String, String>)` - Successfully parsed key-value map
/// * `Err(String)` - Parse error with descriptive message
pub fn parse_map(data: &[char], pointer: &mut usize) -> Result<HashMap<String, String>, String> {
    let left_delim = '{';
    let right_delim = '}';
    let mut map: HashMap<String, String> = HashMap::new();

    // Skip leading whitespace
    while *pointer < data.len() && data[*pointer].is_whitespace() {
        *pointer += 1;
    }

    // Check for opening brace
    if *pointer < data.len() && data[*pointer] == left_delim {
        *pointer += 1; // Skip opening brace
    } else {
        return Err("Missing left brace".to_string());
    }

    // Parse key-value pairs until closing brace
    while *pointer < data.len() && data[*pointer] != right_delim {
        // Skip whitespace before key
        while *pointer < data.len() && data[*pointer].is_whitespace() {
            *pointer += 1;
        }

        // Parse key
        let key = parse_arg(data, pointer, "key")?;

        // Skip whitespace before colon
        while *pointer < data.len() && data[*pointer].is_whitespace() {
            *pointer += 1;
        }

        // Expect colon separator
        if *pointer < data.len() && data[*pointer] == ':' {
            *pointer += 1; // Skip colon
        } else {
            return Err("Missing colon separator".to_string());
        }

        // Parse value
        let value = parse_arg(data, pointer, "value")?;

        // Skip whitespace after value
        while *pointer < data.len() && data[*pointer].is_whitespace() {
            *pointer += 1;
        }

        // Store key-value pair
        map.insert(key, value);

        // Handle comma separator (optional for last item)
        if *pointer < data.len() && data[*pointer] == ',' {
            *pointer += 1; // Skip comma
        } else {
            break; // No comma means we're at the end
        }
    }

    // Skip whitespace before closing brace
    while *pointer < data.len() && data[*pointer].is_whitespace() {
        *pointer += 1;
    }

    // Check for closing brace
    if *pointer < data.len() && data[*pointer] == right_delim {
        *pointer += 1; // Skip closing brace
    } else {
        return Err("Missing right brace".to_string());
    }

    Ok(map)
}

/// Main parsing function - converts a line of text into a Command
/// Handles command identification, argument parsing, and error cases
///
/// # Arguments
/// * `line` - String containing the user input command
///
/// # Returns
/// * `Command` - Parsed command or ParseError with details
pub fn parse_line(line: &str) -> Command {
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

        // Cli-server connection management
        "RECONNECT" => Command::Reconnect,

        // GETLIST ["foo", "bar", "baz"]
        "GETLIST" => match parse_list(&chars, &mut pointer) {
            Ok(keys) => Command::GetList { list: keys },
            Err(e) => Command::ParseError(e),
        },

        // SETLIST names ["foo", "bar", "foofoo", "barbar"]
        "SETLIST" => match parse_arg(&chars, &mut pointer, "key") {
            Ok(key) => match parse_list(&chars, &mut pointer) {
                Ok(l) => {
                    if l.len() % 2 != 0 {
                        Command::ParseError("Invalid number of args".to_string())
                    } else {
                        Command::SetList { key, list: l }
                    }
                }
                Err(e) => Command::ParseError(e),
            },
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

    #[test]
    fn test_parse_setlist() {
        let line = "SETLIST names ['foo', 'bar']";
        assert_eq!(
            parse_line(line),
            Command::SetList {
                key: "names".to_string(),
                list: vec!["foo".to_string(), "bar".to_string()]
            }
        )
    }
}
