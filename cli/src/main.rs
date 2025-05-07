const ADDRESS: &str = "127.0.0.1:7878";

use std::io::{Read, Write, stdin};
use std::net::TcpStream;

use anyhow::Context;

#[derive(Debug, PartialEq)]
enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
    Help,
    ParseError(String),
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

    let parse_arg = |pointer: &mut usize, arg_name: &str| -> Result<String, String> {
        while *pointer < l && chars[*pointer].is_whitespace() {
            *pointer += 1;
        }

        if *pointer >= l {
            return Err(format!("Missing {}", arg_name));
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
                return Err(format!("Unclosed quote for {}", arg_name));
            }
        } else {
            if *pointer >= l {
                return Err(format!("Missing {}", arg_name));
            }

            while *pointer < l && chars[*pointer].is_whitespace() {
                *pointer += 1;
            }

            while *pointer < l && !chars[*pointer].is_whitespace() {
                arg_chars.push(chars[*pointer]);
                *pointer += 1;
            }

            if arg_chars.is_empty() {
                return Err(format!("Missing {} after whitespace", arg_name));
            }
        }
        Ok(String::from_iter(arg_chars))
    };

    match cmd_str.to_uppercase().as_str() {
        "GET" => match parse_arg(&mut pointer, "key") {
            Ok(key) => Command::Get { key },
            Err(e) => Command::ParseError(format!("GET: {}", e)),
        },

        "SET" => match parse_arg(&mut pointer, "key") {
            Ok(key) => match parse_arg(&mut pointer, "value") {
                Ok(value) => Command::Set { key, value },
                Err(e) => Command::ParseError(format!("SET: {}", e)),
            },
            Err(e) => Command::ParseError(format!("SET: {}", e)),
        },

        "DELETE" => match parse_arg(&mut pointer, "key") {
            Ok(key) => Command::Delete { key },
            Err(e) => Command::ParseError(format!("DELETE: {}", e)),
        },

        "HELP" => Command::Help,

        _ => Command::ParseError(format!("Unknown command: {}", cmd_str)),
    }
}

// $<length>\r\n<data>\r\n
struct Bstring(String);

impl Bstring {
    fn new(s: &str) -> Self {
        let mut bstring = String::new();
        let terminator = "\r\n";

        bstring.push_str("$");
        bstring.push_str(&s.len().to_string());
        bstring.push_str(terminator);
        bstring.push_str(s);
        bstring.push_str(terminator);

        Bstring(bstring)
    }
}

// *<number-of-elements>\r\n<element-1>...<element-n>
struct Array(String);

impl Array {
    fn new(elems: &[Bstring]) -> Self {
        let mut arr = String::new();
        let terminator = "\r\n";

        arr.push_str("*");
        arr.push_str(&elems.len().to_string());
        arr.push_str(terminator);

        for s in elems {
            arr.push_str(&s.0);
        }

        Array(arr)
    }
}

fn serialize_request(command: &Command) -> Vec<u8> {
    // Clients send commands to the server as an array of bulk strings.
    // The first (and sometimes also the second) bulk string in the array is
    // the command's name. Subsequent elements of the array are the arguments
    // for the command.
    match command {
        Command::Get { key } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("GET");
            let key = Bstring::new(&key);

            arr.push(cmd);
            arr.push(key);
            let arr = Array::new(&arr);

            return arr.0.as_bytes().to_vec();
        }
        Command::Set { key, value } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("SET");
            let key = Bstring::new(&key);
            let value = Bstring::new(&value);

            arr.push(cmd);
            arr.push(key);
            arr.push(value);
            let arr = Array::new(&arr);

            return arr.0.as_bytes().to_vec();
        }
        Command::Delete { key } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("DELETE");
            let key = Bstring::new(&key);

            arr.push(cmd);
            arr.push(key);
            let arr = Array::new(&arr);

            return arr.0.as_bytes().to_vec();
        }
        _ => unreachable!(),
    }
}

fn help() {
    println!("USAGE: ");
    println!("  SET key value");
    println!("  GET key");
    println!("  DELETE key");
}

fn main() -> anyhow::Result<()> {
    println!("VOLATIX CLI!!");
    println!("If stuck try `HELP`");

    let stdin = stdin();
    let mut line = String::new();
    let mut buffer = [0u8; 1024];
    let mut stream = TcpStream::connect(ADDRESS).context("connect to server")?;

    loop {
        line.clear();
        if let Err(e) = stdin.read_line(&mut line) {
            eprintln!("ERROR: {}", e);
        }

        let line = line.trim();
        let line = line.trim_end_matches('\n');
        if line.is_empty() {
            continue;
        }

        let command = parse_line(&line);
        match command {
            Command::Help => {
                help();
                continue;
            }
            Command::ParseError(err) => {
                eprintln!("ERROR: {}", err);
                continue;
            }
            _ => {
                let req = serialize_request(&command);
                stream.write_all(&req).context("write to stream")?;
            }
        }

        let read = stream.read(&mut buffer)?;
        // The server replies with a RESP type.
        // The reply's type is determined by the command's implementation and
        // possibly by the client's protocol version.
        println!("RECEIVED: {:?}", &buffer[..read]);
    }
}

#[cfg(test)]
mod tests {
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
}
