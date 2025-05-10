use std::io::{Read, Write, stdin, stdout};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};

use anyhow::Context;
use server_lib::parse_request;

#[derive(PartialEq, Debug, Eq)]
enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
    Help,
    ParseError(String),
    GetList { list: Vec<String> },
    SetList { list: Vec<String> },
    // Implement this later
    // SetList { list: Vec<(String, String)> },
    DeleteList { list: Vec<String> },
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
        _ => unreachable!(),
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
            Ok(l) => Command::SetList { list: l },
            Err(e) => Command::ParseError(e),
        },

        "DELETELIST" => match parse_list(&chars, &mut pointer) {
            Ok(keys) => Command::DeleteList { list: keys },
            Err(e) => Command::ParseError(e),
        },

        _ => Command::ParseError(format!("Unknown command: {cmd_str}")),
    }
}

// $<length>\r\n<data>\r\n
struct Bstring(String);

impl Bstring {
    fn new(s: &str) -> Self {
        let mut bstring = String::new();
        let terminator = "\r\n";

        bstring.push('$');
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

        arr.push('*');
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
            let key = Bstring::new(key);

            arr.push(cmd);
            arr.push(key);
            let arr = Array::new(&arr);

            arr.0.as_bytes().to_vec()
        }
        Command::Set { key, value } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("SET");
            let key = Bstring::new(key);
            let value = Bstring::new(value);

            arr.push(cmd);
            arr.push(key);
            arr.push(value);
            let arr = Array::new(&arr);

            arr.0.as_bytes().to_vec()
        }
        Command::Delete { key } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("DELETE");
            let key = Bstring::new(key);

            arr.push(cmd);
            arr.push(key);
            let arr = Array::new(&arr);

            arr.0.as_bytes().to_vec()
        }

        Command::DeleteList { list } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("DELETELIST");
            arr.push(cmd);
            for e in list {
                arr.push(Bstring::new(e));
            }

            let arr = Array::new(&arr);
            arr.0.as_bytes().to_vec()
        }

        Command::GetList { list } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("GETLIST");
            arr.push(cmd);
            for e in list {
                arr.push(Bstring::new(e));
            }

            let arr = Array::new(&arr);
            arr.0.as_bytes().to_vec()
        }

        Command::SetList { list } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("SETLIST");
            arr.push(cmd);
            for e in list {
                arr.push(Bstring::new(e));
            }

            let arr = Array::new(&arr);
            arr.0.as_bytes().to_vec()
        }

        _ => unreachable!(),
    }
}

fn deserialize_response(resp: &[u8]) -> anyhow::Result<Vec<String>> {
    let resp = parse_request(resp).context("deserialise response")?;
    if resp.is_empty() {
        return Ok(Vec::new());
    }

    Ok(resp
        .iter()
        .map(|r| {
            r.content
                .as_ref()
                .map(|c| String::from_utf8_lossy(c).to_string())
        })
        .map(|c| c.unwrap_or("NULL".to_string()))
        .collect())
}

fn help() {
    println!("USAGE: ");
    println!("  SET key value");
    println!("  GET key");
    println!("  DELETE key");
}

fn main() -> anyhow::Result<()> {
    println!("VOLATIX CLI!!");
    println!("If stuck try `HELP` and `EXIT`");

    let stdin = stdin();
    let mut line = String::new();
    // 1MB
    let mut buffer = [0u8; 1024 * 1024];
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 7878));
    let mut stream = TcpStream::connect(addr).context("connect to server")?;
    let mut stdout = stdout();

    loop {
        line.clear();
        print!("> ");
        let _ = stdout.flush();
        if let Err(e) = stdin.read_line(&mut line) {
            eprintln!("ERROR: {e}");
        }

        let line = line.trim();
        let line = line.trim_end_matches('\n');
        if line.is_empty() {
            continue;
        }

        if line == "exit" {
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
                stream.write_all(&req).context("write to stream")?;
            }
        }

        let read = stream.read(&mut buffer)?;
        // The server replies with a RESP type.
        // The reply's type is determined by the command's implementation and
        // possibly by the client's protocol version.
        let resp = deserialize_response(&buffer[..read])?;
        if resp.is_empty() {
            let resp = "NULL";
            println!("{resp}");
        } else {
            println!("{resp:?}");
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
}
