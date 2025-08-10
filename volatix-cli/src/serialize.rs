//! Serialization module
//!
//! Converts high-level `Command` values into the raw wire format expected
//! by the server (RESP/RESP3-style encoding).  
//!
//! Each `Command` is encoded as an array of bulk strings, integers, or maps,
//! with CRLF terminators, matching the protocol specification.

use crate::parse::Command;

/// Encode a string as a Bulk String.
///
/// Format:
/// ```text
/// $<length>\r\n<data>\r\n
/// ```
/// Example:  
/// `"hello"` → `"$5\r\nhello\r\n"`
fn bstring(s: &str) -> String {
    let mut b_string = String::new();
    let terminator = "\r\n";

    b_string.push('$');
    b_string.push_str(&s.len().to_string());
    b_string.push_str(terminator);
    b_string.push_str(s);
    b_string.push_str(terminator);
    b_string
}

/// Encode a signed integer as a RESP Integer.
///
/// Format:
/// ```text
/// :[<+|->]<value>\r\n
/// ```
/// Example:  
/// `42` → `":42\r\n"`
fn integer(n: i64) -> String {
    let mut s = String::new();
    s.push(':');
    s.push_str(&n.to_string());
    s.push_str("\r\n");
    s
}

/// Encode an array of RESP elements.
///
/// Format:
/// ```text
/// *<number-of-elements>\r\n<element-1>...<element-n>
/// ```
/// Example:
/// ```text
/// ["$3\r\nGET\r\n", "$3\r\nkey\r\n"]  
/// → "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
/// ```
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

/// Serialize a high-level `Command` into a raw byte vector
/// according to the server protocol.
///
/// The encoding rules:
/// - Commands are sent as an array of bulk strings.
/// - The first element is always the command name (uppercase).
/// - Remaining elements are arguments (strings, arrays, integers, maps).
///
/// # Returns
/// A `Vec<u8>` containing the CRLF-terminated request in RESP format.
pub fn serialize_request(command: &Command) -> Vec<u8> {
    match command {
        Command::Flush => bstring("FLUSH").as_bytes().to_vec(),

        Command::EvictNow => bstring("EVICTNOW").as_bytes().to_vec(),

        Command::Hello => bstring("HELLO").as_bytes().to_vec(),

        Command::Get { key } => {
            let v = [bstring("GET"), bstring(key)];
            array(&v).as_bytes().to_vec()
        }

        Command::Exists { key } => {
            let v = [bstring("EXISTS"), bstring(key)];
            array(&v).as_bytes().to_vec()
        }

        Command::Set { key, value } => {
            let v = [bstring("SET"), bstring(key), bstring(value)];
            array(&v).as_bytes().to_vec()
        }

        Command::Delete { key } => {
            let arr = vec![bstring("DELETE"), bstring(key)];
            array(&arr).as_bytes().to_vec()
        }

        Command::DeleteList { list } => {
            let mut arr = vec![bstring("DELETELIST")];
            for e in list {
                arr.push(bstring(e));
            }
            array(&arr).as_bytes().to_vec()
        }

        Command::GetList { list } => {
            let mut arr = vec![bstring("GETLIST")];
            for e in list {
                arr.push(bstring(e));
            }
            array(&arr).as_bytes().to_vec()
        }

        Command::SetList { key, list } => {
            let vals: Vec<String> = list.iter().map(|v| bstring(v)).collect();
            let inner_array = array(&[bstring(key), array(&vals)]);
            array(&[bstring("SETLIST"), inner_array])
                .as_bytes()
                .to_vec()
        }

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
                arr_str.push_str(&bstring(key));
                arr_str.push_str(&bstring(value));
            }

            arr_str.as_bytes().to_vec()
        }

        Command::GetStats => bstring("GETSTATS").as_bytes().to_vec(),

        Command::ResetStats => bstring("RESETSTATS").as_bytes().to_vec(),

        Command::ConfGet { key } => {
            let v = [bstring("CONFGET"), bstring(key)];
            array(&v).as_bytes().to_vec()
        }

        Command::ConfSet { key, value } => {
            let v = [bstring("CONFSET"), bstring(key), bstring(value)];
            array(&v).as_bytes().to_vec()
        }

        Command::SetwTtl { key, value, ttl } => {
            // Manually encode due to mixed Bulk String + Integer sequence
            let mut arr = String::new();
            arr.push('*');
            arr.push_str(&4.to_string());
            arr.push_str("\r\n");
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
            array(&v).as_bytes().to_vec()
        }

        Command::Dump { key } => {
            let v = [bstring("DUMP"), bstring(key)];
            array(&v).as_bytes().to_vec()
        }

        Command::ConfOptions => bstring("CONFOPTIONS").as_bytes().to_vec(),

        Command::Incr { key } => {
            let v = [bstring("INCR"), bstring(key)];
            array(&v).as_bytes().to_vec()
        }

        Command::Decr { key } => {
            let v = [bstring("DECR"), bstring(key)];
            array(&v).as_bytes().to_vec()
        }

        Command::Rename { old_key, new_key } => {
            let v = [bstring("RENAME"), bstring(old_key), bstring(new_key)];
            array(&v).as_bytes().to_vec()
        }

        Command::Keys => bstring("KEYS").as_bytes().to_vec(),

        _ => Vec::new(),
    }
}
