use crate::parse::Command;

// $<length>\r\n<data>\r\n
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

pub fn serialize_request(command: &Command) -> Vec<u8> {
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

        // command [key,  [value, value, ..]]
        Command::SetList { key, list } => {
            let mut vals = Vec::new();
            for v in list {
                vals.push(bstring(v));
            }

            let inner_array = vec![bstring(key), array(&vals)];
            let inner_array = array(&inner_array);

            let arr = vec![bstring("SETLIST"), inner_array];

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
