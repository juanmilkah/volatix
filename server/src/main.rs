use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread,
};

use server_lib::{DataType, Request, Storage, parse_request};

fn null_array() -> Vec<u8> {
    b"*-1\r\n".to_vec()
}

// -Error message\r\n
fn simple_error(s: &str) -> Vec<u8> {
    let mut err = String::new();
    err.push('-');
    err.push_str(s);
    err.push_str("\r\n");

    err.as_bytes().to_vec()
}

fn bstring(s: Option<String>) -> Vec<u8> {
    match s {
        Some(rsp) => {
            let mut s = String::new();
            s.push('$');
            let len = rsp.len();
            s.push_str(&len.to_string());
            s.push_str("\r\n");
            s.push_str(&rsp);
            s.push_str("\r\n");

            s.as_bytes().to_vec()
        }
        None => {
            // null $-1\r\n
            b"$-1\r\n".to_vec()
        }
    }
}

fn get_data(req: &Request) -> Option<String> {
    match req.data_type {
        DataType::BulkStrings => req
            .content
            .as_ref()
            .map(|content| String::from_utf8_lossy(content).to_string()),
        _ => None,
    }
}

fn process_request(req: &[Request], storage: &Arc<RwLock<Storage>>) -> Vec<u8> {
    if req.is_empty() {
        // return null array *-1\r\n
        return null_array();
    }

    match req[0].data_type {
        DataType::BulkStrings => {
            if let Some(data) = get_data(&req[0]) {
                match data.as_str() {
                    "GET" => {
                        if req.len() < 2 {
                            return simple_error("Missing key");
                        }
                        if let Some(key) = get_data(&req[1]) {
                            let key = storage.read().unwrap().get_key(&key);
                            return bstring(key);
                        } else {
                            return bstring(None);
                        }
                    }
                    "SET" => {
                        if req.len() < 3 {
                            return simple_error("Missing values");
                        }

                        match get_data(&req[1]) {
                            Some(key) => match get_data(&req[2]) {
                                Some(value) => match storage.write() {
                                    Ok(mut w_guard) => {
                                        w_guard.insert_key(key, value);
                                        return bstring(Some("SUCCESS".to_string()));
                                    }
                                    Err(e) => return simple_error(&format!("ERROR: {e}")),
                                },
                                None => return simple_error("Missing value"),
                            },
                            None => return simple_error("Missing key"),
                        }
                    }
                    "DELETE" => {
                        if req.len() < 2 {
                            return simple_error("Invalid values");
                        }

                        match get_data(&req[1]) {
                            Some(key) => match storage.write() {
                                Ok(mut w_guard) => {
                                    w_guard.remove_key(&key);
                                    return bstring(Some("SUCCESS".to_string()));
                                }
                                Err(e) => return simple_error(&format!("ERROR: {e}")),
                            },
                            None => return simple_error("Missing key"),
                        }
                    }
                    cmd => return simple_error(&format!("Invalid Command: {cmd}")),
                }
            }
            bstring(None)
        }
        _ => simple_error("Invalid request Command"),
    }
}

fn handle_client(mut stream: TcpStream, storage: Arc<RwLock<Storage>>) {
    let mut buffer = [0; 512];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => match parse_request(&buffer[..n]) {
                Ok(req) => {
                    let response = process_request(&req, &storage);

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
    let addr = "127.0.0.1:7878";
    let listener = TcpListener::bind(addr)?;
    println!("Server listening on {addr}");

    let storage = Arc::new(RwLock::new(Storage::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let storage = Arc::clone(&storage);
                thread::spawn(move || handle_client(stream, storage));
            }
            Err(e) => eprintln!("ERROR: {e}"),
        }
    }

    Ok(())
}
