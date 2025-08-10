mod deserialize;
mod parse;
mod serialize;
mod usage;

use std::io::{Read, Write, stdin, stdout};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};

use deserialize::{Response, deserialize_response};
use parse::{Command, parse_line};
use serialize::serialize_request;
use usage::help;

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

fn format_response(resp: &Response) -> String {
    match resp {
        Response::SimpleString { data } => data.to_string(),
        Response::SimpleError { data } => data.to_string(),
        Response::BigNumber { data } => data.to_string(),
        Response::Integer { data } => data.to_string(),
        Response::Boolean { data } => data.to_string(),
        Response::Null => "NULL".into(),
        Response::Array { data } => {
            let elements: Vec<String> = data.iter().map(format_response).collect();
            format!("[{}]", elements.join(", "))
        }
    }
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
            Response::Array { data: _ } => println!("{}", format_response(&resp)),
        }
    }

    Ok(())
}
