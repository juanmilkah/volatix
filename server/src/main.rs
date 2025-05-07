use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use server::parse_request;

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 512];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                let req = parse_request(&buffer[..n]);
                println!("{:#?}", req);

                if let Err(e) = stream.write_all(b"OK") {
                    eprintln!("ERROR: {}", e);
                }
            }
            Err(e) => eprintln!("ERROR: {}", e),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let addr = "127.0.0.1:7878";
    let listener = TcpListener::bind(addr)?;
    println!("Server listening on {}", addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || handle_client(stream));
            }
            Err(e) => eprintln!("ERROR: {}", e),
        }
    }

    Ok(())
}
