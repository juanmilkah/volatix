mod deserialize;
mod parse;
mod serialize;
mod usage;

use std::io::{Read, Write, stdin, stdout};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};

use deserialize::{Response, deserialize_response};
use libvolatix::ascii_art;
use parse::{Command, parse_line};
use serialize::serialize_request;
use usage::help;

/// Performs initial handshake with the Volatix server
/// Sends a HELLO command and expects a HELLO response to establish connection
///
/// # Arguments
/// * `stream` - Reference to the TCP stream connected to the server
///
/// # Returns
/// * `Ok(())` if handshake successful
/// * `Err(String)` if handshake fails with error message
fn handshake(mut stream: &TcpStream) -> Result<(), String> {
    // Serialize and send HELLO command to initiate handshake
    let cmd = serialize_request(&Command::Hello);
    match stream.write_all(&cmd) {
        Ok(_) => (),
        Err(e) => return Err(e.to_string()),
    }

    // FIX: How limited should this be?
    // Read server response (limited buffer since HELLO response is small)
    let mut buffer = [0u8; 14 * 1024];
    match stream.read(&mut buffer) {
        Ok(_) => (),
        Err(e) => return Err(e.to_string()),
    }

    // Deserialize server response using RESP protocol
    let res = match deserialize_response(&buffer) {
        Ok(r) => r,
        Err(e) => return Err(e.to_string()),
    };

    // Verify that server responded with expected HELLO confirmation
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

/// Formats a Response enum for human-readable display
/// Handles all response types including nested arrays
///
/// # Arguments
/// * `resp` - Reference to the Response to format
///
/// # Returns
/// * Formatted string representation of the response
fn format_response(resp: &Response) -> String {
    match resp {
        Response::SimpleString { data } => data.to_string(),
        Response::SimpleError { data } => data.to_string(),
        Response::BigNumber { data } => data.to_string(),
        Response::Integer { data } => data.to_string(),
        Response::Boolean { data } => data.to_string(),
        Response::Null => "NULL".into(),
        // Recursively format array elements
        Response::Array { data } => {
            let elements: Vec<String> = data.iter().map(format_response).collect();
            format!("[{}]", elements.join(", "))
        }
    }
}

/// Main application entry point
/// Establishes connection, runs REPL, and handles user interaction
fn main() -> Result<(), String> {
    // Define server address (localhost:7878)
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 7878));

    // Attempt to establish TCP connection to Volatix server
    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            eprintln!("Failed to connect to Volatix server! Is it running?");
            return Ok(()); // Not a fatal error, just exit gracefully
        }
    };

    // Display welcome message and usage hints
    println!("{art}", art = ascii_art());
    println!("If stuck try `HELP`");
    println!("To end session `QUIT` or `EXIT`");
    println!("lowercase is fine too :)");

    // Initialize REPL components
    let stdin = stdin();
    let mut line = String::new();
    let mut buffer = [0u8; 1024 * 1024]; // 1MB buffer for server responses
    let mut stdout = stdout();

    // Perform handshake before entering REPL loop
    handshake(&stream)?;

    // Main REPL (Read-Eval-Print Loop)
    // BUG: Implement history
    // Support arrow keys movement :) take control of the terminal
    loop {
        line.clear(); // Clear previous input

        // Display prompt and flush output to ensure it appears
        let prompt = "volatix> ";
        print!("{prompt}");
        let _ = stdout.flush();

        // Read user input line
        if let Err(e) = stdin.read_line(&mut line) {
            eprintln!("ERROR: {e}");
            continue;
        }

        // Clean up input (remove whitespace and newlines)
        let line = line.trim();
        let line = line.trim_end_matches('\n');

        // Skip empty lines
        if line.is_empty() {
            continue;
        }

        // Handle session termination commands
        if line == "exit" || line == "quit" {
            break;
        }

        // Parse user input into a Command object
        let command = parse_line(line);
        match command {
            // Handle help command locally (no server communication needed)
            Command::Help => {
                help();
                continue;
            }
            // Display parse errors to user
            Command::ParseError(err) => {
                eprintln!("ERROR: {err}");
                // FIX: Put previous line onto stdin to allow for corrections
                continue;
            }
            // For all other commands, serialize and send to server
            _ => {
                let req = serialize_request(&command);

                // If serialization fails, show help
                if req.is_empty() {
                    help();
                    continue;
                }

                // Send serialized command to server
                if stream.write_all(&req).is_err() {
                    eprintln!("Failed writing to tcp stream");
                    continue;
                }
            }
        }

        // Read server response
        let read = match stream.read(&mut buffer) {
            Ok(n) => n,
            Err(e) => {
                eprintln!("{e}");
                continue;
            }
        };

        // Deserialize server response using RESP protocol
        // The server replies with a RESP type determined by the command's implementation
        // and possibly by the client's protocol version
        let resp = deserialize_response(&buffer[..read])?;

        // Display formatted response to user
        match resp {
            Response::SimpleString { data } => println!("{data}"),
            Response::SimpleError { data } => println!("{data}"),
            Response::Integer { data } => println!("{data}"),
            Response::BigNumber { data } => println!("{data}"),
            Response::Null => println!("NULL"),
            Response::Boolean { data } => println!("{data}"),
            // Use the formatting function for complex arrays
            Response::Array { data: _ } => println!("{}", format_response(&resp)),
        }
    }

    Ok(())
}
