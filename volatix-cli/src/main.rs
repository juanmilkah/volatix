mod deserialize;
mod parse;
mod serialize;
mod usage;

use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};

use ::libvolatix::ascii_art;
use crossterm::{ExecutableCommand, QueueableCommand, cursor, event, terminal};
use deserialize::{Response, deserialize_response};
use parse::{Command, parse_line};
use serialize::serialize_request;
use usage::help;

const HISTORY_CAPACITY: usize = 100;

#[derive(Default, Debug)]
struct History {
    commands: Vec<String>,
    current_index: Option<usize>,
    start: usize,
    len: usize,
}

impl History {
    fn new() -> Self {
        Self {
            commands: vec![String::new(); HISTORY_CAPACITY],
            ..Default::default()
        }
    }

    fn push(&mut self, command: String) {
        if self.len > 0 {
            let last_index = (self.start + self.len - 1) % HISTORY_CAPACITY;
            if self.commands[last_index] == command {
                return;
            }
        }

        if self.len < HISTORY_CAPACITY {
            let index = (self.start + self.len) % HISTORY_CAPACITY;
            self.commands[index] = command;
            self.len += 1;
        } else {
            self.commands[self.start] = command;
            self.start = (self.start + 1) % HISTORY_CAPACITY;
        }

        self.current_index = None;
    }

    fn previous_command(&mut self) -> Option<&String> {
        if self.len == 0 {
            return None;
        }

        match self.current_index {
            None => {
                self.current_index = Some(self.len - 1);
                let idx = (self.start + self.len - 1) % HISTORY_CAPACITY;
                self.commands.get(idx)
            }
            Some(pos) if pos > 0 => {
                self.current_index = Some(pos - 1);

                let idx = (self.start + self.len - 1) % HISTORY_CAPACITY;
                self.commands.get(idx)
            }
            // Already at oldest
            Some(_) => None,
        }
    }

    fn next_command(&mut self) -> Option<&String> {
        if self.len == 0 {
            return None;
        }

        match self.current_index {
            None => None,
            Some(pos) if pos > 0 => {
                self.current_index = Some(pos + 1);

                let idx = (self.start + self.len + 1) % HISTORY_CAPACITY;
                self.commands.get(idx)
            }
            // Already at oldest
            Some(_) => {
                self.current_index = None;
                None
            }
        }
    }
}

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

struct TerminalGuard {}

impl TerminalGuard {
    fn new() -> Result<Self, String> {
        terminal::enable_raw_mode().map_err(|err| err.to_string())?;
        Ok(Self {})
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
    }
}

/// Handle user interaction with the terminal.
/// Returns the line passed in when `\n` char is received
fn read_line(prompt: &str, history: &mut History) -> Result<String, String> {
    let mut line = String::new();
    let mut cursor_pos: usize = 0;
    let mut stdout = io::stdout();

    stdout
        .execute(cursor::MoveToColumn(0))
        .map_err(|err| err.to_string())?;
    stdout
        .queue(terminal::Clear(terminal::ClearType::UntilNewLine))
        .map_err(|err| err.to_string())?;

    print!("{prompt}");

    stdout.flush().map_err(|err| err.to_string())?;

    loop {
        let event = event::read().map_err(|err| err.to_string())?;

        if let event::Event::Key(key_event) = event {
            match key_event.code {
                event::KeyCode::Backspace => {
                    if cursor_pos > 0 {
                        line.remove(cursor_pos - 1);
                        cursor_pos -= 1;

                        // redraw line from cursor position
                        stdout
                            .execute(cursor::MoveLeft(1))
                            .map_err(|err| err.to_string())?;
                        stdout
                            .queue(terminal::Clear(terminal::ClearType::UntilNewLine))
                            .map_err(|err| err.to_string())?;

                        print!("{}", &line[cursor_pos..]);
                        stdout
                            .execute(cursor::MoveToColumn((prompt.len() + cursor_pos) as u16))
                            .map_err(|err| err.to_string())?;
                        stdout.flush().map_err(|err| err.to_string())?;
                    }
                }
                event::KeyCode::Enter => {
                    println!();
                    stdout
                        .execute(cursor::MoveToColumn(0))
                        .map_err(|err| err.to_string())?;
                    return Ok(line);
                }
                event::KeyCode::Left => {
                    if cursor_pos > 0 {
                        cursor_pos -= 1;
                        stdout
                            .execute(cursor::MoveLeft(1))
                            .map_err(|err| err.to_string())?;
                    }
                }
                event::KeyCode::Right => {
                    if cursor_pos < line.len() {
                        cursor_pos += 1;
                        stdout
                            .execute(cursor::MoveRight(1))
                            .map_err(|err| err.to_string())?;
                    }
                }
                event::KeyCode::Up => {
                    if let Some(cmd) = history.previous_command() {
                        stdout
                            .execute(cursor::MoveToColumn(prompt.len() as u16))
                            .map_err(|err| err.to_string())?;
                        stdout
                            .queue(terminal::Clear(terminal::ClearType::UntilNewLine))
                            .map_err(|err| err.to_string())?;

                        // FIX: use references rather than clones
                        line = cmd.clone();
                        cursor_pos = line.len();

                        print!("{line}");
                        stdout.flush().map_err(|err| err.to_string())?;
                    }
                }
                event::KeyCode::Down => {
                    let cmd = history
                        .next_command()
                        .map(|l| l.to_string())
                        .unwrap_or_default();
                    stdout
                        .execute(cursor::MoveToColumn(prompt.len() as u16))
                        .map_err(|err| err.to_string())?;
                    stdout
                        .queue(terminal::Clear(terminal::ClearType::UntilNewLine))
                        .map_err(|err| err.to_string())?;

                    // FIX: use references rather than clones
                    line = cmd.clone();
                    cursor_pos = line.len();

                    print!("{line}");
                    stdout.flush().map_err(|err| err.to_string())?;
                }
                event::KeyCode::Delete => {
                    if cursor_pos < line.len() {
                        line.remove(cursor_pos);

                        // redraw line from cursor position
                        stdout
                            .queue(terminal::Clear(terminal::ClearType::UntilNewLine))
                            .map_err(|err| err.to_string())?;

                        print!("{}", &line[cursor_pos..]);
                        stdout
                            .execute(cursor::MoveToColumn((prompt.len() + cursor_pos) as u16))
                            .map_err(|err| err.to_string())?;
                        stdout.flush().map_err(|err| err.to_string())?;
                    }
                }

                event::KeyCode::Char(c) => {
                    line.insert(cursor_pos, c);
                    cursor_pos += 1;

                    stdout
                        .queue(terminal::Clear(terminal::ClearType::UntilNewLine))
                        .map_err(|err| err.to_string())?;

                    print!("{}", &line[cursor_pos - 1..]);
                    stdout
                        .queue(cursor::MoveToColumn((prompt.len() + cursor_pos) as u16))
                        .map_err(|err| err.to_string())?;
                    stdout.flush().map_err(|err| err.to_string())?;
                }

                event::KeyCode::Esc => return Err("EXIT".to_string()),

                // FIX: Handle this later
                event::KeyCode::Home => todo!(),
                event::KeyCode::End => todo!(),
                event::KeyCode::PageUp => todo!(),
                event::KeyCode::PageDown => todo!(),
                event::KeyCode::Tab => todo!(),
                event::KeyCode::BackTab => todo!(),
                event::KeyCode::Insert => todo!(),
                event::KeyCode::F(_) => todo!(),
                event::KeyCode::Null => todo!(),
                event::KeyCode::CapsLock => todo!(),
                event::KeyCode::ScrollLock => todo!(),
                event::KeyCode::NumLock => todo!(),
                event::KeyCode::PrintScreen => todo!(),
                event::KeyCode::Pause => todo!(),
                event::KeyCode::Menu => todo!(),
                event::KeyCode::KeypadBegin => todo!(),
                event::KeyCode::Media(_media_key_code) => todo!(),
                event::KeyCode::Modifier(_modifier_key_code) => todo!(),
            }
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

    // Perform handshake before entering REPL loop
    handshake(&stream)?;

    // Display welcome message and usage hints
    println!("{art}", art = ascii_art());
    println!("If stuck try `HELP`");
    println!("To end session `QUIT` or `EXIT`");
    println!("Press `Esc` to quit");
    println!("Use ↑/↓ arrows for command history");

    let _guard = TerminalGuard::new().map_err(|err| err.to_string());

    // Initialize REPL components
    let mut buffer = [0u8; 1024 * 1024]; // 1MB buffer for server responses
    let mut hist = History::new();

    // Main REPL (Read-Eval-Print Loop)
    loop {
        let prompt = "volatix> ";
        let line = match read_line(prompt, &mut hist) {
            Ok(line) => line,
            Err(err) => {
                if err.as_str() == "EXIT" {
                    println!("Exiting..");
                    break;
                }
                eprintln!("{err}");
                continue;
            }
        };

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

        hist.push(line.to_string());

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
                // Put previous line onto stdout to allow for corrections
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
