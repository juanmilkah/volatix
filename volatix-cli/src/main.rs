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

/// A fixed-capacity command history implementation using a circular buffer.
///
/// # Structure
/// The history uses a pre-allocated vector of fixed size (`HISTORY_CAPACITY`)
/// to efficiently store and navigate through command history without frequent reallocations.
///
/// ## Circular Buffer Mechanics
/// - `commands`: A pre-allocated vector storing commands
/// - `start`: Index of the oldest command in the buffer
/// - `len`: Number of commands currently stored
/// - `current_index`: Tracks the current position when navigating history
#[derive(Default, Debug)]
struct History {
    /// Fixed-size vector to store commands, pre-initialized with empty strings
    /// This allows constant-time access and prevents repeated memory allocations
    commands: Vec<String>,

    /// Tracks the starting index of the oldest command in the circular buffer
    /// Allows efficient rotation of the buffer without moving elements
    start: usize,

    /// Current number of commands stored in the history
    /// Always <= HISTORY_CAPACITY
    len: usize,

    /// Tracks the current position when navigating through history
    /// - `None`: Not navigating, at the end of history
    /// - `Some(index)`: Currently browsing through past commands
    current_index: Option<usize>,
}

impl History {
    /// Creates a new History instance with pre-allocated command storage
    ///
    /// # Behavior
    /// - Initializes a vector of `HISTORY_CAPACITY` empty strings
    /// - Sets all other fields to their default values
    fn new() -> Self {
        Self {
            // Pre-allocate fixed-size vector to avoid dynamic resizing
            commands: vec![String::new(); HISTORY_CAPACITY],
            ..Default::default()
        }
    }

    /// Adds a new command to the history, avoiding duplicates
    ///
    /// # Behavior
    /// - Prevents adding duplicate consecutive commands
    /// - If history is not full, appends to the end
    /// - If history is full, overwrites the oldest command
    /// - Resets current navigation index
    fn push(&mut self, command: String) {
        // Skip if the command is the same as the last added command
        if self.len > 0 {
            let last_index = (self.start + self.len - 1) % HISTORY_CAPACITY;
            if self.commands[last_index] == command {
                return;
            }
        }

        // Two scenarios for adding a command:
        if self.len < HISTORY_CAPACITY {
            // 1. History is not full: Add to the next available slot
            let index = (self.start + self.len) % HISTORY_CAPACITY;
            self.commands[index] = command;
            self.len += 1;
        } else {
            // 2. History is full: Overwrite the oldest command and rotate start
            self.commands[self.start] = command;
            self.start = (self.start + 1) % HISTORY_CAPACITY;
        }

        // Reset navigation when a new command is added
        self.current_index = None;
    }

    /// Retrieves the previous command when navigating history
    ///
    /// # Navigation Behavior
    /// - First call: Returns the most recent command
    /// - Subsequent calls: Moves backwards through history
    /// - Returns `None` when no more previous commands exist
    fn previous_command(&mut self) -> Option<&String> {
        // Cannot navigate empty history
        if self.len == 0 {
            return None;
        }

        match self.current_index {
            // First navigation: Start from the most recent command
            None => {
                self.current_index = Some(self.len - 1);
                let idx = (self.start + self.len - 1) % HISTORY_CAPACITY;
                Some(&self.commands[idx])
            }
            // Move to previous command if possible
            Some(pos) if pos > 0 => {
                self.current_index = Some(pos - 1);
                let idx = (self.start + pos - 1) % HISTORY_CAPACITY;
                Some(&self.commands[idx])
            }
            // Already at oldest
            Some(_) => None,
        }
    }

    /// Retrieves the next command when navigating forward in history
    ///
    /// # Navigation Behavior
    /// - Returns `None` if not currently navigating history
    /// - Moves forward through previously retrieved commands
    /// - Resets navigation when reaching the end of history
    fn next_command(&mut self) -> Option<&String> {
        // Cannot navigate empty history
        if self.len == 0 {
            return None;
        }

        match self.current_index {
            // Not currently navigating history
            None => None,

            // Move to next command if within history range
            Some(pos) if pos < self.len - 1 => {
                self.current_index = Some(pos + 1);
                let idx = (self.start + pos + 1) % HISTORY_CAPACITY;
                Some(&self.commands[idx])
            }

            // Reached the end of history navigation
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

fn connect_server(addr: SocketAddr) -> Result<TcpStream, String> {
    // Attempt to establish TCP connection to Volatix server
    let stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => return Err(e.to_string()),
    };

    // Perform handshake before entering REPL loop
    handshake(&stream)?;

    Ok(stream)
}

/// Main application entry point
/// Establishes connection, runs REPL, and handles user interaction
fn main() -> Result<(), String> {
    // Define server address (localhost:7878)
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 7878));
    let mut stream = connect_server(addr)?;

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
                    println!("Exiting..\r");
                    break;
                }
                eprintln!("{err}\r");
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
            Command::Help => {
                help();
                continue;
            }
            Command::ParseError(err) => {
                eprintln!("ERROR: {err}\r");
                // Put previous line onto stdout to allow for corrections
                continue;
            }

            Command::Reconnect => {
                println!("Reconnecting...\r");

                // Close current connection
                drop(stream);

                // Try to reconnect with retries
                let mut attempts = 0;
                const MAX_ATTEMPTS: u32 = 3;

                loop {
                    attempts += 1;
                    match connect_server(addr) {
                        Ok(new_stream) => {
                            stream = new_stream;
                            println!("Successfully reconnected\r");
                            break;
                        }
                        Err(e) => {
                            eprintln!("Reconnection attempt {attempts} failed: {e}\r");
                            if attempts >= MAX_ATTEMPTS {
                                eprintln!(
                                    "Could not reconnect after {MAX_ATTEMPTS} attempts. Exiting...\r"
                                );
                                return Err("Failed to reconnect\r".to_string());
                            }
                            std::thread::sleep(std::time::Duration::from_millis(1000));
                        }
                    }
                }
                continue;
            }

            _ => {
                let req = serialize_request(&command);

                // If serialization fails, show help
                if req.is_empty() {
                    help();
                    continue;
                }

                // Send serialized command to server
                if stream.write_all(&req).is_err() {
                    eprintln!("Failed writing to tcp stream\r");
                    continue;
                }
            }
        }

        // Read server response
        let read = match stream.read(&mut buffer) {
            Ok(n) => n,
            Err(e) => {
                eprintln!("{e}\r");
                continue;
            }
        };

        // Deserialize server response using RESP protocol
        // The server replies with a RESP type determined by the command's implementation
        // and possibly by the client's protocol version
        let resp = deserialize_response(&buffer[..read])?;

        // Display formatted response to user
        match resp {
            Response::SimpleString { data } => println!("{data}\r"),
            Response::SimpleError { data } => println!("{data}\r"),
            Response::Integer { data } => println!("{data}\r"),
            Response::BigNumber { data } => println!("{data}\r"),
            Response::Null => println!("NULL\r"),
            Response::Boolean { data } => println!("{data}\r"),
            // Use the formatting function for complex arrays
            Response::Array { data: _ } => println!("{}\r", format_response(&resp)),
        }
    }

    Ok(())
}

#[cfg(test)]
mod test_history {
    use crate::History;

    #[test]
    fn test_history() {
        let mut history = History::new();

        history.push("git clone".to_string());
        history.push("cd project".to_string());

        // Navigate backwards
        assert_eq!(history.previous_command(), Some(&"cd project".to_string()));
        assert_eq!(history.previous_command(), Some(&"git clone".to_string()));
        assert_eq!(history.previous_command(), None);

        // Navigate forwards
        assert_eq!(history.next_command(), Some(&"cd project".to_string()));
    }
}
