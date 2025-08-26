# Volatix CLI

A command-line interface client for the Volatix in-memory database server. The CLI provides an interactive REPL (Read-Eval-Print Loop) for executing database operations and managing data stored in a Volatix server instance.

## Features

- Interactive REPL with command history
- Support for all Volatix database operations
- TCP connection to Volatix server
- RESP (Redis Serialization Protocol) communication
- Comprehensive error handling and user feedback
- Case-insensitive command parsing
- Support for quoted strings and complex data structures

## Architecture

### Overview

The Volatix CLI follows a modular architecture with clear separation of concerns:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   User Input    │───▶│  Command Parser │───▶│   Serializer    │
│     (REPL)      │    │   (parse.rs)    │    │ (serialize.rs)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Response Format │◀───│  Deserializer   │◀───│  TCP Transport  │
│   (Display)     │    │(deserialize.rs) │    │   (main.rs)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │ Volatix Server  │
                                               │  (TCP Socket)   │
                                               └─────────────────┘
```

### Data Flow

1. **Input Processing**:
   ```
   User Input → Lexical Analysis → Command Parsing → Validation
   ```

2. **Network Communication**:
   ```
   Command Object → RESP Serialization → TCP Send → Server Processing
   ```

3. **Response Handling**:
   ```
   TCP Receive → RESP Deserialization → Type Conversion → Display Formatting
   ```

## Usage

The CLI will attempt to connect to a Volatix server at `127.0.0.1:7878`. Ensure the Volatix server is running before starting the CLI.

### Command Examples

#### Basic Operations
```bash
volatix> SET name "John Doe"
OK

volatix> GET name
"John Doe"

volatix> EXISTS name
SUCCESS

volatix> DELETE name
SUCCESS
```

#### List Operations
```bash
volatix> SETLIST users ["alice", "bob", "charlie"]
SUCCESS

volatix> GETLIST ["users", "settings"]
[["alice", "bob", "charlie"], NULL]
```

#### Map Operations
```bash
volatix> SETMAP {"user:1": "alice", "user:2": "bob"}
SUCCESS
```

#### TTL Operations
```bash
volatix> SETWTTL session_token "abc123" 3600
SUCCESS

volatix> GETTTL session_token
3599

volatix> EXPIRE session_token 1800
SUCCESS
```

#### Configuration
```bash
volatix> CONFGET MAXCAP
1000

volatix> CONFSET GLOBALTTL 7200
SUCCESS

volatix> CONFRESET
SUCCESS
```

#### Statistics
```bash
volatix> GETSTATS
{"keys": 42, "evicted_entries": 10, ...}

volatix> DUMP user:1
{"access_count": 15, "last_accessed": "2025-01-15T10:30:00Z", ...}
```

### Interactive Features

- **Command History**: Use arrow keys to navigate previous commands
- **Case Insensitive**: Commands work in both uppercase and lowercase
- **Quoted Strings**: Support for spaces in keys/values using quotes
- **Help System**: Type `HELP` for comprehensive command documentation
- **Graceful Exit**: Use `QUIT` or `EXIT` or press `Esc` to close the session

## Error Messages

The CLI provides descriptive error messages:

```bash
volatix> SET
ERROR: SET: Missing key

volatix> GET "unclosed
ERROR: GET: Unclosed quote for key

volatix> INVALIDCMD
ERROR: Unknown command: INVALIDCMD
```

## Troubleshooting

### Command Errors
- Use `HELP` to see correct command syntax
- Check for proper quoting of strings with spaces
- Verify data structure format for lists and maps

### Performance Considerations
- The CLI uses a 1MB buffer for responses
- Large batch operations may require server-side limits
