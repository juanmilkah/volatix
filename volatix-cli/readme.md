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

### Module Breakdown

#### 1. **main.rs** - Application Entry Point & Network Layer
- Establishes TCP connection to Volatix server (127.0.0.1:7878)
- Implements the interactive REPL loop
- Handles connection management and handshake protocol
- Coordinates between parsing, serialization, and response formatting
- Manages user session lifecycle

#### 2. **parse.rs** - Command Parsing Engine
- **Command Enum**: Defines all supported database operations with their parameters
- **Lexical Analysis**: Tokenizes user input with support for quoted strings, arrays, and maps
- **Syntax Parsing**: Converts text commands into structured Command objects
- **Error Handling**: Provides detailed error messages for malformed commands
- **Data Structure Parsing**: Handles complex inputs like JSON objects and arrays

**Key Functions:**
- `parse_line()`: Main entry point for command parsing
- `parse_arg()`: Extracts individual arguments with quote handling
- `parse_list()`: Parses array-like structures `[item1, item2, ...]`
- `parse_map()`: Parses JSON-like objects `{"key": "value"}`

#### 3. **serialize.rs** - RESP Protocol Serialization
- Converts parsed commands into RESP (Redis Serialization Protocol) format
- Handles different RESP data types:
  - **Bulk Strings**: `$<length>\r\n<data>\r\n`
  - **Arrays**: `*<count>\r\n<element1><element2>...`
  - **Integers**: `:<value>\r\n`
  - **Maps**: `%<count>\r\n<key1><value1>...`

**RESP Format Examples:**
```
SET key value → *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
GET key       → *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
```

#### 4. **deserialize.rs** - Response Processing
- **Response Enum**: Represents all possible server response types
- **Protocol Parsing**: Uses libvolatix to parse RESP responses
- **Type Conversion**: Converts binary RESP data to Rust types
- **Recursive Processing**: Handles nested arrays and complex responses
- **Display Formatting**: Provides human-readable output formatting

#### 5. **usage.rs** - Help System
- Comprehensive command documentation
- Organized by operation categories (Basic, Batch, Configuration, Stats, TTL)
- Usage examples and parameter descriptions
- Configuration options and eviction policies

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

### Command Categories

#### Basic Operations
- `SET`, `GET`, `DELETE`, `EXISTS` - Core key-value operations
- `INCR`, `DECR` - Numeric operations
- `RENAME` - Key management
- `KEYS`, `FLUSH` - Database inspection and management

#### Batch Operations
- `SETLIST`, `GETLIST`, `DELETELIST` - Multi-key operations
- `SETMAP` - Bulk key-value insertion

#### Configuration Management
- `CONFSET`, `CONFGET`, `CONFOPTIONS` - Server configuration
- `MAXCAP`, `GLOBALTTL`, `COMPRESSION` - Runtime parameters

#### Statistics and Monitoring
- `GETSTATS`, `RESETSTATS` - Global statistics
- `DUMP` - Per-key statistics

#### TTL (Time-to-Live) Management
- `SETWTTL` - Set key with expiration
- `EXPIRE` - Modify key expiration
- `GETTTL` - Query remaining TTL
- `EVICTNOW` - Manual eviction trigger

### Error Handling Strategy

The CLI implements comprehensive error handling at multiple levels:

1. **Parse Errors**: Invalid syntax, missing arguments, malformed data structures
2. **Network Errors**: Connection failures, socket errors, timeout handling
3. **Protocol Errors**: Invalid RESP responses, unexpected data types
4. **Server Errors**: Database errors propagated from server responses

### Protocol Compatibility

The CLI uses RESP (Redis Serialization Protocol) for communication, ensuring:
- Binary-safe data transmission
- Efficient serialization/deserialization
- Type preservation across network boundaries
- Compatibility with Redis-like protocols

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
```

#### Statistics
```bash
volatix> GETSTATS
{"keys": 42, "evicted_entries": 10, ...}

volatix> DUMP user:1
{"access_count": 15, "last_accessed": "2025-01-15T10:30:00Z", ...}
```

### Interactive Features

- [ ] **Command History**: Use arrow keys to navigate previous commands
- [X] **Case Insensitive**: Commands work in both uppercase and lowercase
- [X] **Quoted Strings**: Support for spaces in keys/values using quotes
- [X] **Help System**: Type `HELP` for comprehensive command documentation
- [X] **Graceful Exit**: Use `QUIT` or `EXIT` to close the session

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

### Connection Issues
- Ensure Volatix server is running on port 7878
- Check firewall settings
- Verify network connectivity

### Command Errors
- Use `HELP` to see correct command syntax
- Check for proper quoting of strings with spaces
- Verify data structure format for lists and maps

### Performance Considerations
- The CLI uses a 1MB buffer for responses
- Large batch operations may require server-side limits
- Network latency affects interactive performance
