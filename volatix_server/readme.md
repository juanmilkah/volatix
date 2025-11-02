# Volatix - High-Performance In-Memory Cache Server

Volatix is a Redis-compatible in-memory cache server built in Rust. It supports the RESP3 protocol and provides advanced features like compression, TTL management, and multiple eviction policies.

## Architecture Design

### System Overview
```
┌─────────────────────────────────────────────────────────┐
│                    Client Applications                  │
└─────────────────────┬───────────────────────────────────┘
                      │ RESP3 Protocol
┌─────────────────────▼───────────────────────────────────┐
│                  TCP Server Layer                       │
│  ┌─────────────────────────────────────────────────────┐│
│  │         Connection Handler (Tokio Async)            ││
│  └─────────────────┬───────────────────────────────────┘│
└────────────────────┼────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                Protocol Layer (RESP3)                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │   Parser    │  │  Serializer │  │ Request Handler │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
└────────────────────┼────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                 Command Layer                           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────┐ │
│  │ GET/SET  │ │ TTL Mgmt │ │ Batching │ │ Config Mgmt │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────┘ │
└────────────────────┼────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                Storage Engine                           │
│  ┌─────────────────────────────────────────────────────┐│
│  │              LockedStorage                          ││
│  │  ┌──────────────┐  ┌──────────────┐                 ││
│  │  │   HashMap    │  │   RwLock     │                 ││
│  │  │ (Key-Value)  │  │(Thread-Safe) │                 ││
│  │  └──────────────┘  └──────────────┘                 ││ 
│  └─────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────┐│
│  │              Storage Features                       ││
│  │  ┌──────────┐┌──────────┐┌─────────┐┌─────────────┐ ││
│  │  │   TTL    ││Eviction  ││Compress-││ Statistics  │ ││
│  │  │Management││Policies  ││ion      ││   Tracking  │ ││
│  │  └──────────┘└──────────┘└─────────┘└─────────────┘ ││
│  └─────────────────────────────────────────────────────┘│
└────────────────────┼────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│               Persistence Layer                         │
│  ┌─────────────────────────────────────────────────────┐│
│  │           Bincode Serialization                     ││
│  │  ┌──────────────┐           ┌──────────────────────┐││
│  │  │    Disk      │           │    Background        │││
│  │  │  Snapshots   │  <------> │    Snapshots         │││
│  │  │              │           │    (300s interval)   │││
│  │  └──────────────┘           └──────────────────────┘││
│  └─────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

## Features

### Core Features
- **Redis-compatible RESP3 protocol**
- **Thread-safe concurrent access**
- **Multiple data types**: Int, Float, Bool, Text, Bytes, List, Map
- **TTL support** with automatic expiration
- **Disk persistence** with background snapshots
- **Configurable eviction policies**

### Advanced Features
- **Compression**: Automatic compression for large values
- **Batch operations**: Multi-key get/set/delete
- **Statistics tracking**: Hit/miss ratios, evictions
- **Runtime configuration**: Modify settings without restart
- **Memory management**: Configurable capacity limits

### Eviction Policies
1. **Oldest**: Remove entries by creation time
2. **LRU (Least Recently Used)**: Remove least accessed entries
3. **LFU (Least Frequently Used)**: Remove entries with lowest access count
4. **Size-Aware**: Remove largest entries first

### Run Server
```bash
# Default port (7878)
volatix-server

# Custom port
volatix-server --port 8080

# Custom snapshot interval time in seconds
volatix-server --snapshots_interval 400
```

## Usage Examples

#### Connect and Test
```bash
# Using netcat to connect
nc 127.0.0.1 7878

# Send handshake
HELLO
# Response: $5\r\nHELLO\r\n
```

#### Set and Get Operations
```bash
# Set a key-value pair
*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nJohn\r\n

# Get a value
*2\r\n$3\r\nGET\r\n$4\r\nname\r\n
# Response: $4\r\nJohn\r\n

# Check if key exists
*2\r\n$6\r\nEXISTS\r\n$4\r\nname\r\n
# Response: #t\r\n (true)
```

#### TTL Operations
```bash
# Set with TTL (key, value, ttl_seconds)
*4\r\n$8\r\nSETWTTL\r\n$7\r\nsession\r\n$10\r\nsession123\r\n:3600\r\n

# Get TTL for a key
*2\r\n$6\r\nGETTTL\r\n$7\r\nsession\r\n
# Response: :3600\r\n

# Extend TTL (add 1800 seconds)
*3\r\n$6\r\nEXPIRE\r\n$7\r\nsession\r\n:1800\r\n
```

#### Batch Operations
```bash
# Get multiple keys
*4\r\n$7\r\nGETLIST\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n

# Delete multiple keys
*4\r\n$10\r\nDELETELIST\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n
```

#### Advanced Data Types

##### Lists
```bash
# Set a list
*3\r\n$7\r\nSETLIST\r\n$5\r\nitems\r\n*3\r\n$5\r\napple\r\n$6\r\nbanana\r\n$6\r\norange\r\n
```

##### Maps (JSON-like)
```bash
# Set multiple key-value pairs
*2\r\n$6\r\nSETMAP\r\n%3\r\n$4\r\nname\r\n$4\r\nJohn\r\n$3\r\nage\r\n:25\r\n$4\r\ncity\r\n$7\r\nSeattle\r\n
```

### Runtime Configuration Keys
- `GLOBALTTL`: Default Time To Live in seconds
- `MAXCAP`: Maximum number of entries
- `EVICTPOLICY`: `OLDEST`, `LRU`, `LFU`, `SIZEAWARE`
- `COMPRESSION`: `ENABLE`, `DISABLE`
- `COMPRESSIONTHRESHOLD`: Size threshold for compression

## Performance Characteristics

### Benchmarks
- **Memory**: ~100 bytes overhead per entry
- **Throughput**: 100K+ operations/second
- **Latency**: Micro seconds for cache hits
- **Concurrency**: Scales linearly with CPU cores

### Persistence
- **Snapshot frequency**: 300 seconds (configurable)
- **Serialization format**: Bincode (binary)
- **Startup time**: <1 second for 1M entries

## FAQ

**Q: Is this production-ready?**
A: This is a work in-progres project. For production use, consider Redis or KeyDB.

**Q: How does it compare to Redis?**
A: Volatix focuses on core caching features with modern Rust performance. Redis has a much broader feature set.

**Q: Can I use existing Redis clients?**
A: Partially. Basic RESP3 operations work, but advanced Redis features are not supported.

**Q: What's the maximum memory usage?**
A: Limited by available system memory and the configured max_capacity setting.
