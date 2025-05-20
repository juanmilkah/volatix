# Volatix

![Performance](performance.png)

An In memory database. Still in development <skill issues :)>

## Build the project

```bash
cargo build --release
```

## Start server

```bash
  cargo run --release --bin server [--threads<1..>]
```

## Start cli repl

```bash
  cargo run --release --bin cli
```

Run some commands

```bash
HELP
SET key value
GET key
DELETE key

SETLIST [key, value, key, value, ..]
SETLIST {key, value, ..}
SETMAP  {"key" : "value", "key": "value"}

GETLIST {key, key, ..}
GETLIST [key, key, ..]

DELETELIST {key, key, ..}
DELETELIST [key, key, ..]

GETSTATS
RESETSTATS
ENTRYSTATS key

CONFOPTIONS
CONFSET key value
CONFGET key

SETWTTL key value ttl  # u64 ttl
EXTENDTTL key value # i64 addition 
GETTTL key
```

### Config Options
```bash
- MAXCAP      # U64
- GLOBALTTL   # U64
- COMPRESSION # Compress values past comp threshold
  - ENABLE
  - DISABLE  # default
- COMPTHRESHOLD # U64, size of value in bytes
- EVICTPOLICY #The storage layer eviction policy
  - OLDEST    # Oldest first
  - LFU       # Least Frequently Used
  - LRU       # Least Recently Used
  - SIZEAWARE # Largest first
```

## Benchmarking

```bash
cargo run --release --bin volatix-bench \
[--duration<secs>] [--ratio<0..1>] [--threads<1..>]
```
Defaults: 
- 60 secs duration
- 0.7 mixed-ratio
- 4 threads

This project is licensed under the [MIT](LICENSE).

