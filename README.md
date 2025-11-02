# Volatix

- ![Performance Info](images/screenshots-2025-11-02-15:41:26.png)
- ![Performance Info](images/screenshots-2025-11-02-15:42:35.png)

An In memory database :)

Documentation: 
- [Server](volatix_server/README.md) 
- [Cli](volatix_cli/README.md)

## Build the project

```bash
cargo build --release
```

## Start server

```bash
  ./target/release/volatix_server
  [--port<u16>] [--snapshots_interval<secs>]
```

## Start cli repl

```bash
  ./target/release/volatix_cli
```

## Benchmarking

```bash
  ./target/release/volatix_bench 
[--duration<secs>] [--ratio<0..1>] [--workers<1..>] [--compress] [--vsize<1..>]
```
Defaults: 
- 30 secs duration
- 0.7 mixed-ratio
- 4 cpu worker threads
- Compression disabled
- 2Kb sized mock value entries.

This project is licensed under the [GPL2](LICENSE).

