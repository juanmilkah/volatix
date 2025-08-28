# Volatix

![Performance](performance.png)
![Memory Info](flamegraph.svg)

An In memory database :)

## Build the project

```bash
cargo build --release
```

## Start server

```bash
  ./target/release/volatix-server
  [--port<u16>] [--snapshots_interval<secs>]
```

## Start cli repl

```bash
  ./target/release/volatix-cli
```

## Benchmarking

```bash
  ./target/release/volatix-bench 
[--duration<secs>] [--ratio<0..1>] [--workers<1..> [--compress]]
```
Defaults: 
- 30 secs duration
- 0.7 mixed-ratio
- 4 cpu worker threads
- Compression disabled

This project is licensed under the [MIT](LICENSE).

