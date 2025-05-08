# Volatix

![Perfomance](performance.png)

An In memory database. Still in development <skill issues :)>

## Build the project

```bash
cargo build --release
```

## Start server

```
  cargo run --release --bin server
```

## Start cli repl

```
  cargo run --release --bin cli
```

Run some commands

```bash
SET name foo
GET name
DELETE name
HELP
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
