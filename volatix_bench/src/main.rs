use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, atomic::AtomicUsize},
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use parking_lot::RwLock;

const ADDRESS: &str = "127.0.0.1:7878";
const DEFAULT_WORKER_COUNT: usize = 4;
const DEFAULT_BENCH_DURATION: u64 = 30;
const DEFAULT_RATIO: f64 = 0.7; // 70% read, 30% writes
const DEFAULT_VALUESIZE: usize = 2 * 1024;

// $<length>\r\n<data>\r\n
struct Bstring(String);

impl Bstring {
    fn new(s: &str) -> Self {
        let mut bstring = String::new();
        let terminator = "\r\n";

        bstring.push('$');
        bstring.push_str(&s.len().to_string());
        bstring.push_str(terminator);
        bstring.push_str(s);
        bstring.push_str(terminator);

        Bstring(bstring)
    }
}

// *<number-of-elements>\r\n<element-1>...<element-n>
struct Array(String);

impl Array {
    fn new(elems: &[Bstring]) -> Self {
        let mut arr = String::new();
        let terminator = "\r\n";

        arr.push('*');
        arr.push_str(&elems.len().to_string());
        arr.push_str(terminator);

        for s in elems {
            arr.push_str(&s.0);
        }

        Array(arr)
    }
}

enum Command<'cmd> {
    Get { key: &'cmd str },
    Set { key: &'cmd str, value: &'cmd str },
    ConfSet { key: &'cmd str, value: &'cmd str },
    Flush,
}

fn serialize_request(command: &Command) -> Vec<u8> {
    match command {
        Command::Get { key } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("GET");
            let key = Bstring::new(key);

            arr.push(cmd);
            arr.push(key);
            let arr = Array::new(&arr);

            arr.0.as_bytes().to_vec()
        }
        Command::Set { key, value } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("SET");
            let key = Bstring::new(key);
            let value = Bstring::new(value);

            arr.push(cmd);
            arr.push(key);
            arr.push(value);
            let arr = Array::new(&arr);

            arr.0.as_bytes().to_vec()
        }
        Command::ConfSet { key, value } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("CONFSET");
            let key = Bstring::new(key);
            let value = Bstring::new(value);

            arr.push(cmd);
            arr.push(key);
            arr.push(value);
            let arr = Array::new(&arr);

            arr.0.as_bytes().to_vec()
        }

        Command::Flush => {
            let cmd = Bstring::new("FLUSH");
            cmd.0.as_bytes().to_vec()
        }
    }
}

fn random_data(vsize: usize) -> String {
    let allowed = ('a'..='z').collect::<Vec<char>>();
    let mut data = String::new();
    let n = rand::random_range(..vsize).max(1);
    for _ in 0..n {
        data.push(allowed[rand::random_range(..allowed.len())]);
    }

    data.shrink_to_fit();
    data
}

fn get_key() -> String {
    let mut key = String::new();
    let mut allowed = ('a'..='z').collect::<Vec<char>>();
    let caps = ('A'..='Z').collect::<Vec<char>>();
    allowed.extend(caps);

    for _ in 0..5 {
        key.push(allowed[rand::random_range(..allowed.len())]);
    }

    key
}

struct ThreadResult {
    read_latencies: Vec<Duration>,
    write_latencies: Vec<Duration>,
    operations: usize,
    errors: usize,
}

struct Config {
    compress: bool,
    mixed_ratio: f64,
    duration: Duration,
    operation_count: Arc<AtomicUsize>,
    error_count: Arc<AtomicUsize>,
    keys: Arc<RwLock<Vec<String>>>,
    values: Arc<RwLock<Vec<String>>>,
}

fn worker_thread(id: usize, config: &Config) -> ThreadResult {
    let mut stream = match TcpStream::connect(ADDRESS) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Thread {id} failed to connect: {e}");
            return ThreadResult {
                read_latencies: vec![],
                write_latencies: vec![],
                operations: 0,
                errors: 1,
            };
        }
    };

    let mut buffer = [0u8; 1024 * 1024];

    // Setup compression (once per thread — acceptable)
    if config.compress {
        let req = serialize_request(&Command::ConfSet {
            key: "COMPRESSION",
            value: "ENABLE",
        });
        let _ = stream.write_all(&req);
        let _ = stream.read(&mut buffer);
    }

    let start_time = Instant::now();
    let key_count = 100_000;
    let vals_count = 100_000;

    let mut local_read_lats = Vec::with_capacity(50_000);
    let mut local_write_lats = Vec::with_capacity(20_000);
    let mut ops = 0;
    let mut errs = 0;

    let mut i = 0;
    while start_time.elapsed() < config.duration {
        let op_type = if rand::random::<f64>() < config.mixed_ratio {
            Command::Get {
                key: &config.keys.read()[i % key_count],
            }
        } else {
            Command::Set {
                key: &config.keys.read()[i % key_count],
                value: &config.values.read()[i % vals_count],
            }
        };
        i += 1;

        let req = serialize_request(&op_type);
        let op_start = Instant::now();

        if stream.write_all(&req).is_err() {
            errs += 1;
            config
                .error_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        }

        match stream.read(&mut buffer) {
            Ok(n) if n > 0 => {
                let latency = op_start.elapsed();
                match op_type {
                    Command::Get { .. } => local_read_lats.push(latency),
                    Command::Set { .. } => local_write_lats.push(latency),
                    _ => {}
                }
                ops += 1;
                config
                    .operation_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            _ => {
                errs += 1;
                config
                    .error_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    ThreadResult {
        read_latencies: local_read_lats,
        write_latencies: local_write_lats,
        operations: ops,
        errors: errs,
    }
}

#[derive(Debug, Parser)]
struct Cli {
    #[arg(
        short = 'c',
        long = "compress",
        help = "Enable compression of large text/blobs"
    )]
    compression: bool,

    #[arg(
        short = 'd',
        long = "duration",
        help = "Duration of the benchmark in seconds"
    )]
    duration: Option<u64>,

    #[arg(short = 'r', long = "ratio", help = "The ratio of reads to writes")]
    ratio: Option<f64>,

    #[arg(short = 'w', long = "workers", help = "Number of worker threads")]
    workers: Option<usize>,

    #[arg(
        short = 'v',
        long = "vsize",
        help = "Maximum size of value entries in bytes"
    )]
    vsize: Option<usize>,
}

fn main() {
    let args = Cli::parse();

    let thread_count = args.workers.unwrap_or(DEFAULT_WORKER_COUNT);
    let duration_secs = args.duration.unwrap_or(DEFAULT_BENCH_DURATION);
    let mixed_ratio = args.ratio.unwrap_or(DEFAULT_RATIO);
    let compress = args.compression;
    let vsize = args.vsize.unwrap_or(DEFAULT_VALUESIZE);

    println!("Starting performance test with:");
    println!("  - {thread_count} threads");
    println!("  - {duration_secs} seconds duration");
    println!(
        "  - {}% reads / {}% writes",
        (mixed_ratio * 100.0) as u32,
        ((1.0 - mixed_ratio) * 100.0) as u32
    );
    println!();

    let duration = Duration::from_secs(duration_secs);
    let operation_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    // Pre-generate keys and values
    println!("Generating key-value mock data!");
    let keys: Vec<_> = (0..100_000).map(|_| get_key()).collect();
    let values: Vec<_> = (0..100_000).map(|_| random_data(vsize)).collect();
    let keys = Arc::new(RwLock::new(keys));
    let values = Arc::new(RwLock::new(values));

    let mut handles = Vec::new();

    for i in 0..thread_count {
        let op_count = Arc::clone(&operation_count);
        let err_count = Arc::clone(&error_count);
        let config = Config {
            duration,
            operation_count: op_count,
            error_count: err_count,
            mixed_ratio,
            compress,
            keys: Arc::clone(&keys),
            values: Arc::clone(&values),
        };

        let handle = thread::spawn(move || worker_thread(i, &config));
        handles.push(handle);
    }

    // Progress reporting
    let start_time = Instant::now();
    let mut last_ops = 0;
    while start_time.elapsed() < duration {
        thread::sleep(Duration::from_secs(1));
        let current_ops = operation_count.load(std::sync::atomic::Ordering::Relaxed);
        let ops_delta = current_ops - last_ops;
        last_ops = current_ops;

        println!(
            "Progress: {:.1}s / {:.1}s | Operations: {} | Rate: {} ops/sec",
            start_time.elapsed().as_secs_f64(),
            duration.as_secs_f64(),
            current_ops,
            ops_delta
        );
    }

    // Collect results
    let mut all_read_lats = Vec::new();
    let mut all_write_lats = Vec::new();
    let mut total_ops = 0;
    let mut total_errors = 0;

    for handle in handles {
        match handle.join() {
            Ok(result) => {
                all_read_lats.extend(result.read_latencies);
                all_write_lats.extend(result.write_latencies);
                total_ops += result.operations;
                total_errors += result.errors;
            }
            Err(_) => total_errors += 1,
        }
    }

    let total_time = start_time.elapsed();

    // Final report
    println!("\n=== PERFORMANCE TEST RESULTS ===");
    println!("Control Params");
    println!("  - {thread_count} threads");
    println!("  - {duration_secs} seconds duration");
    println!(
        "  - Compression {}",
        if compress { "Enabled" } else { "Disabled" }
    );
    println!(
        "  - {}% reads / {}% writes",
        (mixed_ratio * 100.0) as u32,
        ((1.0 - mixed_ratio) * 100.0) as u32
    );
    println!();
    println!("Total operations: {total_ops}");
    println!("Total errors: {total_errors}");
    println!("Total time: {:.2} seconds", total_time.as_secs_f64());
    println!(
        "Throughput: {:.2} operations/second",
        total_ops as f64 / total_time.as_secs_f64()
    );

    // Read latency stats
    if !all_read_lats.is_empty() {
        let avg = all_read_lats.iter().map(|d| d.as_micros()).sum::<u128>() as f64
            / all_read_lats.len() as f64;
        all_read_lats.sort_unstable();
        let p50 = all_read_lats[all_read_lats.len() * 50 / 100].as_micros();
        let p99 = all_read_lats[all_read_lats.len() * 99 / 100].as_micros();
        println!("\nREAD Latency (µs):");
        println!("  Average: {avg:.2}");
        println!("  P50:     {p50}");
        println!("  P99:     {p99}");
    }

    // Write latency stats
    if !all_write_lats.is_empty() {
        let avg = all_write_lats.iter().map(|d| d.as_micros()).sum::<u128>() as f64
            / all_write_lats.len() as f64;
        all_write_lats.sort_unstable();
        let p50 = all_write_lats[all_write_lats.len() * 50 / 100].as_micros();
        let p99 = all_write_lats[all_write_lats.len() * 99 / 100].as_micros();
        println!("\nWRITE Latency (µs):");
        println!("  Average: {avg:.2}");
        println!("  P50:     {p50}");
        println!("  P99:     {p99}");
    }

    // Cleanup
    let cmd = serialize_request(&Command::Flush);
    if let Ok(mut stream) = TcpStream::connect(ADDRESS) {
        let _ = stream.write_all(&cmd);
    }
}
