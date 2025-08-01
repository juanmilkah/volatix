use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, atomic::AtomicUsize},
    thread,
    time::{Duration, Instant},
};

use clap::Parser;

const ADDRESS: &str = "127.0.0.1:7878";
const DEFAULT_WORKER_COUNT: usize = 4;
const DEFAULT_BENCH_DURATION: u64 = 30;
const DEFAULT_RATIO: f64 = 0.7; // 70% read, 30% writes

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

#[allow(dead_code)]
enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
    ConfSet { key: String, value: String },
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
        Command::Delete { key } => {
            let mut arr = Vec::new();
            let cmd = Bstring::new("DELETE");
            let key = Bstring::new(key);

            arr.push(cmd);
            arr.push(key);
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

fn random_data() -> String {
    let allowed = ('a'..='z').collect::<Vec<char>>();
    let mut data = String::new();
    let n = rand::random_range(..(1024 * 2) as usize); //0 - 2kb data size
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

struct Config {
    duration: Duration,
    operation_count: Arc<AtomicUsize>,
    error_count: Arc<AtomicUsize>,
    read_latencies: Arc<parking_lot::Mutex<Vec<Duration>>>,
    write_latencies: Arc<parking_lot::Mutex<Vec<Duration>>>,
    mixed_ratio: f64,
    compress: bool,
}

fn worker_thread(id: usize, config: &Config) {
    let tcp_stream = match TcpStream::connect(ADDRESS) {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Thread {id} failed to connect: {e}");
            return;
        }
    };
    let mut stream = tcp_stream;

    // setup compression
    if config.compress {
        let req = serialize_request(&Command::ConfSet {
            key: "COMPRESSION".to_string(),
            value: "ENABLE".to_string(),
        });
        stream
            .write_all(&req)
            .map_err(|err| eprintln!("Failed to enable compression: {err}"))
            .unwrap();
    }

    let mut buffer = [0u8; 1024];
    let start_time = Instant::now();

    // pre generate some keys and values
    let key_count = 1000;
    let keys: Vec<_> = (0..key_count).map(|_| get_key()).collect();

    while start_time.elapsed() < config.duration {
        let op_type = if rand::random::<f64>() < config.mixed_ratio {
            // Get operation
            let key_idx = rand::random::<u64>() % key_count;

            Command::Get {
                key: keys[key_idx as usize].clone(),
            }
        } else {
            // Set operation
            let key_idx = rand::random::<u64>() % key_count;
            let value = random_data();
            Command::Set {
                key: keys[key_idx as usize].clone(),
                value,
            }
        };

        let req = serialize_request(&op_type);

        let op_start = Instant::now();

        match stream.write_all(&req) {
            Ok(_) => {}
            Err(_) => {
                config
                    .error_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                continue;
            }
        }

        match stream.read(&mut buffer) {
            Ok(n) => {
                if n == 0 {
                    continue;
                }
                let latency = op_start.elapsed();

                match op_type {
                    Command::Get { .. } => {
                        let mut latencies = config.read_latencies.lock();
                        latencies.push(latency);
                    }
                    Command::Set { .. } => {
                        let mut latencies = config.write_latencies.lock();
                        latencies.push(latency);
                    }
                    _ => {}
                }

                config
                    .operation_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Err(_) => {
                config
                    .error_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long = "compress", help = "Enable compression of large text/blobs")]
    compression: bool,
    #[arg(
        short = 'd',
        long = "duration",
        help = "Duration of the benchmark in seconds"
    )]
    duration: Option<u64>,
    #[arg(short = 'r', long = "ratio", help = "The ratio of reads to writes")]
    ratio: Option<f64>,
    #[arg(
        short = 'w',
        long = "workers",
        help = "Number of worker threads(actual cpu threads)"
    )]
    workers: Option<usize>,
}

fn main() {
    let args = Cli::parse();

    let thread_count = args.workers.unwrap_or(DEFAULT_WORKER_COUNT);
    let duration_secs = args.duration.unwrap_or(DEFAULT_BENCH_DURATION);
    let mixed_ratio = args.ratio.unwrap_or(DEFAULT_RATIO);
    let compress = args.compression;

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
    let read_latencies = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let write_latencies = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let mut handles = Vec::new();

    let start_time = Instant::now();

    //create and start worker threads
    for i in 0..thread_count {
        let op_count = Arc::clone(&operation_count);
        let err_count = Arc::clone(&error_count);
        let read_lats = Arc::clone(&read_latencies);
        let write_lats = Arc::clone(&write_latencies);
        let config = Config {
            duration,
            operation_count: op_count,
            error_count: err_count,
            read_latencies: read_lats,
            write_latencies: write_lats,
            mixed_ratio,
            compress,
        };

        let handle = thread::spawn(move || worker_thread(i, &config));
        handles.push(handle);
    }

    // show progress
    let progress_interval = Duration::from_secs(1);
    let mut last_ops = 0;

    while start_time.elapsed() < duration {
        thread::sleep(progress_interval);
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

    // wait for threads
    for handle in handles {
        handle.join().unwrap();
    }

    let total_time = start_time.elapsed();
    let total_errors = error_count.load(std::sync::atomic::Ordering::Relaxed);
    let total_ops = operation_count.load(std::sync::atomic::Ordering::Relaxed);

    // Calculate statistics
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

    // Calculate read latency statistics
    let read_lats = read_latencies.lock();
    if !read_lats.is_empty() {
        let read_avg =
            read_lats.iter().sum::<Duration>().as_micros() as f64 / read_lats.len() as f64;
        let mut sorted_reads = read_lats.clone();
        sorted_reads.sort();
        println!("\nREAD Latency:");
        println!("  Average: {read_avg:.2} µs");
    }

    // Calculate write latency statistics
    let write_lats = write_latencies.lock();
    if !write_lats.is_empty() {
        let write_avg =
            write_lats.iter().sum::<Duration>().as_micros() as f64 / write_lats.len() as f64;
        let mut sorted_writes = write_lats.clone();
        sorted_writes.sort();
        println!("\nWRITE Latency:");
        println!("  Average: {write_avg:.2} µs");
    }

    // cleanup
    let cmd = serialize_request(&Command::Flush);
    let mut tcp_stream = TcpStream::connect(ADDRESS).unwrap();
    if tcp_stream.write_all(&cmd).is_err() {
        eprintln!("Cleanup failed!");
    }
}
