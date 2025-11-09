use std::{
    env,
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Sender, channel},
    },
    thread::JoinHandle,
    time::Duration,
};

mod process;

use clap::Parser;
use parking_lot::RwLock;
use volatix_core::{
    LockedStorage, Message, StorageOptions, handle_messages, parse_request, volatix_ascii_art,
};

use crate::process::process_request;

// start the server backend on this port
const DEFAULT_PORT: u16 = 7878;
// FLush snapshots to disk in this interval
const SNAPSHOTS_INTERVAL_TIME: u64 = 60 * 5; // In seconds

fn handle_client(
    mut stream: TcpStream,
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
    message_tx: Arc<Sender<Message>>,
) {
    let mut buffer = [0; 1024 * 14];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => match parse_request(&buffer[..n]) {
                Ok(req) => {
                    let storage = Arc::clone(&storage);
                    let response = process_request(&req, storage, Arc::clone(&message_tx));

                    if let Err(e) = stream.write_all(&response) {
                        let _ =
                            message_tx.send(Message::Error(format!("Writing to tcp Stream: {e}")));
                    }
                }
                Err(err) => {
                    let err = format!("Invalid request: {err}");
                    let _ = message_tx.send(Message::Error(format!("Invalid request: {err}")));

                    if let Err(e) = stream.write_all(err.as_bytes()) {
                        let _ =
                            message_tx.send(Message::Error(format!("Writing to tcp Stream: {e}")));
                    }
                }
            },
            Err(e) => {
                let _ = message_tx.send(Message::Error(format!("Reading from tcp stream: {e}")));
            }
        }
    }
}

#[derive(Debug, Parser)]
struct Cli {
    #[arg(short = 'p', long = "port", help = "Run server in a custom port")]
    port: Option<u16>,
    #[arg(
        short = 's',
        long = "snapshots_interval",
        help = "Flush data to disk in every interval seconds"
    )]
    snapshots_interval: Option<u64>,
}

// FIX: This may misbehave outside of unix environments
fn get_persistent_path(filename: &str) -> anyhow::Result<PathBuf> {
    let home = match env::home_dir() {
        Some(v) => v,
        None => {
            return Err(anyhow::anyhow!(
                "Failed to get $HOME env variable".to_string()
            ));
        }
    };

    let home = Path::new(&home);
    Ok(home.to_path_buf().join(filename))
}

fn get_log_file() -> anyhow::Result<PathBuf> {
    let home = match env::home_dir() {
        Some(v) => v,
        None => {
            return Err(anyhow::anyhow!(
                "Failed to get $HOME env variable".to_string()
            ));
        }
    };

    let home = Path::new(&home);
    Ok(home.to_path_buf().join(".volatix.logs"))
}

pub struct Worker {
    pub id: String,
    pub handle: JoinHandle<()>,
}

fn main() -> anyhow::Result<()> {
    println!("{art}", art = volatix_ascii_art());

    let args = Cli::parse();
    let port: u16 = args.port.unwrap_or(DEFAULT_PORT);

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    println!("Server listening on {addr}");

    // set up log messages handling
    let (message_tx, message_rx) = channel();
    let message_tx = Arc::new(message_tx);
    let log_file = get_log_file()?;

    let mut workers: Vec<Worker> = Vec::new();
    workers.push(Worker {
        id: "logs_handler".into(),
        handle: std::thread::spawn(move || {
            let _ = handle_messages(&log_file, message_rx);
        }),
    });

    // Intialise storage data
    let options = StorageOptions::default();
    let storage: Arc<RwLock<LockedStorage>> =
        Arc::new(parking_lot::RwLock::new(LockedStorage::new(options)));
    let persistent_path = get_persistent_path(".volatix.bin")?;
    {
        // avoid deadlock
        storage.write().load_from_disk(&persistent_path)?;
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_signal: Arc<AtomicBool> = Arc::clone(&shutdown);

    ctrlc::set_handler(move || {
        shutdown_signal.store(true, Ordering::Relaxed);
        println!("Received SIGTERM signal! Shutting down ...");
    })?;

    let snapshots_storage: Arc<RwLock<LockedStorage>> = Arc::clone(&storage);
    let snapshots_persistent_path = persistent_path.clone();
    let snapshots_shutdown = Arc::clone(&shutdown);
    workers.push(Worker {
        id: "snapshots_handler".into(),
        handle: std::thread::spawn(move || {
            let interval = args.snapshots_interval.unwrap_or(SNAPSHOTS_INTERVAL_TIME);
            loop {
                for _ in 0..(interval) / 5 {
                    if snapshots_shutdown.load(Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(Duration::from_secs(5));
                }

                if snapshots_storage.read().should_flush()
                    && snapshots_storage
                        .read()
                        .save_to_disk(&snapshots_persistent_path)
                        .is_ok()
                {
                    snapshots_storage.write().toggle_dirty_flag();
                }
            }
        }),
    });

    let listener_shutdown = Arc::clone(&shutdown);
    let listener_storage = Arc::clone(&storage);
    let listener_message_tx = Arc::clone(&message_tx);
    workers.push(Worker {
        id: "client_handler".into(),
        handle: std::thread::spawn(move || {
            loop {
                if listener_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                match listener.accept() {
                    Ok((client, _)) => {
                        let client_storage = Arc::clone(&listener_storage);
                        let message_tx = Arc::clone(&listener_message_tx);
                        handle_client(client, client_storage, message_tx);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }),
    });

    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    // Kill the logs message handler
    let _res = message_tx.send(Message::Break);

    for worker in workers {
        println!("Cleaning up {}", worker.id);
        let _ = worker.handle.join();
    }

    if storage.read().should_flush() {
        println!("Saving data to disk...");
        storage.read().save_to_disk(&persistent_path)?;
        println!("Complete saving data!");
    }

    Ok(())
}
