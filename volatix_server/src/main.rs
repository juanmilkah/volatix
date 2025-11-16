use std::{
    env,
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender, channel},
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

enum Response {
    Data { inner: Vec<u8>, stream: TcpStream },
    Break,
}

fn write_to_stream(response_rx: Receiver<Response>, message_tx: Arc<Sender<Message>>) {
    loop {
        if let Ok(r) = response_rx.recv() {
            match r {
                Response::Data { inner, mut stream } => {
                    if let Err(e) = stream.write_all(&inner) {
                        let _ =
                            message_tx.send(Message::Error(format!("Writing to tcp Stream: {e}")));
                    }
                }
                Response::Break => break,
            }
        }
    }
}

enum Task {
    Process { data: Vec<u8>, stream: TcpStream },
    Break,
}

fn read_from_stream(
    mut stream: TcpStream,
    message_tx: Arc<Sender<Message>>,
    task_tx: Arc<Sender<Task>>,
) {
    let mut buffer = vec![0u8; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break, // Client disconnected
            Ok(n) => {
                let data = buffer[..n].to_vec();

                // Clone stream for response
                match stream.try_clone() {
                    Ok(stream_clone) => {
                        let _ = task_tx.send(Task::Process {
                            data,
                            stream: stream_clone,
                        });
                    }
                    Err(e) => {
                        let _ = message_tx.send(Message::Error(e.to_string()));
                        break;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            Err(e) => {
                let _ = message_tx.send(Message::Error(e.to_string()));
                break;
            }
        }
    }
}

fn task_handler(
    task_rx: Receiver<Task>,
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
    message_tx: Arc<Sender<Message>>,
    response_tx: Arc<Sender<Response>>,
) {
    loop {
        if let Ok(t) = task_rx.recv() {
            match t {
                Task::Process { data, stream } => match parse_request(&data) {
                    Ok(req) => {
                        let storage = Arc::clone(&storage);
                        let response = process_request(&req, storage, Arc::clone(&message_tx));

                        let _ = response_tx.send(Response::Data {
                            inner: response,
                            stream,
                        });
                    }
                    Err(err) => {
                        let err = format!("Invalid request: {err}");
                        let _ = message_tx.send(Message::Error(format!("Invalid request: {err}")));

                        let _ = response_tx.send(Response::Data {
                            inner: err.as_bytes().to_vec(),
                            stream,
                        });
                    }
                },
                Task::Break => break,
            }
        }
    }
}

fn snapshots_handler(
    interval: u64,
    storage: Arc<RwLock<LockedStorage>>,
    shutdown: Arc<AtomicBool>,
    path: &PathBuf,
) {
    loop {
        for _ in 0..(interval) / 5 {
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(Duration::from_secs(5));
        }

        if storage.read().should_flush() && storage.read().save_to_disk(path).is_ok() {
            storage.write().toggle_dirty_flag();
        }
    }
}

fn client_handler(
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
    message_tx: Arc<Sender<Message>>,
    task_tx: Arc<Sender<Task>>,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        match listener.accept() {
            Ok((client, _)) => {
                let message_tx = Arc::clone(&message_tx);
                let task_tx = Arc::clone(&task_tx);
                std::thread::spawn(move || read_from_stream(client, message_tx, task_tx));
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(_) => {
                continue;
            }
        }
    }
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
            snapshots_handler(
                interval,
                snapshots_storage,
                snapshots_shutdown,
                &snapshots_persistent_path,
            );
        }),
    });

    let (task_tx, task_rx) = channel::<Task>();
    let task_tx = Arc::new(task_tx);

    let listener_shutdown = Arc::clone(&shutdown);
    let listener_message_tx = Arc::clone(&message_tx);
    let listener_task_tx = Arc::clone(&task_tx);

    workers.push(Worker {
        id: "client_handler".into(),
        handle: std::thread::spawn(move || {
            client_handler(
                listener,
                listener_shutdown,
                listener_message_tx,
                listener_task_tx,
            );
        }),
    });

    let (response_tx, response_rx) = channel::<Response>();
    let response_tx = Arc::new(response_tx);

    let handler_storage = Arc::clone(&storage);
    let handler_message_tx = Arc::clone(&message_tx);
    let handler_res_tx = Arc::clone(&response_tx);
    workers.push(Worker {
        id: "task_handler".into(),
        handle: std::thread::spawn(move || {
            task_handler(task_rx, handler_storage, handler_message_tx, handler_res_tx)
        }),
    });

    let responder_message_tx = Arc::clone(&message_tx);
    workers.push(Worker {
        id: "response_writer".into(),
        handle: std::thread::spawn(move || write_to_stream(response_rx, responder_message_tx)),
    });

    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    // Kill the logs message handler
    let _ = message_tx.send(Message::Break);
    let _ = response_tx.send(Response::Break);
    let _ = task_tx.send(Task::Break);

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
