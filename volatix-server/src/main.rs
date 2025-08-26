use std::{
    env,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use libvolatix::{LockedStorage, StorageOptions, ascii_art, parse_request, process_request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// start the server backend on this port
const DEFAULT_PORT: u16 = 7878;
// FLush snapshots to disk in this interval
const SNAPSHOTS_INTERVAL_TIME: u64 = 60 * 5; // In seconds

async fn handle_client(
    mut stream: tokio::net::TcpStream,
    storage: Arc<parking_lot::RwLock<LockedStorage>>,
) {
    // pre-allocating a higher value may lead to
    // the tokio worker overflowing it's stack
    let mut buffer = [0; 1024];

    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => match parse_request(&buffer[..n]) {
                Ok(req) => {
                    let storage = Arc::clone(&storage);
                    let response = process_request(&req, storage);

                    if let Err(e) = stream.write_all(&response).await {
                        eprintln!("ERROR: {e}");
                    }
                }
                Err(err) => {
                    let err = format!("Error parsing request: {err}");
                    if let Err(e) = stream.write_all(err.as_bytes()).await {
                        eprintln!("ERROR: {e}");
                    }
                }
            },
            Err(e) => eprintln!("ERROR: {e}"),
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("{art}", art = ascii_art());

    let args = Cli::parse();
    let port: u16 = args.port.unwrap_or(DEFAULT_PORT);

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Server listening on {addr}");

    // Intialise storage data
    let options = StorageOptions::default();
    let storage = Arc::new(parking_lot::RwLock::new(LockedStorage::new(options)));
    let persistent_path = get_persistent_path("volatix.db")?;
    {
        // avoid deadlock
        storage.write().load_from_disk(&persistent_path)?;
    }

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_sig = shutdown_tx.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await?;
        let _ = shutdown_sig.send(());
        Ok::<(), std::io::Error>(())
    });

    let snapshots_storage = Arc::clone(&storage);
    let snapshots_persistent_path = persistent_path.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(
            args.snapshots_interval.unwrap_or(SNAPSHOTS_INTERVAL_TIME),
        ))
        .await;
        let _ = snapshots_storage
            .read()
            .save_to_disk(&snapshots_persistent_path);
    });

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() =>{
                println!("Received Shutdown Signal!");
                break;
            },
            Ok((socket, _)) = listener.accept() =>{
                let storage = Arc::clone(&storage);
                tokio::spawn(async move{
                    handle_client(socket, storage).await;
                });
            }
        }
    }

    println!("Saving data to disk...");
    storage.read().save_to_disk(&persistent_path)?;
    println!("Data saved successfully.");

    Ok(())
}
