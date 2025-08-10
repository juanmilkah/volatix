use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use libvolatix::{LockedStorage, StorageOptions, parse_request, process_request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DEFAULT_PORT: u16 = 7878;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let port: u16 = args.port.unwrap_or(DEFAULT_PORT);

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Server listening on {addr}");

    let options = StorageOptions::default();
    let storage = Arc::new(parking_lot::RwLock::new(LockedStorage::new(options)));
    let persistent_path = "volatix.db";
    {
        // avoid deadlock
        storage.write().load_from_disk(persistent_path)?;
    }

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_sig = shutdown_tx.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await?;
        let _ = shutdown_sig.send(());
        Ok::<(), std::io::Error>(())
    });

    let snapshots_storage = Arc::clone(&storage);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let _ = snapshots_storage.read().save_to_disk(persistent_path);
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
    storage.read().save_to_disk(persistent_path)?;
    println!("Data saved successfully.");

    Ok(())
}
