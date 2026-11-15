use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::io::Write;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

const LAP_TABLE: TableDefinition<&str, &str> = TableDefinition::new("cozy_lap_history");

#[derive(Parser, Debug)]
#[command(
    name = "cozy-cli",
    about = "Cozy-Kart Live Telemetry & redb Persistence CLI",
    version = "0.3.0"
)]
struct Args {
    #[arg(short, long, default_value = "ws://localhost:8080/ws")]
    server: String,

    #[arg(short, long, default_value = "cozy_stream.redb")]
    db: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Listen to live telemetry, print to console, and persist to redb
    Monitor,
    /// Query and display stored lap history from the local redb database
    History,
    /// Export redb lap history to a JSON or CSV file for post-stream analysis
    Export {
        /// Output format: json or csv
        #[arg(short, long, default_value = "json")]
        format: String,
        /// Output file path prefix (without extension)
        #[arg(short, long, default_value = "cozy_lap_history")]
        output: String,
    },
    /// Send a custom race event or banner broadcast to the stream overlay
    Broadcast {
        #[arg(short, long)]
        message: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TelemetryPacket {
    event_type: String,
    driver: String,
    lap: u32,
    position: u8,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    // Initialize local redb database
    let db = Database::create(&args.db)?;
    let create_txn = db.begin_write()?;
    {
        create_txn.open_table(LAP_TABLE)?;
    }
    create_txn.commit()?;

    match args.command {
        Commands::Monitor => {
            let url = Url::parse(&args.server)?;
            println!("Connecting to Cozy-Kart telemetry server at {}...", url);
            let (ws_stream, _) = connect_async(url).await?;
            let (_, mut read) = ws_stream.split();

            println!("Listening and recording to redb ('{}')... Press Ctrl+C to exit.", args.db);

            while let Some(msg) = read.next().await {
                match msg? {
                    Message::Text(text) => {
                        match serde_json::from_str::<TelemetryPacket>(&text) {
                            Ok(packet) => {
                                println!(
                                    "[LAP {}] Driver: {:<12} | Position: #{} | Event: {}",
                                    packet.lap, packet.driver, packet.position, packet.event_type
                                );

                                // Store in redb
                                let db_clone = db.clone();
                                let p_clone = packet.clone();
                                tokio::task::spawn_blocking(move || -> Result<(), redb::Error> {
                                    let write_txn = db_clone.begin_write()?;
                                    {
                                        let mut table = write_txn.open_table(LAP_TABLE)?;
                                        let key = format!("{:013}_{}", p_clone.timestamp, p_clone.driver);
                                        let val = serde_json::to_string(&p_clone).unwrap();
                                        table.insert(key.as_str(), val.as_str())?;
                                    }
                                    write_txn.commit()?;
                                    Ok(())
                                }).await??;
                            }
                            Err(_) => {
                                println!("[RAW STREAM] {}", text);
                            }
                        }
                    }
                    Message::Close(_) => {
                        println!("WebSocket connection closed by server.");
                        break;
                    }
                    _ => {}
                }
            }
        }
        Commands::History => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(LAP_TABLE)?;

            println!("{:<22} | {:<8} | {:<12} | {:<8} | {:<15}", "TIMESTAMP", "LAP", "DRIVER", "POS", "EVENT");
            println!("{}", "-".repeat(75));

            let mut count = 0;
            for entry in table.iter()? {
                let (_, val) = entry?;
                if let Ok(packet) = serde_json::from_str::<TelemetryPacket>(val.value()) {
                    let dt = DateTime::<Utc>::from_timestamp((packet.timestamp / 1000) as i64, 0)
                        .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "Unknown".to_string());

                    println!(
                        "{:<22} | {:<8} | {:<12} | #{:<7} | {:<15}",
                        dt, packet.lap, packet.driver, packet.position, packet.event_type
                    );
                    count += 1;
                }
            }
            println!("{}", "-".repeat(75));
            println!("Total recorded telemetry events in redb: {}", count);
        }
        Commands::Export { format, output } => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(LAP_TABLE)?;

            let mut packets = Vec::new();
            for entry in table.iter()? {
                let (_, val) = entry?;
                if let Ok(packet) = serde_json::from_str::<TelemetryPacket>(val.value()) {
                    packets.push(packet);
                }
            }

            let format_lower = format.to_lowercase();
            if format_lower == "json" {
                let filename = format!("{}.json", output);
                let file = std::fs::File::create(&filename)?;
                serde_json::to_writer_pretty(file, &packets)?;
                println!("Successfully exported {} telemetry packets to '{}'", packets.len(), filename);
            } else if format_lower == "csv" {
                let filename = format!("{}.csv", output);
                let mut wtr = std::fs::File::create(&filename)?;
                writeln!(wtr, "timestamp,driver,lap,position,event_type")?;
                for p in &packets {
                    writeln!(
                        wtr,
                        "{},{},{},{},{}",
                        p.timestamp, p.driver, p.lap, p.position, p.event_type
                    )?;
                }
                println!("Successfully exported {} telemetry packets to '{}'", packets.len(), filename);
            } else {
                eprintln!("Unsupported export format '{}'. Choose either 'json' or 'csv'.", format);
            }
        }
        Commands::Broadcast { message } => {
            let url = Url::parse(&args.server)?;
            let (ws_stream, _) = connect_async(url).await?;
            let (mut write, _) = ws_stream.split();

            let payload = serde_json::json!({
                "event_type": "CLI_BROADCAST",
                "message": message,
            });
            write.send(Message::Text(payload.to_string())).await?;
            println!("Successfully broadcast message to stream overlay: '{}'", message);
        }
    }

    Ok(())
}
