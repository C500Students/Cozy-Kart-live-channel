use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

const LAP_TABLE: TableDefinition<&str, &str> = TableDefinition::new("cozy_lap_history");
const DEVICE_TABLE: TableDefinition<&str, &str> = TableDefinition::new("cozy_device_profiles");

#[derive(Parser, Debug)]
#[command(
    name = "cozy-cli",
    about = "Cozy-Kart Telemetry, redb Persistence & Hardware Profiler",
    version = "0.4.0"
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
    /// Listen to live telemetry, lap standings, and device handshakes; persist to redb
    Monitor,
    /// Query and display stored lap history
    History,
    /// Query and display player input devices, hardware brands, and network connection profiles
    Profiles,
    /// Export redb telemetry history to JSON or CSV
    Export {
        #[arg(short, long, default_value = "json")]
        format: String,
        #[arg(short, long, default_value = "cozy_stream_export")]
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
    event_type: String, // "LAP", "HANDSHAKE", "TELEMETRY"
    driver: String,
    #[serde(default)]
    lap: u32,
    #[serde(default)]
    position: u8,
    #[serde(default)]
    input_device: String, // e.g., "Gamepad: Xbox Wireless", "Keyboard: Wooting 60HE"
    #[serde(default)]
    brand: String,        // e.g., "Microsoft", "Wooting", "Logitech"
    #[serde(default)]
    connection_type: String, // e.g., "Ethernet", "Wi-Fi 6", "Cellular 5G"
    #[serde(default)]
    latency_ms: u32,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    // Initialize local redb database with both tables
    let db = Database::create(&args.db)?;
    let create_txn = db.begin_write()?;
    {
        create_txn.open_table(LAP_TABLE)?;
        create_txn.open_table(DEVICE_TABLE)?;
    }
    create_txn.commit()?;

    match args.command {
        Commands::Monitor => {
            let url = Url::parse(&args.server)?;
            println!("Connecting to Cozy-Kart telemetry server at {}...", url);
            let (ws_stream, _) = connect_async(url).await?;
            let (_, mut read) = ws_stream.split();

            println!("Listening and recording telemetry to redb ('{}')... Press Ctrl+C to exit.", args.db);

            while let Some(msg) = read.next().await {
                match msg? {
                    Message::Text(text) => {
                        match serde_json::from_str::<TelemetryPacket>(&text) {
                            Ok(packet) => {
                                match packet.event_type.as_str() {
                                    "HANDSHAKE" => {
                                        println!(
                                            "[HARDWARE] Driver: {:<12} | Device: {} ({}) | Net: {} | Ping: {}ms",
                                            packet.driver, packet.input_device, packet.brand, packet.connection_type, packet.latency_ms
                                        );

                                        // Store device profile in redb (keyed by driver name for latest profile)
                                        let db_clone = db.clone();
                                        let p_clone = packet.clone();
                                        tokio::task::spawn_blocking(move || -> Result<(), redb::Error> {
                                            let write_txn = db_clone.begin_write()?;
                                            {
                                                let mut table = write_txn.open_table(DEVICE_TABLE)?;
                                                let val = serde_json::to_string(&p_clone).unwrap();
                                                table.insert(p_clone.driver.as_str(), val.as_str())?;
                                            }
                                            write_txn.commit()?;
                                            Ok(())
                                        }).await??;
                                    }
                                    _ => {
                                        println!(
                                            "[LAP {}] Driver: {:<12} | Position: #{} | Event: {}",
                                            packet.lap, packet.driver, packet.position, packet.event_type
                                        );

                                        // Store lap event in redb
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
                                }
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
        Commands::Profiles => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(DEVICE_TABLE)?;

            println!("{:<14} | {:<24} | {:<12} | {:<14} | {:<8}", "DRIVER", "INPUT DEVICE", "BRAND", "CONNECTION", "LATENCY");
            println!("{}", "-".repeat(78));

            let mut count = 0;
            for entry in table.iter()? {
                let (_, val) = entry?;
                if let Ok(packet) = serde_json::from_str::<TelemetryPacket>(val.value()) {
                    println!(
                        "{:<14} | {:<24} | {:<12} | {:<14} | {}ms",
                        packet.driver, packet.input_device, packet.brand, packet.connection_type, packet.latency_ms
                    );
                    count += 1;
                }
            }
            println!("{}", "-".repeat(78));
            println!("Total tracked player hardware profiles in redb: {}", count);
        }
        Commands::History => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(LAP_TABLE)?;

            println!("{:<22} | {:<8} | {:<12} | {:<8} | {:<15}", "TIMESTAMP", "LAP", "DRIVER", "POS", "EVENT");
            println!("{}", "-".repeat(75));

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
                }
            }
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

            if format.to_lowercase() == "json" {
                let filename = format!("{}.json", output);
                let file = std::fs::File::create(&filename)?;
                serde_json::to_writer_pretty(file, &packets)?;
                println!("Exported {} packets to '{}'", packets.len(), filename);
            } else {
                eprintln!("Supported export format: json");
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
            println!("Broadcast sent: '{}'", message);
        }
    }

    Ok(())
}
