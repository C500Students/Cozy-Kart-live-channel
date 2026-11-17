use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

#[derive(Parser, Debug)]
#[command(name = "cozy-cli", about = "Cozy-Kart Live Telemetry & Session CLI", version = "0.1.0")]
struct Args {
    #[arg(short, long, default_value = "ws://localhost:8080/ws")]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Listen to live race telemetry and lap standings
    Monitor,
    /// Send a custom race event or banner broadcast to the stream overlay
    Broadcast {
        #[arg(short, long)]
        message: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
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
    let url = Url::parse(&args.server)?;

    println!("Connecting to Cozy-Kart telemetry server at {}...", url);
    let (ws_stream, _response) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    match args.command {
      Commands::Monitor => {
        println!("Listening for live race telemetry... Press Ctrl+C to exit.");
        while let Some(msg) = read.next().await {
    match msg? {
      Message::Text(text) => {
          match serde_json::from_str::<TelemetryPacket>(&text) {
          Ok(packet) => {
    println!(
     "[LAP {}] Driver: {:<12} | Position: #{} | Event: {}",
    packet.lap, 
    packet.driver, 
    packet.position, 
    packet.event_type
                                );
                            }
  Err(_) => {
      // Fallback for raw text ticks / broadcast messages
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
        Commands::Broadcast { message } => {
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
