// hes-cli: Interactive JSON-RPC console for the HES (Head-End System).
//
// Usage:
//   hes-cli [--host 127.0.0.1] [--port 6600]
//
// Commands:
//   config list
//   config get <key>
//   config set <key> <value>
//   config propagate <key> <value>
//   cluster peers
//   cluster status
//   device list [--limit N] [--offset N]
//   device info <uuid>
//   device delegate <device_id> <target_node_id>
//   version
//   help
//   exit / quit

use clap::Parser;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::warn;

#[derive(Parser, Debug)]
#[command(name = "hes-cli", about = "Interactive admin console for HES")]
struct Args {
    /// HES node host
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// HES JSON-RPC port
    #[arg(long, default_value = "6600")]
    port: u16,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let addr = format!("{}:{}", args.host, args.port);
    println!("Connecting to HES at {addr}...");

    let stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to {addr}: {e}");
            std::process::exit(1);
        }
    };

    println!("Connected. Type 'help' for commands, 'exit' to quit.");

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let mut rl = match DefaultEditor::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to initialize readline: {e}");
            std::process::exit(1);
        }
    };

    let mut next_id: u64 = 1;
    let mut response_line = String::new();

    loop {
        let readline = rl.readline("hes> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(trimmed);

                if trimmed == "exit" || trimmed == "quit" {
                    break;
                }

                if trimmed == "help" {
                    print_help();
                    continue;
                }

                let rpc_req = match parse_command(trimmed, next_id) {
                    Some(r) => r,
                    None => {
                        eprintln!("Unknown command. Type 'help' for usage.");
                        continue;
                    }
                };

                // Send request
                let mut req_bytes = match serde_json::to_vec(&rpc_req) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Serialization error: {e}");
                        continue;
                    }
                };
                req_bytes.push(b'\n');

                if let Err(e) = write_half.write_all(&req_bytes).await {
                    eprintln!("Write error: {e}");
                    break;
                }

                // Read response
                response_line.clear();
                match reader.read_line(&mut response_line).await {
                    Ok(0) => {
                        eprintln!("Connection closed by server.");
                        break;
                    }
                    Ok(_) => {
                        pretty_print_response(&response_line);
                    }
                    Err(e) => {
                        eprintln!("Read error: {e}");
                        break;
                    }
                }

                next_id += 1;
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                break;
            }
            Err(e) => {
                warn!("Readline error: {e}");
                break;
            }
        }
    }
}

/// Parses a user-friendly command into a JSON-RPC request Value.
fn parse_command(input: &str, id: u64) -> Option<Value> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    let (method, params) = match parts.as_slice() {
        ["version"] => ("system.version", json!({})),

        ["config", "list"] => ("config.list", json!({})),
        ["config", "get", key] => ("config.get", json!({ "key": key })),
        ["config", "set", key, value] => ("config.set", json!({ "key": key, "value": value })),
        ["config", "propagate", key, value] => {
            ("config.propagate", json!({ "key": key, "value": value }))
        }

        ["cluster", "peers"] => ("cluster.peers", json!({})),
        ["cluster", "status"] => ("cluster.status", json!({})),

        // device list [--limit N] [--offset N]
        parts if parts.first() == Some(&"device") && parts.get(1) == Some(&"list") => {
            let mut limit = 100u64;
            let mut offset = 0u64;
            let mut i = 2;
            while i < parts.len() {
                if parts[i] == "--limit" && i + 1 < parts.len() {
                    limit = parts[i + 1].parse().unwrap_or(100);
                    i += 2;
                } else if parts[i] == "--offset" && i + 1 < parts.len() {
                    offset = parts[i + 1].parse().unwrap_or(0);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            ("device.list", json!({ "limit": limit, "offset": offset }))
        }

        ["device", "info", uuid] => ("device.info", json!({ "device_id": uuid })),
        ["device", "delegate", device_id, target_node_id] => (
            "device.delegate",
            json!({ "device_id": device_id, "target_node_id": target_node_id }),
        ),

        _ => return None,
    };

    Some(json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": id,
    }))
}

fn pretty_print_response(raw: &str) {
    match serde_json::from_str::<Value>(raw.trim()) {
        Ok(val) => {
            if let Some(result) = val.get("result") {
                // Pretty print the result
                match serde_json::to_string_pretty(result) {
                    Ok(s) => println!("{s}"),
                    Err(_) => println!("{result}"),
                }
            } else if let Some(error) = val.get("error") {
                let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
                let msg = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error");
                eprintln!("Error [{code}]: {msg}");
            } else {
                println!("{val}");
            }
        }
        Err(_) => println!("{}", raw.trim()),
    }
}

fn print_help() {
    println!(
        r#"Available commands:
  version                              Show server version and node ID
  config list                          List all config keys and values
  config get <key>                     Get a config value
  config set <key> <value>             Set a config value (persists to disk)
  config propagate <key> <value>       Set locally + broadcast to cluster
  cluster peers                        List cluster peers
  cluster status                       Cluster summary (nodes, devices)
  device list [--limit N] [--offset N] List devices owned by this node
  device info <uuid>                   Show device details
  device delegate <did> <nid>          Delegate device to another node
  help                                 Show this help
  exit / quit                          Disconnect"#
    );
}
