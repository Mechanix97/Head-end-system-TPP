/// Mock IoT device for testing the HES periodic session flow.
///
/// Registers with the HES backdoor and then keeps running as a UDP server,
/// handling periodic sessions initiated by the HES (HANDSHAKE → READ → WRITE → ACK).
/// Designed to be used with `make run` (which enables --test-mode, connecting every 5 min).
///
/// Usage:
///   cargo run -p mock_device -- --backdoor-ip 127.0.0.1
///   cargo run -p mock_device -- --backdoor-ip 127.0.0.1 --device-port 6060 --imei 000000000000001
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use clap::Parser;
use tokio::net::UdpSocket;
use tokio::time::{sleep, timeout};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{Level, info, warn};

use common::messages::codec::MessageCodec;
use common::messages::message::{Message, MessagePayload};
use common::messages::read::ObisValue;

const OBIS_WATER_VOLUME: &str = "1.0.1";
const OBIS_BATTERY: &str = "C.6.1";
const OBIS_CLOCK: &str = "0.9.4";
const SESSION_RECV_TIMEOUT_SECS: u64 = 60;

#[derive(Parser)]
#[command(about = "Mock IoT device for HES test-mode sessions")]
struct Args {
    /// HES backdoor IP
    #[arg(long = "backdoor-ip", default_value = "127.0.0.1")]
    backdoor_ip: String,

    /// HES backdoor port
    #[arg(long = "backdoor-port", default_value = "6565")]
    backdoor_port: String,

    /// UDP port this device listens on for HES periodic sessions (must match DEVICE_UDP_PORT in HES)
    #[arg(long = "device-port", default_value = "6060")]
    device_port: u16,

    /// IMEI to use for registration (15 digits)
    #[arg(long = "imei", default_value = "000000000000001")]
    imei: String,

    /// Battery level to report (0–100)
    #[arg(long = "battery", default_value = "85")]
    battery: u8,

    /// Initial water volume in liters
    #[arg(long = "initial-volume", default_value = "12340")]
    initial_volume: u64,

    /// Liters to add per session (simulates consumption)
    #[arg(long = "liters-per-session", default_value = "100")]
    liters_per_session: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Bind on the device port so the HES can reach us later.
    // We send the registration FROM this same socket, so the HES records the correct source IP.
    let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{}", args.device_port)).await?);
    info!("Device socket bound on 0.0.0.0:{}", args.device_port);

    let backdoor_addr = format!("{}:{}", args.backdoor_ip, args.backdoor_port);
    let device_id = register(&socket, &backdoor_addr, &args.imei).await?;

    info!(
        "Registered as device 0x{:x}. Listening for sessions on port {}.",
        device_id, args.device_port
    );

    let volume = Arc::new(AtomicU64::new(args.initial_volume));
    let mut session_count: u32 = 0;

    loop {
        info!("Waiting for next session from HES...");
        match handle_session(
            &socket,
            device_id,
            &volume,
            args.battery,
            args.liters_per_session,
        )
        .await
        {
            Ok(()) => {
                session_count += 1;
                info!(
                    "Session {} done. Total volume: {} L",
                    session_count,
                    volume.load(Ordering::Relaxed)
                );
            }
            Err(e) => {
                warn!("Session error: {}. Waiting for next session...", e);
                // Brief pause to avoid busy-looping on persistent errors
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// Registers the device with the HES backdoor.
/// Sends REGISTER_REQUEST and waits for REGISTER_RESPONSE, then ACKs.
async fn register(
    socket: &UdpSocket,
    backdoor_addr: &str,
    imei: &str,
) -> Result<u128, Box<dyn Error>> {
    let mut buf = BytesMut::new();

    let register_req =
        Message::new_register_request_message(imei.to_string(), "fe80::1".to_string())?;
    MessageCodec.encode(register_req, &mut buf)?;
    socket.send_to(&buf, backdoor_addr).await?;
    info!("REGISTER_REQUEST sent to {}", backdoor_addr);

    // Wait for REGISTER_RESPONSE (with timeout)
    let mut raw = [0u8; 4096];
    let (n, _from) = timeout(Duration::from_secs(10), socket.recv_from(&mut raw))
        .await
        .map_err(|_| "Timed out waiting for REGISTER_RESPONSE")??;

    let mut buf = BytesMut::from(&raw[..n]);
    let response = MessageCodec
        .decode(&mut buf)?
        .ok_or("Empty REGISTER_RESPONSE")?;

    let device_id = response.device_id;
    info!("REGISTER_RESPONSE received. Device ID: 0x{:x}", device_id);

    // ACK the registration
    buf = BytesMut::new();
    let ack = Message::new_ack_message(device_id, response.seq + 1)?;
    MessageCodec.encode(ack, &mut buf)?;
    socket.send_to(&buf, backdoor_addr).await?;
    info!("ACK sent to backdoor");

    // Consume the backdoor's confirmation ACK (double handshake) so it
    // doesn't pollute the socket buffer when handle_session starts listening.
    let mut raw = [0u8; 4096];
    let (n, _) = timeout(Duration::from_secs(5), socket.recv_from(&mut raw))
        .await
        .map_err(|_| "Timed out waiting for registration confirmation ACK")??;
    let mut buf = BytesMut::from(&raw[..n]);
    let confirm = MessageCodec
        .decode(&mut buf)?
        .ok_or("Empty registration confirmation")?;
    info!("Registration confirmed by backdoor (seq={})", confirm.seq);

    Ok(device_id)
}

/// Handles one full periodic session initiated by the HES.
///
/// Waits for HANDSHAKE, responds with HANDSHAKE_RESPONSE, handles READ and WRITE,
/// and waits for the final ACK that closes the session.
async fn handle_session(
    socket: &UdpSocket,
    device_id: u128,
    volume: &Arc<AtomicU64>,
    battery: u8,
    liters_per_session: u64,
) -> Result<(), Box<dyn Error>> {
    let mut seq: u32 = 0;

    // Wait for HANDSHAKE (blocking; HES connects when the cron job fires)
    let (msg, hes_addr) = recv_from(socket, SESSION_RECV_TIMEOUT_SECS).await?;

    let MessagePayload::Handshake(_) = msg.payload else {
        return Err(format!("Expected HANDSHAKE, got {}", msg.msg_type.as_str()).into());
    };
    info!("HANDSHAKE received from {}", hes_addr);

    // HANDSHAKE_RESPONSE
    send_to(
        socket,
        Message::new_handshake_response_message(device_id, seq, 0)?,
        hes_addr,
    )
    .await?;
    seq += 1;
    info!("HANDSHAKE_RESPONSE sent");

    // READ_REQUEST
    let (msg, _) = recv_from(socket, SESSION_RECV_TIMEOUT_SECS).await?;
    let MessagePayload::ReadRequest(req) = msg.payload else {
        return Err(format!("Expected READ_REQUEST, got {}", msg.msg_type.as_str()).into());
    };
    info!("READ_REQUEST received. OBIS codes: {:?}", req.obis_codes);

    // Build READ_RESPONSE: respond to whatever OBIS codes the HES asked for
    let current_volume = volume.load(Ordering::Relaxed);
    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let values: Vec<ObisValue> = req
        .obis_codes
        .iter()
        .map(|code| match code.as_str() {
            OBIS_WATER_VOLUME => ObisValue {
                code: code.clone(),
                value: current_volume.to_be_bytes().to_vec(),
            },
            OBIS_BATTERY => ObisValue {
                code: code.clone(),
                value: vec![battery],
            },
            OBIS_CLOCK => ObisValue {
                code: code.clone(),
                value: now_ts.to_be_bytes().to_vec(),
            },
            _ => ObisValue {
                code: code.clone(),
                value: vec![],
            },
        })
        .collect();

    send_to(
        socket,
        Message::new_read_response_message(device_id, seq, values)?,
        hes_addr,
    )
    .await?;
    seq += 1;
    info!(
        "READ_RESPONSE sent (volume={} L, battery={}%, clock={})",
        current_volume, battery, now_ts
    );

    // WRITE_REQUEST
    let (msg, _) = recv_from(socket, SESSION_RECV_TIMEOUT_SECS).await?;
    let MessagePayload::WriteRequest(req) = msg.payload else {
        return Err(format!("Expected WRITE_REQUEST, got {}", msg.msg_type.as_str()).into());
    };
    let written_codes: Vec<String> = req.parameters.iter().map(|p| p.code.clone()).collect();
    info!("WRITE_REQUEST received. Parameters: {:?}", written_codes);

    send_to(
        socket,
        Message::new_write_response_message(device_id, seq, true, written_codes)?,
        hes_addr,
    )
    .await?;
    info!("WRITE_RESPONSE sent");

    // ACK (session close from HES)
    let (msg, _) = recv_from(socket, SESSION_RECV_TIMEOUT_SECS).await?;
    let MessagePayload::Ack = msg.payload else {
        return Err(format!("Expected ACK, got {}", msg.msg_type.as_str()).into());
    };
    info!("ACK received — session closed");

    // Simulate consumption for next session
    volume.fetch_add(liters_per_session, Ordering::Relaxed);

    Ok(())
}

async fn send_to(socket: &UdpSocket, msg: Message, addr: SocketAddr) -> Result<(), Box<dyn Error>> {
    let mut buf = BytesMut::new();
    MessageCodec.encode(msg, &mut buf)?;
    socket.send_to(&buf, addr).await?;
    Ok(())
}

async fn recv_from(
    socket: &UdpSocket,
    timeout_secs: u64,
) -> Result<(Message, SocketAddr), Box<dyn Error>> {
    let mut raw = [0u8; 4096];
    let (n, from) = timeout(
        Duration::from_secs(timeout_secs),
        socket.recv_from(&mut raw),
    )
    .await
    .map_err(|_| format!("Timed out after {timeout_secs}s waiting for message"))??;

    let mut buf = BytesMut::from(&raw[..n]);
    let msg = MessageCodec
        .decode(&mut buf)?
        .ok_or("Empty or incomplete message received")?;

    Ok((msg, from))
}
