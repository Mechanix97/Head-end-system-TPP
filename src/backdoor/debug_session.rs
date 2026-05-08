use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use chrono::Utc;
use common::database::api::Database;
use common::messages::codec::MessageCodec;
use common::messages::message::{Message, MessagePayload, MsgType};
use common::messages::write::WriteParameter;
use device_manager::DeviceManager;
use tokio::net::UdpSocket;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, timeout};
use tokio_util::codec::Encoder;
use tracing::{info, warn};
use uuid::Uuid;

use crate::BackdoorError;

const RESPONSE_TIMEOUT_SECS: u64 = 30;
const BATTERY_READ_INTERVAL_DAYS: i64 = 7;
const OBIS_WATER_VOLUME: &str = "1.0.1";
const OBIS_BATTERY: &str = "C.6.1";
const OBIS_CLOCK: &str = "0.9.4";
const OBIS_NEXT_WAKE: &str = "0.0.1";

/// Routes inbound messages from a specific device address to the debug-session
/// task currently driving that conversation.
pub type DebugRouter = Arc<RwLock<HashMap<SocketAddr, mpsc::UnboundedSender<Message>>>>;

pub fn new_router() -> DebugRouter {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Called by the backdoor main loop before the normal match.
/// Returns `true` if the message was forwarded to a waiting debug session
/// (the main loop should then skip normal handling).
///
/// Only session-response types are forwarded. Registration messages (RegisterRequest,
/// Ack, SessionStartRequest) always fall through to normal backdoor handling so that
/// stale ACKs from the preceding registration flow don't corrupt the session.
pub async fn try_route(router: &DebugRouter, addr: SocketAddr, msg: &Message) -> bool {
    match msg.msg_type {
        MsgType::HandshakeResponse | MsgType::ReadResponse | MsgType::WriteResponse => {}
        _ => return false,
    }
    let guard = router.read().await;
    if let Some(tx) = guard.get(&addr) {
        let _ = tx.send(msg.clone());
        return true;
    }
    false
}

/// Entry point invoked from the backdoor when a `SessionStartRequest` arrives.
pub async fn handle_session_start(
    socket: Arc<UdpSocket>,
    msg: Message,
    addr: SocketAddr,
    database: Database,
    device_manager: Arc<RwLock<DeviceManager>>,
    router: DebugRouter,
) -> Result<(), BackdoorError> {
    let device_id = Uuid::from_u128(msg.device_id);

    // Cancel pending scheduler job so the normal path doesn't race with us.
    match device_manager
        .write()
        .await
        .cancel_wakeup_job(device_id)
        .await
    {
        Ok(true) => info!("[debug] cancelled wake-up job for device {device_id}"),
        Ok(false) => info!("[debug] no pending job to cancel for device {device_id}"),
        Err(e) => warn!("[debug] failed to cancel job for device {device_id}: {e}"),
    }

    // Register inbound channel so subsequent packets from this addr are forwarded here.
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    router.write().await.insert(addr, tx);

    let result = run_session_over_backdoor(&socket, addr, device_id, &mut rx, &database).await;

    // Always clean up the router entry, then reschedule the next daily wake.
    router.write().await.remove(&addr);

    if let Err(e) = device_manager
        .write()
        .await
        .schedule_next_wakeup(device_id)
        .await
    {
        warn!("[debug] failed to reschedule next wakeup for device {device_id}: {e}");
    } else {
        info!("[debug] next wake scheduled for device {device_id}");
    }

    result
}

async fn run_session_over_backdoor(
    socket: &UdpSocket,
    addr: SocketAddr,
    device_id: Uuid,
    rx: &mut mpsc::UnboundedReceiver<Message>,
    database: &Database,
) -> Result<(), BackdoorError> {
    let device_id_u128 = device_id.as_u128();
    let mut seq: u32 = 0;

    let needs_battery = match database.get_scheduled_connection(device_id).await {
        Ok(conn) => match conn.last_battery_read {
            None => true,
            Some(last) => {
                (Utc::now().naive_utc() - last).num_days() >= BATTERY_READ_INTERVAL_DAYS
            }
        },
        Err(_) => true,
    };

    // HANDSHAKE
    // TODO: replace with cryptographically secure random nonce
    send(
        socket,
        addr,
        Message::new_handshake_message(device_id_u128, seq, vec![0u8; 32])?,
    )
    .await?;
    info!("[debug] HANDSHAKE sent to {addr}");
    seq += 1;

    // HANDSHAKE_RESPONSE
    let resp = recv(rx).await?;
    let got = resp.msg_type.as_str();
    let MessagePayload::HandshakeResponse(_) = resp.payload else {
        return Err(BackdoorError::ParseError(format!(
            "expected handshake_response, got {got}"
        )));
    };
    info!("[debug] HANDSHAKE_RESPONSE received");

    // READ_REQUEST
    let mut obis_codes = vec![OBIS_WATER_VOLUME.to_string(), OBIS_CLOCK.to_string()];
    if needs_battery {
        obis_codes.push(OBIS_BATTERY.to_string());
        info!("[debug] including battery OBIS in READ_REQUEST");
    }
    send(
        socket,
        addr,
        Message::new_read_request_message(device_id_u128, seq, obis_codes)?,
    )
    .await?;
    seq += 1;

    // READ_RESPONSE
    let resp = recv(rx).await?;
    let got = resp.msg_type.as_str();
    let MessagePayload::ReadResponse(data) = resp.payload else {
        return Err(BackdoorError::ParseError(format!(
            "expected read_response, got {got}"
        )));
    };
    info!("[debug] READ_RESPONSE received ({} values)", data.values.len());
    // TODO: persist data.values to database (water volume, clock)

    // WRITE_REQUEST: tell device when to wake next.
    let next_wake_ts = (Utc::now() + chrono::Duration::days(1)).timestamp() as u64;
    let parameters = vec![WriteParameter {
        code: OBIS_NEXT_WAKE.to_string(),
        value: next_wake_ts.to_be_bytes().to_vec(),
    }];
    send(
        socket,
        addr,
        Message::new_write_request_message(device_id_u128, seq, parameters)?,
    )
    .await?;
    seq += 1;

    // WRITE_RESPONSE
    let resp = recv(rx).await?;
    let got = resp.msg_type.as_str();
    let MessagePayload::WriteResponse(_) = resp.payload else {
        return Err(BackdoorError::ParseError(format!(
            "expected write_response, got {got}"
        )));
    };
    info!("[debug] WRITE_RESPONSE received");

    // ACK — session close
    send(socket, addr, Message::new_ack_message(device_id_u128, seq)?).await?;
    info!("[debug] ACK sent — session closed");

    if needs_battery {
        if let Err(e) = database
            .update_last_battery_read(device_id, Utc::now().naive_utc())
            .await
        {
            warn!("[debug] failed to update last_battery_read: {e}");
        }
    }

    Ok(())
}

async fn send(socket: &UdpSocket, addr: SocketAddr, msg: Message) -> Result<(), BackdoorError> {
    let mut buf = BytesMut::new();
    MessageCodec
        .encode(msg, &mut buf)
        .map_err(|e| BackdoorError::ParseError(e.to_string()))?;
    socket.send_to(&buf, addr).await?;
    Ok(())
}

async fn recv(
    rx: &mut mpsc::UnboundedReceiver<Message>,
) -> Result<Message, BackdoorError> {
    timeout(Duration::from_secs(RESPONSE_TIMEOUT_SECS), rx.recv())
        .await
        .map_err(|_| BackdoorError::ParseError("device did not respond within timeout".to_string()))?
        .ok_or_else(|| BackdoorError::ParseError("debug session channel closed".to_string()))
}
