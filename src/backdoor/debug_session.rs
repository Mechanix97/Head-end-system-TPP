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
use tracing::{debug, info, warn};
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
        debug!(
            msg_type = msg.msg_type.as_str(),
            device_id = %Uuid::from_u128(msg.device_id),
            seq = msg.seq,
            "routed to active debug session",
        );
        let _ = tx.send(msg.clone());
        return true;
    }
    debug!(
        msg_type = msg.msg_type.as_str(),
        device_id = %Uuid::from_u128(msg.device_id),
        "no active debug session for {addr} — falling through",
    );
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

    info!("Session started — device {device_id} at {addr}");

    // Cancel the pending scheduler job and abort any already-running wake-up task
    // so the normal path doesn't race with the debug session.
    {
        let mut dm = device_manager.write().await;
        match dm.cancel_wakeup_job(device_id).await {
            Ok(true) => debug!("cancelled wake-up job for device {device_id}"),
            Ok(false) => debug!("no pending job to cancel for device {device_id}"),
            Err(e) => warn!("failed to cancel job for device {device_id}: {e}"),
        }
        if dm.abort_active_wakeup(device_id) {
            debug!("aborted running wake-up task for device {device_id}");
        }
    }

    // Schedule the next wakeup before the session so we can tell the device the real time.
    let next_wake_ts = match device_manager
        .write()
        .await
        .schedule_next_wakeup(device_id)
        .await
    {
        Ok(()) => match database.get_scheduled_connection(device_id).await {
            Ok(conn) => {
                let ts = conn.schedule_time.and_utc().timestamp() as u64;
                let utc_str = conn
                    .schedule_time
                    .and_utc()
                    .format("%Y-%m-%d %H:%M:%S UTC")
                    .to_string();
                debug!("next wake scheduled for device {device_id} at {utc_str}");
                ts
            }
            Err(e) => {
                warn!("could not read scheduled time for {device_id}: {e} — falling back to +1 day");
                (Utc::now() + chrono::Duration::days(1)).timestamp() as u64
            }
        },
        Err(e) => {
            warn!("failed to schedule next wakeup for device {device_id}: {e} — falling back to +1 day");
            (Utc::now() + chrono::Duration::days(1)).timestamp() as u64
        }
    };

    // Register inbound channel so subsequent packets from this addr are forwarded here.
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    router.write().await.insert(addr, tx);

    let result =
        run_session_over_backdoor(&socket, addr, device_id, &mut rx, &database, next_wake_ts)
            .await;

    match &result {
        Ok(()) => info!("Session ended — device {device_id}: ok"),
        Err(e) => info!("Session ended — device {device_id}: error: {e}"),
    }

    // Clean up the router entry.
    router.write().await.remove(&addr);

    result
}

async fn run_session_over_backdoor(
    socket: &UdpSocket,
    addr: SocketAddr,
    device_id: Uuid,
    rx: &mut mpsc::UnboundedReceiver<Message>,
    database: &Database,
    next_wake_ts: u64,
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
    debug!("→ HANDSHAKE  seq={seq}  to={addr}");
    seq += 1;

    // HANDSHAKE_RESPONSE
    let resp = recv(rx).await?;
    let got = resp.msg_type.as_str();
    let MessagePayload::HandshakeResponse(hs_resp) = resp.payload else {
        return Err(BackdoorError::ParseError(format!(
            "expected handshake_response, got {got}"
        )));
    };
    debug!("← HANDSHAKE_RESPONSE  seq={}", resp.seq);
    debug!(status = hs_resp.status, "HANDSHAKE_RESPONSE detail");

    // READ_REQUEST
    let mut obis_codes = vec![OBIS_WATER_VOLUME.to_string(), OBIS_CLOCK.to_string()];
    if needs_battery {
        obis_codes.push(OBIS_BATTERY.to_string());
        debug!("battery OBIS ({OBIS_BATTERY}) added to READ_REQUEST — last read was >{}d ago", BATTERY_READ_INTERVAL_DAYS);
    }
    debug!("→ READ_REQUEST  seq={seq}  codes={:?}", obis_codes);
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
    debug!("← READ_RESPONSE  seq={}  values={}", resp.seq, data.values.len());
    for v in &data.values {
        debug!(
            code = v.code,
            value = %obis_value_decimal(&v.value),
            "READ_RESPONSE value",
        );
    }
    // TODO: persist data.values to database (water volume, clock)

    // WRITE_REQUEST: sync device clock and tell it when to wake next.
    let now_ts = Utc::now().timestamp() as u64;
    let next_wake_utc = chrono::DateTime::from_timestamp(next_wake_ts as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("{next_wake_ts}"));
    let parameters = vec![
        WriteParameter {
            code: OBIS_CLOCK.to_string(),
            value: now_ts.to_be_bytes().to_vec(),
        },
        WriteParameter {
            code: OBIS_NEXT_WAKE.to_string(),
            value: next_wake_ts.to_be_bytes().to_vec(),
        },
    ];
    debug!("→ WRITE_REQUEST  seq={seq}  clock_sync={now_ts}  next_wake={next_wake_utc}");
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
    let MessagePayload::WriteResponse(wr_resp) = resp.payload else {
        return Err(BackdoorError::ParseError(format!(
            "expected write_response, got {got}"
        )));
    };
    debug!("← WRITE_RESPONSE  seq={}", resp.seq);
    debug!(success = wr_resp.success, codes = ?wr_resp.written_codes, "WRITE_RESPONSE detail");

    // ACK — session close
    debug!("→ ACK  seq={seq}  session closed");
    send(socket, addr, Message::new_ack_message(device_id_u128, seq)?).await?;

    if needs_battery {
        if let Err(e) = database
            .update_last_battery_read(device_id, Utc::now().naive_utc())
            .await
        {
            warn!("failed to update last_battery_read: {e}");
        }
    }

    Ok(())
}

/// Renders an OBIS value as a decimal number.
///
/// Values travel as big-endian unsigned integers: 8 bytes for the water volume
/// and the clock, 1 for the battery level. Anything that does not fit a u64 is
/// not a number we can render, so it falls back to hex.
fn obis_value_decimal(bytes: &[u8]) -> String {
    if bytes.is_empty() || bytes.len() > 8 {
        return bytes.iter().map(|b| format!("{b:02x}")).collect();
    }
    let mut value: u64 = 0;
    for byte in bytes {
        value = (value << 8) | u64::from(*byte);
    }
    value.to_string()
}

async fn send(socket: &UdpSocket, addr: SocketAddr, msg: Message) -> Result<(), BackdoorError> {
    debug!(
        msg_type = msg.msg_type.as_str(),
        device_id = %Uuid::from_u128(msg.device_id),
        seq = msg.seq,
        "sending to {addr}",
    );
    let mut buf = BytesMut::new();
    MessageCodec
        .encode(msg, &mut buf)
        .map_err(|e| BackdoorError::ParseError(e.to_string()))?;
    debug!(bytes = buf.len(), "UDP send_to {addr}");
    socket.send_to(&buf, addr).await?;
    Ok(())
}

async fn recv(
    rx: &mut mpsc::UnboundedReceiver<Message>,
) -> Result<Message, BackdoorError> {
    debug!("waiting for message (timeout={}s)", RESPONSE_TIMEOUT_SECS);
    let msg = timeout(Duration::from_secs(RESPONSE_TIMEOUT_SECS), rx.recv())
        .await
        .map_err(|_| BackdoorError::ParseError("device did not respond within timeout".to_string()))?
        .ok_or_else(|| BackdoorError::ParseError("debug session channel closed".to_string()))?;
    debug!(
        msg_type = msg.msg_type.as_str(),
        device_id = %Uuid::from_u128(msg.device_id),
        seq = msg.seq,
        "received from channel",
    );
    Ok(msg)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use std::net::SocketAddr;

    use common::messages::message::Message;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn device() -> u128 {
        Uuid::new_v4().as_u128()
    }

    // --- try_route: types that MUST be forwarded ---

    #[tokio::test]
    async fn try_route_forwards_handshake_response() {
        let router = new_router();
        let a = addr(19100);
        let (tx, mut rx) = mpsc::unbounded_channel();
        router.write().await.insert(a, tx);

        let msg = Message::new_handshake_response_message(device(), 1, 0).unwrap();
        assert!(try_route(&router, a, &msg).await);
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn try_route_forwards_read_response() {
        let router = new_router();
        let a = addr(19101);
        let (tx, mut rx) = mpsc::unbounded_channel();
        router.write().await.insert(a, tx);

        let msg = Message::new_read_response_message(device(), 2, vec![]).unwrap();
        assert!(try_route(&router, a, &msg).await);
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn try_route_forwards_write_response() {
        let router = new_router();
        let a = addr(19102);
        let (tx, mut rx) = mpsc::unbounded_channel();
        router.write().await.insert(a, tx);

        let msg = Message::new_write_response_message(device(), 3, true, vec![]).unwrap();
        assert!(try_route(&router, a, &msg).await);
        assert!(rx.try_recv().is_ok());
    }

    // --- try_route: types that MUST fall through ---

    #[tokio::test]
    async fn try_route_does_not_forward_ack() {
        let router = new_router();
        let a = addr(19103);
        let (tx, mut rx) = mpsc::unbounded_channel();
        router.write().await.insert(a, tx);

        let msg = Message::new_ack_message(device(), 0).unwrap();
        assert!(!try_route(&router, a, &msg).await);
        assert!(rx.try_recv().is_err()); // nothing sent to channel
    }

    // --- try_route: addr not registered ---

    #[tokio::test]
    async fn try_route_returns_false_for_unknown_addr() {
        let router = new_router();
        let registered = addr(19104);
        let other = addr(19105);
        let (tx, mut rx) = mpsc::unbounded_channel();
        router.write().await.insert(registered, tx);

        let msg = Message::new_handshake_response_message(device(), 1, 0).unwrap();
        assert!(!try_route(&router, other, &msg).await);
        assert!(rx.try_recv().is_err()); // channel for `registered` untouched
    }

    #[tokio::test]
    async fn try_route_returns_false_for_empty_router() {
        let router = new_router();
        let msg = Message::new_handshake_response_message(device(), 0, 0).unwrap();
        assert!(!try_route(&router, addr(19106), &msg).await);
    }
}
