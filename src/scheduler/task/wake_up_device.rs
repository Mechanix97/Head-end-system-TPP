use bytes::BytesMut;
use chrono::Utc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep, timeout};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{error, info, warn};
use uuid::Uuid;

use common::database::api::Database;
use common::messages::codec::MessageCodec;
use common::messages::message::{Message, MessagePayload};
use common::messages::write::WriteParameter;
use metrics::metrics_connections::METRICS_CONNECTIONS;

use crate::error::TaskError;

const DEVICE_UDP_PORT: u16 = 6060;
const MAX_RETRIES: u32 = 5;
const RETRY_DELAY_SECS: u64 = 60;
const RESPONSE_TIMEOUT_SECS: u64 = 30;

const OBIS_WATER_VOLUME: &str = "1.0.1";
const OBIS_BATTERY: &str = "C.6.1";
const OBIS_CLOCK: &str = "0.9.4";
const OBIS_NEXT_WAKE: &str = "0.0.1";

const BATTERY_READ_INTERVAL_DAYS: i64 = 7;

/// Connects to a device at its scheduled wake-up time to collect consumption data.
///
/// Runs the full periodic session: HANDSHAKE → READ → WRITE → ACK. On success,
/// sends `device_id` through `reschedule_tx` so the next job gets scheduled.
pub async fn wake_up_device(
    job_id: Uuid,
    device_id: Uuid,
    database: &Database,
    reschedule_tx: Option<mpsc::Sender<Uuid>>,
) -> Result<(), TaskError> {
    info!("[Job {:#x}] Wake up device {:#x}", job_id, device_id);
    METRICS_CONNECTIONS.hes_device_session_active.inc();

    let result = run_wake_up(job_id, device_id, database, reschedule_tx).await;

    METRICS_CONNECTIONS.hes_device_session_active.dec();

    match &result {
        Ok(()) => {
            METRICS_CONNECTIONS
                .hes_device_connection_outcome_total
                .with_label_values(&["success"])
                .inc();
        }
        Err(TaskError::DeviceWithNoIP) => {
            METRICS_CONNECTIONS
                .hes_device_connection_outcome_total
                .with_label_values(&["no_ip"])
                .inc();
        }
        Err(TaskError::MaxRetriesExceeded) => {
            METRICS_CONNECTIONS
                .hes_device_connection_outcome_total
                .with_label_values(&["max_retries"])
                .inc();
        }
        Err(_) => {
            METRICS_CONNECTIONS
                .hes_device_connection_outcome_total
                .with_label_values(&["error"])
                .inc();
        }
    }

    result
}

async fn run_wake_up(
    job_id: Uuid,
    device_id: Uuid,
    database: &Database,
    reschedule_tx: Option<mpsc::Sender<Uuid>>,
) -> Result<(), TaskError> {
    let db_start = std::time::Instant::now();
    let device_result = database.get_device(device_id).await;
    let db_elapsed = db_start.elapsed().as_millis() as f64;
    let db_result_label = if device_result.is_ok() { "ok" } else { "error" };
    METRICS_CONNECTIONS
        .hes_db_query_duration_ms
        .with_label_values(&["get_device", db_result_label])
        .observe(db_elapsed);

    let device = device_result.inspect_err(|_| {
        METRICS_CONNECTIONS
            .hes_db_errors_total
            .with_label_values(&["get_device"])
            .inc();
    })?;

    let ip = match (device.ipv4, device.ipv6) {
        (Some(ip), None) => ip,
        (None, Some(ip)) => ip,
        _ => return Err(TaskError::DeviceWithNoIP),
    };

    let remote_addr = format!("{ip}:{DEVICE_UDP_PORT}");

    for attempt in 1..=MAX_RETRIES {
        info!(
            "[Job {:#x}] Connection attempt {}/{} to {}",
            job_id, attempt, MAX_RETRIES, remote_addr
        );
        METRICS_CONNECTIONS
            .connections_tracker
            .with_label_values(&["periodic_attempt"])
            .inc();

        if attempt > 1 {
            METRICS_CONNECTIONS.hes_device_retry_count_total.inc();
        }

        match run_session(job_id, device_id, &remote_addr, database).await {
            Ok(()) => {
                info!("[Job {:#x}] Session completed successfully", job_id);
                METRICS_CONNECTIONS
                    .connections_tracker
                    .with_label_values(&["periodic_success"])
                    .inc();
                if let Some(tx) = reschedule_tx {
                    if let Err(e) = tx.send(device_id).await {
                        error!("[Job {:#x}] Failed to signal reschedule: {}", job_id, e);
                    }
                }
                return Ok(());
            }
            Err(e) => {
                warn!(
                    "[Job {:#x}] Attempt {}/{} failed: {}",
                    job_id, attempt, MAX_RETRIES, e
                );
                METRICS_CONNECTIONS
                    .errors_total
                    .with_label_values(&["scheduler", "connection_failed"])
                    .inc();

                if attempt < MAX_RETRIES {
                    sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                }
            }
        }
    }

    error!(
        "[Job {:#x}] Failed to connect after {} attempts, marking as lost",
        job_id, MAX_RETRIES
    );
    METRICS_CONNECTIONS
        .errors_total
        .with_label_values(&["scheduler", "max_retries_exceeded"])
        .inc();

    Err(TaskError::MaxRetriesExceeded)
}

/// Executes the full periodic session with a device.
///
/// Protocol flow:
///   HES → HANDSHAKE
///   Device → HANDSHAKE_RESPONSE
///   HES → READ_REQUEST  (water volume [+ battery every 7 days] + clock)
///   Device → READ_RESPONSE
///   HES → WRITE_REQUEST (clock sync, next wake time)
///   Device → WRITE_RESPONSE
///   HES → ACK           (session close)
async fn run_session(
    job_id: Uuid,
    device_id: Uuid,
    remote_addr: &str,
    database: &Database,
) -> Result<(), TaskError> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(remote_addr).await?;

    let device_id_u128 = device_id.as_u128();
    let mut seq: u32 = 0;

    // Decide whether to read battery based on last_battery_read
    let needs_battery = match database.get_scheduled_connection(device_id).await {
        Ok(conn) => match conn.last_battery_read {
            None => true,
            Some(last) => (Utc::now().naive_utc() - last).num_days() >= BATTERY_READ_INTERVAL_DAYS,
        },
        Err(_) => true, // no record → read it
    };

    // HANDSHAKE
    // TODO: replace with cryptographically secure random nonce
    let nonce = vec![0u8; 32];
    send_msg(&socket, Message::new_handshake_message(device_id_u128, seq, nonce)?).await?;
    seq += 1;

    // HANDSHAKE_RESPONSE
    let resp = recv_msg(&socket).await?;
    let got = resp.msg_type.as_str();
    let MessagePayload::HandshakeResponse(_) = resp.payload else {
        return Err(TaskError::UnexpectedMsgType { expected: "handshake_response", got });
    };
    info!("[Job {:#x}] HANDSHAKE_RESPONSE received", job_id);

    // READ_REQUEST
    let mut obis_codes = vec![OBIS_WATER_VOLUME.to_string(), OBIS_CLOCK.to_string()];
    if needs_battery {
        obis_codes.push(OBIS_BATTERY.to_string());
        info!("[Job {:#x}] Including battery OBIS in READ_REQUEST", job_id);
    }
    send_msg(
        &socket,
        Message::new_read_request_message(device_id_u128, seq, obis_codes)?,
    )
    .await?;
    seq += 1;

    // READ_RESPONSE
    let resp = recv_msg(&socket).await?;
    let got = resp.msg_type.as_str();
    let MessagePayload::ReadResponse(data) = resp.payload else {
        return Err(TaskError::UnexpectedMsgType { expected: "read_response", got });
    };
    info!(
        "[Job {:#x}] READ_RESPONSE received ({} values)",
        job_id,
        data.values.len()
    );
    // TODO: persist data.values to database (water volume, clock)

    // WRITE_REQUEST: tell device when to wake next.
    // Clock sync is not needed here — the device reads the envelope timestamp directly.
    let next_wake_ts = (Utc::now() + chrono::Duration::days(1)).timestamp() as u64;
    let parameters = vec![WriteParameter {
        code: OBIS_NEXT_WAKE.to_string(),
        value: next_wake_ts.to_be_bytes().to_vec(),
    }];
    send_msg(
        &socket,
        Message::new_write_request_message(device_id_u128, seq, parameters)?,
    )
    .await?;
    seq += 1;

    // WRITE_RESPONSE
    let resp = recv_msg(&socket).await?;
    let got = resp.msg_type.as_str();
    let MessagePayload::WriteResponse(_) = resp.payload else {
        return Err(TaskError::UnexpectedMsgType { expected: "write_response", got });
    };
    info!("[Job {:#x}] WRITE_RESPONSE received", job_id);

    // ACK — session close
    send_msg(&socket, Message::new_ack_message(device_id_u128, seq)?).await?;
    info!("[Job {:#x}] Session closed with ACK", job_id);

    // Persist battery read timestamp if we read it this session
    if needs_battery {
        if let Err(e) = database
            .update_last_battery_read(device_id, Utc::now().naive_utc())
            .await
        {
            warn!("[Job {:#x}] Failed to update last_battery_read: {}", job_id, e);
        }
    }

    Ok(())
}

async fn send_msg(socket: &UdpSocket, msg: Message) -> Result<(), TaskError> {
    let mut buf = BytesMut::new();
    MessageCodec.encode(msg, &mut buf)?;
    socket.send(&buf).await?;
    Ok(())
}

async fn recv_msg(socket: &UdpSocket) -> Result<Message, TaskError> {
    let mut raw = [0u8; 4096];
    let n = timeout(
        Duration::from_secs(RESPONSE_TIMEOUT_SECS),
        socket.recv(&mut raw),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "device did not respond"))??;

    let mut buf = BytesMut::from(&raw[..n]);
    MessageCodec.decode(&mut buf)?.ok_or_else(|| {
        TaskError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "incomplete message",
        ))
    })
}
