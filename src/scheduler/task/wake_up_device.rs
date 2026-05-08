use bytes::BytesMut;
use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep, timeout};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{error, info, warn};
use uuid::Uuid;

use common::database::api::Database;
use common::messages::codec::MessageCodec;
use common::messages::message::Message;
use common::messages::MsgCodecError;
use metrics::metrics_connections::METRICS_CONNECTIONS;
use metrics::metrics_protocol::METRICS_PROTOCOL;

use crate::error::TaskError;

const DEVICE_UDP_PORT: u16 = 6060;
const MAX_RETRIES: u32 = 5;
const RETRY_DELAY_SECS: u64 = 60;
const RESPONSE_TIMEOUT_SECS: u64 = 30;

/// Connects to a device at its scheduled wake-up time to collect consumption data.
pub async fn wake_up_device(
    job_id: Uuid,
    device_id: Uuid,
    database: &Database,
) -> Result<(), TaskError> {
    info!("[Job {:#x}] Wake up device {:#x}", job_id, device_id);
    METRICS_CONNECTIONS.hes_device_session_active.inc();

    let result = run_wake_up(job_id, device_id, database).await;

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

        match try_connect(&remote_addr, device_id).await {
            Ok(_socket) => {
                info!("[Job {:#x}] Device responded to HANDSHAKE", job_id);
                METRICS_CONNECTIONS
                    .connections_tracker
                    .with_label_values(&["periodic_success"])
                    .inc();

                // TODO: Send HANDSHAKE message
                // TODO: Receive HANDSHAKE_RESPONSE
                // TODO: Send READ_REQUEST for OBIS data
                // TODO: Receive READ_RESPONSE
                // TODO: Send WRITE_REQUEST to update next wake time
                // TODO: Send ACK and close

                // TODO: Assign bucket to delegated devices after first successful connection
                // Check if device has bucket assigned (delegated devices don't have one initially)
                // If not: assign to least-loaded local bucket using get_bucket_with_less_devices()
                // This allows delegated devices to connect at original schedule_time once,
                // then subsequent connections use the new bucket-based schedule

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

async fn try_connect(remote_addr: &str, device_id: Uuid) -> Result<UdpSocket, TaskError> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(remote_addr).await?;

    // Send HANDSHAKE message as per protocol specification
    // TODO: Generate cryptographically secure random nonce
    let nonce = vec![0u8; 32];
    let handshake = Message::new_handshake_message(device_id.as_u128(), 0, nonce)?;

    let mut buf = BytesMut::new();
    let mut codec = MessageCodec;
    codec.encode(handshake, &mut buf)?;

    let encoded_len = buf.len();
    socket.send(&buf).await?;
    METRICS_CONNECTIONS
        .messages_total
        .with_label_values(&["handshake", "outbound"])
        .inc();
    METRICS_PROTOCOL
        .hes_message_size_bytes
        .with_label_values(&["handshake"])
        .observe(encoded_len as f64);

    // Wait for HANDSHAKE_RESPONSE from device
    let mut response_buf = [0u8; 1024];
    let n = timeout(
        Duration::from_secs(RESPONSE_TIMEOUT_SECS),
        socket.recv(&mut response_buf),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Device did not respond"))??;

    if n == 0 {
        return Err(
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Empty response").into(),
        );
    }

    // Decode and validate HANDSHAKE_RESPONSE
    let mut decode_buf = bytes::BytesMut::from(&response_buf[..n]);
    match MessageCodec.decode(&mut decode_buf) {
        Ok(Some(msg)) => {
            METRICS_PROTOCOL
                .hes_message_size_bytes
                .with_label_values(&[msg.msg_type.as_str()])
                .observe(n as f64);
            // MAC verification not yet implemented (issue #9).
            METRICS_PROTOCOL
                .hes_mac_verification_total
                .with_label_values(&["skipped"])
                .inc();

            // TODO: Verify msg_type is HandshakeResponse and validate nonce.
            // When ReadResponse processing is implemented, iterate the OBIS values:
            //   for value in &read_response.values {
            //       METRICS_PROTOCOL.hes_obis_read_total
            //           .with_label_values(&[&value.code])
            //           .inc();
            //   }
        }
        Ok(None) => {
            warn!("[Job {:#x}] Incomplete HANDSHAKE_RESPONSE from {}", device_id, remote_addr);
        }
        Err(e) => {
            let error_kind = match &e {
                MsgCodecError::InvalidLength => "invalid_length",
                MsgCodecError::UnknownMsgType => "unknown_msg_type",
                MsgCodecError::PayloadDecodeError(_) => "payload_decode",
                MsgCodecError::IoError(_) => "io",
            };
            METRICS_PROTOCOL
                .hes_message_decode_errors_total
                .with_label_values(&[error_kind])
                .inc();
            warn!("[Job {:#x}] Failed to decode HANDSHAKE_RESPONSE: {}", device_id, e);
        }
    }

    Ok(socket)
}
