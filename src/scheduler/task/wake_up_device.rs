use bytes::BytesMut;
use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep, timeout};
use tokio_util::codec::Encoder;
use tracing::{error, info, warn};
use uuid::Uuid;

use common::database::api::Database;
use common::messages::codec::MessageCodec;
use common::messages::message::Message;

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

    let device = database.get_device(device_id).await?;

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

        match try_connect(&remote_addr, device_id).await {
            Ok(_socket) => {
                info!("[Job {:#x}] Device responded to HANDSHAKE", job_id);

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

    Err(TaskError::MaxRetriesExceeded)
}

async fn try_connect(remote_addr: &str, device_id: Uuid) -> Result<UdpSocket, TaskError> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(remote_addr).await?;

    // Send HANDSHAKE message as per protocol specification
    // TODO: Generate cryptographically secure random nonce
    let nonce = vec![0u8; 32];  // Temporary: use 32-byte zero nonce
    let handshake = Message::new_handshake_message(device_id.as_u128(), 0, nonce)?;

    let mut buf = BytesMut::new();
    let mut codec = MessageCodec;
    codec.encode(handshake, &mut buf)?;

    socket.send(&buf).await?;

    // Wait for HANDSHAKE_RESPONSE from device
    let mut response_buf = [0u8; 1024];
    let n = timeout(
        Duration::from_secs(RESPONSE_TIMEOUT_SECS),
        socket.recv(&mut response_buf),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Device did not respond"))??;

    // TODO: Decode and validate HANDSHAKE_RESPONSE
    // For now, just check that we got some data back
    if n == 0 {
        return Err(
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Empty response").into(),
        );
    }

    Ok(socket)
}
