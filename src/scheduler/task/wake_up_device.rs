use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};
use uuid::Uuid;

use common::database::api::Database;
use common::scheduled_connection::ScheduledStatus;

use crate::error::TaskError;

const DEVICE_UDP_PORT: u16 = 6060;
const MAX_RETRIES: u32 = 5;
const RETRY_DELAY_SECS: u64 = 60;

/// Connects to a device at its scheduled wake-up time to collect consumption data.
pub async fn wake_up_device(
    job_id: Uuid,
    device_id: Uuid,
    database: Database,
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

        match try_connect(&remote_addr).await {
            Ok(_socket) => {
                info!("[Job {:#x}] Connected successfully", job_id);

                // TODO: Send HANDSHAKE message
                // TODO: Receive HANDSHAKE_RESPONSE
                // TODO: Send READ_REQUEST for OBIS data
                // TODO: Receive READ_RESPONSE
                // TODO: Send WRITE_REQUEST to update next wake time
                // TODO: Send ACK and close

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

    let mut connection = database.get_scheduled_connection(device_id).await?;
    connection.status = ScheduledStatus::Lost;
    connection.renewable = false;
    connection.job_id = None;
    database.update_scheduled_connection(&connection).await?;

    Err(TaskError::MaxRetriesExceeded)
}

async fn try_connect(remote_addr: &str) -> Result<UdpSocket, TaskError> {
    // open a socket on any available port
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(remote_addr).await?;
    Ok(socket)
}
