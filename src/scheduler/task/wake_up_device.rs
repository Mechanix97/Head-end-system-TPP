use tracing::info;
use uuid::Uuid;

use common::database::api::Database;

/// Task executed when a device's scheduled wake-up time arrives.
///
/// This function is called by the scheduler when it's time to connect to a device
/// and collect consumption data.
///
/// TODO: Implement the actual connection logic:
/// 1. Connect to device's IPv6:port as UDP client
/// 2. Send HANDSHAKE message
/// 3. Send READ_REQUEST for consumption data (OBIS codes)
/// 4. Send WRITE_REQUEST to update next wake time
/// 5. Close connection with ACK
///
/// Currently this just logs the device ID as a placeholder.
pub async fn wake_up_device(job_id: Uuid, device_id: Uuid, _database: Database) {
    info!("[Job id: {:#x}] Connection ID: {}", job_id, device_id);
}
