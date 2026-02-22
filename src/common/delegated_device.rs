//! Delegated device representation for cluster delegation.
//!
//! This module contains the `DelegatedDevice` struct used when transferring
//! device ownership between cluster nodes.

use chrono::NaiveDateTime;
use uuid::Uuid;

/// Represents a device being delegated to another cluster node.
///
/// Contains all the information needed for the receiving node to:
/// 1. Take ownership of the device
/// 2. Schedule the connection at the original time
/// 3. Connect to the device when the time comes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegatedDevice {
    /// The device's unique identifier
    pub device_id: Uuid,
    /// IPv4 address of the device (if available)
    pub ipv4: Option<String>,
    /// IPv6 address of the device (if available)
    pub ipv6: Option<String>,
    /// The scheduled time for the next connection (preserved from original schedule)
    pub schedule_time: NaiveDateTime,
}

impl DelegatedDevice {
    /// Creates a new DelegatedDevice.
    pub fn new(
        device_id: Uuid,
        ipv4: Option<String>,
        ipv6: Option<String>,
        schedule_time: NaiveDateTime,
    ) -> Self {
        Self {
            device_id,
            ipv4,
            ipv6,
            schedule_time,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_delegated_device_creation() {
        let device_id = Uuid::new_v4();
        let schedule_time = Utc::now().naive_utc();

        let device = DelegatedDevice::new(
            device_id,
            Some("192.168.1.100".to_string()),
            None,
            schedule_time,
        );

        assert_eq!(device.device_id, device_id);
        assert_eq!(device.ipv4, Some("192.168.1.100".to_string()));
        assert_eq!(device.ipv6, None);
        assert_eq!(device.schedule_time, schedule_time);
    }
}
