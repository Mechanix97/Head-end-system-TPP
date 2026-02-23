//! Device ownership and delegation management.
//!
//! Re-exports `DeviceManager` from the `device_manager` crate so the rest of the cluster
//! module can import it from `crate::device_manager`.

pub use device_manager::DeviceManager;
pub use device_manager::DeviceManagerError;
