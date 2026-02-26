use std::collections::HashSet;
use std::time::Duration;

use chrono::NaiveDateTime;
use chrono::{Datelike, Timelike, Utc};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info};
use uuid::Uuid;

use crate::error::SchedulerError;
use crate::schedule::Schedule;
use crate::task::wake_up_device::wake_up_device;
use common::database::api::Database;
use common::device::Device;
use common::scheduled_connection::ScheduledConnection;
use common::scheduled_connection::ScheduledStatus;
use metrics::metrics_connections::METRICS_CONNECTIONS;

/// Trait for cluster synchronization with the scheduler.
///
/// This trait allows the cluster manager to configure the scheduler
/// without creating a circular dependency.
pub trait SchedulerClusterSync {
    /// Enables cluster mode with the given owned devices.
    fn enable_cluster_mode(&mut self, owned_devices: HashSet<Uuid>);
}

/// Manages scheduled connections to IoT devices using a time-bucket algorithm.
///
/// The scheduler divides the 24-hour day into N equal time slots (buckets) and
/// distributes devices across them to avoid network congestion. Each device is
/// assigned to a bucket and wakes up at the scheduled time to expose a UDP server
/// for the HES to connect to.
///
/// Uses `tokio-cron-scheduler` for job scheduling and stores state in the database
/// for recovery after HES restarts.
pub struct Scheduler {
    /// Total number of time buckets to divide the day into (e.g., 48 = 30min intervals)
    pub bucket_number: i32,
    /// Cron-based job scheduler for executing periodic tasks
    pub job_scheduler: JobScheduler,
    /// Database handle for persisting scheduler state
    pub database: Database,
    /// Devices owned by this node (only used in cluster mode, None = owns all)
    pub owned_devices: Option<HashSet<Uuid>>,
    /// UUID of this node (used to tag bucket assignments in the database)
    pub local_node_id: Uuid,
}

impl Scheduler {
    /// Creates a new scheduler with the specified number of time buckets.
    ///
    /// Initializes the job scheduler and attempts to restore any previously
    /// scheduled connections from the database (for HES restarts).
    pub async fn new(bucket_number: usize, database: Database, local_node_id: Uuid) -> Result<Self, SchedulerError> {
        let mut scheduler = Self {
            bucket_number: bucket_number as i32,
            job_scheduler: JobScheduler::new().await?,
            database,
            owned_devices: None, // None means single-node mode, owns all devices
            local_node_id,
        };
        scheduler.start().await?;
        scheduler.reload_active_connections().await?;

        Ok(scheduler)
    }

    /// Starts the job scheduler and sets up graceful shutdown on Ctrl+C.
    pub async fn start(&mut self) -> Result<(), SchedulerError> {
        self.job_scheduler.start().await?;
        self.job_scheduler.shutdown_on_ctrl_c();
        self.job_scheduler.set_shutdown_handler(Box::new(|| {
            Box::pin(async move {
                info!("Shuting down job scheduler");
            })
        }));
        Ok(())
    }

    /// Registers a new device in the scheduler by assigning it to a time bucket.
    ///
    /// The device is assigned to the bucket with the fewest devices (load balancing).
    /// The bucket number determines when the device will wake up each day.
    ///
    /// Note: This only stores the assignment in the database. The actual job is
    /// scheduled later when the device sends an ACK.
    pub async fn register_device(&mut self, device: &Device) -> Result<(), SchedulerError> {
        METRICS_CONNECTIONS
            .connections_tracker
            .with_label_values(&["new_connection"])
            .inc();
        let bucket_number = self.get_bucket_number().await;
        let next_wake_up = self.get_next_schedule(bucket_number);

        self.database
            .add_device_to_bucket(device.id, bucket_number as i32, self.local_node_id)
            .await?;

        // Update scheduler metrics
        METRICS_CONNECTIONS.scheduled_devices_total.inc();
        METRICS_CONNECTIONS
            .devices_per_bucket
            .with_label_values(&[&bucket_number.to_string()])
            .inc();

        info!(
            "Device id: {:#x} in bucket {} next wake scheduled at {}",
            device.id, bucket_number, next_wake_up
        );
        Ok(())
    }

    /// Restores scheduled connections from the database after HES restart.
    ///
    /// This checks for any previously scheduled connections and reschedules them
    /// if their next wake-up time hasn't expired yet (with a 5-minute safety margin).
    ///
    /// In cluster mode, only loads connections for buckets owned by this node.
    async fn reload_active_connections(&mut self) -> Result<(), SchedulerError> {
        let scheduled_connections = self.database.get_scheduled_connections().await?;

        for mut connection in scheduled_connections {
            // In cluster mode, check if we own this device
            if !self.owns_device(connection.fk_device) {
                info!(
                    "Device {:#x} not owned by this node, skipping",
                    connection.fk_device
                );
                continue;
            }

            info!("Loading connection from db {:#x}", connection.fk_device);
            if connection.schedule_time < Utc::now().naive_local() + Duration::from_secs(300) {
                info!(
                    "Connection {:#x} expired, changing status to lost in db",
                    connection.fk_device
                );
                connection.status = ScheduledStatus::Lost;
                connection.job_id = None;
                self.database
                    .update_scheduled_connection(&connection)
                    .await?;
                continue;
            }

            let next_wake_up = connection.schedule_time;
            let job_id = self
                .create_wakeup_job(connection.fk_device, next_wake_up)
                .await?;

            connection.job_id = Some(job_id);

            self.database
                .update_scheduled_connection(&connection)
                .await?;

            METRICS_CONNECTIONS
                .connections_tracker
                .with_label_values(&["new_connection"])
                .inc();
        }
        Ok(())
    }

    pub async fn schedule_next_wakeup_job(
        &mut self,
        device_id: Uuid,
    ) -> Result<(), SchedulerError> {
        let bucket_number = self.database.get_bucket_number(device_id).await?;

        let next_wake_up = self.get_next_schedule(bucket_number as usize);

        let next_wake_up =
            NaiveDateTime::parse_from_str(&next_wake_up.to_string(), "%H:%M:%S %d/%m/%Y").map_err(
                |e| {
                    error!("Error parsing next_wakeup from Schedule: {}", e);
                    SchedulerError::ParseError(e.to_string())
                },
            )?;

        let job_id = self.create_wakeup_job(device_id, next_wake_up).await?;

        self.database
            .schedule_connection(device_id, next_wake_up, job_id)
            .await?;

        info!(
            "[Job id {}]Scheduled next connetion to divice {:#x} at {}",
            job_id, device_id, next_wake_up
        );

        Ok(())
    }

    pub async fn get_bucket_number(&self) -> usize {
        self.database
            .get_bucket_with_less_devices(self.bucket_number)
            .await
            .unwrap_or(0) as usize
    }

    pub fn get_next_schedule(&self, bucket_number: usize) -> Schedule {
        let (sec, min, hour) = self.get_time_from_bucket_number(bucket_number);
        let (day, mon, year) = get_date_from_hour(hour);

        Schedule {
            sec,
            min,
            hour,
            day,
            mon,
            year,
        }
    }

    /// Converts a bucket number to a time of day (HH:MM:SS).
    ///
    /// The algorithm divides 86400 seconds (24 hours) by the total number of buckets
    /// to determine the duration of each bucket. Then it multiplies by the bucket number
    /// to get the offset from midnight.
    ///
    /// Example: With 48 buckets (30min intervals), bucket 0 = 00:00:00, bucket 1 = 00:30:00, etc.
    ///
    /// Returns: (seconds, minutes, hours)
    pub fn get_time_from_bucket_number(&self, bucket_number: usize) -> (usize, usize, usize) {
        let secs_per_day: usize = 24 * 60 * 60;

        let total_buckets = self.bucket_number as usize;

        // Calculate how many seconds each bucket represents
        let secs_per_bucket = secs_per_day / total_buckets;

        // Calculate the offset from midnight for this bucket
        let slot_in_secs = secs_per_bucket * bucket_number;

        // Convert total seconds to (sec, min, hour)
        (
            slot_in_secs % 3600 % 60, // Seconds component
            slot_in_secs % 3600 / 60, // Minutes component
            slot_in_secs / 3600,      // Hours component
        )
    }

    pub async fn get_scheduled_connections(
        &self,
    ) -> Result<Vec<ScheduledConnection>, SchedulerError> {
        self.database
            .get_scheduled_connections()
            .await
            .map_err(SchedulerError::DatabaseError)
    }

    /// Sets the devices owned by this node (for cluster mode).
    pub fn set_owned_devices(&mut self, devices: HashSet<Uuid>) {
        self.owned_devices = Some(devices);
    }

    /// Checks if this node owns a specific device.
    ///
    /// In single-node mode (owned_devices = None), owns all devices.
    /// In cluster mode, only owns devices in the owned_devices set.
    pub fn owns_device(&self, device_id: Uuid) -> bool {
        match &self.owned_devices {
            None => true, // Single-node mode, owns all devices
            Some(owned) => owned.contains(&device_id),
        }
    }

    /// Enables cluster mode with the given owned devices.
    pub fn enable_cluster_mode(&mut self, owned_devices: HashSet<Uuid>) {
        info!(
            "Enabling cluster mode with {} owned devices",
            owned_devices.len()
        );
        self.owned_devices = Some(owned_devices);
    }

    /// Adds a device to the owned set (cluster mode only).
    pub fn add_owned_device(&mut self, device_id: Uuid) {
        if let Some(owned) = &mut self.owned_devices {
            owned.insert(device_id);
            info!(
                "Added device {:?} to owned set (now have {})",
                device_id,
                owned.len()
            );
        }
    }

    /// Removes a device from the owned set (cluster mode only).
    pub fn remove_owned_device(&mut self, device_id: Uuid) {
        if let Some(owned) = &mut self.owned_devices {
            owned.remove(&device_id);
            info!(
                "Removed device {:?} from owned set (now have {})",
                device_id,
                owned.len()
            );
        }
    }

    /// Schedules a delegated device at its original schedule time.
    ///
    /// This is called when accepting delegation from another node.
    /// The device will be connected at the specified time, then reassigned to a local bucket.
    pub async fn schedule_delegated_device(
        &mut self,
        device_id: Uuid,
        schedule_time: NaiveDateTime,
    ) -> Result<(), SchedulerError> {
        info!(
            "Scheduling delegated device {:?} at original time {}",
            device_id, schedule_time
        );

        let job_id = self.create_wakeup_job(device_id, schedule_time).await?;

        self.database
            .schedule_connection(device_id, schedule_time, job_id)
            .await?;

        // Add to owned devices set
        self.add_owned_device(device_id);

        Ok(())
    }

    async fn create_wakeup_job(
        &mut self,
        device_id: Uuid,
        next_wake_up: NaiveDateTime,
    ) -> Result<Uuid, SchedulerError> {
        let db_clone = self.database.clone();

        let job = Job::new_async_tz(
            next_wake_up.format("%S %M %H %d %m * %Y").to_string(),
            chrono_tz::UTC,
            move |job_id, _l| {
                let db_clone = db_clone.clone();
                Box::pin(async move {
                    if let Err(e) = wake_up_device(job_id, device_id, db_clone.clone()).await {
                        error!("[Job {:#x}] Wake up device failed: {}", job_id, e);

                        let mut connection = db_clone
                            .get_scheduled_connection(device_id)
                            .await
                            .inspect_err(|e| {
                                error!(
                                    "[Job {:#x}] Failed to get scheduled connection: {}",
                                    job_id, e
                                )
                            })
                            .ok();

                        if let Some(conn) = &mut connection {
                            conn.status = ScheduledStatus::Lost;
                            conn.renewable = false;
                            conn.job_id = None;

                            db_clone
                                .update_scheduled_connection(conn)
                                .await
                                .inspect_err(|e| {
                                    error!(
                                        "[Job {:#x}] Failed to update connection status: {}",
                                        job_id, e
                                    )
                                })
                                .ok();
                        }
                    }
                })
            },
        )
        .map_err(|e| {
            error!("Failed to create job: {}", e);
            SchedulerError::JobSchedulerError(e)
        })?;

        let job_id = self.job_scheduler.add(job).await.map_err(|e| {
            error!("Failed to add job to scheduler: {}", e);
            SchedulerError::JobSchedulerError(e)
        })?;

        Ok(job_id)
    }
}

impl SchedulerClusterSync for Scheduler {
    fn enable_cluster_mode(&mut self, owned_devices: HashSet<Uuid>) {
        self.enable_cluster_mode(owned_devices);
    }
}

/// Determines the date (day/month/year) for a scheduled wake-up based on the hour.
///
/// This function decides whether a scheduled connection should happen today or tomorrow:
/// - If the scheduled hour is more than 1 hour in the future, schedule for today
/// - Otherwise, schedule for tomorrow
///
/// The "+1 hour" margin provides a safety buffer to avoid race conditions where
/// we're scheduling a time that's about to pass or has just passed.
///
/// Example: If it's currently 23:00 (11 PM) and the device should wake at 00:30,
/// we schedule it for tomorrow at 00:30, not today (which would be in the past).
///
/// Returns: (day, month, year)
fn get_date_from_hour(hour: usize) -> (usize, usize, usize) {
    let today = Utc::now();
    let tomorrow = today + chrono::Duration::days(1);

    // If scheduled hour is more than 1 hour in the future, use today's date
    if hour > today.hour() as usize + 1 {
        return (
            today.day() as usize,
            today.month() as usize,
            today.year() as usize,
        );
    }

    // Otherwise, use tomorrow's date (scheduled time is imminent or has passed)
    (
        tomorrow.day() as usize,
        tomorrow.month() as usize,
        tomorrow.year() as usize,
    )
}
