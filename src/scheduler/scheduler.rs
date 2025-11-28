use std::time::Duration;

use chrono::NaiveDateTime;
use chrono::{Datelike, Timelike, Utc};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::error;
use tracing::info;
use uuid::Uuid;

use crate::error::SchedulerError;
use crate::schedule::Schedule;
use common::database::api::Database;
use common::device::Device;
use metrics::metrics_connections::METRICS_CONNECTIONS;

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
}

impl Scheduler {
    /// Creates a new scheduler with the specified number of time buckets.
    ///
    /// Initializes the job scheduler and attempts to restore any previously
    /// scheduled connections from the database (for HES restarts).
    pub async fn new(bucket_number: usize, database: Database) -> Result<Self, SchedulerError> {
        let mut scheduler = Self {
            bucket_number: bucket_number as i32,
            job_scheduler: JobScheduler::new().await?,
            database,
        };

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
            .add_device_to_bucket(device.id, bucket_number as i32)
            .await?;

        info!(
            "Device id: {:#x} in bucket {} next wake scheduled at {}",
            device.id, bucket_number, next_wake_up
        );
        Ok(())
    }

    /// Restores scheduled connections from the database after HES restart.
    ///
    /// This checks for any previously scheduled connections and reschedules them
    /// if their next wake-up time hasn't expired yet (with a 5-minute safety margin).from_secs
    async fn reload_active_connections(&mut self) -> Result<(), SchedulerError> {
        // TODO: Re-implement this after finalizing database schema
        // Currently commented out due to schema changes in progress
        let scheduled_connections = self.database.get_scheduled_connections().await?;

        for (device_id, scheduled_time) in scheduled_connections {
            info!("Loading connection from db {:#x}", device_id);
            if scheduled_time < Utc::now().naive_local() + Duration::from_secs(300) {
                info!(
                    "Connection {:#x} expired, changing status to lost in db",
                    device_id
                );
                //self.database.update_scheduled_connection_status(device_id,lost ).await?;
                continue;
            }
            self.schedule_next_wakeup_job(device_id).await?;
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

        let db_clone = self.database.clone();
        let job_id = self
            .job_scheduler
            .add(Job::new_async_tz(
                next_wake_up.format("%S %M %H %d %m * %Y").to_string(),
                chrono_tz::UTC,
                move |_uuid, _l| {
                    let db_clone = db_clone.clone();
                    Box::pin(async move {
                        periodically_task(device_id, db_clone).await;
                    })
                },
            )?)
            .await?;

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
    ) -> Result<Vec<(Uuid, NaiveDateTime)>, SchedulerError> {
        self.database
            .get_scheduled_connections()
            .await
            .map_err(SchedulerError::DatabaseError)
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

/// Periodic task executed when a device's scheduled wake-up time arrives.
///
/// TODO: Implement the actual connection logic:
/// 1. Connect to device's IPv6:port as UDP client
/// 2. Send HANDSHAKE message
/// 3. Send READ_REQUEST for consumption data (OBIS codes)
/// 4. Send WRITE_REQUEST to update next wake time
/// 5. Close connection with ACK
///
/// Currently this just logs the device ID as a placeholder.
async fn periodically_task(device_id: Uuid, _database: Database) {
    info!("Conection ID: {}", device_id);
}
