use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use chrono::NaiveDateTime;
use chrono::{Datelike, Timelike, Utc};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info};
use uuid::Uuid;

use crate::error::SchedulerError;

const TEST_MODE_WAKEUP_SECS: i64 = 150;
use crate::schedule::Schedule;
use crate::task::wake_up_device::wake_up_device;
use common::database::api::Database;
use common::device::Device;
use common::scheduled_connection::ScheduledConnection;
use common::scheduled_connection::ScheduledStatus;
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
    /// Devices owned by this node (only used in cluster mode, None = owns all)
    pub owned_devices: Option<HashSet<Uuid>>,
    /// UUID of this node (used to tag bucket assignments in the database)
    pub local_node_id: Uuid,
    /// Channel to signal main.rs to reschedule a device after a successful session
    reschedule_tx: Option<mpsc::Sender<Uuid>>,
    /// When true, all connections are scheduled within 5 minutes instead of normal daily buckets
    test_mode: bool,
    /// AbortHandles for wake_up_device tasks that are currently executing.
    /// Keyed by device_id so a debug session can abort a running task before taking over.
    active_tasks: Arc<Mutex<HashMap<Uuid, AbortHandle>>>,
}

impl Scheduler {
    /// Creates a new scheduler with the specified number of time buckets.
    ///
    /// Initializes the job scheduler and attempts to restore any previously
    /// scheduled connections from the database (for HES restarts).
    pub async fn new(
        bucket_number: usize,
        database: Database,
        local_node_id: Uuid,
        test_mode: bool,
    ) -> Result<Self, SchedulerError> {
        if test_mode {
            info!("Scheduler running in TEST MODE — all connections scheduled within 5 minutes");
        }
        let mut scheduler = Self {
            bucket_number: bucket_number as i32,
            job_scheduler: JobScheduler::new().await?,
            database,
            owned_devices: None, // None means single-node mode, owns all devices
            local_node_id,
            reschedule_tx: None,
            test_mode,
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
        };
        scheduler.start().await?;

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
    pub async fn register_device(
        &mut self,
        device: &Device,
    ) -> Result<NaiveDateTime, SchedulerError> {
        METRICS_CONNECTIONS
            .connections_tracker
            .with_label_values(&["new_connection"])
            .inc();
        let bucket_number = self.get_bucket_number().await;
        let next_wake_up = self.next_wakeup(bucket_number)?;

        self.database
            .add_device_to_bucket(device.id, bucket_number as i32, self.local_node_id)
            .await?;

        // Update scheduler metrics
        METRICS_CONNECTIONS.scheduled_devices_total.inc();
        METRICS_CONNECTIONS
            .devices_per_bucket
            .with_label_values(&[&format!("{bucket_number:02}")])
            .inc();

        info!(
            "Device id: {:#x} in bucket {} next wake scheduled at {}",
            device.id, bucket_number, next_wake_up
        );
        Ok(next_wake_up)
    }

    /// Restores scheduled connections from the database after HES restart.
    ///
    /// This checks for any previously scheduled connections and reschedules them
    /// if their next wake-up time hasn't expired yet (with a 5-minute safety margin).
    ///
    /// In cluster mode, only loads connections for devices owned by this node.
    /// Must be called after `enable_cluster_mode()` so that the ownership set is populated.
    pub async fn reload_active_connections(&mut self) -> Result<(), SchedulerError> {
        let db_start = std::time::Instant::now();
        let connections_result = self.database.get_scheduled_connections().await;
        let db_elapsed = db_start.elapsed().as_millis() as f64;
        METRICS_CONNECTIONS
            .hes_db_query_duration_ms
            .with_label_values(&[
                "get_scheduled_connections",
                if connections_result.is_ok() {
                    "ok"
                } else {
                    "error"
                },
            ])
            .observe(db_elapsed);

        let scheduled_connections = connections_result.map_err(|e| {
            METRICS_CONNECTIONS
                .hes_db_errors_total
                .with_label_values(&["get_scheduled_connections"])
                .inc();
            SchedulerError::DatabaseError(e)
        })?;

        let mut overdue_count: i64 = 0;

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
                overdue_count += 1;
                connection.status = ScheduledStatus::Lost;
                connection.renewable = false;
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

        METRICS_CONNECTIONS
            .hes_scheduler_overdue_devices_total
            .set(overdue_count);

        Ok(())
    }

    /// Cancel a pending wake-up job for a device.
    ///
    /// Returns `Ok(true)` if a job was removed, `Ok(false)` if none was scheduled.
    pub async fn cancel_wakeup_job(&mut self, device_id: Uuid) -> Result<bool, SchedulerError> {
        let conn = self.database.get_scheduled_connection(device_id).await?;
        let Some(job_id) = conn.job_id else {
            return Ok(false);
        };
        self.job_scheduler.remove(&job_id).await?;
        let mut conn = conn;
        conn.job_id = None;
        conn.renewable = false;
        self.database.update_scheduled_connection(&conn).await?;
        Ok(true)
    }

    pub async fn schedule_next_wakeup_job(
        &mut self,
        device_id: Uuid,
    ) -> Result<(), SchedulerError> {
        let bucket_number = self.database.get_bucket_number(device_id).await?;

        let next_wake_up = self.next_wakeup(bucket_number as usize)?;

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

    /// Returns the next wakeup datetime for a device.
    ///
    /// In test mode, returns `now + 5 minutes` regardless of bucket assignment.
    /// In normal mode, converts the bucket number to the next scheduled time of day.
    fn next_wakeup(&self, bucket_number: usize) -> Result<NaiveDateTime, SchedulerError> {
        if self.test_mode {
            Ok((Utc::now() + chrono::Duration::seconds(TEST_MODE_WAKEUP_SECS)).naive_utc())
        } else {
            let schedule = self.get_next_schedule(bucket_number);
            NaiveDateTime::try_from(&schedule).map_err(|e| {
                error!("Error building NaiveDateTime from Schedule: {}", e);
                SchedulerError::ParseError(e)
            })
        }
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

    /// Aborts the `wake_up_device` task currently running for `device_id`, if any.
    ///
    /// Returns `true` if a task was found and aborted, `false` if none was running.
    /// The aborted task receives a cancellation signal at its next `.await` point and
    /// exits cleanly without updating metrics or the database.
    pub fn abort_active_wakeup(&self, device_id: Uuid) -> bool {
        let mut tasks = self.active_tasks.lock().expect("active_tasks lock poisoned");
        if let Some(handle) = tasks.remove(&device_id) {
            handle.abort();
            true
        } else {
            false
        }
    }

    /// Sets the sender side of the reschedule channel.
    ///
    /// After a successful periodic session, `wake_up_device` sends the device UUID
    /// through this channel so the listener in main.rs can schedule the next job.
    pub fn set_reschedule_sender(&mut self, tx: mpsc::Sender<Uuid>) {
        self.reschedule_tx = Some(tx);
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
        let database = self.database.clone();
        let reschedule_tx = self.reschedule_tx.clone();
        let active_tasks = self.active_tasks.clone();

        let job = Job::new_async_tz(
            next_wake_up.format("%S %M %H %d %m * %Y").to_string(),
            chrono_tz::UTC,
            move |job_id, _l| {
                let db = database.clone();
                let tx = reschedule_tx.clone();
                let active_tasks = active_tasks.clone();
                Box::pin(async move {
                    // Measure how late the job fired vs its scheduled time.
                    let actual_now = Utc::now().naive_utc();
                    let drift_ms = (actual_now - next_wake_up).num_milliseconds().max(0) as f64;
                    METRICS_CONNECTIONS
                        .hes_scheduler_wake_drift_ms
                        .observe(drift_ms);

                    let job_start = std::time::Instant::now();

                    // Spawn the task so it can be aborted externally if a debug session
                    // takes over the device while retries are in progress.
                    let db_for_task = db.clone();
                    let handle = tokio::spawn(async move {
                        wake_up_device(job_id, device_id, &db_for_task, tx).await
                    });
                    active_tasks
                        .lock()
                        .expect("active_tasks lock poisoned")
                        .insert(device_id, handle.abort_handle());

                    let result = match handle.await {
                        Ok(res) => res,
                        Err(e) if e.is_cancelled() => {
                            info!(
                                "[Job {:#x}] wake-up task aborted — debug session took over",
                                job_id
                            );
                            active_tasks
                                .lock()
                                .expect("active_tasks lock poisoned")
                                .remove(&device_id);
                            return;
                        }
                        Err(e) => {
                            error!("[Job {:#x}] wake-up task panicked: {}", job_id, e);
                            active_tasks
                                .lock()
                                .expect("active_tasks lock poisoned")
                                .remove(&device_id);
                            return;
                        }
                    };

                    active_tasks
                        .lock()
                        .expect("active_tasks lock poisoned")
                        .remove(&device_id);

                    let elapsed_ms = job_start.elapsed().as_millis() as f64;
                    METRICS_CONNECTIONS
                        .hes_scheduler_job_execution_duration_ms
                        .observe(elapsed_ms);

                    if let Err(e) = result {
                        error!("[Job {:#x}] Wake up device failed: {}", job_id, e);

                        match db.get_scheduled_connection(device_id).await {
                            Err(e) => error!(
                                "[Job {:#x}] Failed to get scheduled connection: {}",
                                job_id, e
                            ),
                            Ok(mut conn) => {
                                conn.status = ScheduledStatus::Lost;
                                conn.renewable = false;
                                conn.job_id = None;

                                if let Err(e) = db.update_scheduled_connection(&conn).await {
                                    error!(
                                        "[Job {:#x}] Failed to update connection status: {}",
                                        job_id, e
                                    );
                                }
                            }
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use common::database::{DatabaseConfig, api::Database};

    async fn make_scheduler() -> Scheduler {
        let db = Database::new(DatabaseConfig::in_memory()).await.unwrap();
        Scheduler::new(1, db, Uuid::new_v4(), false).await.unwrap()
    }

    // --- abort_active_wakeup ---

    #[tokio::test]
    async fn abort_returns_false_when_no_task_running() {
        let scheduler = make_scheduler().await;
        assert!(!scheduler.abort_active_wakeup(Uuid::new_v4()));
    }

    #[tokio::test]
    async fn abort_returns_true_and_cancels_task() {
        let scheduler = make_scheduler().await;
        let device_id = Uuid::new_v4();

        // Simulate a long-running wake_up_device by spawning a task that sleeps forever.
        let handle = tokio::spawn(async {
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        });
        let abort_handle = handle.abort_handle();
        scheduler.active_tasks.lock().unwrap().insert(device_id, abort_handle);

        assert!(scheduler.abort_active_wakeup(device_id));

        // The spawned task should have been cancelled.
        assert!(handle.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn abort_removes_entry_so_second_call_returns_false() {
        let scheduler = make_scheduler().await;
        let device_id = Uuid::new_v4();

        let handle = tokio::spawn(async {
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        });
        scheduler.active_tasks.lock().unwrap().insert(device_id, handle.abort_handle());

        assert!(scheduler.abort_active_wakeup(device_id));
        assert!(!scheduler.abort_active_wakeup(device_id)); // entry already gone

        handle.abort(); // cleanup so the test doesn't leak the task
    }

    #[tokio::test]
    async fn abort_only_targets_the_specified_device() {
        let scheduler = make_scheduler().await;
        let device_a = Uuid::new_v4();
        let device_b = Uuid::new_v4();

        let handle_a = tokio::spawn(async {
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        });
        let handle_b = tokio::spawn(async {
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        });
        {
            let mut tasks = scheduler.active_tasks.lock().unwrap();
            tasks.insert(device_a, handle_a.abort_handle());
            tasks.insert(device_b, handle_b.abort_handle());
        }

        // Abort only device_a.
        assert!(scheduler.abort_active_wakeup(device_a));

        // device_b's task should still be running.
        assert!(!handle_b.is_finished());
        assert_eq!(scheduler.active_tasks.lock().unwrap().len(), 1);

        handle_b.abort(); // cleanup
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
