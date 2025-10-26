use chrono::NaiveDateTime;
use chrono::{Datelike, Timelike, Utc};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::error;
use tracing::info;

use crate::error::SchedulerError;
use crate::schedule::Schedule;
use common::connection::ConnectionStatus;
use common::{connection::Connection, database::api::Database};
use metrics::metrics_connections::METRICS_CONNECTIONS;

type Bucket = Vec<Connection>;

pub struct Scheduler {
    pub buckets: Vec<Bucket>,
    pub job_scheduler: JobScheduler,
    pub database: Database,
}

impl Scheduler {
    pub async fn new(bucket_number: usize, database: Database) -> Result<Self, SchedulerError> {
        let mut scheduler = Self {
            buckets: vec![Vec::new(); bucket_number],
            job_scheduler: JobScheduler::new().await?,
            database,
        };

        scheduler.load_active_connections().await?;

        Ok(scheduler)
    }

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

    pub async fn add_connection(
        &mut self,
        connection: &mut Connection,
    ) -> Result<(), SchedulerError> {
        METRICS_CONNECTIONS
            .connections_tracker
            .with_label_values(&["new_connection"])
            .inc();

        let bucket_number = self.get_bucket_number();
        let next_wake_up = self.get_next_schedule(bucket_number);

        connection.bucket = Some(bucket_number as i32);
        connection.next_wakeup = Some(
            NaiveDateTime::parse_from_str(&next_wake_up.to_string(), "%H:%M:%S %d/%m/%Y").map_err(
                |e| {
                    error!("Error parsing next_wakeup from Schedule: {}", e);
                    SchedulerError::ParseError(e.to_string())
                },
            )?,
        );

        self.buckets[bucket_number].push(connection.clone());
        self.database.add_new_connection(connection).await?;

        info!(
            "Connection id: {:#x} in bucket {} next wake scheduled at {}",
            connection.device_id, bucket_number, next_wake_up
        );

        Ok(())
    }
    async fn load_active_connections(&mut self) -> Result<(), SchedulerError> {
        Ok(())
    }

    pub async fn start_task(&mut self, mut connection: Connection) -> Result<(), SchedulerError> {
        connection.status = ConnectionStatus::Active;

        let Some(next_wake_up) = connection.next_wakeup else {
            return Err(SchedulerError::NoScheduleDefined);
        };

        let next_wake_up = next_wake_up.format("%S %M %H %d %m * %Y").to_string();
        let cc2 = connection.clone();
        self.job_scheduler
            .add(Job::new_async(next_wake_up.clone(), move |_uuid, _l| {
                let cc = cc2.clone();

                Box::pin(async move {
                    periodically_task(cc).await;
                })
            })?)
            .await?;

        info!(
            "Scheduled next connetion to divice {:#x} at {}",
            connection.device_id, next_wake_up
        );

        self.database.update_connection(&connection).await?;

        Ok(())
    }

    pub fn get_bucket_number(&self) -> usize {
        self.buckets
            .iter()
            .enumerate()
            .min_by_key(|(_, bucket)| bucket.len())
            .map(|(index, _)| index)
            .unwrap_or(0)
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

    pub fn get_time_from_bucket_number(&self, bucket_number: usize) -> (usize, usize, usize) {
        let secs_per_day: usize = 24 * 60 * 60;

        let total_buckets = self.buckets.len();

        let secs_per_bucket = secs_per_day / total_buckets;

        let slot_in_secs = secs_per_bucket * bucket_number;

        (
            slot_in_secs % 3600 % 60,
            slot_in_secs % 3600 / 60,
            slot_in_secs / 3600,
        )
    }

    pub async fn get_active_connections(&self) -> Result<Vec<Connection>, SchedulerError> {
        self.database
            .get_active_connections()
            .await
            .map_err(SchedulerError::DatabaseError)
    }
}

fn get_date_from_hour(hour: usize) -> (usize, usize, usize) {
    let today = Utc::now();
    let tomorrow = today + chrono::Duration::days(1);

    if hour > today.hour() as usize + 1 {
        return (
            today.day() as usize,
            today.month() as usize,
            today.year() as usize,
        );
    }
    (
        tomorrow.day() as usize,
        tomorrow.month() as usize,
        tomorrow.year() as usize,
    )
}

async fn periodically_task(conn: Connection) {
    info!(
        "Conection ID: {} IP: {}",
        conn.device_id,
        conn.ip.unwrap_or("No ip".to_string())
    );
}
