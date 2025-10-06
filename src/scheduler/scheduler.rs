use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::info;

use crate::error::SchedulerError;
use common::connection::Connection;
use metrics::metrics_connections::METRICS_CONNECTIONS;

type Bucket = Vec<Connection>;

pub struct Scheduler {
    pub buckets: Vec<Bucket>,
    pub job_scheduler: JobScheduler,
}

impl Scheduler {
    pub async fn new(bucket_number: usize) -> Result<Self, SchedulerError> {
        Ok(Self {
            buckets: vec![Vec::new(); bucket_number],
            job_scheduler: JobScheduler::new().await?,
        })
    }

    pub async fn start(&mut self) -> Result<(), SchedulerError> {
        self.job_scheduler.start().await?;
        Ok(())
    }

    pub async fn add_connection(&mut self, connection: Connection) -> Result<(), SchedulerError> {
        self.buckets[0].push(connection.clone());

        METRICS_CONNECTIONS
            .connections_tracker
            .with_label_values(&["new_connection"])
            .inc();

        self.job_scheduler
            .add(Job::new_async("1/10 * * * * *", move |_uuid, _l| {
                let cc = connection.clone();

                Box::pin(async move {
                    periodically_task(cc.id, cc.ip).await;
                })
            })?)
            .await?;
        Ok(())
    }
}

async fn periodically_task(id: u128, ip: String) {
    info!("Conection ID: {id} IP: {ip}");
}
