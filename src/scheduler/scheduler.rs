use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::info;

use crate::error::SchedulerError;
use common::connection_data::Conection;

const TOTAL_BUCKETS: usize = 48;

type Bucket = Vec<Conection>;

pub struct Scheduler {
    pub buckets: Vec<Bucket>,
    pub job_scheduler: JobScheduler,
}

impl Scheduler {
    pub async fn new() -> Result<Self, SchedulerError> {
        Ok(Self {
            buckets: vec![Vec::new(); TOTAL_BUCKETS],
            job_scheduler: JobScheduler::new().await?,
        })
    }

    pub async fn start(&mut self) -> Result<(), SchedulerError> {
        self.job_scheduler.start().await?;
        Ok(())
    }

    pub async fn add_connection(&mut self, connection: Conection) -> Result<(), SchedulerError> {
        self.buckets[0].push(connection.clone());

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

async fn periodically_task(id: u32, ip: String) {
    info!("Conection ID: {id} IP: {ip}");
}
