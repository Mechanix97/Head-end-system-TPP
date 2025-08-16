use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::info;

use crate::error::SchedulerError;

pub async fn init_scheduler() -> Result<(), SchedulerError> {
    info!("init_scheduler");

    let sched = JobScheduler::new().await?;

    sched
        .add(Job::new_async("1/10 * * * * *", |_uuid, _l| {
            Box::pin(async move {
                periodically_task().await;
            })
        })?)
        .await?;
    sched.start().await?;

    Ok(())
}

async fn periodically_task() {
    info!("periodically_task");
}
