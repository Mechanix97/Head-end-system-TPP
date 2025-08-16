use std::error::Error;
use tracing::info;

use backdoor::backdoor::init_backdoor;
use scheduler::scheduler::init_scheduler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt().init();

    info!("Head-End System starting");

    init_scheduler().await?;

    init_backdoor();

    loop {}
}
