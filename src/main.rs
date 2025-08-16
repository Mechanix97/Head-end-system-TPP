use tracing::info;

use scheduler::scheduler::init_scheduler;
use backdoor::backdoor:init_backdoor;

fn main() {
    tracing_subscriber::fmt().init();

    info!("Head-End System starting");

    init_scheduler();

    init_backdoor();
}
