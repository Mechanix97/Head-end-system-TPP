use tracing::info;

use backdoor::backdoor::init_backdoor;
use scheduler::scheduler::init_scheduler;

fn main() {
    tracing_subscriber::fmt().init();

    info!("Head-End System starting");

    init_scheduler();

    init_backdoor();
}
