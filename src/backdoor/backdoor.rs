use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;

use crate::BackdoorError;
use common::connection::Conection;
use scheduler::scheduler::Scheduler;

pub async fn init_backdoor(
    scheduler: Arc<Mutex<Scheduler>>,
    ip: String,
    port: String,
) -> Result<(), BackdoorError> {
    let listener = TcpListener::bind(format!("{}:{}", ip, port)).await?;
    info!("Listening for device registration on {}:{}", ip, port);

    let sc: Arc<Mutex<Scheduler>> = scheduler.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut _stream, addr)) => {
                    info!("New connection from {}", addr);
                    if let Err(err) = sc
                        .lock()
                        .await
                        .add_connection(Conection {
                            id: 1234,
                            ip: "String".to_string(),
                        })
                        .await
                    {
                        error!("Error adding new connection to scheduler: {err}");
                        continue;
                    }
                }
                Err(e) => {
                    info!("Error accepting connection: {:?}", e);
                }
            }
        }
    });
    Ok(())
}
