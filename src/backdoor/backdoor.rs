use futures_util::stream::StreamExt;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio_util::udp::UdpFramed;
use tracing::error;
use tracing::info;

use crate::BackdoorError;
use common::connection::Conection;
use common::messages::codec::MessageCodec;
use common::messages::message::MsgType;
use scheduler::scheduler::Scheduler;

pub async fn init_backdoor(
    scheduler: Arc<Mutex<Scheduler>>,
    ip: String,
    port: String,
) -> Result<(), BackdoorError> {
    let socket = UdpSocket::bind(format!("{ip}:{port}")).await?;
    info!("Listening for device registration on {ip}:{port} via UDP");

    let sc: Arc<Mutex<Scheduler>> = scheduler.clone();
    let codec = MessageCodec;
    let mut framed = UdpFramed::new(socket, codec);

    tokio::spawn(async move {
        while let Some(result) = framed.next().await {
            match result {
                Ok((msg, addr)) => {
                    info!("Received message from {}: {:?}", addr, msg);
                    if let MsgType::RegisterRequest = msg.msg_type {
                        if let Err(err) = sc
                            .lock()
                            .await
                            .add_connection(Conection {
                                id: msg.device_id,
                                ip: addr.ip().to_string(),
                            })
                            .await
                        {
                            error!("Error adding new connection to scheduler: {err}");
                            continue;
                        }
                        // let response = ;
                        // if let Err(err) = framed.send((response, addr)).await {
                        //     error!("Error sending response: {err}");
                        // }
                    } else {
                        info!("Invalid msg");
                    }
                }
                Err(e) => {
                    error!("Error receiving message: {:?}", e);
                }
            }
        }
    });
    Ok(())
}
