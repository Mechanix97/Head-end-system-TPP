use bytes::BytesMut;
use clap::Parser;
use std::error::Error;
use std::time::Duration;
use tokio::net::UdpSocket;

use tokio::time::sleep;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use tracing::{Level, info};

use common::messages::codec::MessageCodec;
use common::messages::message::Message;
use common::messages::message::MessagePayload;
use common::messages::message::MsgType;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "4")]
    number: u32,
    /// Backdoor IP
    #[arg(
        long = "backdoor-ip",
        default_value = "mechardo3d.mooo.com",
        help = "Prometheus metrics api IP"
    )]
    backdoor_ip: String,

    /// Backdoor port
    #[arg(long = "backdoor-port", default_value = "6565", help = "Backdoor port")]
    backdoor_port: String,
}

/// 1. sends registration request msg
/// 2. receives registration response msg
/// 3. sends ack response
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Sending {} registration messages", args.number);

    for i in 1..args.number + 1 {
        info!("Sending registration {i}/{}", args.number);

        // 1. sends registration request msg
        let register_request = Message::new_register_request_message()?;
        let device_socket = UdpSocket::bind("0.0.0.0:0").await?;
        let mut buffer = BytesMut::new();

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(
                &buffer,
                format!("{}:{}", args.backdoor_ip, args.backdoor_port),
            )
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(1000)).await;

        // 2. receives registration response msg
        buffer = BytesMut::new();
        device_socket.recv_buf(&mut buffer).await.unwrap();
        let response = codec.decode(&mut buffer).unwrap().unwrap();

        // 3. sends ack response
        let ack_msg = Message {
            version: 1,
            msg_type: MsgType::Ack,
            device_id: response.device_id,
            seq: response.seq + 1,
            timestamp: 0,
            payload: MessagePayload::Ack,
            mac: 0,
        };

        buffer = BytesMut::new();
        codec.encode(ack_msg.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(
                &buffer,
                format!("{}:{}", args.backdoor_ip, args.backdoor_port),
            )
            .await
            .expect("Failed to send RegisterRequest");
    }
    Ok(())
}
