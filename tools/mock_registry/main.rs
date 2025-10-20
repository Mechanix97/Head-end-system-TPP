use bytes::BytesMut;
use clap::Parser;

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
use common::messages::registry::RegistryRequestMessage;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "1")]
    number: u32,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Sending {} registration messages", args.number);

    // 1. sends registration request msg
    let register_request = Message {
        version: 1,
        msg_type: MsgType::RegisterRequest,
        device_id: 0,
        seq: 0,
        timestamp: 0,
        payload: MessagePayload::RegistryRequest(RegistryRequestMessage {}),
        mac: 0,
    };
    let device_socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let mut buffer = BytesMut::new();

    let mut codec = MessageCodec;
    codec.encode(register_request.clone(), &mut buffer).unwrap();

    device_socket
        .send_to(&buffer, format!("mechardo3d.mooo.com:{}", 6565))
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
        .send_to(&buffer, format!("mechardo3d.mooo.com:{}", 6565))
        .await
        .expect("Failed to send RegisterRequest");
}
