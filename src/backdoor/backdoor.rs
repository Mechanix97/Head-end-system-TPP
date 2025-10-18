use bytes::BytesMut;
use futures::sink::SinkExt;
use futures_util::stream::StreamExt;
use rand::Rng;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::codec::Encoder;
use tokio_util::udp::UdpFramed;
use tracing::{error, info, warn};

use crate::BackdoorError;
use common::connection::Connection;
use common::messages::codec::MessageCodec;
use common::messages::message::Message;
use common::messages::message::MsgType;
use scheduler::scheduler::Scheduler;

const ACK_TIMEOUT_DURATION_MS: u64 = 300000;

pub async fn init_backdoor(
    scheduler: Arc<Mutex<Scheduler>>,
    ip: String,
    port: String,
    ack_timeout_duration: Option<u64>,
) -> Result<JoinHandle<()>, BackdoorError> {
    let socket = UdpSocket::bind(format!("{ip}:{port}")).await?;
    info!("Listening for device registration on {ip}:{port} via UDP");

    let ack_timeout_duration = ack_timeout_duration.unwrap_or(ACK_TIMEOUT_DURATION_MS);

    let scheduler_clone: Arc<Mutex<Scheduler>> = scheduler.clone();
    let codec = MessageCodec;
    let mut framed: UdpFramed<MessageCodec> = UdpFramed::new(socket, codec);

    let join_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        let pending_connections: Arc<Mutex<HashSet<Connection>>> =
            Arc::new(Mutex::new(HashSet::new()));

        loop {
            let Some(frame) = framed.next().await else {
                warn!("Invalid codec conversion");
                continue;
            };

            let Ok((msg, socket_addr)) = frame else {
                warn!("Invalid codec conversion");
                continue;
            };
            match msg.msg_type {
                MsgType::RegisterRequest => {
                    info!("RegisterRequest received");
                    if let Err(err) = handle_backdoor_register_msg(
                        &mut framed,
                        msg,
                        socket_addr,
                        pending_connections.clone(),
                        ack_timeout_duration,
                    )
                    .await
                    {
                        error!("Error handle register request: {err}");
                    }
                }
                MsgType::Ack => {
                    info!("Ack received");
                    if let Err(err) = handle_backdoor_ack_msg(
                        &scheduler_clone,
                        msg,
                        socket_addr,
                        pending_connections.clone(),
                    )
                    .await
                    {
                        error!("Error handle register request: {err}");
                    }
                }

                _ => {
                    warn!("Received incompatible msg in backdoor: {:?}", msg.msg_type);
                }
            }
        }
    });
    Ok(join_handle)
}

/// This function receives a new registration msg from a new device.
/// The msg contains all the information from the device (such as batch_id)
/// The HES will verify that the information is correct and register the device on the network.
/// The HES will provide unique device_id and inform the device the next schedule connection.
/// After sending the response msg (RegisterResponse) the HES expects a ACK message to start with the schedule sequence.
async fn handle_backdoor_register_msg(
    framed: &mut UdpFramed<MessageCodec>,
    msg: Message,
    socket_addr: SocketAddr,
    pending_connections: Arc<Mutex<HashSet<Connection>>>,
    ack_timeout_duration: u64,
) -> Result<(), BackdoorError> {
    // TODO: check that the information provided is correct #10
    if msg.device_id != 0 {
        return Err(BackdoorError::RegisterRequestInvalidId);
    }

    let device_id = rand::rng().random::<u128>();

    let ack_msg = Message::new_ack_message(device_id, 3)?;
    let mut buffer: BytesMut = BytesMut::new();
    let mut codec = MessageCodec;
    codec
        .encode(ack_msg, &mut buffer)
        .expect("Error encoding msg");
    info!("ACK Message expected: {}", hex::encode(&buffer));

    let connection = Connection::new(device_id, socket_addr.ip().to_string());

    {
        pending_connections.lock().await.insert(connection.clone());
    }

    let response = Message::new_register_response(device_id, msg.seq + 1)?;
    if let Err(err) = (*framed).send((response, socket_addr)).await {
        error!("Error sending response: {err}");
    }

    let pending_connections_clone = pending_connections.clone();
    let connection_clone = connection.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(ack_timeout_duration)).await;
        if pending_connections_clone
            .lock()
            .await
            .remove(&connection_clone)
        {
            info!("Ack from {} not received", connection.id);
        }
    });

    Ok(())
}

/// This functions handles the ack from the new device.
/// Checks if the device has requested the registration in the interval (300 ms)
/// and starts the scheduler sequence.
async fn handle_backdoor_ack_msg(
    scheduler: &Arc<Mutex<Scheduler>>,
    msg: Message,
    socket_addr: SocketAddr,
    pending_connections: Arc<Mutex<HashSet<Connection>>>,
) -> Result<(), BackdoorError> {
    let connection = Connection::new(msg.device_id, socket_addr.ip().to_string());

    if pending_connections.lock().await.remove(&connection) {
        info!("Adding new connection, device_id: {}", msg.device_id);
        let mut scheduler_lock = scheduler.lock().await;
        scheduler_lock.add_connection(connection).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use common::messages::message::MessagePayload;
    use common::messages::registry::RegistryRequestMessage;
    use scheduler::scheduler::Scheduler;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio_util::codec::Decoder;
    use tokio_util::codec::Encoder;

    use super::*;

    /// This test checks the normal backdoor registration event
    /// 1. sends registration request msg
    /// 2. receives registration response msg
    /// 3. sends ack response
    #[tokio::test]
    async fn test_new_connection() {
        // 0. intial backdoor setup
        let backdoor_port = "8081";
        let scheduler = Arc::new(Mutex::new(Scheduler::new(1).await.unwrap()));
        init_backdoor(
            scheduler.clone(),
            "0.0.0.0".to_string(),
            backdoor_port.to_string(),
            Some(300),
        )
        .await
        .unwrap();

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
        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer = BytesMut::new();

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;
        let connecitons_number = scheduler.lock().await.buckets[0].len();
        assert_eq!(connecitons_number, 0);

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
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        let connecitons_number = scheduler.lock().await.buckets[0].len();
        assert_eq!(connecitons_number, 1);
    }

    /// This test checks the ACK timeout in the backdoor registration event
    /// 1. sends registration request msg
    /// 2. receives registration response msg
    /// 3. adds some delay to trigger the ack timeout
    /// 4. sends ack response
    #[tokio::test]
    async fn test_ack_timeout() {
        // 0. intial backdoor setup
        let backdoor_port = "8082";
        let scheduler = Arc::new(Mutex::new(Scheduler::new(1).await.unwrap()));
        init_backdoor(
            scheduler.clone(),
            "0.0.0.0".to_string(),
            backdoor_port.to_string(),
            Some(300),
        )
        .await
        .unwrap();

        let connecitons_number = scheduler.lock().await.buckets[0].len();
        assert_eq!(connecitons_number, 0);

        let register_request = Message {
            version: 1,
            msg_type: MsgType::RegisterRequest,
            device_id: 0,
            seq: 0,
            timestamp: 0,
            payload: MessagePayload::RegistryRequest(RegistryRequestMessage {}),
            mac: 0,
        };
        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer = BytesMut::new();

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        let connecitons_number = scheduler.lock().await.buckets[0].len();
        assert_eq!(connecitons_number, 0);

        // 2. receives registration response msg
        buffer = BytesMut::new();
        device_socket.recv_buf(&mut buffer).await.unwrap();
        let response = codec.decode(&mut buffer).unwrap().unwrap();

        // 3. adds some delay to trigger the ack timeout
        sleep(Duration::from_millis(500)).await;

        // 4. sends ack response
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
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        let connecitons_number = scheduler.lock().await.buckets[0].len();
        assert_eq!(connecitons_number, 0);
    }

    // test 10 secuential connections
    #[tokio::test]
    async fn test_multiple_connections() {
        // 0. intial backdoor setup
        let backdoor_port = "8083";
        let scheduler = Arc::new(Mutex::new(Scheduler::new(1).await.unwrap()));
        init_backdoor(
            scheduler.clone(),
            "0.0.0.0".to_string(),
            backdoor_port.to_string(),
            Some(300),
        )
        .await
        .unwrap();

        for i in 0..10 {
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
            let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut buffer = BytesMut::new();

            let mut codec = MessageCodec;
            codec.encode(register_request.clone(), &mut buffer).unwrap();

            device_socket
                .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
                .await
                .expect("Failed to send RegisterRequest");
            sleep(Duration::from_millis(100)).await;
            let connecitons_number = scheduler.lock().await.buckets[0].len();
            assert_eq!(connecitons_number, i);

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
                .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
                .await
                .expect("Failed to send RegisterRequest");
            sleep(Duration::from_millis(100)).await;
        }
        let connecitons_number = scheduler.lock().await.buckets[0].len();
        assert_eq!(connecitons_number, 10);
    }

    // test 2 parallels connections
    #[tokio::test]
    async fn test_parallel_connections() {
        // 0. intial backdoor setup
        let backdoor_port = "8084";
        let scheduler = Arc::new(Mutex::new(Scheduler::new(1).await.unwrap()));
        init_backdoor(
            scheduler.clone(),
            "0.0.0.0".to_string(),
            backdoor_port.to_string(),
            Some(300),
        )
        .await
        .unwrap();

        // 1a. sends registration request msg
        let register_request = Message {
            version: 1,
            msg_type: MsgType::RegisterRequest,
            device_id: 0,
            seq: 0,
            timestamp: 0,
            payload: MessagePayload::RegistryRequest(RegistryRequestMessage {}),
            mac: 0,
        };
        let device_socket_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer: BytesMut = BytesMut::new();

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket_a
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;
        let connecitons_number = scheduler.lock().await.buckets[0].len();
        assert_eq!(connecitons_number, 0);

        // 1a. sends registration request msg
        let register_request = Message {
            version: 1,
            msg_type: MsgType::RegisterRequest,
            device_id: 0,
            seq: 0,
            timestamp: 0,
            payload: MessagePayload::RegistryRequest(RegistryRequestMessage {}),
            mac: 0,
        };
        let device_socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer: BytesMut = BytesMut::new();

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket_b
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;
        let connecitons_number = scheduler.lock().await.buckets[0].len();
        assert_eq!(connecitons_number, 0);

        // 2a. receives registration response msg
        buffer = BytesMut::new();
        device_socket_a.recv_buf(&mut buffer).await.unwrap();
        let response_a = codec.decode(&mut buffer).unwrap().unwrap();

        // 2b. receives registration response msg
        buffer = BytesMut::new();
        device_socket_b.recv_buf(&mut buffer).await.unwrap();
        let response_b = codec.decode(&mut buffer).unwrap().unwrap();

        // 3a. sends ack response
        let ack_msg_a = Message {
            version: 1,
            msg_type: MsgType::Ack,
            device_id: response_a.device_id,
            seq: response_a.seq + 1,
            timestamp: 0,
            payload: MessagePayload::Ack,
            mac: 0,
        };

        buffer = BytesMut::new();
        codec.encode(ack_msg_a.clone(), &mut buffer).unwrap();

        device_socket_a
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        // 3b. sends ack response
        let ack_msg_b = Message {
            version: 1,
            msg_type: MsgType::Ack,
            device_id: response_b.device_id,
            seq: response_b.seq + 1,
            timestamp: 0,
            payload: MessagePayload::Ack,
            mac: 0,
        };

        buffer = BytesMut::new();
        codec.encode(ack_msg_b.clone(), &mut buffer).unwrap();

        device_socket_a
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        let connecitons_number = scheduler.lock().await.buckets[0].len();
        assert_eq!(connecitons_number, 2);
    }
}
