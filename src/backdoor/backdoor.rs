use bytes::BytesMut;
use chrono::DateTime;
use chrono::Utc;
use common::database::api::Database;
use futures::sink::SinkExt;
use futures_util::stream::StreamExt;
use metrics::metrics_connections::METRICS_CONNECTIONS;
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
use uuid::Uuid;

use crate::BackdoorError;
use common::device::Device;
use common::messages::codec::MessageCodec;
use common::messages::message::Message;
use common::messages::message::MsgType;
use scheduler::scheduler::Scheduler;

const ACK_TIMEOUT_DURATION_MS: u64 = 30000;

pub async fn init_backdoor(
    scheduler: Arc<Mutex<Scheduler>>,
    ip: String,
    port: String,
    ack_timeout_duration: Option<u64>,
    database: Database,
) -> Result<JoinHandle<()>, BackdoorError> {
    let socket = UdpSocket::bind(format!("{ip}:{port}")).await?;
    info!("Listening for device registration on {ip}:{port} via UDP");

    let ack_timeout_duration = ack_timeout_duration.unwrap_or(ACK_TIMEOUT_DURATION_MS);

    let scheduler_clone: Arc<Mutex<Scheduler>> = scheduler.clone();
    let codec = MessageCodec;
    let mut framed: UdpFramed<MessageCodec> = UdpFramed::new(socket, codec);

    let join_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        loop {
            let Some(frame) = framed.next().await else {
                warn!("Invalid codec conversion");
                continue;
            };

            let (msg, socket_addr) = match frame {
                Ok((msg, socket_addr)) => (msg, socket_addr),
                Err(e) => {
                    warn!("Invalid codec conversion: {e}");
                    continue;
                }
            };

            match msg.msg_type {
                MsgType::RegisterRequest => {
                    info!("RegisterRequest received");
                    if msg.device_id == 0 {
                        if let Err(err) = handle_backdoor_register_msg(
                            &mut framed,
                            msg,
                            socket_addr,
                            &scheduler_clone,
                            ack_timeout_duration,
                            database.clone(),
                        )
                        .await
                        {
                            error!("Error handle register request: {err}");
                        }
                    } else {
                        // TODO handle ip change
                    }
                }
                MsgType::Ack => {
                    info!("Ack received");
                    if let Err(err) = handle_backdoor_ack_msg(
                        &scheduler_clone,
                        msg,
                        socket_addr,
                        database.clone(),
                    )
                    .await
                    {
                        error!("Error handle ack msg: {err}");
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
    scheduler: &Arc<Mutex<Scheduler>>,
    ack_timeout_duration: u64,
    database: Database,
) -> Result<(), BackdoorError> {
    // TODO: check that the information provided is correct #10
    if msg.device_id != 0 {
        return Err(BackdoorError::RegisterRequestInvalidId);
    }

    let device = Device::new(socket_addr, None, None, None);
    database.add_device(&device).await?;

    scheduler.lock().await.register_device(&device).await?;

    // Convert milliseconds to seconds and nanoseconds
    let secs = (msg.timestamp / 1000) as i64;
    let nanos = ((msg.timestamp % 1000) * 1_000_000) as u32;
    let timestamp = DateTime::from_timestamp(secs, nanos)
        .ok_or(BackdoorError::InvalidTimeStamp)?
        .naive_utc();
    database.register_device(device.id, timestamp).await?;

    let response = Message::new_register_response_message(device.id.as_u128(), msg.seq + 1)?;
    if let Err(err) = (*framed).send((response, socket_addr)).await {
        error!("Error sending response: {err}");
    }

    spawn_ack_timeout_task(database.clone(), ack_timeout_duration, device.id);

    // TEMPORARY.
    // REMOVE LATER
    let ack_msg = Message::new_ack_message(device.id.as_u128(), 3)?;
    let mut buffer: BytesMut = BytesMut::new();
    let mut codec = MessageCodec;
    codec
        .encode(ack_msg, &mut buffer)
        .expect("Error encoding msg");
    info!("ACK Message expected: {}", hex::encode(&buffer));
    // TEMPORARY.
    // REMOVE LATER

    Ok(())
}

/// This functions handles the ack from the new device.
/// Checks if the device has requested the registration in the interval (300 ms)
/// and starts the scheduler sequence.
async fn handle_backdoor_ack_msg(
    scheduler: &Arc<Mutex<Scheduler>>,
    msg: Message,
    socket_addr: SocketAddr,
    database: Database,
) -> Result<(), BackdoorError> {
    let device = database.get_device(Uuid::from_u128(msg.device_id)).await?;

    if Some(socket_addr.ip().to_string()) != device.ipv4 {
        error!(
            "Invalid IP, expected {}, recvd {:?}",
            socket_addr.ip().to_string(),
            device.ipv4
        );
        return Err(BackdoorError::InvalidIp);
    }

    // Convert milliseconds to seconds and nanoseconds
    let secs = (msg.timestamp / 1000) as i64;
    let nanos = ((msg.timestamp % 1000) * 1_000_000) as u32;
    let timestamp = DateTime::from_timestamp(secs, nanos)
        .ok_or(BackdoorError::InvalidTimeStamp)?
        .naive_utc();

    // registration_ack returns the response time in seconds
    let duration_seconds = database.registration_ack(device.id, timestamp).await?;
    let duration_ms = duration_seconds * 1000.0;

    info!(
        "ACK timing - device {:#x}: {:.6}s = {:.3}ms",
        msg.device_id, duration_seconds, duration_ms
    );

    // Update metrics
    METRICS_CONNECTIONS
        .ack_response_time_avg_ms
        .set(duration_ms);

    // If response time exceeded the timeout, increment the timeout counter
    if duration_ms > ACK_TIMEOUT_DURATION_MS as f64 {
        METRICS_CONNECTIONS.ack_timeout_count.inc();
        info!(
            "ACK timeout detected for device {:#x}: {:.2}ms (threshold: {}ms)",
            msg.device_id, duration_ms, ACK_TIMEOUT_DURATION_MS
        );
    }

    info!(
        "Adding new connection, device_id: {:#x}, ACK response time: {:.2}ms",
        msg.device_id, duration_ms
    );
    scheduler
        .lock()
        .await
        .schedule_wakeup_job(device.id)
        .await?;

    Ok(())
}

fn spawn_ack_timeout_task(database: Database, ack_timeout_duration: u64, device_id: Uuid) {
    tokio::spawn(async move {
        sleep(Duration::from_millis(ack_timeout_duration)).await;
        match database
            .registration_timeout(device_id, Utc::now().naive_utc())
            .await
        {
            Ok(true) => {
                info!("Ack from {} not received", device_id);
            }
            Err(e) => {
                error!("Error during device registration ack timeout check {e}");
            }
            _ => {}
        }
    });
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use common::database::DatabaseType;
    use common::database::api::Database;
    use scheduler::scheduler::Scheduler;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio_util::codec::Decoder;
    use tokio_util::codec::Encoder;

    use super::*;

    async fn set_up_hes(backdoor_port: &str) -> Arc<Mutex<Scheduler>> {
        let db = Database::new(DatabaseType::InMemory, None).await.unwrap();
        let scheduler: Arc<Mutex<Scheduler>> =
            Arc::new(Mutex::new(Scheduler::new(1, db.clone()).await.unwrap()));
        init_backdoor(
            scheduler.clone(),
            "0.0.0.0".to_string(),
            backdoor_port.to_string(),
            Some(300),
            db.clone(),
        )
        .await
        .unwrap();
        scheduler
    }

    /// This test checks the normal backdoor registration event
    /// 1. sends registration request msg
    /// 2. receives registration response msg
    /// 3. sends ack response
    #[tokio::test]
    async fn test_new_connection() {
        // 0. intial backdoor setup
        let backdoor_port = "8081";
        let scheduler = set_up_hes(backdoor_port).await;

        // 1. sends registration request msg
        let register_request: Message = Message::new_register_request_message().unwrap();
        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer = BytesMut::new();

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;
        let connecitons_number = scheduler
            .lock()
            .await
            .get_active_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);

        // 2. receives registration response msg
        buffer = BytesMut::new();
        device_socket.recv_buf(&mut buffer).await.unwrap();
        let response = codec.decode(&mut buffer).unwrap().unwrap();

        // 3. sends ack response
        let ack_msg = Message::new_ack_message(response.device_id, response.seq + 1).unwrap();

        buffer = BytesMut::new();
        codec.encode(ack_msg.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        let connecitons_number = scheduler
            .lock()
            .await
            .get_active_connections()
            .await
            .unwrap()
            .len();
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
        let scheduler = set_up_hes(backdoor_port).await;

        let connecitons_number = scheduler
            .lock()
            .await
            .get_active_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);

        let register_request: Message = Message::new_register_request_message().unwrap();
        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer = BytesMut::new();

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        let connecitons_number = scheduler
            .lock()
            .await
            .get_active_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);

        // 2. receives registration response msg
        buffer = BytesMut::new();
        device_socket.recv_buf(&mut buffer).await.unwrap();
        let response = codec.decode(&mut buffer).unwrap().unwrap();

        // 3. adds some delay to trigger the ack timeout
        sleep(Duration::from_millis(500)).await;

        // 4. sends ack response
        let ack_msg = Message::new_ack_message(response.device_id, response.seq + 1).unwrap();
        buffer = BytesMut::new();
        codec.encode(ack_msg.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        let connecitons_number = scheduler
            .lock()
            .await
            .get_active_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);
    }

    // test 10 secuential connections
    #[tokio::test]
    async fn test_multiple_connections() {
        // 0. intial backdoor setup
        let backdoor_port = "8083";
        let scheduler = set_up_hes(backdoor_port).await;

        for i in 0..10 {
            // 1. sends registration request msg
            let register_request: Message = Message::new_register_request_message().unwrap();
            let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut buffer = BytesMut::new();

            let mut codec = MessageCodec;
            codec.encode(register_request.clone(), &mut buffer).unwrap();

            device_socket
                .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
                .await
                .expect("Failed to send RegisterRequest");
            sleep(Duration::from_millis(100)).await;
            let connecitons_number = scheduler
                .lock()
                .await
                .get_active_connections()
                .await
                .unwrap()
                .len();
            assert_eq!(connecitons_number, i);

            // 2. receives registration response msg
            buffer = BytesMut::new();
            device_socket.recv_buf(&mut buffer).await.unwrap();
            let response = codec.decode(&mut buffer).unwrap().unwrap();

            // 3. sends ack response
            let ack_msg = Message::new_ack_message(response.device_id, response.seq + 1).unwrap();
            buffer = BytesMut::new();
            codec.encode(ack_msg.clone(), &mut buffer).unwrap();

            device_socket
                .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
                .await
                .expect("Failed to send RegisterRequest");
            sleep(Duration::from_millis(100)).await;
        }
        let connecitons_number = scheduler
            .lock()
            .await
            .get_active_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 10);
    }

    // test 2 parallels connections
    #[tokio::test]
    async fn test_parallel_connections() {
        // 0. intial backdoor setup
        let backdoor_port = "8084";
        let scheduler = set_up_hes(backdoor_port).await;

        // 1a. sends registration request msg
        let register_request: Message = Message::new_register_request_message().unwrap();
        let device_socket_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer: BytesMut = BytesMut::new();

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket_a
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;
        let connecitons_number = scheduler
            .lock()
            .await
            .get_active_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);

        // 1a. sends registration request msg
        let register_request: Message = Message::new_register_request_message().unwrap();
        let device_socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer: BytesMut = BytesMut::new();

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket_b
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;
        let connecitons_number = scheduler
            .lock()
            .await
            .get_active_connections()
            .await
            .unwrap()
            .len();
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
        let ack_msg_a = Message::new_ack_message(response_a.device_id, response_a.seq + 1).unwrap();

        buffer = BytesMut::new();
        codec.encode(ack_msg_a.clone(), &mut buffer).unwrap();

        device_socket_a
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        // 3b. sends ack response
        let ack_msg_b = Message::new_ack_message(response_b.device_id, response_b.seq + 1).unwrap();

        buffer = BytesMut::new();
        codec.encode(ack_msg_b.clone(), &mut buffer).unwrap();

        device_socket_a
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        let connecitons_number = scheduler
            .lock()
            .await
            .get_active_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 2);
    }
}
