use bytes::BytesMut;
use chrono::Utc;
use common::database::api::Database;
use metrics::metrics_connections::METRICS_CONNECTIONS;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{RwLock, Semaphore, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::BackdoorError;
use common::device::Device;
use common::messages::codec::MessageCodec;
use common::messages::message::Message;
use common::messages::message::MsgType;
use common::messages::nack::{NACK_DEVICE_NOT_FOUND, NACK_INTERNAL_ERROR};
use common::registration_status::RegistrationStatus;
use device_manager::DeviceManager;

const ACK_TIMEOUT_DURATION_MS: u64 = 300000;
const UDP_BUFFER_SIZE: usize = 1024;
const DEFAULT_MAX_CONCURRENT_HANDLERS: usize = 500;

pub struct BackdoorConfig {
    pub ip: String,
    pub port: String,
    pub ack_timeout_duration: Option<u64>,
    pub database: Database,
    pub node_id: uuid::Uuid,
    pub device_manager: Arc<RwLock<DeviceManager>>,
    pub max_concurrent_handlers: Option<usize>,
    pub rebind_rx: watch::Receiver<(String, String)>,
}

pub async fn init_backdoor(cfg: BackdoorConfig) -> Result<JoinHandle<()>, BackdoorError> {
    let BackdoorConfig {
        ip,
        port,
        ack_timeout_duration,
        database,
        node_id,
        device_manager,
        max_concurrent_handlers,
        mut rebind_rx,
    } = cfg;

    let socket = Arc::new(UdpSocket::bind(format!("{ip}:{port}")).await?);
    info!("Listening for device registration on {ip}:{port} via UDP");

    let ack_timeout_duration = ack_timeout_duration.unwrap_or(ACK_TIMEOUT_DURATION_MS);
    let max_handlers = max_concurrent_handlers.unwrap_or(DEFAULT_MAX_CONCURRENT_HANDLERS);
    let semaphore = Arc::new(Semaphore::new(max_handlers));

    let join_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        let mut buf = vec![0u8; UDP_BUFFER_SIZE];
        let mut current_socket = socket;
        loop {
            let (len, addr) = tokio::select! {
                biased;
                recv = current_socket.recv_from(&mut buf) => match recv {
                    Ok(result) => result,
                    Err(e) => {
                        warn!("Error receiving UDP packet: {e}");
                        continue;
                    }
                },
                _ = rebind_rx.changed() => {
                    let (new_ip, new_port) = rebind_rx.borrow().clone();
                    match UdpSocket::bind(format!("{new_ip}:{new_port}")).await {
                        Ok(s) => {
                            current_socket = Arc::new(s);
                            info!("Backdoor rebound to {new_ip}:{new_port}");
                        }
                        Err(e) => warn!("Failed to rebind backdoor to {new_ip}:{new_port}: {e}"),
                    }
                    continue;
                }
            };

            let mut bytes = BytesMut::from(&buf[..len]);
            let msg = match MessageCodec.decode(&mut bytes) {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    warn!("Incomplete message received from {addr}");
                    continue;
                }
                Err(e) => {
                    warn!("Invalid codec conversion from {addr}: {e}");
                    continue;
                }
            };

            match msg.msg_type {
                MsgType::RegisterRequest => {
                    info!("RegisterRequest received");
                    METRICS_CONNECTIONS
                        .messages_total
                        .with_label_values(&["register_request", "inbound"])
                        .inc();
                    if msg.device_id == 0 {
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(permit) => permit,
                            Err(_) => {
                                error!("Semaphore closed, stopping backdoor");
                                return;
                            }
                        };
                        let socket = current_socket.clone();
                        let database = database.clone();
                        let device_manager = device_manager.clone();
                        tokio::spawn(async move {
                            // Binds the permit to this task's scope so it is dropped
                            // (and the semaphore slot released) only when the task
                            // finishes. Using bare `_` would drop it immediately.
                            let _permit = permit;
                            if let Err(err) = handle_backdoor_register_msg(
                                socket,
                                msg,
                                addr,
                                ack_timeout_duration,
                                database,
                                node_id,
                                device_manager,
                            )
                            .await
                            {
                                error!("Error handle register request: {err}");
                                METRICS_CONNECTIONS
                                    .errors_total
                                    .with_label_values(&["backdoor", "register_request"])
                                    .inc();
                            }
                        });
                    } else {
                        // Non-zero device_id: already-registered device reporting an IP change.
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(permit) => permit,
                            Err(_) => {
                                error!("Semaphore closed, stopping backdoor");
                                return;
                            }
                        };
                        let socket = current_socket.clone();
                        let database = database.clone();
                        let device_manager = device_manager.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(err) = handle_backdoor_ip_update_msg(
                                socket,
                                msg,
                                addr,
                                database,
                                device_manager,
                            )
                            .await
                            {
                                error!("Error handling IP update: {err}");
                                METRICS_CONNECTIONS
                                    .errors_total
                                    .with_label_values(&["backdoor", "ip_update"])
                                    .inc();
                            }
                        });
                    }
                }
                MsgType::Ack => {
                    info!("Ack received");
                    METRICS_CONNECTIONS
                        .messages_total
                        .with_label_values(&["ack", "inbound"])
                        .inc();
                    let permit = match semaphore.clone().acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            error!("Semaphore closed, stopping backdoor");
                            return;
                        }
                    };
                    let socket = current_socket.clone();
                    let database = database.clone();
                    let device_manager = device_manager.clone();
                    tokio::spawn(async move {
                        // Same as above: keeps the permit alive for the duration
                        // of this task, not just until the end of the statement.
                        let _permit = permit;
                        if let Err(err) =
                            handle_backdoor_ack_msg(socket, device_manager, msg, addr, database).await
                        {
                            error!("Error handle ack msg: {err}");
                            METRICS_CONNECTIONS
                                .errors_total
                                .with_label_values(&["backdoor", "ack_handler"])
                                .inc();
                        }
                    });
                }

                _ => {
                    warn!("Received incompatible msg in backdoor: {:?}", msg.msg_type);
                    METRICS_CONNECTIONS
                        .errors_total
                        .with_label_values(&["backdoor", "invalid_msg_type"])
                        .inc();
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
    socket: Arc<UdpSocket>,
    msg: Message,
    socket_addr: SocketAddr,
    ack_timeout_duration: u64,
    database: Database,
    _node_id: uuid::Uuid,
    device_manager: Arc<RwLock<DeviceManager>>,
) -> Result<(), BackdoorError> {
    // TODO: check that the information provided is correct #10
    if msg.device_id != 0 {
        return Err(BackdoorError::RegisterRequestInvalidId);
    }

    let imei = match &msg.payload {
        common::messages::message::MessagePayload::RegistryRequest(req) => Some(req.imei.clone()),
        _ => None,
    };
    let device = Device::new(socket_addr, imei);

    database.add_device(&device).await?;
    let next_wake_up = device_manager
        .write()
        .await
        .register_device(&device)
        .await?;
    database
        .register_device(device.id, msg.get_timestamp()?)
        .await?;

    let next_wake_time = next_wake_up.and_utc().timestamp() as u64;
    let response = Message::new_register_response_message(
        device.id.as_u128(),
        msg.seq + 1,
        0,
        next_wake_time,
    )?;

    send_msg(&socket, response, socket_addr, "register_response").await;

    spawn_ack_timeout_task(database.clone(), ack_timeout_duration, device.id);

    Ok(())
}

/// Handles an ACK from the device and confirms receipt with another ACK (double handshake).
async fn handle_backdoor_ack_msg(
    socket: Arc<UdpSocket>,
    device_manager: Arc<RwLock<DeviceManager>>,
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

    // registration_ack returns the response time in seconds
    let mut device_registration = database.get_device_registration(device.id).await?;

    match device_registration.registration_status {
        RegistrationStatus::AckTimeout => {
            // Registration window expired; reject with NACK so the device re-registers.
            warn!(
                "Late ACK from device {:#x}: registration timed out, sending NACK",
                msg.device_id
            );
            if let Ok(nack) = Message::new_nack_message(msg.device_id, msg.seq + 1, NACK_INTERNAL_ERROR) {
                send_msg(&socket, nack, socket_addr, "nack").await;
            }
        }
        RegistrationStatus::Registered => {
            // ACK for an IP-update RegisterResponse: confirm with ACK (double handshake).
            info!(
                "IP update ACK confirmed from device {:#x} at {}",
                msg.device_id, socket_addr
            );
            METRICS_CONNECTIONS
                .messages_total
                .with_label_values(&["ack", "ip_update_confirmed"])
                .inc();
            if let Ok(ack) = Message::new_ack_message(msg.device_id, msg.seq + 1) {
                send_msg(&socket, ack, socket_addr, "ack").await;
            }
        }
        RegistrationStatus::PendingAck => {
            let registration_duration =
                (msg.get_timestamp()? - device_registration.registration_time).num_milliseconds();
            METRICS_CONNECTIONS
                .ack_response_time_ms
                .observe(registration_duration as f64);

            info!(
                "Adding new connection, device_id: {:#x}, ACK response time: {:.2}ms",
                msg.device_id, registration_duration
            );
            device_registration.registration_status = RegistrationStatus::Registered;
            device_registration.registration_time = msg.get_timestamp()?;

            // There is a small chance that the ack timeout is triggered between
            // the DB read and the DB update; both operations could be combined.
            database
                .update_device_registration(
                    device.id,
                    Some(RegistrationStatus::Registered),
                    Some(msg.get_timestamp()?),
                )
                .await?;

            device_manager
                .write()
                .await
                .schedule_next_wakeup(device.id)
                .await?;

            // Confirm to the device that registration is complete (double handshake).
            if let Ok(ack) = Message::new_ack_message(msg.device_id, msg.seq + 1) {
                send_msg(&socket, ack, socket_addr, "ack").await;
            }
        }
    }
    Ok(())
}

/// Handles a `RegisterRequest` from an already-registered device whose IP has changed.
///
/// CAT-M1 cellular networks assign dynamic IPs, so devices may reconnect from a
/// different address after power-on or network re-attach. The device sends a
/// `RegisterRequest` with its existing UUID so the HES can update the stored IP
/// and respond with the next scheduled wake-up time.
///
/// No ACK timeout is started here because the device is already registered and
/// scheduled; if the ACK never arrives, the next periodic connection will use
/// the updated IP regardless.
async fn handle_backdoor_ip_update_msg(
    socket: Arc<UdpSocket>,
    msg: Message,
    socket_addr: SocketAddr,
    database: Database,
    device_manager: Arc<RwLock<DeviceManager>>,
) -> Result<(), BackdoorError> {
    let device_id = Uuid::from_u128(msg.device_id);

    let mut device = match database.get_device(device_id).await {
        Ok(d) => d,
        Err(e) => {
            warn!("IP update for unknown device {:#x}", msg.device_id);
            if let Ok(nack) = Message::new_nack_message(msg.device_id, msg.seq + 1, NACK_DEVICE_NOT_FOUND) {
                send_msg(&socket, nack, socket_addr, "nack").await;
            }
            return Err(BackdoorError::DatabaseError(e));
        }
    };

    let old_ip = device.ipv4.clone().or(device.ipv6.clone()).unwrap_or_default();
    match socket_addr {
        SocketAddr::V4(_) => {
            device.ipv4 = Some(socket_addr.ip().to_string());
            device.ipv6 = None;
        }
        SocketAddr::V6(_) => {
            device.ipv4 = None;
            device.ipv6 = Some(socket_addr.ip().to_string());
        }
    }
    info!(
        "Device {:#x} IP change: {} -> {}",
        msg.device_id,
        old_ip,
        socket_addr.ip()
    );

    if let Err(e) = database.modify_device(&device).await {
        error!("Failed to update IP for device {:#x}: {e}", msg.device_id);
        if let Ok(nack) = Message::new_nack_message(msg.device_id, msg.seq + 1, NACK_INTERNAL_ERROR) {
            send_msg(&socket, nack, socket_addr, "nack").await;
        }
        return Err(BackdoorError::DatabaseError(e));
    }

    let next_wake_time = match get_next_wake_time(device_id, &database, &device_manager).await {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to get next wake time for device {:#x}: {e}", msg.device_id);
            if let Ok(nack) = Message::new_nack_message(msg.device_id, msg.seq + 1, NACK_INTERNAL_ERROR) {
                send_msg(&socket, nack, socket_addr, "nack").await;
            }
            return Err(e);
        }
    };

    if let Ok(response) = Message::new_register_response_message(msg.device_id, msg.seq + 1, 0, next_wake_time) {
        send_msg(&socket, response, socket_addr, "register_response").await;
    }

    Ok(())
}

/// Encodes and sends a message to the given address, logging any errors.
async fn send_msg(socket: &UdpSocket, msg: Message, addr: SocketAddr, label: &str) {
    let mut buf = BytesMut::with_capacity(1024);
    match MessageCodec.encode(msg, &mut buf) {
        Err(err) => {
            error!("Error encoding {label}: {err}");
            METRICS_CONNECTIONS
                .errors_total
                .with_label_values(&["backdoor", label])
                .inc();
        }
        Ok(()) => {
            if let Err(err) = socket.send_to(&buf, addr).await {
                error!("Error sending {label}: {err}");
                METRICS_CONNECTIONS
                    .errors_total
                    .with_label_values(&["backdoor", label])
                    .inc();
            } else {
                METRICS_CONNECTIONS
                    .messages_total
                    .with_label_values(&[label, "outbound"])
                    .inc();
            }
        }
    }
}

/// Returns the next scheduled wake-up time (Unix seconds) for an existing device.
///
/// If a scheduled connection already exists, its timestamp is used directly.
/// If none exists (e.g., initial ACK timed out and no job was ever created),
/// a fresh connection is scheduled using the device's existing bucket assignment.
async fn get_next_wake_time(
    device_id: Uuid,
    database: &Database,
    device_manager: &Arc<RwLock<DeviceManager>>,
) -> Result<u64, BackdoorError> {
    match database.get_scheduled_connection(device_id).await {
        Ok(conn) => Ok(conn.schedule_time.and_utc().timestamp() as u64),
        Err(_) => {
            device_manager.write().await.schedule_next_wakeup(device_id).await?;
            let conn = database.get_scheduled_connection(device_id).await?;
            Ok(conn.schedule_time.and_utc().timestamp() as u64)
        }
    }
}

fn spawn_ack_timeout_task(database: Database, ack_timeout_duration: u64, device_id: Uuid) {
    tokio::spawn(async move {
        sleep(Duration::from_millis(ack_timeout_duration)).await;
        match database
            .registration_timeout(device_id, Utc::now().naive_utc())
            .await
        {
            Ok(true) => {
                METRICS_CONNECTIONS.ack_timeout_count.inc();
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
    use common::database::{DatabaseConfig, api::Database};
    use device_manager::DeviceManager;
    use scheduler::scheduler::Scheduler;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tokio_util::codec::Decoder;
    use tokio_util::codec::Encoder;

    use super::*;

    async fn set_up_hes(backdoor_port: &str) -> Arc<RwLock<DeviceManager>> {
        let db = Database::new(DatabaseConfig::in_memory()).await.unwrap();
        let node_id = uuid::Uuid::new_v4();
        let scheduler = Scheduler::new(1, db.clone(), node_id).await.unwrap();
        let device_manager = Arc::new(RwLock::new(DeviceManager::new(
            node_id,
            1,
            db.clone(),
            scheduler,
        )));
        let (_, rebind_rx) =
            tokio::sync::watch::channel(("0.0.0.0".to_string(), backdoor_port.to_string()));
        init_backdoor(BackdoorConfig {
            ip: "0.0.0.0".to_string(),
            port: backdoor_port.to_string(),
            ack_timeout_duration: Some(300),
            database: db.clone(),
            node_id,
            device_manager: device_manager.clone(),
            max_concurrent_handlers: Some(50),
            rebind_rx,
        })
        .await
        .unwrap();
        device_manager
    }

    /// This test checks the normal backdoor registration event
    /// 1. sends registration request msg
    /// 2. receives registration response msg
    /// 3. sends ack response
    #[tokio::test]
    async fn test_new_connection() {
        // 0. intial backdoor setup
        let backdoor_port = "8081";
        let dm = set_up_hes(backdoor_port).await;

        // 1. sends registration request msg
        let register_request: Message = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer = BytesMut::with_capacity(1024);

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;
        let connecitons_number = dm
            .read()
            .await
            .get_scheduled_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);

        // 2. receives registration response msg
        buffer = BytesMut::with_capacity(1024);
        device_socket.recv_buf(&mut buffer).await.unwrap();
        let response = codec.decode(&mut buffer).unwrap().unwrap();

        // 3. sends ack response
        let ack_msg = Message::new_ack_message(response.device_id, response.seq + 1).unwrap();

        buffer = BytesMut::with_capacity(1024);
        codec.encode(ack_msg.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        let connecitons_number = dm
            .read()
            .await
            .get_scheduled_connections()
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
        let dm = set_up_hes(backdoor_port).await;

        let connecitons_number = dm
            .read()
            .await
            .get_scheduled_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);

        let register_request: Message = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer = BytesMut::with_capacity(1024);

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        let connecitons_number = dm
            .read()
            .await
            .get_scheduled_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);

        // 2. receives registration response msg
        buffer = BytesMut::with_capacity(1024);
        device_socket.recv_buf(&mut buffer).await.unwrap();
        let response = codec.decode(&mut buffer).unwrap().unwrap();

        // 3. adds some delay to trigger the ack timeout
        sleep(Duration::from_millis(500)).await;

        // 4. sends ack response
        let ack_msg = Message::new_ack_message(response.device_id, response.seq + 1).unwrap();
        buffer = BytesMut::with_capacity(1024);
        codec.encode(ack_msg.clone(), &mut buffer).unwrap();

        device_socket
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        let connecitons_number = dm
            .read()
            .await
            .get_scheduled_connections()
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
        let dm = set_up_hes(backdoor_port).await;

        for i in 0..10 {
            // 1. sends registration request msg
            let register_request: Message = Message::new_register_request_message(
                "123456789012345".to_string(),
                "fe80::1".to_string(),
            )
            .unwrap();
            let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut buffer = BytesMut::with_capacity(1024);

            let mut codec = MessageCodec;
            codec.encode(register_request.clone(), &mut buffer).unwrap();

            device_socket
                .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
                .await
                .expect("Failed to send RegisterRequest");
            sleep(Duration::from_millis(100)).await;
            let connecitons_number = dm
                .read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len();
            assert_eq!(connecitons_number, i);

            // 2. receives registration response msg
            buffer = BytesMut::with_capacity(1024);
            device_socket.recv_buf(&mut buffer).await.unwrap();
            let response = codec.decode(&mut buffer).unwrap().unwrap();

            // 3. sends ack response
            let ack_msg = Message::new_ack_message(response.device_id, response.seq + 1).unwrap();
            buffer = BytesMut::with_capacity(1024);
            codec.encode(ack_msg.clone(), &mut buffer).unwrap();

            device_socket
                .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
                .await
                .expect("Failed to send RegisterRequest");
            sleep(Duration::from_millis(100)).await;
        }
        let connecitons_number = dm
            .read()
            .await
            .get_scheduled_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 10);
    }

    /// Tests that N devices can register truly concurrently without blocking each other.
    /// All N tasks are spawned simultaneously, each independently doing the full cycle:
    /// RegisterRequest → RegisterResponse → ACK. All N connections must be scheduled.
    #[tokio::test]
    async fn test_truly_concurrent_registrations() {
        let backdoor_port = "8085";
        let dm = set_up_hes(backdoor_port).await;
        let n: usize = 20;

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let port = backdoor_port.to_string();
                tokio::spawn(async move {
                    let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                    let mut codec = MessageCodec;

                    // Send RegisterRequest
                    let request = Message::new_register_request_message(
                        "123456789012345".to_string(),
                        "fe80::1".to_string(),
                    )
                    .unwrap();
                    let mut buf = BytesMut::with_capacity(1024);
                    codec.encode(request, &mut buf).unwrap();
                    device_socket
                        .send_to(&buf, format!("127.0.0.1:{}", port))
                        .await
                        .unwrap();

                    // Receive RegisterResponse
                    let mut resp = BytesMut::with_capacity(1024);
                    device_socket.recv_buf(&mut resp).await.unwrap();
                    let response = codec.decode(&mut resp).unwrap().unwrap();
                    assert!(matches!(response.msg_type, MsgType::RegisterResponse));

                    // Send ACK
                    let ack =
                        Message::new_ack_message(response.device_id, response.seq + 1).unwrap();
                    let mut ack_buf = BytesMut::with_capacity(1024);
                    codec.encode(ack, &mut ack_buf).unwrap();
                    device_socket
                        .send_to(&ack_buf, format!("127.0.0.1:{}", port))
                        .await
                        .unwrap();
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }
        sleep(Duration::from_millis(200)).await;

        let connections = dm
            .read()
            .await
            .get_scheduled_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connections, n);
    }

    /// Tests that a RegisterRequest with a non-zero device_id is silently ignored:
    /// - No response is sent to the sender
    /// - No connection is scheduled
    /// - The backdoor continues to process subsequent valid requests
    #[tokio::test]
    async fn test_invalid_device_id_is_ignored() {
        use common::messages::message::MessagePayload;
        use std::time::{SystemTime, UNIX_EPOCH};
        use tokio::time::timeout;

        let backdoor_port = "8086";
        let dm = set_up_hes(backdoor_port).await;

        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut codec = MessageCodec;

        // Construct a RegisterRequest with device_id != 0 (simulates a device
        // that already has an id trying to re-register via the backdoor)
        let invalid_request = Message {
            version: 1,
            msg_type: MsgType::RegisterRequest,
            device_id: 0xDEAD_BEEF,
            seq: 0,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            payload: MessagePayload::Ack,
            mac: 0,
        };

        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(invalid_request, &mut buf).unwrap();
        device_socket
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        // No response should arrive for an invalid device_id
        let mut resp = BytesMut::with_capacity(1024);
        let recv_result = timeout(
            Duration::from_millis(200),
            device_socket.recv_buf(&mut resp),
        )
        .await;
        assert!(
            recv_result.is_err(),
            "Should not receive a response for a RegisterRequest with non-zero device_id"
        );
        assert_eq!(
            dm.read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len(),
            0
        );

        // Backdoor must still be functional: a valid request should succeed
        let valid_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(request, &mut buf).unwrap();
        valid_socket
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        let mut resp = BytesMut::with_capacity(1024);
        valid_socket.recv_buf(&mut resp).await.unwrap();
        let response = codec.decode(&mut resp).unwrap().unwrap();
        assert!(matches!(response.msg_type, MsgType::RegisterResponse));
    }

    /// Tests that a malformed UDP packet (too short to be a valid message) does not
    /// crash the backdoor, and that subsequent valid registrations still work.
    #[tokio::test]
    async fn test_malformed_packet_doesnt_crash_backdoor() {
        let backdoor_port = "8087";
        let dm = set_up_hes(backdoor_port).await;

        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // Send garbage bytes — much shorter than MIN_MSG_LEN (46 bytes)
        device_socket
            .send_to(b"garbage", format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;

        // Backdoor must survive: a valid registration should still complete
        let mut codec = MessageCodec;
        let request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(request, &mut buf).unwrap();
        device_socket
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        let mut resp = BytesMut::with_capacity(1024);
        device_socket.recv_buf(&mut resp).await.unwrap();
        let response = codec.decode(&mut resp).unwrap().unwrap();
        assert!(matches!(response.msg_type, MsgType::RegisterResponse));

        // Complete the registration with an ACK
        let ack = Message::new_ack_message(response.device_id, response.seq + 1).unwrap();
        let mut ack_buf = BytesMut::with_capacity(1024);
        codec.encode(ack, &mut ack_buf).unwrap();
        device_socket
            .send_to(&ack_buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            dm.read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    /// Tests that a valid message with a type not handled by the backdoor
    /// (e.g. Handshake) is silently discarded without crashing.
    #[tokio::test]
    async fn test_wrong_msg_type_in_backdoor() {
        use tokio::time::timeout;

        let backdoor_port = "8088";
        let dm = set_up_hes(backdoor_port).await;

        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut codec = MessageCodec;

        // Handshake is a valid message type but not expected on the backdoor port
        let handshake = Message::new_handshake_message(0xDEAD_BEEF, 0, vec![]).unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(handshake, &mut buf).unwrap();
        device_socket
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        // No response should be sent for an unrecognized message type
        let mut resp = BytesMut::with_capacity(1024);
        let recv_result = timeout(
            Duration::from_millis(200),
            device_socket.recv_buf(&mut resp),
        )
        .await;
        assert!(
            recv_result.is_err(),
            "Backdoor should not respond to Handshake messages"
        );
        assert_eq!(
            dm.read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len(),
            0
        );

        // Backdoor must still be functional after receiving the wrong type
        let request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(request, &mut buf).unwrap();
        device_socket
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        let mut resp = BytesMut::with_capacity(1024);
        device_socket.recv_buf(&mut resp).await.unwrap();
        let response = codec.decode(&mut resp).unwrap().unwrap();
        assert!(matches!(response.msg_type, MsgType::RegisterResponse));
    }

    /// Tests that an ACK for an unknown device_id is handled gracefully:
    /// the spawned handler logs the error but the backdoor keeps running.
    #[tokio::test]
    async fn test_ack_unknown_device() {
        let backdoor_port = "8089";
        let dm = set_up_hes(backdoor_port).await;

        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut codec = MessageCodec;

        // Send an ACK referencing a device_id that was never registered
        let fake_id = 0xDEAD_BEEF_CAFE_1234_u128;
        let ack = Message::new_ack_message(fake_id, 0).unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(ack, &mut buf).unwrap();
        device_socket
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            dm.read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len(),
            0
        );

        // Backdoor must still work after the failed ACK handler
        let request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(request, &mut buf).unwrap();
        device_socket
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        let mut resp = BytesMut::with_capacity(1024);
        device_socket.recv_buf(&mut resp).await.unwrap();
        let response = codec.decode(&mut resp).unwrap().unwrap();
        assert!(matches!(response.msg_type, MsgType::RegisterResponse));

        let ack = Message::new_ack_message(response.device_id, response.seq + 1).unwrap();
        let mut ack_buf = BytesMut::with_capacity(1024);
        codec.encode(ack, &mut ack_buf).unwrap();
        device_socket
            .send_to(&ack_buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            dm.read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    /// Stress test: 100 devices register concurrently. Verifies that:
    /// - All 100 complete the full registration cycle
    /// - Each device receives back its own unique device_id (no cross-talk)
    /// - The semaphore does not deadlock under load
    #[tokio::test]
    async fn test_stress_100_concurrent_registrations() {
        let backdoor_port = "8090";
        let dm = set_up_hes(backdoor_port).await;
        let n: usize = 100;

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let port = backdoor_port.to_string();
                tokio::spawn(async move {
                    let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                    let mut codec = MessageCodec;

                    // Send RegisterRequest
                    let request = Message::new_register_request_message(
                        "123456789012345".to_string(),
                        "fe80::1".to_string(),
                    )
                    .unwrap();
                    let mut buf = BytesMut::with_capacity(1024);
                    codec.encode(request, &mut buf).unwrap();
                    device_socket
                        .send_to(&buf, format!("127.0.0.1:{}", port))
                        .await
                        .unwrap();

                    // Receive RegisterResponse
                    let mut resp = BytesMut::with_capacity(1024);
                    device_socket.recv_buf(&mut resp).await.unwrap();
                    let response = codec.decode(&mut resp).unwrap().unwrap();
                    assert!(matches!(response.msg_type, MsgType::RegisterResponse));
                    // The device_id assigned by HES must be non-zero
                    assert_ne!(response.device_id, 0, "HES should assign a non-zero UUID");

                    // Send ACK with the device_id we received
                    let ack =
                        Message::new_ack_message(response.device_id, response.seq + 1).unwrap();
                    let mut ack_buf = BytesMut::with_capacity(1024);
                    codec.encode(ack, &mut ack_buf).unwrap();
                    device_socket
                        .send_to(&ack_buf, format!("127.0.0.1:{}", port))
                        .await
                        .unwrap();

                    // Return the device_id to verify uniqueness across devices
                    response.device_id
                })
            })
            .collect();

        let mut device_ids = Vec::with_capacity(n);
        for h in handles {
            device_ids.push(h.await.unwrap());
        }
        sleep(Duration::from_millis(300)).await;

        // All device_ids must be unique (no cross-talk between responses)
        device_ids.sort();
        device_ids.dedup();
        assert_eq!(device_ids.len(), n, "All device_ids should be unique");

        let connections = dm
            .read()
            .await
            .get_scheduled_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connections, n);
    }

    /// Tests the full IP update flow:
    /// 1. Device registers and completes the full registration cycle (including ACK).
    /// 2. A new socket simulates a reconnect from a different source address.
    /// 3. Device sends RegisterRequest with its existing UUID.
    /// 4. HES updates the IP and responds with RegisterResponse carrying the same UUID
    ///    and a valid next_wake_time.
    /// 5. The number of scheduled connections remains unchanged.
    #[tokio::test]
    async fn test_ip_update_success() {
        let backdoor_port = "8091";
        let dm = set_up_hes(backdoor_port).await;

        // Step 1: initial registration from socket_a
        let socket_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut codec = MessageCodec;

        let request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(request, &mut buf).unwrap();
        socket_a
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        let mut resp = BytesMut::with_capacity(1024);
        socket_a.recv_buf(&mut resp).await.unwrap();
        let reg_response = codec.decode(&mut resp).unwrap().unwrap();
        assert!(matches!(reg_response.msg_type, MsgType::RegisterResponse));
        let device_id = reg_response.device_id;

        let ack = Message::new_ack_message(device_id, reg_response.seq + 1).unwrap();
        let mut ack_buf = BytesMut::with_capacity(1024);
        codec.encode(ack, &mut ack_buf).unwrap();
        socket_a
            .send_to(&ack_buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();
        sleep(Duration::from_millis(150)).await;

        assert_eq!(
            dm.read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len(),
            1
        );

        // Step 2: simulate IP change — a different source address (new port on loopback)
        let socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // Step 3: send RegisterRequest with existing device_id (non-zero)
        let mut ip_update_request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::2".to_string(),
        )
        .unwrap();
        ip_update_request.device_id = device_id;

        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(ip_update_request, &mut buf).unwrap();
        socket_b
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        // Step 4: HES responds to the new address with same device_id and a valid wake time
        let mut resp = BytesMut::with_capacity(1024);
        socket_b.recv_buf(&mut resp).await.unwrap();
        let ip_response = codec.decode(&mut resp).unwrap().unwrap();

        assert!(matches!(ip_response.msg_type, MsgType::RegisterResponse));
        assert_eq!(
            ip_response.device_id, device_id,
            "Response device_id must match the registred device"
        );
        if let common::messages::message::MessagePayload::RegistryResponse(r) = &ip_response.payload
        {
            assert!(r.next_wake_time > 0, "next_wake_time must be non-zero");
        } else {
            panic!("Expected RegistryResponse payload");
        }

        // Step 5: no new connections were created by the IP update
        assert_eq!(
            dm.read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    /// Tests that a RegisterRequest with an unknown non-zero device_id is rejected
    /// silently: no response is sent, and the backdoor keeps processing valid requests.
    #[tokio::test]
    async fn test_ip_update_unknown_device_id() {
        use tokio::time::timeout;

        let backdoor_port = "8092";
        let dm = set_up_hes(backdoor_port).await;

        let device_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut codec = MessageCodec;

        // Send RegisterRequest with a non-zero device_id that was never registered
        let mut request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        request.device_id = 0xDEAD_BEEF_1234_5678_u128;

        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(request, &mut buf).unwrap();
        device_socket
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        // A NACK must arrive for an unknown device_id
        let mut resp = BytesMut::with_capacity(1024);
        device_socket.recv_buf(&mut resp).await.unwrap();
        let nack = codec.decode(&mut resp).unwrap().unwrap();
        assert!(
            matches!(nack.msg_type, MsgType::Nack),
            "Should receive a NACK for an unknown device_id"
        );

        assert_eq!(
            dm.read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len(),
            0
        );

        // Backdoor must still be functional after the failed IP update
        let valid_request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(valid_request, &mut buf).unwrap();
        device_socket
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        let mut resp = BytesMut::with_capacity(1024);
        device_socket.recv_buf(&mut resp).await.unwrap();
        let response = codec.decode(&mut resp).unwrap().unwrap();
        assert!(matches!(response.msg_type, MsgType::RegisterResponse));
    }

    /// Tests the complete IP update cycle including the ACK from the device.
    /// After the ACK is processed the backdoor must remain healthy.
    #[tokio::test]
    async fn test_ip_update_with_ack() {
        let backdoor_port = "8093";
        let dm = set_up_hes(backdoor_port).await;

        // Initial registration
        let socket_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut codec = MessageCodec;

        let request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(request, &mut buf).unwrap();
        socket_a
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        let mut resp = BytesMut::with_capacity(1024);
        socket_a.recv_buf(&mut resp).await.unwrap();
        let reg_response = codec.decode(&mut resp).unwrap().unwrap();
        let device_id = reg_response.device_id;

        let ack = Message::new_ack_message(device_id, reg_response.seq + 1).unwrap();
        let mut ack_buf = BytesMut::with_capacity(1024);
        codec.encode(ack, &mut ack_buf).unwrap();
        socket_a
            .send_to(&ack_buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();
        sleep(Duration::from_millis(150)).await;

        // IP update from a new socket
        let socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let mut ip_update_request = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::2".to_string(),
        )
        .unwrap();
        ip_update_request.device_id = device_id;

        let mut buf = BytesMut::with_capacity(1024);
        codec.encode(ip_update_request, &mut buf).unwrap();
        socket_b
            .send_to(&buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();

        let mut resp = BytesMut::with_capacity(1024);
        socket_b.recv_buf(&mut resp).await.unwrap();
        let ip_response = codec.decode(&mut resp).unwrap().unwrap();
        assert!(matches!(ip_response.msg_type, MsgType::RegisterResponse));

        // Device ACKs the IP update response
        let ack = Message::new_ack_message(device_id, ip_response.seq + 1).unwrap();
        let mut ack_buf = BytesMut::with_capacity(1024);
        codec.encode(ack, &mut ack_buf).unwrap();
        socket_b
            .send_to(&ack_buf, format!("127.0.0.1:{}", backdoor_port))
            .await
            .unwrap();
        sleep(Duration::from_millis(100)).await;

        // Connections count unchanged: IP update does not create a new scheduled connection
        assert_eq!(
            dm.read()
                .await
                .get_scheduled_connections()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    // test 2 parallels connections
    #[tokio::test]
    async fn test_parallel_connections() {
        // 0. intial backdoor setup
        let backdoor_port = "8084";
        let dm = set_up_hes(backdoor_port).await;

        // 1a. sends registration request msg
        let register_request: Message = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let device_socket_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer: BytesMut = BytesMut::with_capacity(1024);

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket_a
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;
        let connecitons_number = dm
            .read()
            .await
            .get_scheduled_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);

        // 1a. sends registration request msg
        let register_request: Message = Message::new_register_request_message(
            "123456789012345".to_string(),
            "fe80::1".to_string(),
        )
        .unwrap();
        let device_socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer: BytesMut = BytesMut::with_capacity(1024);

        let mut codec = MessageCodec;
        codec.encode(register_request.clone(), &mut buffer).unwrap();

        device_socket_b
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;
        let connecitons_number = dm
            .read()
            .await
            .get_scheduled_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 0);

        // 2a. receives registration response msg
        buffer = BytesMut::with_capacity(1024);
        device_socket_a.recv_buf(&mut buffer).await.unwrap();
        let response_a = codec.decode(&mut buffer).unwrap().unwrap();

        // 2b. receives registration response msg
        buffer = BytesMut::with_capacity(1024);
        device_socket_b.recv_buf(&mut buffer).await.unwrap();
        let response_b = codec.decode(&mut buffer).unwrap().unwrap();

        // 3a. sends ack response
        let ack_msg_a = Message::new_ack_message(response_a.device_id, response_a.seq + 1).unwrap();

        buffer = BytesMut::with_capacity(1024);
        codec.encode(ack_msg_a.clone(), &mut buffer).unwrap();

        device_socket_a
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        // 3b. sends ack response
        let ack_msg_b = Message::new_ack_message(response_b.device_id, response_b.seq + 1).unwrap();

        buffer = BytesMut::with_capacity(1024);
        codec.encode(ack_msg_b.clone(), &mut buffer).unwrap();

        device_socket_a
            .send_to(&buffer, format!("127.0.0.1:{}", backdoor_port))
            .await
            .expect("Failed to send RegisterRequest");
        sleep(Duration::from_millis(100)).await;

        let connecitons_number = dm
            .read()
            .await
            .get_scheduled_connections()
            .await
            .unwrap()
            .len();
        assert_eq!(connecitons_number, 2);
    }
}
