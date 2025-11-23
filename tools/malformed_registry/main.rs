/// Test script to reproduce the infinite loop bug when msg_type is missing
use bytes::BytesMut;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a malformed message: RegisterRequest WITHOUT msg_type byte
    let mut buffer = BytesMut::new();

    // Manually encode a RegisterRequest message WITHOUT the msg_type field
    // Normal format: version(1) + msg_type(1) + device_id(16) + seq(4) + timestamp(8) + payload(var) + mac(16)
    // Malformed: version(1) + [MISSING msg_type] + device_id(16) + seq(4) + timestamp(8) + payload(var) + mac(16)

    use bytes::BufMut;

    // Version
    buffer.put_u8(1);

    // SKIP msg_type (THIS IS THE BUG SIMULATION)
    // buffer.put_u8(0x02); // <-- This line is commented (simulating codec.rs:91 commented)

    // Device ID (0 for registration)
    buffer.put_u128(0);

    // Sequence
    buffer.put_u32(0);

    // Timestamp
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    buffer.put_u64(timestamp);

    // No payload for RegisterRequest

    // MAC (16 bytes of zeros)
    buffer.put_u128(0);

    println!(
        "Malformed message size: {} bytes (should be 46, got 45)",
        buffer.len()
    );
    println!("Hex dump: {}", hex::encode(&buffer));

    // Send to backdoor
    let socket = UdpSocket::bind("0.0.0.0:0").await?;

    println!("\nSending malformed message to backdoor...");
    socket.send_to(&buffer, "127.0.0.1:6565").await?;

    println!("Message sent. Check HES logs for loop behavior.");
    println!("\nExpected behavior: HES should log 'Invalid codec conversion 2' repeatedly");

    sleep(Duration::from_secs(2)).await;

    Ok(())
}
