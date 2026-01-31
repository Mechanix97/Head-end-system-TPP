//! Integration tests for cluster functionality.

#[cfg(test)]
mod tests {
    use super::super::protocol::*;
    use super::super::node::*;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};
    use uuid::Uuid;

    #[test]
    fn test_heartbeat_message_full_cycle() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        // Create a realistic heartbeat payload
        let payload = HeartbeatPayload {
            status: NodeStatus::Active,
            bucket_count: 15,
            device_count: 250,
            load_percent: 65,
            known_nodes: vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()],
        };

        let msg = ClusterMessage::heartbeat(node_id, 42, payload.clone());

        // Encode
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("Should encode");

        // Decode
        let decoded = codec
            .decode(&mut buf)
            .expect("Should decode")
            .expect("Should have message");

        assert_eq!(decoded.node_id, node_id);
        assert_eq!(decoded.seq, 42);
        assert_eq!(decoded.msg_type, ClusterMessageType::Heartbeat);

        if let ClusterPayload::Heartbeat(p) = decoded.payload {
            assert_eq!(p.status, NodeStatus::Active);
            assert_eq!(p.bucket_count, 15);
            assert_eq!(p.device_count, 250);
            assert_eq!(p.load_percent, 65);
            assert_eq!(p.known_nodes.len(), 3);
        } else {
            panic!("Expected Heartbeat payload");
        }
    }

    #[test]
    fn test_node_join_message_full_cycle() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        let payload = NodeJoinPayload {
            node_name: "production-node-01".to_string(),
            cluster_addr: "192.168.1.100:6570".parse().unwrap(),
            backdoor_port: 6565,
        };

        let msg = ClusterMessage::node_join(node_id, 1, payload);

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("Should encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("Should decode")
            .expect("Should have message");

        if let ClusterPayload::NodeJoin(p) = decoded.payload {
            assert_eq!(p.node_name, "production-node-01");
            assert_eq!(p.cluster_addr.to_string(), "192.168.1.100:6570");
            assert_eq!(p.backdoor_port, 6565);
        } else {
            panic!("Expected NodeJoin payload");
        }
    }

    #[test]
    fn test_delegate_request_with_many_buckets() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        // Create 100 devices for delegation
        let devices: Vec<DelegatedDevicePayload> = (0..100)
            .map(|i| DelegatedDevicePayload {
                device_id: Uuid::new_v4(),
                ipv4: Some(format!("192.168.1.{}", i % 255)),
                ipv6: None,
                schedule_time: 1234567890 + i as i64,
            })
            .collect();

        let payload = DelegateRequestPayload {
            devices,
            reason: DelegationReason::Rebalance,
        };

        let msg = ClusterMessage::delegate_request(node_id, 10, payload);

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("Should encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("Should decode")
            .expect("Should have message");

        if let ClusterPayload::DelegateRequest(p) = decoded.payload {
            assert_eq!(p.devices.len(), 100);
            assert_eq!(p.devices[0].ipv4, Some("192.168.1.0".to_string()));
            assert_eq!(p.devices[99].schedule_time, 1234567890 + 99);
            assert_eq!(p.reason, DelegationReason::Rebalance);
        } else {
            panic!("Expected DelegateRequest payload");
        }
    }

    #[test]
    fn test_status_response_comprehensive() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        let owned_buckets: Vec<i32> = vec![0, 5, 10, 15, 20, 25, 30];
        let payload = StatusResponsePayload {
            node_name: "cluster-node-alpha".to_string(),
            status: NodeStatus::Active,
            bucket_count: owned_buckets.len() as u16,
            device_count: 350,
            load_percent: 45,
            owned_buckets,
        };

        let msg = ClusterMessage::status_response(node_id, 100, payload);

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("Should encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("Should decode")
            .expect("Should have message");

        if let ClusterPayload::StatusResponse(p) = decoded.payload {
            assert_eq!(p.node_name, "cluster-node-alpha");
            assert_eq!(p.status, NodeStatus::Active);
            assert_eq!(p.bucket_count, 7);
            assert_eq!(p.device_count, 350);
            assert_eq!(p.load_percent, 45);
            assert_eq!(p.owned_buckets, vec![0, 5, 10, 15, 20, 25, 30]);
        } else {
            panic!("Expected StatusResponse payload");
        }
    }

    #[test]
    fn test_all_message_types_decode_correctly() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        let messages = vec![
            ClusterMessage::heartbeat(
                node_id,
                1,
                HeartbeatPayload {
                    status: NodeStatus::Active,
                    bucket_count: 10,
                    device_count: 100,
                    load_percent: 50,
                    known_nodes: vec![],
                },
            ),
            ClusterMessage::heartbeat_ack(node_id, 2),
            ClusterMessage::node_join(
                node_id,
                3,
                NodeJoinPayload {
                    node_name: "test".to_string(),
                    cluster_addr: "127.0.0.1:6570".parse().unwrap(),
                    backdoor_port: 6565,
                },
            ),
            ClusterMessage::node_leave(node_id, 4),
            ClusterMessage::node_suspect(node_id, 5, Uuid::new_v4()),
            ClusterMessage::node_dead(node_id, 6, Uuid::new_v4()),
            ClusterMessage::delegate_request(
                node_id,
                7,
                DelegateRequestPayload {
                    devices: vec![
                        DelegatedDevicePayload {
                            device_id: Uuid::new_v4(),
                            ipv4: Some("192.168.1.1".to_string()),
                            ipv6: None,
                            schedule_time: 1234567890,
                        },
                    ],
                    reason: DelegationReason::Shutdown,
                },
            ),
            ClusterMessage::delegate_accept(
                node_id,
                8,
                DelegateAcceptPayload {
                    accepted_device_ids: vec![Uuid::new_v4()],
                },
            ),
            ClusterMessage::delegate_reject(node_id, 9, "Overloaded".to_string()),
            ClusterMessage::status_request(node_id, 10),
        ];

        for msg in messages {
            let expected_type = msg.msg_type;
            let mut buf = BytesMut::new();

            codec.encode(msg, &mut buf).expect("Should encode");

            let decoded = codec
                .decode(&mut buf)
                .expect("Should decode")
                .expect("Should have message");

            assert_eq!(
                decoded.msg_type, expected_type,
                "Message type mismatch for {:?}",
                expected_type
            );
        }
    }

    #[test]
    fn test_message_size_limits() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        // Very long node name (255 chars)
        let long_name = "x".repeat(255);
        let payload = NodeJoinPayload {
            node_name: long_name.clone(),
            cluster_addr: "127.0.0.1:6570".parse().unwrap(),
            backdoor_port: 6565,
        };

        let msg = ClusterMessage::node_join(node_id, 1, payload);

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("Should encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("Should decode")
            .expect("Should have message");

        if let ClusterPayload::NodeJoin(p) = decoded.payload {
            assert_eq!(p.node_name, long_name);
        } else {
            panic!("Expected NodeJoin payload");
        }
    }

    #[test]
    fn test_reject_message_with_long_reason() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();

        let long_reason = "Node is currently experiencing high load due to processing \
                          a large batch of device updates and cannot accept additional \
                          buckets at this time. Please try again later.".to_string();

        let msg = ClusterMessage::delegate_reject(node_id, 1, long_reason.clone());

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("Should encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("Should decode")
            .expect("Should have message");

        if let ClusterPayload::DelegateReject(p) = decoded.payload {
            assert_eq!(p.reason, long_reason);
        } else {
            panic!("Expected DelegateReject payload");
        }
    }

    #[test]
    fn test_malformed_message_detection() {
        let mut codec = ClusterMessageCodec;

        // Create a buffer with invalid data
        let mut buf = BytesMut::from(&[0u8; 10][..]); // Too short

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_buffer() {
        let mut codec = ClusterMessageCodec;
        let mut buf = BytesMut::new();

        let result = codec.decode(&mut buf);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_delegation_reason_all_variants() {
        let reasons = vec![
            DelegationReason::Overload,
            DelegationReason::Shutdown,
            DelegationReason::Rebalance,
            DelegationReason::NodeFailure,
        ];

        for reason in reasons {
            let mut codec = ClusterMessageCodec;
            let node_id = Uuid::new_v4();

            let payload = DelegateRequestPayload {
                devices: vec![DelegatedDevicePayload {
                    device_id: Uuid::new_v4(),
                    ipv4: Some("192.168.1.1".to_string()),
                    ipv6: None,
                    schedule_time: 1234567890,
                }],
                reason,
            };

            let msg = ClusterMessage::delegate_request(node_id, 1, payload);

            let mut buf = BytesMut::new();
            codec.encode(msg, &mut buf).expect("Should encode");

            let decoded = codec
                .decode(&mut buf)
                .expect("Should decode")
                .expect("Should have message");

            if let ClusterPayload::DelegateRequest(p) = decoded.payload {
                assert_eq!(p.reason, reason);
            } else {
                panic!("Expected DelegateRequest payload");
            }
        }
    }

    #[test]
    fn test_probe_messages() {
        let mut codec = ClusterMessageCodec;
        let node_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();

        // Probe request
        let msg = ClusterMessage::probe_request(node_id, 1, target_id);
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("Should encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("Should decode")
            .expect("Should have message");

        if let ClusterPayload::ProbeRequest(p) = decoded.payload {
            assert_eq!(p.target_node_id, target_id);
        } else {
            panic!("Expected ProbeRequest payload");
        }

        // Probe response (alive)
        let msg = ClusterMessage::probe_response(node_id, 2, target_id, true);
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("Should encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("Should decode")
            .expect("Should have message");

        if let ClusterPayload::ProbeResponse(p) = decoded.payload {
            assert_eq!(p.target_node_id, target_id);
            assert!(p.is_alive);
        } else {
            panic!("Expected ProbeResponse payload");
        }

        // Probe response (dead)
        let msg = ClusterMessage::probe_response(node_id, 3, target_id, false);
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("Should encode");

        let decoded = codec
            .decode(&mut buf)
            .expect("Should decode")
            .expect("Should have message");

        if let ClusterPayload::ProbeResponse(p) = decoded.payload {
            assert_eq!(p.target_node_id, target_id);
            assert!(!p.is_alive);
        } else {
            panic!("Expected ProbeResponse payload");
        }
    }
}
