//! Tests for node types and configuration.

#[cfg(test)]
mod tests {
    use super::super::node::*;
    use std::str::FromStr;

    #[test]
    fn test_node_status_code_roundtrip() {
        let statuses = vec![
            NodeStatus::Starting,
            NodeStatus::Active,
            NodeStatus::Draining,
            NodeStatus::Suspect,
            NodeStatus::Dead,
        ];

        for status in statuses {
            let code = status.code();
            let decoded = NodeStatus::from_code(code).expect("Should decode successfully");
            assert_eq!(status, decoded);
        }
    }

    #[test]
    fn test_node_status_from_str() {
        assert_eq!(
            NodeStatus::from_str("starting").unwrap(),
            NodeStatus::Starting
        );
        assert_eq!(NodeStatus::from_str("active").unwrap(), NodeStatus::Active);
        assert_eq!(
            NodeStatus::from_str("draining").unwrap(),
            NodeStatus::Draining
        );
        assert_eq!(NodeStatus::from_str("suspect").unwrap(), NodeStatus::Suspect);
        assert_eq!(NodeStatus::from_str("dead").unwrap(), NodeStatus::Dead);
        assert!(NodeStatus::from_str("invalid").is_err());
    }

    #[test]
    fn test_node_status_as_str() {
        assert_eq!(NodeStatus::Starting.as_str(), "starting");
        assert_eq!(NodeStatus::Active.as_str(), "active");
        assert_eq!(NodeStatus::Draining.as_str(), "draining");
        assert_eq!(NodeStatus::Suspect.as_str(), "suspect");
        assert_eq!(NodeStatus::Dead.as_str(), "dead");
    }

    #[test]
    fn test_node_info_availability() {
        let mut node = NodeInfo::new_local(
            uuid::Uuid::new_v4(),
            "test-node".to_string(),
            "127.0.0.1:6570".parse().unwrap(),
            6565,
        );

        // Starting nodes are not available
        node.status = NodeStatus::Starting;
        assert!(!node.is_available());

        // Active nodes are available
        node.status = NodeStatus::Active;
        assert!(node.is_available());

        // Draining nodes are not available
        node.status = NodeStatus::Draining;
        assert!(!node.is_available());

        // Dead nodes are not available
        node.status = NodeStatus::Dead;
        assert!(!node.is_available());
    }

    #[test]
    fn test_node_info_reachability() {
        let mut node = NodeInfo::new_local(
            uuid::Uuid::new_v4(),
            "test-node".to_string(),
            "127.0.0.1:6570".parse().unwrap(),
            6565,
        );

        // Starting, Active, and Draining are reachable
        node.status = NodeStatus::Starting;
        assert!(node.is_reachable());

        node.status = NodeStatus::Active;
        assert!(node.is_reachable());

        node.status = NodeStatus::Draining;
        assert!(node.is_reachable());

        // Suspect and Dead are not reachable
        node.status = NodeStatus::Suspect;
        assert!(!node.is_reachable());

        node.status = NodeStatus::Dead;
        assert!(!node.is_reachable());
    }

    #[test]
    fn test_cluster_config_new() {
        let config = ClusterConfig::new(
            "test-node".to_string(),
            "0.0.0.0".to_string(),
            6570,
            6565,
            48,
        );

        assert_eq!(config.node_name, "test-node");
        assert_eq!(config.cluster_ip, "0.0.0.0");
        assert_eq!(config.cluster_port, 6570);
        assert_eq!(config.backdoor_port, 6565);
        assert_eq!(config.total_buckets, 48);
        assert!(config.cluster_seeds.is_empty());
    }

    #[test]
    fn test_cluster_config_with_seeds() {
        let config = ClusterConfig::new(
            "test-node".to_string(),
            "0.0.0.0".to_string(),
            6570,
            6565,
            48,
        )
        .with_seeds("127.0.0.1:6570,127.0.0.1:6571")
        .expect("Should parse seeds");

        assert_eq!(config.cluster_seeds.len(), 2);
        assert_eq!(
            config.cluster_seeds[0],
            "127.0.0.1:6570".parse().unwrap()
        );
        assert_eq!(
            config.cluster_seeds[1],
            "127.0.0.1:6571".parse().unwrap()
        );
    }

    #[test]
    fn test_cluster_config_with_invalid_seeds() {
        let result = ClusterConfig::new(
            "test-node".to_string(),
            "0.0.0.0".to_string(),
            6570,
            6565,
            48,
        )
        .with_seeds("invalid-address");

        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_config_from_cli_args() {
        let node_id = uuid::Uuid::new_v4();
        let config = ClusterConfig::from_cli_args(
            node_id,
            Some("my-node".to_string()),
            "0.0.0.0".to_string(),
            6570,
            6565,
            48,
            Some("127.0.0.1:6570".to_string()),
        )
        .expect("Should create config");

        assert_eq!(config.node_id, node_id);
        assert_eq!(config.node_name, "my-node");
        assert_eq!(config.cluster_seeds.len(), 1);
    }

    #[test]
    fn test_cluster_config_from_cli_args_auto_hostname() {
        let node_id = uuid::Uuid::new_v4();
        let config = ClusterConfig::from_cli_args(
            node_id,
            None, // No node name provided
            "0.0.0.0".to_string(),
            6570,
            6565,
            48,
            None,
        )
        .expect("Should create config");

        assert_eq!(config.node_id, node_id);
        // Should have a default name (either hostname or "hes-node")
        assert!(!config.node_name.is_empty());
    }

    #[test]
    fn test_cluster_config_cluster_addr() {
        let config = ClusterConfig::new(
            "test-node".to_string(),
            "127.0.0.1".to_string(),
            6570,
            6565,
            48,
        );

        let addr = config.cluster_addr().expect("Should parse address");
        assert_eq!(addr, "127.0.0.1:6570".parse().unwrap());
    }

    #[test]
    fn test_node_info_heartbeat_timing() {
        use std::time::Duration;

        let mut node = NodeInfo::new_local(
            uuid::Uuid::new_v4(),
            "test-node".to_string(),
            "127.0.0.1:6570".parse().unwrap(),
            6565,
        );

        // Initial heartbeat should be very recent
        let time_since = node.time_since_heartbeat();
        assert!(time_since < Duration::from_secs(1));

        // Update heartbeat
        std::thread::sleep(Duration::from_millis(100));
        node.update_heartbeat();

        // Should be recent again
        let time_since = node.time_since_heartbeat();
        assert!(time_since < Duration::from_secs(1));
    }
}
