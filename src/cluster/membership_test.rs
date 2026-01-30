//! Tests for membership list management.

#[cfg(test)]
mod tests {
    use super::super::membership::*;
    use super::super::node::*;
    use std::time::Duration;
    use uuid::Uuid;

    fn create_test_config() -> ClusterConfig {
        ClusterConfig::new(
            "test-node".to_string(),
            "127.0.0.1".to_string(),
            6570,
            6565,
            48,
        )
    }

    fn create_test_node(name: &str, port: u16) -> NodeInfo {
        NodeInfo::new_local(
            Uuid::new_v4(),
            name.to_string(),
            format!("127.0.0.1:{}", port).parse().unwrap(),
            6565,
        )
    }

    #[test]
    fn test_membership_list_creation() {
        let config = create_test_config();
        let membership = MembershipList::new(config.clone()).expect("Should create membership");

        assert_eq!(membership.local_node_id(), config.node_id);
        assert_eq!(membership.node_count(), 1); // Only local node
        assert_eq!(membership.nodes().len(), 0); // No remote nodes
    }

    #[test]
    fn test_add_node() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let node = create_test_node("remote-1", 6571);
        let node_id = node.node_id;

        membership.add_or_update_node(node);

        assert_eq!(membership.node_count(), 2); // Local + 1 remote
        assert!(membership.get_node(node_id).is_some());
    }

    #[test]
    fn test_dont_add_self() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config.clone()).expect("Should create membership");

        let local_id = membership.local_node_id();
        let self_node = NodeInfo::new_local(
            local_id,
            "local-node".to_string(),
            "127.0.0.1:6570".parse().unwrap(),
            6565,
        );

        membership.add_or_update_node(self_node);

        assert_eq!(membership.node_count(), 1); // Still only local node
        assert_eq!(membership.nodes().len(), 0); // No remote nodes added
    }

    #[test]
    fn test_update_existing_node() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let mut node = create_test_node("remote-1", 6571);
        node.status = NodeStatus::Starting;
        node.bucket_count = 10;
        let node_id = node.node_id;

        membership.add_or_update_node(node.clone());

        // Update the node
        node.status = NodeStatus::Active;
        node.bucket_count = 20;
        membership.add_or_update_node(node);

        let updated = membership.get_node(node_id).expect("Node should exist");
        assert_eq!(updated.status, NodeStatus::Active);
        assert_eq!(updated.bucket_count, 20);
    }

    #[test]
    fn test_mark_suspect() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let mut node = create_test_node("remote-1", 6571);
        node.status = NodeStatus::Active;
        let node_id = node.node_id;

        membership.add_or_update_node(node);
        membership.mark_suspect(node_id);

        let suspect_node = membership.get_node(node_id).expect("Node should exist");
        assert_eq!(suspect_node.status, NodeStatus::Suspect);
    }

    #[test]
    fn test_mark_dead() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let mut node = create_test_node("remote-1", 6571);
        node.status = NodeStatus::Suspect;
        let node_id = node.node_id;

        membership.add_or_update_node(node);
        membership.mark_dead(node_id);

        let dead_node = membership.get_node(node_id).expect("Node should exist");
        assert_eq!(dead_node.status, NodeStatus::Dead);
    }

    #[test]
    fn test_remove_node() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let node = create_test_node("remote-1", 6571);
        let node_id = node.node_id;

        membership.add_or_update_node(node);
        assert_eq!(membership.node_count(), 2);

        let removed = membership.remove_node(node_id);
        assert!(removed.is_some());
        assert_eq!(membership.node_count(), 1);
        assert!(membership.get_node(node_id).is_none());
    }

    #[test]
    fn test_active_nodes() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let mut node1 = create_test_node("node-1", 6571);
        node1.status = NodeStatus::Active;
        membership.add_or_update_node(node1);

        let mut node2 = create_test_node("node-2", 6572);
        node2.status = NodeStatus::Starting;
        membership.add_or_update_node(node2);

        let mut node3 = create_test_node("node-3", 6573);
        node3.status = NodeStatus::Dead;
        membership.add_or_update_node(node3);

        let active = membership.active_nodes();
        assert_eq!(active.len(), 1); // Only node1 is active
    }

    #[test]
    fn test_reachable_nodes() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let mut node1 = create_test_node("node-1", 6571);
        node1.status = NodeStatus::Active;
        membership.add_or_update_node(node1);

        let mut node2 = create_test_node("node-2", 6572);
        node2.status = NodeStatus::Draining;
        membership.add_or_update_node(node2);

        let mut node3 = create_test_node("node-3", 6573);
        node3.status = NodeStatus::Dead;
        membership.add_or_update_node(node3);

        let reachable = membership.reachable_nodes();
        assert_eq!(reachable.len(), 2); // Active and Draining
    }

    #[test]
    fn test_heartbeat_payload() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        membership.update_local_stats(5, 100, 50);

        let node1 = create_test_node("node-1", 6571);
        let node2 = create_test_node("node-2", 6572);
        membership.add_or_update_node(node1);
        membership.add_or_update_node(node2);

        let payload = membership.create_heartbeat_payload();

        assert_eq!(payload.bucket_count, 5);
        assert_eq!(payload.device_count, 100);
        assert_eq!(payload.load_percent, 50);
        assert_eq!(payload.known_nodes.len(), 2); // 2 remote nodes
    }

    #[test]
    fn test_suspect_candidates() {
        let config = ClusterConfig {
            suspect_timeout: Duration::from_millis(100),
            ..create_test_config()
        };
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let mut node = create_test_node("node-1", 6571);
        node.status = NodeStatus::Active;
        let node_id = node.node_id;

        membership.add_or_update_node(node);

        // Initially no suspects
        let suspects = membership.get_suspect_candidates();
        assert_eq!(suspects.len(), 0);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(150));

        // Now should be suspect candidate
        let suspects = membership.get_suspect_candidates();
        assert_eq!(suspects.len(), 1);
        assert_eq!(suspects[0], node_id);
    }

    #[test]
    fn test_update_heartbeat_resets_timeout() {
        let config = ClusterConfig {
            suspect_timeout: Duration::from_millis(100),
            ..create_test_config()
        };
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let mut node = create_test_node("node-1", 6571);
        node.status = NodeStatus::Active;
        let node_id = node.node_id;

        membership.add_or_update_node(node);

        // Wait half timeout
        std::thread::sleep(Duration::from_millis(60));

        // Update heartbeat
        membership.update_heartbeat(node_id);

        // Wait another 60ms (total 120ms from original, but only 60ms from update)
        std::thread::sleep(Duration::from_millis(60));

        // Should not be suspect yet
        let suspects = membership.get_suspect_candidates();
        assert_eq!(suspects.len(), 0);
    }

    #[test]
    fn test_node_count() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        assert_eq!(membership.node_count(), 1); // Only local

        membership.add_or_update_node(create_test_node("node-1", 6571));
        assert_eq!(membership.node_count(), 2);

        membership.add_or_update_node(create_test_node("node-2", 6572));
        assert_eq!(membership.node_count(), 3);
    }

    #[test]
    fn test_active_node_count() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        // Set local node as active
        membership.set_local_status(NodeStatus::Active);
        assert_eq!(membership.active_node_count(), 1);

        let mut node1 = create_test_node("node-1", 6571);
        node1.status = NodeStatus::Active;
        membership.add_or_update_node(node1);
        assert_eq!(membership.active_node_count(), 2);

        let mut node2 = create_test_node("node-2", 6572);
        node2.status = NodeStatus::Starting;
        membership.add_or_update_node(node2);
        assert_eq!(membership.active_node_count(), 2); // Starting doesn't count

        // Set local node as draining
        membership.set_local_status(NodeStatus::Draining);
        assert_eq!(membership.active_node_count(), 1); // Only node1
    }

    #[test]
    fn test_seq_counter() {
        let config = create_test_config();
        let mut membership = MembershipList::new(config).expect("Should create membership");

        let seq1 = membership.next_seq();
        let seq2 = membership.next_seq();
        let seq3 = membership.next_seq();

        assert_eq!(seq2, seq1 + 1);
        assert_eq!(seq3, seq2 + 1);
    }
}
