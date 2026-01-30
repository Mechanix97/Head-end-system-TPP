//! Tests for bucket management.

#[cfg(test)]
mod tests {
    use super::super::bucket_manager::*;
    use super::super::membership::MembershipList;
    use super::super::node::*;
    use common::database::{api::Database, DatabaseType};
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    async fn create_test_setup() -> (BucketManager, Arc<RwLock<MembershipList>>, Database) {
        let node_id = Uuid::new_v4();
        let total_buckets = 10;
        let db = Database::new(DatabaseType::InMemory, None)
            .await
            .expect("Should create in-memory DB");

        let config = ClusterConfig::new(
            "test-node".to_string(),
            "127.0.0.1".to_string(),
            6570,
            6565,
            total_buckets,
        );

        let membership = Arc::new(
            RwLock::new(MembershipList::new(config).expect("Should create membership")),
        );

        let bucket_manager = BucketManager::new(node_id, total_buckets, db.clone(), membership.clone());

        (bucket_manager, membership, db)
    }

    #[tokio::test]
    async fn test_new_bucket_manager() {
        let (manager, _, _) = create_test_setup().await;

        assert_eq!(manager.bucket_count(), 0);
        assert!(manager.owned_buckets().is_empty());
    }

    #[tokio::test]
    async fn test_owns_bucket() {
        let (mut manager, _, _) = create_test_setup().await;

        assert!(!manager.owns_bucket(0));
        assert!(!manager.owns_bucket(5));

        // Manually add a bucket
        manager.owned_buckets.insert(5);

        assert!(!manager.owns_bucket(0));
        assert!(manager.owns_bucket(5));
    }

    #[tokio::test]
    async fn test_claim_unassigned_buckets() {
        let (mut manager, _, db) = create_test_setup().await;

        // No buckets assigned yet
        let claimed = manager
            .claim_unassigned_buckets()
            .await
            .expect("Should claim buckets");

        assert_eq!(claimed.len(), 10); // All 10 buckets should be claimed
        assert_eq!(manager.bucket_count(), 10);

        // Verify in database
        let assignments = db.get_bucket_assignments().await.expect("Should get assignments");
        assert_eq!(assignments.len(), 10);
    }

    #[tokio::test]
    async fn test_claim_only_unassigned_buckets() {
        let (mut manager, _, db) = create_test_setup().await;

        // Pre-assign some buckets to another node
        let other_node = Uuid::new_v4();
        db.assign_bucket(0, other_node)
            .await
            .expect("Should assign");
        db.assign_bucket(1, other_node)
            .await
            .expect("Should assign");

        // Claim remaining
        let claimed = manager
            .claim_unassigned_buckets()
            .await
            .expect("Should claim buckets");

        assert_eq!(claimed.len(), 8); // Only 8 unassigned buckets
        assert_eq!(manager.bucket_count(), 8);
        assert!(!manager.owns_bucket(0));
        assert!(!manager.owns_bucket(1));
        assert!(manager.owns_bucket(2));
    }

    #[tokio::test]
    async fn test_accept_delegation() {
        let (mut manager, _, db) = create_test_setup().await;

        let buckets = vec![1, 2, 3];
        manager
            .accept_delegation(buckets.clone())
            .await
            .expect("Should accept delegation");

        assert_eq!(manager.bucket_count(), 3);
        assert!(manager.owns_bucket(1));
        assert!(manager.owns_bucket(2));
        assert!(manager.owns_bucket(3));

        // Verify in database
        let assignments = db.get_bucket_assignments().await.expect("Should get assignments");
        assert_eq!(assignments.len(), 3);
    }

    #[tokio::test]
    async fn test_release_buckets() {
        let (mut manager, _, _) = create_test_setup().await;

        // Add buckets
        manager.owned_buckets.insert(1);
        manager.owned_buckets.insert(2);
        manager.owned_buckets.insert(3);
        assert_eq!(manager.bucket_count(), 3);

        // Release some
        manager.release_buckets(&[1, 3]);

        assert_eq!(manager.bucket_count(), 1);
        assert!(!manager.owns_bucket(1));
        assert!(manager.owns_bucket(2));
        assert!(!manager.owns_bucket(3));
    }

    #[tokio::test]
    async fn test_buckets_for_shutdown() {
        let (mut manager, _, _) = create_test_setup().await;

        manager.owned_buckets.insert(1);
        manager.owned_buckets.insert(5);
        manager.owned_buckets.insert(9);

        let buckets = manager.buckets_for_shutdown();

        assert_eq!(buckets.len(), 3);
        assert!(buckets.contains(&1));
        assert!(buckets.contains(&5));
        assert!(buckets.contains(&9));
    }

    #[tokio::test]
    async fn test_assign_buckets_on_join() {
        let (mut manager, membership, db) = create_test_setup().await;

        // Set up local node with all buckets
        for i in 0..10 {
            db.assign_bucket(i, manager.local_node_id)
                .await
                .expect("Should assign");
            manager.owned_buckets.insert(i);
        }
        assert_eq!(manager.bucket_count(), 10);

        // Set local node as active
        {
            let mut m = membership.write().await;
            m.set_local_status(NodeStatus::Active);
        }

        // New node joins
        let new_node_id = Uuid::new_v4();
        let given = manager
            .assign_buckets_on_join(new_node_id)
            .await
            .expect("Should assign buckets");

        // Should give away half (fair share is 10/2 = 5, so give away 5)
        assert_eq!(given.len(), 5);
        assert_eq!(manager.bucket_count(), 5);

        // Verify buckets were assigned to new node in DB
        let new_node_buckets = db
            .get_buckets_by_node(new_node_id)
            .await
            .expect("Should get buckets");
        assert_eq!(new_node_buckets.len(), 5);
    }

    #[tokio::test]
    async fn test_assign_buckets_fair_share() {
        let (mut manager, membership, db) = create_test_setup().await;

        // Give manager 8 buckets
        for i in 0..8 {
            db.assign_bucket(i, manager.local_node_id)
                .await
                .expect("Should assign");
            manager.owned_buckets.insert(i);
        }

        // Set up 3 total active nodes (local + 2 others)
        {
            let mut m = membership.write().await;
            m.set_local_status(NodeStatus::Active);

            let mut node1 = NodeInfo::new_local(
                Uuid::new_v4(),
                "node-1".to_string(),
                "127.0.0.1:6571".parse().unwrap(),
                6565,
            );
            node1.status = NodeStatus::Active;
            node1.bucket_count = 1;
            m.add_or_update_node(node1);
        }

        // New node joins (4th node total)
        let new_node_id = Uuid::new_v4();
        let given = manager
            .assign_buckets_on_join(new_node_id)
            .await
            .expect("Should assign buckets");

        // Fair share is 10/4 = 2.5 ≈ 2
        // Manager has 8, should give away 8-2 = 6 (but only to bring down to fair share)
        // Actually gives 8 - 2 = 6 buckets
        assert!(given.len() > 0);
        assert!(manager.bucket_count() < 8);
    }

    #[tokio::test]
    async fn test_redistribute_from_failed() {
        let (mut manager, membership, db) = create_test_setup().await;

        // Create a failed node with buckets
        let failed_node_id = Uuid::new_v4();
        for i in 0..5 {
            db.assign_bucket(i, failed_node_id)
                .await
                .expect("Should assign");
        }

        // Set local node as active
        {
            let mut m = membership.write().await;
            m.set_local_status(NodeStatus::Active);
        }

        // Redistribute from failed node
        manager
            .redistribute_from_failed(failed_node_id)
            .await
            .expect("Should redistribute");

        // Manager should now own some or all of the failed node's buckets
        assert!(manager.bucket_count() > 0);
        assert!(manager.bucket_count() <= 5);
    }

    #[tokio::test]
    async fn test_redistribute_among_multiple_nodes() {
        let (mut manager, membership, db) = create_test_setup().await;

        // Set up multiple active nodes
        {
            let mut m = membership.write().await;
            m.set_local_status(NodeStatus::Active);

            let mut node1 = NodeInfo::new_local(
                Uuid::new_v4(),
                "node-1".to_string(),
                "127.0.0.1:6571".parse().unwrap(),
                6565,
            );
            node1.status = NodeStatus::Active;
            node1.bucket_count = 0;
            m.add_or_update_node(node1);

            let mut node2 = NodeInfo::new_local(
                Uuid::new_v4(),
                "node-2".to_string(),
                "127.0.0.1:6572".parse().unwrap(),
                6565,
            );
            node2.status = NodeStatus::Active;
            node2.bucket_count = 0;
            m.add_or_update_node(node2);
        }

        // Failed node had 9 buckets
        let failed_node_id = Uuid::new_v4();
        for i in 0..9 {
            db.assign_bucket(i, failed_node_id)
                .await
                .expect("Should assign");
        }

        // Redistribute
        manager
            .redistribute_from_failed(failed_node_id)
            .await
            .expect("Should redistribute");

        // All 9 buckets should be redistributed
        let total_assigned = db
            .get_bucket_assignments()
            .await
            .expect("Should get assignments")
            .len();
        assert_eq!(total_assigned, 9);

        // No bucket should still belong to failed node
        let failed_buckets = db
            .get_buckets_by_node(failed_node_id)
            .await
            .expect("Should get buckets");
        assert_eq!(failed_buckets.len(), 0);
    }

    #[tokio::test]
    async fn test_load_from_database() {
        let (mut manager, _, db) = create_test_setup().await;

        // Pre-populate database with bucket assignments
        db.assign_bucket(1, manager.local_node_id)
            .await
            .expect("Should assign");
        db.assign_bucket(3, manager.local_node_id)
            .await
            .expect("Should assign");
        db.assign_bucket(7, manager.local_node_id)
            .await
            .expect("Should assign");

        // Assign some to other node
        let other_node = Uuid::new_v4();
        db.assign_bucket(2, other_node)
            .await
            .expect("Should assign");

        // Load from database
        manager
            .load_from_database()
            .await
            .expect("Should load from DB");

        // Should have loaded only own buckets
        assert_eq!(manager.bucket_count(), 3);
        assert!(manager.owns_bucket(1));
        assert!(manager.owns_bucket(3));
        assert!(manager.owns_bucket(7));
        assert!(!manager.owns_bucket(2));
    }
}
