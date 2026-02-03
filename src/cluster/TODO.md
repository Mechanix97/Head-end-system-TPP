# Cluster Module - TODO List

## High Priority (Functionality Blockers)

### 1. Delegation Response Handling
**File**: `delegation.rs:124`
**Issue**: Currently returns success immediately without waiting for DELEGATE_ACCEPT/REJECT
**Impact**:
- Devices can become orphaned if target rejects
- No timeout mechanism if target is unresponsive
- Race conditions during delegation

**Solution**:
```rust
// Use oneshot channels to wait for response
let (tx, rx) = oneshot::channel();
pending_delegations.insert(delegation_id, tx);
tokio::time::timeout(5s, rx.await)?
```

**Files to modify**:
- `delegation.rs` - Add pending_delegations HashMap, implement timeout logic
- `server.rs` - Complete oneshot channel when receiving DELEGATE_ACCEPT/REJECT
- `protocol/payload.rs` - Add delegation_id field to track request-response pairs

---

### 2. Indirect Probe Response Matching
**File**: `failure_detector.rs:223`
**Issue**: No mechanism to match PROBE_RESPONSE to PROBE_REQUEST
**Impact**:
- Can't distinguish which probe a response is for
- Timing is unreliable (just sleeps 5 seconds)
- Multiple concurrent probes interfere with each other

**Solution**:
```rust
// Track pending probes with request ID
let (tx, rx) = oneshot::channel();
pending_probes.insert(probe_id, tx);
tokio::time::timeout(probe_timeout, rx.await)?
```

**Implementation**:
- Add `probe_id: Uuid` to ProbeRequest/Response payloads
- Store pending probes in HashMap<Uuid, oneshot::Sender<bool>>
- Complete channel in server handler when response arrives

---

## Medium Priority (Performance & Robustness)

### 3. Automatic Rebalancing After Shutdown Delegation
**File**: `delegation.rs:231`
**Issue**: When accepting devices from shutting down node while overloaded, no automatic rebalancing
**Impact**: Node stays overloaded until manual intervention

**Solution**:
- Spawn background task to redistribute devices
- Calculate target load (e.g., 70%)
- Select least-loaded nodes
- Call `request_delegation()` with `DelegationReason::Rebalance`

**Notes**:
- Should run asynchronously (don't block DELEGATE_ACCEPT response)
- Add cooldown to prevent oscillation
- Consider hysteresis (only rebalance if > 10% over target)

---

### 4. Device Manager Tests
**File**: `mod.rs:44`
**Issue**: No tests for DeviceManager

**Tests needed**:
- `test_load_from_database()` - Device ownership recovery
- `test_claim_unassigned_devices()` - Orphan device claiming
- `test_accept_delegation()` - Delegation acceptance flow
- `test_release_devices()` - Device release
- `test_redistribute_from_failed()` - Failure recovery

---

## Low Priority (Nice to Have)

### 5. Delegation Retry Logic
**Related to**: #1 (Delegation Response Handling)
**Enhancement**: If delegation fails, automatically retry with another node

**Implementation**:
- Get list of available nodes sorted by load
- Try delegation in order
- If all fail, log error and keep devices temporarily

---

### 6. Load Metrics Improvement
**Enhancement**: More sophisticated load calculation

**Current**: Simple percentage
**Proposed**:
- Factor in device count
- Factor in bucket distribution
- Consider network latency
- Predictive load (upcoming scheduled connections)

---

### 7. Graceful Overload Handling
**Enhancement**: Better behavior when all nodes are overloaded

**Ideas**:
- Queue delegation requests
- Temporary ownership by coordinator
- Shed least critical devices first
- Alert/monitoring integration

---

## Notes

- All TODOs should be implemented with proper error handling
- Add metrics for delegation success/failure rates
- Consider adding cluster state machine visualization for debugging
- Protocol changes should maintain wire format compatibility where possible
