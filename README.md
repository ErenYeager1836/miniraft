# miniraft

A distributed consensus engine and replicated state-machine key-value store in **Go**, implementing the **Raft Consensus Protocol** (Ongaro & Ousterhout).

`miniraft` guarantees linearizable state machine replication across a multi-node cluster, providing automatic leader election, randomized election timeouts, log consistency checks, and transparent failover under node crashes.

---

## Features

- **Leader Election**:
  - Randomized election timeouts (150ms–300ms) to prevent split-vote scenarios.
  - Safe majority quorum voting (`> N/2`).
  - Term validation and up-to-date log comparison (`lastLogTerm` and `lastLogIndex`).
- **Log Replication**:
  - `AppendEntries` RPC with consistency verification (`prevLogIndex`, `prevLogTerm`).
  - Conflicting log entries are safely truncated and overwritten.
  - Advancing `commitIndex` strictly when replicated to a cluster majority.
- **State Machine Application**:
  - Asynchronous decoupled state machine execution via `ApplyMsg` channel.
  - In-memory replicated Key-Value Store (`PUT`, `GET`, `DELETE`).
- **Fault Tolerance & Auto-Recovery**:
  - Detects leader crashes and smoothly transitions through terms without state loss.
  - Clean concurrency control with mutex-protected state transitions and goroutine isolation.

---

## Architecture

```text
[ Client Request ]
       │
       ▼ (Submit "PUT key=val")
[ Leader Node ]
  ├── 1. Append entry to local log
  ├── 2. Broadcast AppendEntries RPCs in parallel ────┐
  │                                                   │
  │   ┌───────────────────────────────────────────────┘
  │   ▼
[ Follower 1 ] ──> Validates term & logs ──> Replies SUCCESS ──┐
[ Follower 2 ] ──> Validates term & logs ──> Replies SUCCESS ──┤
                                                               │
      ┌────────────────────────────────────────────────────────┘
      ▼
[ Majority Acknowledged (> N/2) ]
  ├── 3. Commit entry (commitIndex++)
  └── 4. Apply to State Machine (KV Store)
```

---

## Quick Start

### 1. Run the Interactive 3-Node Cluster Demo
```bash
go run .
```

### 2. Run Comprehensive Consensus Tests
```bash
go test -v ./...
```
Tests cover:
- `TestInitialElection`: Verifies single leader election within randomized timeout.
- `TestLeaderFailover`: Simulates leader crash and verifies automatic election of a new leader.
- `TestLogReplication`: Verifies uncommitted entries achieve quorum and commit.
- `TestReplicatedKVCluster`: Verifies replicated Key-Value store state machine consistency across all cluster nodes.

---

## License
MIT
