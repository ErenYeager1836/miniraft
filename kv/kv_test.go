package kv

import (
	"testing"
	"time"

	"github.com/ErenYeager1836/miniraft/raft"
)

func TestReplicatedKVCluster(t *testing.T) {
	n := 3
	nodes := make([]*raft.RaftNode, n)
	applyChs := make([]chan raft.ApplyMsg, n)
	stores := make([]*KVStore, n)

	for i := 0; i < n; i++ {
		applyChs[i] = make(chan raft.ApplyMsg, 100)
		nodes[i] = raft.NewRaftNode(i, n, applyChs[i])
		stores[i] = NewKVStore(nodes[i], applyChs[i])
	}

	for i := 0; i < n; i++ {
		nodes[i].SetPeers(nodes)
		nodes[i].Start()
	}

	defer func() {
		for i := 0; i < n; i++ {
			nodes[i].Kill()
			stores[i].Close()
		}
	}()

	// Wait for election
	time.Sleep(600 * time.Millisecond)

	var leaderStore *KVStore
	var leaderID int
	for i, node := range nodes {
		if _, isLeader := node.GetState(); isLeader {
			leaderStore = stores[i]
			leaderID = i
			break
		}
	}

	if leaderStore == nil {
		t.Fatalf("No leader found in cluster")
	}

	// 1. Put key through leader
	ok := leaderStore.Put("cluster_name", "alpha-mesh")
	if !ok {
		t.Fatalf("Failed to Put key on leader %d", leaderID)
	}

	// Wait for replication to followers
	time.Sleep(200 * time.Millisecond)

	// 2. Verify state machine consistency across all 3 nodes!
	for i, s := range stores {
		val, exists := s.DirectGet("cluster_name")
		if !exists || val != "alpha-mesh" {
			t.Fatalf("Node %d state machine inconsistent: val=%q, exists=%v", i, val, exists)
		}
	}
}
