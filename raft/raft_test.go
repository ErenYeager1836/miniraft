package raft

import (
	"testing"
	"time"
)

func makeCluster(n int) ([]*RaftNode, []chan ApplyMsg) {
	nodes := make([]*RaftNode, n)
	applyChs := make([]chan ApplyMsg, n)

	for i := 0; i < n; i++ {
		applyChs[i] = make(chan ApplyMsg, 100)
		nodes[i] = NewRaftNode(i, n, applyChs[i])
	}

	for i := 0; i < n; i++ {
		nodes[i].SetPeers(nodes)
		nodes[i].Start()
	}

	return nodes, applyChs
}

func checkSingleLeader(nodes []*RaftNode) int {
	leaderCount := 0
	leaderID := -1

	for _, n := range nodes {
		if !n.IsDead() && n.GetRole() == Leader {
			leaderCount++
			leaderID = n.GetID()
		}
	}

	if leaderCount == 1 {
		return leaderID
	}
	return -1
}

func TestInitialElection(t *testing.T) {
	nodes, _ := makeCluster(3)
	defer func() {
		for _, n := range nodes {
			n.Kill()
		}
	}()

	// Wait for election to resolve
	time.Sleep(600 * time.Millisecond)

	leaderID := checkSingleLeader(nodes)
	if leaderID == -1 {
		t.Fatalf("Expected exactly 1 leader among 3 nodes, got %d", leaderID)
	}

	term, isLeader := nodes[leaderID].GetState()
	if !isLeader || term < 1 {
		t.Fatalf("Leader %d invalid state: term=%d, isLeader=%v", leaderID, term, isLeader)
	}
}

func TestLeaderFailover(t *testing.T) {
	nodes, _ := makeCluster(3)
	defer func() {
		for _, n := range nodes {
			n.Kill()
		}
	}()

	time.Sleep(600 * time.Millisecond)
	initialLeader := checkSingleLeader(nodes)
	if initialLeader == -1 {
		t.Fatalf("No initial leader elected")
	}

	// Kill the leader
	nodes[initialLeader].Kill()

	// Wait for failover election
	time.Sleep(800 * time.Millisecond)

	newLeader := checkSingleLeader(nodes)
	if newLeader == -1 {
		t.Fatalf("No new leader elected after leader failure")
	}
	if newLeader == initialLeader {
		t.Fatalf("Dead leader should not be re-elected")
	}
}

func TestLogReplication(t *testing.T) {
	nodes, _ := makeCluster(3)
	defer func() {
		for _, n := range nodes {
			n.Kill()
		}
	}()

	time.Sleep(600 * time.Millisecond)
	leaderID := checkSingleLeader(nodes)
	if leaderID == -1 {
		t.Fatalf("No leader elected")
	}

	leader := nodes[leaderID]
	cmd := "SET foo=bar"
	idx, _, ok := leader.Submit(cmd)
	if !ok || idx != 1 {
		t.Fatalf("Failed to submit command to leader: idx=%d, ok=%v", idx, ok)
	}

	// Wait for append entries & commit
	time.Sleep(300 * time.Millisecond)

	leader.mu.Lock()
	commitIdx := leader.commitIndex
	leader.mu.Unlock()

	if commitIdx < 1 {
		t.Fatalf("Command was not committed to majority, commitIndex=%d", commitIdx)
	}
}
