package main

import (
	"fmt"
	"time"

	"github.com/ErenYeager1836/miniraft/kv"
	"github.com/ErenYeager1836/miniraft/raft"
)

func main() {
	fmt.Println("=============================================================")
	fmt.Println(" 🚀 miniraft - Distributed Raft Consensus Engine in Go")
	fmt.Println("=============================================================")

	numNodes := 3
	nodes := make([]*raft.RaftNode, numNodes)
	applyChs := make([]chan raft.ApplyMsg, numNodes)
	stores := make([]*kv.KVStore, numNodes)

	fmt.Printf("[+] Bootstrapping %d-node Raft consensus cluster...\n", numNodes)
	for i := 0; i < numNodes; i++ {
		applyChs[i] = make(chan raft.ApplyMsg, 100)
		nodes[i] = raft.NewRaftNode(i, numNodes, applyChs[i])
		stores[i] = kv.NewKVStore(nodes[i], applyChs[i])
	}

	for i := 0; i < numNodes; i++ {
		nodes[i].SetPeers(nodes)
		nodes[i].Start()
	}

	// Wait for leader election
	fmt.Println("[*] Running leader election with randomized timeouts...")
	time.Sleep(600 * time.Millisecond)

	var leaderID = -1
	for i, n := range nodes {
		term, isLeader := n.GetState()
		role := n.GetRole()
		fmt.Printf("    Node %d: Role=%-9s Term=%d\n", i, role, term)
		if isLeader {
			leaderID = i
		}
	}

	if leaderID == -1 {
		fmt.Println("[!] Election unresolved. Exiting.")
		return
	}

	fmt.Printf("\n[✓] Cluster Leader elected: Node %d\n", leaderID)

	// Replicating distributed writes
	fmt.Println("\n[*] Client submitting writes through cluster leader...")
	writes := map[string]string{
		"config.cluster_id": "us-east-mesh-01",
		"config.max_shards": "64",
		"service.status":    "healthy",
	}

	for k, v := range writes {
		if ok := stores[leaderID].Put(k, v); ok {
			fmt.Printf("    [Replicated] PUT %s = %q (committed to majority)\n", k, v)
		}
	}

	// Verify consistency across followers
	fmt.Println("\n[*] Verifying replicated state machine consistency across all nodes:")
	time.Sleep(200 * time.Millisecond)
	for i, s := range stores {
		val, _ := s.DirectGet("config.cluster_id")
		fmt.Printf("    Node %d State Machine: config.cluster_id = %q\n", i, val)
	}

	// Simulate Leader Failure & Auto-Failover
	fmt.Printf("\n[!] Simulating unexpected crash of Leader (Node %d)...\n", leaderID)
	nodes[leaderID].Kill()

	fmt.Println("[*] Waiting for surviving nodes to detect heartbeat timeout and failover...")
	time.Sleep(800 * time.Millisecond)

	newLeaderID := -1
	for i, n := range nodes {
		if !n.IsDead() {
			term, isLeader := n.GetState()
			fmt.Printf("    Node %d: Role=%-9s Term=%d\n", i, n.GetRole(), term)
			if isLeader {
				newLeaderID = i
			}
		}
	}

	if newLeaderID != -1 {
		fmt.Printf("[✓] Auto-failover succeeded! New Leader elected: Node %d\n", newLeaderID)
		val, _ := stores[newLeaderID].DirectGet("config.cluster_id")
		fmt.Printf("    New leader state machine intact: config.cluster_id = %q\n", val)
	}

	fmt.Println("=============================================================")
}
