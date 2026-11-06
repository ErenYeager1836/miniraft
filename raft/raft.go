package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type LogEntry struct {
	Term    int         `json:"term"`
	Index   int         `json:"index"`
	Command interface{} `json:"command"`
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RaftNode struct {
	mu        sync.Mutex
	peers     []*RaftNode
	id        int
	dead      bool

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry // 1-based indexing, log[0] is dummy

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	role        Role
	applyCh     chan ApplyMsg
	lastContact time.Time
}

func NewRaftNode(id int, numPeers int, applyCh chan ApplyMsg) *RaftNode {
	rn := &RaftNode{
		id:          id,
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0, Index: 0, Command: nil}}, // Dummy index 0
		commitIndex: 0,
		lastApplied: 0,
		role:        Follower,
		applyCh:     applyCh,
		lastContact: time.Now(),
		nextIndex:   make([]int, numPeers),
		matchIndex:  make([]int, numPeers),
	}
	return rn
}

func (rn *RaftNode) SetPeers(peers []*RaftNode) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.peers = peers
}

func (rn *RaftNode) Start() {
	go rn.electionLoop()
}

func (rn *RaftNode) Kill() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.dead = true
}

func (rn *RaftNode) IsDead() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.dead
}

func (rn *RaftNode) GetState() (int, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm, rn.role == Leader
}

func (rn *RaftNode) GetID() int {
	return rn.id
}

func (rn *RaftNode) GetRole() Role {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.role
}

func (rn *RaftNode) electionTimeout() time.Duration {
	// 150-300ms randomized timeout
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (rn *RaftNode) electionLoop() {
	for !rn.IsDead() {
		timeout := rn.electionTimeout()
		time.Sleep(timeout)

		rn.mu.Lock()
		if rn.dead {
			rn.mu.Unlock()
			return
		}

		if rn.role != Leader && time.Since(rn.lastContact) >= timeout {
			rn.startElection()
		}
		rn.mu.Unlock()
	}
}

func (rn *RaftNode) startElection() {
	rn.role = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.lastContact = time.Now()

	term := rn.currentTerm
	candidateID := rn.id
	lastLogIndex := rn.lastLogIndex()
	lastLogTerm := rn.log[lastLogIndex].Term
	votesReceived := 1
	totalPeers := len(rn.peers)

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for i, peer := range rn.peers {
		if i == rn.id {
			continue
		}

		go func(p *RaftNode) {
			var reply RequestVoteReply
			if err := p.RequestVote(&args, &reply); err == nil {
				rn.mu.Lock()
				defer rn.mu.Unlock()

				if rn.role != Candidate || rn.currentTerm != term {
					return
				}

				if reply.Term > rn.currentTerm {
					rn.becomeFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					votesReceived++
					if votesReceived > totalPeers/2 && rn.role == Candidate {
						rn.becomeLeader()
					}
				}
			}
		}(peer)
	}
}

func (rn *RaftNode) becomeLeader() {
	rn.role = Leader
	for i := range rn.peers {
		rn.nextIndex[i] = rn.lastLogIndex() + 1
		rn.matchIndex[i] = 0
	}
	go rn.heartbeatLoop()
}

func (rn *RaftNode) becomeFollower(term int) {
	rn.role = Follower
	rn.currentTerm = term
	rn.votedFor = -1
	rn.lastContact = time.Now()
}

func (rn *RaftNode) heartbeatLoop() {
	for !rn.IsDead() {
		rn.mu.Lock()
		if rn.dead || rn.role != Leader {
			rn.mu.Unlock()
			return
		}
		rn.broadcastAppendEntries()
		rn.mu.Unlock()

		time.Sleep(50 * time.Millisecond) // 50ms heartbeat interval
	}
}

func (rn *RaftNode) broadcastAppendEntries() {
	for i, peer := range rn.peers {
		if i == rn.id {
			continue
		}

		prevIndex := rn.nextIndex[i] - 1
		if prevIndex < 0 {
			prevIndex = 0
		}
		prevTerm := rn.log[prevIndex].Term

		var entries []LogEntry
		if rn.lastLogIndex() >= rn.nextIndex[i] {
			entries = append([]LogEntry(nil), rn.log[rn.nextIndex[i]:]...)
		}

		args := AppendEntriesArgs{
			Term:         rn.currentTerm,
			LeaderID:     rn.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rn.commitIndex,
		}

		go func(peerIdx int, p *RaftNode, req AppendEntriesArgs) {
			var reply AppendEntriesReply
			if err := p.AppendEntries(&req, &reply); err == nil {
				rn.mu.Lock()
				defer rn.mu.Unlock()

				if rn.role != Leader || rn.currentTerm != req.Term {
					return
				}

				if reply.Term > rn.currentTerm {
					rn.becomeFollower(reply.Term)
					return
				}

				if reply.Success {
					rn.matchIndex[peerIdx] = req.PrevLogIndex + len(req.Entries)
					rn.nextIndex[peerIdx] = rn.matchIndex[peerIdx] + 1
					rn.checkAndUpdateCommitIndex()
				} else {
					if rn.nextIndex[peerIdx] > 1 {
						rn.nextIndex[peerIdx]--
					}
				}
			}
		}(i, peer, args)
	}
}

func (rn *RaftNode) checkAndUpdateCommitIndex() {
	for n := rn.commitIndex + 1; n <= rn.lastLogIndex(); n++ {
		if rn.log[n].Term == rn.currentTerm {
			matches := 1
			for i := range rn.peers {
				if i != rn.id && rn.matchIndex[i] >= n {
					matches++
				}
			}
			if matches > len(rn.peers)/2 {
				rn.commitIndex = n
				rn.applyLogs()
			}
		}
	}
}

func (rn *RaftNode) applyLogs() {
	for rn.commitIndex > rn.lastApplied {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]
		rn.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
	}
}

func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.dead {
		return fmt.Errorf("node is dead")
	}

	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
	}

	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	if args.Term < rn.currentTerm {
		return nil
	}

	upToDate := false
	myLastIndex := rn.lastLogIndex()
	myLastTerm := rn.log[myLastIndex].Term

	if args.LastLogTerm > myLastTerm {
		upToDate = true
	} else if args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex {
		upToDate = true
	}

	if (rn.votedFor == -1 || rn.votedFor == args.CandidateID) && upToDate {
		rn.votedFor = args.CandidateID
		rn.lastContact = time.Now()
		reply.VoteGranted = true
	}

	return nil
}

func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.dead {
		return fmt.Errorf("node is dead")
	}

	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
	}

	reply.Term = rn.currentTerm
	reply.Success = false

	if args.Term < rn.currentTerm {
		return nil
	}

	rn.lastContact = time.Now()

	// Check if log contains entry at PrevLogIndex matching PrevLogTerm
	if args.PrevLogIndex > rn.lastLogIndex() || rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil
	}

	// Insert new entries, truncating conflicting entries if any
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rn.lastLogIndex() {
			if rn.log[idx].Term != entry.Term {
				rn.log = rn.log[:idx]
				rn.log = append(rn.log, entry)
			}
		} else {
			rn.log = append(rn.log, entry)
		}
	}

	reply.Success = true

	// Update commitIndex if leaderCommit > commitIndex
	if args.LeaderCommit > rn.commitIndex {
		if args.LeaderCommit < rn.lastLogIndex() {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = rn.lastLogIndex()
		}
		rn.applyLogs()
	}

	return nil
}

func (rn *RaftNode) Submit(command interface{}) (int, int, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != Leader || rn.dead {
		return -1, -1, false
	}

	index := rn.lastLogIndex() + 1
	term := rn.currentTerm
	rn.log = append(rn.log, LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	})

	return index, term, true
}

func (rn *RaftNode) lastLogIndex() int {
	return len(rn.log) - 1
}
