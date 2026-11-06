package kv

import (
	"sync"
	"time"

	"github.com/ErenYeager1836/miniraft/raft"
)

type OpType string

const (
	OpPut    OpType = "PUT"
	OpGet    OpType = "GET"
	OpDelete OpType = "DELETE"
)

type Op struct {
	Type  OpType
	Key   string
	Value string
	ReqID int64
}

type KVStore struct {
	mu      sync.Mutex
	data    map[string]string
	rf      *raft.RaftNode
	applyCh chan raft.ApplyMsg
	notify  map[int]chan OpResult
	dead    bool
}

type OpResult struct {
	Success bool
	Value   string
	Err     string
}

func NewKVStore(rf *raft.RaftNode, applyCh chan raft.ApplyMsg) *KVStore {
	kv := &KVStore{
		data:    make(map[string]string),
		rf:      rf,
		applyCh: applyCh,
		notify:  make(map[int]chan OpResult),
	}
	go kv.applyLoop()
	return kv
}

func (kv *KVStore) Close() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.dead = true
}

func (kv *KVStore) applyLoop() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		kv.mu.Lock()
		res := OpResult{Success: true}
		switch op.Type {
		case OpPut:
			kv.data[op.Key] = op.Value
		case OpDelete:
			delete(kv.data, op.Key)
		case OpGet:
			val, exists := kv.data[op.Key]
			if exists {
				res.Value = val
			} else {
				res.Success = false
				res.Err = "Key not found"
			}
		}

		if ch, ok := kv.notify[msg.CommandIndex]; ok {
			ch <- res
			delete(kv.notify, msg.CommandIndex)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVStore) Put(key, value string) bool {
	op := Op{Type: OpPut, Key: key, Value: value, ReqID: time.Now().UnixNano()}
	idx, _, isLeader := kv.rf.Submit(op)
	if !isLeader {
		return false
	}

	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.notify[idx] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		return res.Success
	case <-time.After(1 * time.Second):
		return false
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	op := Op{Type: OpGet, Key: key, ReqID: time.Now().UnixNano()}
	idx, _, isLeader := kv.rf.Submit(op)
	if !isLeader {
		return "", false
	}

	ch := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.notify[idx] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		return res.Value, res.Success
	case <-time.After(1 * time.Second):
		return "", false
	}
}

func (kv *KVStore) DirectGet(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.data[key]
	return val, ok
}
