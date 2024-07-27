package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	
	"github.com/cold-bin/dkv/internal/raftsvc"
	"github.com/cold-bin/dkv/pkg/logx"
	"github.com/dgraph-io/badger/v3"
)

// LSM 持久化
type Persister struct {
	mu sync.Mutex
	db *badger.DB
}

// PersistentStatus 持久化状态
type PersistentStatus struct {
	Log               []*raftsvc.LogEntry
	CurrentTerm       int
	VotedFor          int
	LastIncludedIndex int
	LastIncludedTerm  int
}

func NewPersister(path string) (*Persister, error) {
	opts := badger.DefaultOptions(path).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	
	return &Persister{db: db}, nil
}

func (ps *Persister) Close() {
	if err := ps.db.Close(); err != nil {
		log.Fatal(err)
	}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	var data []byte
	err := ps.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("raftstate"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			data = val
			return nil
		})
	})
	if err != nil {
		return nil
	}
	return data
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	var size int
	err := ps.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("raftstate"))
		if err != nil {
			return err
		}
		size = int(item.ValueSize())
		return nil
	})
	if err != nil {
		return 0
	}
	return size
}

func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	err := ps.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("raftstate"), raftstate); err != nil {
			return err
		}
		if err := txn.Set([]byte("snapshot"), snapshot); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to save state: %v", err)
	}
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	var data []byte
	err := ps.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("snapshot"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			data = val
			return nil
		})
	})
	if err != nil {
		return nil
	}
	return data
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	var size int
	err := ps.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("snapshot"))
		if err != nil {
			return err
		}
		size = int(item.ValueSize())
		return nil
	})
	if err != nil {
		return 0
	}
	return size
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persist() {
	defer func() {
		logx.Debug(logx.DPersist,
			"S%d save: curTerm:%d,lastTerm:%d,lastIndex:%d,commit:%d,apply:%d",
			rf.me, rf.currentTerm, rf.log[0].Term, rf.lastIncludedIndex, rf.commitIndex, rf.lastApplied)
	}()
	
	rf.persister.Save(rf.PersistStatusBytes(), rf.snapshot)
}

func (rf *Raft) PersistStatusBytes() []byte {
	status := &PersistentStatus{
		Log:               rf.log,
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  int(rf.log[0].Term),
	}
	
	w := new(bytes.Buffer)
	if err := gob.NewEncoder(w).Encode(status); err != nil {
		logx.Debug(logx.DError, "encode err:%v", err)
		return nil
	}
	
	return w.Bytes()
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	defer func() {
		logx.Debug(logx.DPersist,
			"S%d recover: curTerm:%d,commit:%d,apply:%d,last:%d",
			rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex)
	}()
	
	persistentStatus := &PersistentStatus{}
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(persistentStatus); err != nil {
		logx.Debug(logx.DError, "decode err:%v", err)
		return
	}
	
	// 裁减剩下的log
	rf.log = persistentStatus.Log
	rf.currentTerm = persistentStatus.CurrentTerm
	rf.votedFor = persistentStatus.VotedFor
	// 最新的快照点
	rf.lastIncludedIndex = persistentStatus.LastIncludedIndex
	rf.log[0].Term = int64(persistentStatus.LastIncludedTerm)
	// 之前被快照的数据，一定是被applied
	rf.commitIndex = persistentStatus.LastIncludedIndex
	rf.lastApplied = persistentStatus.LastIncludedIndex
	// 加载上一次的快照
	rf.snapshot = rf.persister.ReadSnapshot()
}
