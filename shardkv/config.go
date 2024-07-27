package shardkv

import (
	"github.com/cold-bin/dkv/internal/raftsvc"
	"github.com/cold-bin/dkv/internal/shardctrlersvc"
	"github.com/cold-bin/dkv/internal/shardkvsvc"
	"github.com/cold-bin/dkv/raft"
)

type Config struct {
	Me           int
	Gid          int
	Rf           *raft.Raft
	MakeEnd      func(string) shardkvsvc.ShardKVClient
	Masters      []shardctrlersvc.ShardctrlerClient
	Servers      []raftsvc.RaftClient
	Persister    *raft.Persister
	MaxRaftState int // snapshot if log grows this big
}

func NewConfig(
	me int, gid int, rf *raft.Raft, maxRaftState int,
	servers []raftsvc.RaftClient, persister *raft.Persister,
	makeEnd func(string) shardkvsvc.ShardKVClient, masters []shardctrlersvc.ShardctrlerClient,
) *Config {
	return &Config{
		Me:           me,
		Gid:          gid,
		Rf:           rf,
		MakeEnd:      makeEnd,
		Masters:      masters,
		Servers:      servers,
		Persister:    persister,
		MaxRaftState: maxRaftState,
	}
}
