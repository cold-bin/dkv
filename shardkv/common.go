package shardkv

import (
	"github.com/cold-bin/dkv/internal/shardctrlersvc"
	"github.com/cold-bin/dkv/internal/shardkvsvc"
)

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ShardNotArrived     = "ShardNotArrived"
	ConfigNotArrived    = "ConfigNotArrived"
	ErrInconsistentData = "ErrInconsistentData"
	ErrOverTime         = "ErrOverTime"
)

const (
	Put           = "Put"
	Append        = "Append"
	Get           = "Get"
	UpgradeConfig = "UpgradeConfig"
	AddShard      = "AddShard"
	RemoveShard   = "RemoveShard"
)

type SnapshotStatus struct {
	Shards       []*shardkvsvc.Shard
	Duptable     map[int64]int
	MaxRaftState int
	Cfg          *shardctrlersvc.Config
	LastCfg      *shardctrlersvc.Config
}
