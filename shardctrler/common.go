package shardctrler

import (
	"github.com/cold-bin/dkv/internal/shardctrlersvc"
)

// The number of shards.
const NShards = 10

func CloneCfg(config *shardctrlersvc.Config) *shardctrlersvc.Config {
	gs := make(map[int64]*shardctrlersvc.Servers, len(config.Groups))
	for k, v := range config.Groups {
		gs[k] = &shardctrlersvc.Servers{
			Hosts: append([]string{}, v.GetHosts()...),
		}
	}
	return &shardctrlersvc.Config{
		Num:    config.Num,
		Shards: append([]int64{}, config.Shards...),
		Groups: gs,
	}
}

func EmptyCfg() *shardctrlersvc.Config {
	return &shardctrlersvc.Config{
		Num:    0,
		Shards: make([]int64, NShards),
		Groups: make(map[int64]*shardctrlersvc.Servers),
	}
}

const (
	OK = "OK"
)
