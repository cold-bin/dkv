package shardkv

import (
	"context"
	"time"
	
	"github.com/cold-bin/dkv/internal/shardctrlersvc"
	"github.com/cold-bin/dkv/internal/shardkvsvc"
	"github.com/cold-bin/dkv/pkg/idx"
	"github.com/cold-bin/dkv/shardctrler"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type Clerk struct {
	sm         *shardctrler.Clerk
	config     shardctrlersvc.Config
	make_end   func(string) shardkvsvc.ShardKVClient
	clientId   int64
	sequenceId int64
}

func MakeClerk(
	ctrlers []shardctrlersvc.ShardctrlerClient, make_end func(string) shardkvsvc.ShardKVClient, node int64,
) *Clerk {
	ck := new(Clerk)
	
	ck.sm = shardctrler.MakeClerk(ctrlers, node)
	ck.make_end = make_end
	ck.clientId = idx.ID(node)
	ck.sequenceId = 0
	ck.config = *ck.sm.Query(-1)
	
	return ck
}

func (ck *Clerk) Get(key string) string {
	ck.sequenceId++
	for {
		args := shardkvsvc.GetArgs{
			Key:        key,
			ClientId:   ck.clientId,
			SequenceId: ck.sequenceId,
		}
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			hosts := servers.GetHosts()
			for si := 0; si < len(hosts); si++ {
				srv := ck.make_end(hosts[si])
				reply, err := srv.Get(context.Background(), &args)
				if err == nil && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if err == nil && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = *ck.sm.Query(-1)
	}
	
	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.sequenceId++
	
	for {
		args := shardkvsvc.PutAppendArgs{
			Key:        key,
			Value:      value,
			Op:         op,
			ClientId:   ck.clientId,
			SequenceId: ck.sequenceId,
		}
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			hosts := servers.GetHosts()
			for si := 0; si < len(hosts); si++ {
				srv := ck.make_end(hosts[si])
				reply, err := srv.PutAppend(context.Background(), &args)
				if err == nil && reply.Err == OK {
					return
				}
				if err == nil && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = *ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
