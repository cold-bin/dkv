package shardctrler

import (
	"context"
	"time"
	
	"github.com/cold-bin/dkv/internal/shardctrlersvc"
	"github.com/cold-bin/dkv/pkg/idx"
	"github.com/cold-bin/dkv/pkg/logx"
)

type Clerk struct {
	servers    []shardctrlersvc.ShardctrlerClient // raft集群
	clientId   int64                              // 标识客户端
	sequenceId int64                              // 表示相同客户端发起的不同rpc. 重复rpc具有相同的此项值
}

func MakeClerk(servers []shardctrlersvc.ShardctrlerClient, node int64) *Clerk {
	return &Clerk{
		servers:    servers,
		clientId:   idx.ID(node),
		sequenceId: 0,
	}
}

func (ck *Clerk) Query(num int64) *shardctrlersvc.Config {
	defer func() {
		logx.Debug(logx.DClient, "", ck.clientId, ck.sequenceId)
	}()
	
	args := &shardctrlersvc.QueryArgs{
		Num:        num,
		ClientId:   ck.clientId,
		SequenceId: ck.sequenceId,
	}
	ck.sequenceId++
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			reply, err := srv.Query(context.Background(), args)
			if err == nil && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int64]*shardctrlersvc.Servers) {
	args := &shardctrlersvc.JoinArgs{
		Servers:    servers,
		ClientId:   ck.clientId,
		SequenceId: ck.sequenceId,
	}
	ck.sequenceId++
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			reply, err := srv.Join(context.Background(), args)
			if err == nil && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int64) {
	args := &shardctrlersvc.LeaveArgs{
		GIDs:       gids,
		ClientId:   ck.clientId,
		SequenceId: ck.sequenceId,
	}
	ck.sequenceId++
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			reply, err := srv.Leave(context.Background(), args)
			if err == nil && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int64, gid int64) {
	args := &shardctrlersvc.MoveArgs{
		Shard:      shard,
		GID:        gid,
		ClientId:   ck.clientId,
		SequenceId: ck.sequenceId,
	}
	ck.sequenceId++
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			reply, err := srv.Move(context.Background(), args)
			if err == nil && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
