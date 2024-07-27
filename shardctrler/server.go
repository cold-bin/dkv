package shardctrler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
	
	"github.com/cold-bin/dkv/internal/raftsvc"
	"github.com/cold-bin/dkv/internal/shardctrlersvc"
	"github.com/cold-bin/dkv/pkg/logx"
	"github.com/cold-bin/dkv/raft"
)

// ShardCtrler 分片控制器：决定副本组与分片的对应关系（config）
type ShardCtrler struct {
	shardctrlersvc.UnimplementedShardctrlerServer
	
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	
	persister  *raft.Persister
	duptable   map[int64]int64                // 判重
	wakeClient map[int]chan reqIdentification // 存储每个index处的term，用以唤醒客户端
	
	configs []*shardctrlersvc.Config // indexed by config num
}

func (sc *ShardCtrler) Join(ctx context.Context, args *shardctrlersvc.JoinArgs) (
	reply *shardctrlersvc.JoinReply, err error,
) {
	reply = new(shardctrlersvc.JoinReply)
	// 判重
	sc.mu.Lock()
	if preSeq, ok := sc.duptable[args.ClientId]; ok && preSeq >= args.SequenceId {
		reply.WrongLeader, reply.Err = false, OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	
	op := &Op{
		Type:       JOIN,
		Args:       args,
		ClientId:   args.ClientId,
		SequenceId: args.SequenceId,
	}
	
	// 先提交给raft
	index, _, isLeader := sc.rf.Start(op.Marshal())
	if !isLeader {
		reply.WrongLeader, reply.Err = true, OK
		return
	}
	
	sc.mu.Lock()
	ch := make(chan reqIdentification)
	sc.wakeClient[index] = ch
	sc.mu.Unlock()
	
	// 延迟释放资源
	defer func() {
		sc.clean(index)
	}()
	
	// 等待raft提交
	select {
	case <-time.After(rpcTimeout) /*超时还没有提交*/ :
	case r := <-ch /*阻塞等待唤醒*/ :
		if r.ClientId == args.ClientId && r.SequenceId == args.SequenceId {
			reply.WrongLeader, reply.Err = false, OK
			return
		}
	}
	
	reply.WrongLeader, reply.Err = true, OK
	return
	
}

func (sc *ShardCtrler) Leave(ctx context.Context, args *shardctrlersvc.LeaveArgs) (
	reply *shardctrlersvc.LeaveReply, err error,
) {
	reply = new(shardctrlersvc.LeaveReply)
	// 判重
	sc.mu.Lock()
	if preSeq, ok := sc.duptable[args.ClientId]; ok && preSeq >= args.SequenceId {
		reply.WrongLeader, reply.Err = false, OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	
	op := &Op{
		Type:       LEAVE,
		Args:       args,
		ClientId:   args.ClientId,
		SequenceId: args.SequenceId,
	}
	
	// 先提交给raft
	index, _, isLeader := sc.rf.Start(op.Marshal())
	if !isLeader {
		reply.WrongLeader, reply.Err = true, OK
		return
	}
	
	sc.mu.Lock()
	ch := make(chan reqIdentification)
	sc.wakeClient[index] = ch
	sc.mu.Unlock()
	
	// 延迟释放资源
	defer func() {
		sc.clean(index)
	}()
	
	// 等待raft提交
	select {
	case <-time.After(rpcTimeout) /*超时还没有提交*/ :
	case r := <-ch /*阻塞等待唤醒*/ :
		if r.ClientId == args.ClientId && r.SequenceId == args.SequenceId {
			reply.WrongLeader, reply.Err = false, OK
			return
		}
	}
	
	reply.WrongLeader, reply.Err = true, OK
	return
	
}

func (sc *ShardCtrler) Move(ctx context.Context, args *shardctrlersvc.MoveArgs) (
	reply *shardctrlersvc.MoveReply, err error,
) {
	reply = new(shardctrlersvc.MoveReply)
	sc.mu.Lock()
	// 判重
	if preSeq, ok := sc.duptable[args.ClientId]; ok && preSeq >= args.SequenceId {
		reply.WrongLeader, reply.Err = false, OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	
	op := &Op{
		Type:       MOVE,
		Args:       args,
		ClientId:   args.ClientId,
		SequenceId: args.SequenceId,
	}
	
	// 先提交给raft
	index, _, isLeader := sc.rf.Start(op.Marshal())
	if !isLeader {
		reply.WrongLeader, reply.Err = true, OK
		return
	}
	
	sc.mu.Lock()
	ch := make(chan reqIdentification)
	sc.wakeClient[index] = ch
	sc.mu.Unlock()
	
	// 延迟释放资源
	defer func() {
		sc.clean(index)
	}()
	
	// 等待raft提交
	select {
	case <-time.After(rpcTimeout) /*超时还没有提交*/ :
	case r := <-ch /*阻塞等待唤醒*/ :
		if r.ClientId == args.ClientId && r.SequenceId == args.SequenceId {
			reply.WrongLeader, reply.Err = false, OK
			return
		}
	}
	
	reply.WrongLeader, reply.Err = true, OK
	return
	
}

func (sc *ShardCtrler) Query(ctx context.Context, args *shardctrlersvc.QueryArgs) (
	reply *shardctrlersvc.QueryReply, err error,
) {
	reply = new(shardctrlersvc.QueryReply)
	op := &Op{
		Type:       QUERY,
		Args:       args,
		ClientId:   args.ClientId,
		SequenceId: args.SequenceId,
	}
	
	// 先提交给raft
	index, _, isLeader := sc.rf.Start(op.Marshal())
	if !isLeader {
		reply.WrongLeader, reply.Err, reply.Config = true, OK, EmptyCfg()
		return
	}
	
	sc.mu.Lock()
	ch := make(chan reqIdentification)
	sc.wakeClient[index] = ch
	sc.mu.Unlock()
	
	// 延迟释放资源
	defer func() {
		sc.clean(index)
	}()
	
	// 等待raft提交
	select {
	case <-time.After(rpcTimeout) /*超时还没有提交*/ :
	case r := <-ch /*阻塞等待唤醒*/ :
		if r.ClientId == args.ClientId && r.SequenceId == args.SequenceId {
			idx := -1
			sc.mu.Lock()
			if args.Num == -1 || args.Num > sc.configs[len(sc.configs)-1].Num /*最新配置*/ {
				idx = len(sc.configs) - 1
			} else /*编号num的配置*/ {
				ok := false
				for i, cfg := range sc.configs {
					if cfg.Num == args.Num {
						idx = i
						ok = true
						break
					}
				}
				if !ok /*配置编号不存在*/ {
					reply.WrongLeader, reply.Err, reply.Config = true, OK, EmptyCfg()
					sc.mu.Unlock()
					return
				}
			}
			
			cfg := CloneCfg(sc.configs[idx])
			sc.mu.Unlock()
			
			reply.WrongLeader, reply.Err, reply.Config = false, OK, cfg
			return
		}
	}
	
	reply.WrongLeader, reply.Err, reply.Config = true, OK, EmptyCfg()
	return
	
}

// 唯一对应一个请求
type reqIdentification struct {
	ClientId   int64
	SequenceId int64
	Err        error
}

type OpType string

const (
	JOIN  OpType = "Join"
	LEAVE OpType = "Leave"
	MOVE  OpType = "Move"
	QUERY OpType = "Query"
)

const rpcTimeout = time.Second

type Op struct {
	Type       OpType
	Args       any
	ClientId   int64
	SequenceId int64
}

func (o *Op) Marshal() []byte {
	bs, err := json.Marshal(o)
	if err != nil {
		logx.Debug(logx.DError, err.Error())
		return nil
	}
	return bs
}

func (o *Op) Unmarshal(bs []byte) {
	if err := json.Unmarshal(bs, &o); err != nil {
		logx.Debug(logx.DError, err.Error())
	}
}

func (sc *ShardCtrler) clean(index int) {
	sc.mu.Lock()
	delete(sc.wakeClient, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Stop() {
	sc.rf.Stop()
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []raftsvc.RaftClient, me int, persister *raft.Persister) *ShardCtrler {
	applyCh := make(chan raft.ApplyMsg)
	
	sc := &ShardCtrler{
		me:         me,
		rf:         raft.Make(servers, me, persister, applyCh),
		applyCh:    applyCh,
		persister:  persister,
		duptable:   make(map[int64]int64),
		wakeClient: make(map[int]chan reqIdentification),
	}
	
	sc.configs = []*shardctrlersvc.Config{
		{
			Num:    0,
			Shards: make([]int64, NShards),
			Groups: make(map[int64]*shardctrlersvc.Servers),
		},
	}
	sc.configs[0].Groups = make(map[int64]*shardctrlersvc.Servers)
	
	go sc.apply()
	
	return sc
}

func (sc *ShardCtrler) apply() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.mu.Lock()
			index := msg.CommandIndex
			opBs := msg.Command.([]byte)
			op := &Op{}
			op.Unmarshal(opBs)
			
			if preSequenceId, ok := sc.duptable[op.ClientId]; ok &&
				preSequenceId == op.SequenceId /*应用前需要再判一次重*/ {
			} else /*没有重复，可以应用状态机并记录在table里*/ {
				sc.duptable[op.ClientId] = op.SequenceId
				switch op.Type {
				case JOIN:
					sc.handleJoin(op.Args.(*shardctrlersvc.JoinArgs))
				case LEAVE:
					sc.handleLeave(op.Args.(*shardctrlersvc.LeaveArgs))
				case MOVE:
					sc.handleMove(op.Args.(*shardctrlersvc.MoveArgs))
				case QUERY:
					/*nothing*/
				}
			}
			
			ch := sc.waitCh(index)
			sc.mu.Unlock()
			ch <- reqIdentification{
				ClientId:   op.ClientId,
				SequenceId: op.SequenceId,
				Err:        errors.New(OK),
			}
		}
	}
}

func (sc *ShardCtrler) waitCh(index int) chan reqIdentification {
	ch, exist := sc.wakeClient[index]
	if !exist {
		sc.wakeClient[index] = make(chan reqIdentification, 1)
		ch = sc.wakeClient[index]
	}
	return ch
}

func (sc *ShardCtrler) handleJoin(args *shardctrlersvc.JoinArgs) {
	oldCfg := CloneCfg(sc.configs[len(sc.configs)-1])
	
	// 新配置中的副本组
	for gid, servers := range args.Servers {
		oldCfg.Groups[gid] = servers
	}
	
	// 副本组对应的分片数目
	g2ShardCnt := make(map[int]int)
	for gid := range oldCfg.Groups /*初始化*/ {
		g2ShardCnt[int(gid)] = 0
	}
	// 计算分组与分片数
	for _, gid := range oldCfg.Shards {
		if gid != 0 {
			g2ShardCnt[int(gid)]++
		}
	}
	
	shards := []int64{}
	for i := 0; i < NShards; i++ {
		shards = append(shards, 0)
	}
	newConfig := &shardctrlersvc.Config{
		Num:    int64(len(sc.configs)),
		Shards: shards,
		Groups: oldCfg.Groups,
	}
	if len(g2ShardCnt) != 0 {
		newConfig.Shards = sc.balance(g2ShardCnt, oldCfg.Shards)
	}
	
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) handleLeave(args *shardctrlersvc.LeaveArgs) {
	// 先转化为map方便快速定位gid
	isDelete := make(map[int]bool)
	for _, gid := range args.GIDs {
		isDelete[int(gid)] = true
	}
	
	oldCfg := CloneCfg(sc.configs[len(sc.configs)-1])
	
	// 删除对应的gid的值
	for _, gid := range args.GIDs {
		delete(oldCfg.Groups, gid)
	}
	
	g2ShardCnt := make(map[int]int)
	newShard := oldCfg.Shards
	
	// 初始化 g2ShardCnt
	for gid_ := range oldCfg.Groups {
		gid := int(gid_)
		
		if !isDelete[gid] /*剩下的副本组重新初始化*/ {
			g2ShardCnt[gid] = 0
		}
	}
	
	for shard, gid_ := range oldCfg.Shards {
		gid := int(gid_)
		if gid != 0 {
			if isDelete[gid] /*删除这个副本组*/ {
				newShard[shard] = 0
			} else {
				g2ShardCnt[gid]++
			}
		}
	}
	
	shards := []int64{}
	for i := 0; i < NShards; i++ {
		shards = append(shards, 0)
	}
	newConfig := &shardctrlersvc.Config{
		Num:    int64(len(sc.configs)),
		Shards: shards,
		Groups: oldCfg.Groups,
	}
	if len(g2ShardCnt) != 0 {
		newConfig.Shards = sc.balance(g2ShardCnt, newShard)
	}
	
	sc.configs = append(sc.configs, newConfig)
}

// 将当前所有者的shard分片移除并交给GID副本组
func (sc *ShardCtrler) handleMove(args *shardctrlersvc.MoveArgs) {
	// 创建新的配置
	newConfig := CloneCfg(sc.configs[len(sc.configs)-1])
	newConfig.Num++
	
	// 找到并去掉指定的副本组
	if args.Shard >= NShards && args.Shard < 0 /*不在分片*/ {
		return
	}
	newConfig.Shards[args.Shard] = args.GID
	
	sc.configs = append(sc.configs, newConfig)
}
