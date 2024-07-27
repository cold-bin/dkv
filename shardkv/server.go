package shardkv

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/cold-bin/dkv/internal/shardctrlersvc"
	"github.com/cold-bin/dkv/internal/shardkvsvc"
	"github.com/cold-bin/dkv/pkg/logx"
	"github.com/cold-bin/dkv/raft"
	"github.com/cold-bin/dkv/shardctrler"
)

const (
	UpgradeConfigDuration = 100 * time.Millisecond
	rpcTimeout            = 500 * time.Millisecond
)

type Shard struct {
	Data      map[string]string
	ConfigNum int // what version this Shard is in
}

type Op struct {
	ClientId   int64
	SequenceId int64 // latest cfg num in config
	OpType     string
	
	// for get/put/append
	Key   string
	Value string
	
	// for config
	UpgradeCfg *shardctrlersvc.Config
	
	// for shard remove/add
	ShardId  int
	Shard    *shardkvsvc.Shard
	Duptable map[int64]int64 // only for shard add
}

func (o *Op) Marshal() []byte {
	bs, err := json.Marshal(o)
	if err != nil {
		logx.Debug("err", err.Error())
		return nil
	}
	return bs
}

func (o *Op) Unmarshal(bs []byte) {
	if err := json.Unmarshal(bs, &o); err != nil {
		logx.Debug("err", err.Error())
	}
}

// OpReply is used to wake waiting RPC caller after Op arrived from applyCh
type OpReply struct {
	ClientId   int64
	SequenceId int64
	Err        string
}

type ShardKV struct {
	shardkvsvc.UnimplementedShardKVServer
	
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) shardkvsvc.ShardKVClient
	gid          int
	masters      []shardctrlersvc.ShardctrlerClient
	maxRaftState int                    // snapshot if log grows this big
	dead         int32                  // set by Stop()
	cfg          *shardctrlersvc.Config // 需要更新的最新的配置
	lastCfg      *shardctrlersvc.Config // 更新之前的配置，用于比对是否全部更新完了
	shards       []*shardkvsvc.Shard    // ShardId -> Shard 如果Data == nil则说明当前的数据不归当前分片管
	wakeClient   map[int]chan OpReply
	duptable     map[int64]int
	mck          *shardctrler.Clerk
}

func (kv *ShardKV) Get(ctx context.Context, args *shardkvsvc.GetArgs) (reply *shardkvsvc.GetReply, err error) {
	reply = new(shardkvsvc.GetReply)
	shardId := key2shard(args.Key)
	
	kv.mu.Lock()
	if kv.cfg.Shards[shardId] != int64(kv.gid) {
		reply.Err = ErrWrongGroup
	} else if kv.shards[shardId].Data == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	
	cmd := &Op{
		OpType:     Get,
		ClientId:   args.ClientId,
		SequenceId: args.SequenceId,
		Key:        args.Key,
	}
	
	if err := kv.startCmd(cmd); err != OK {
		reply.Err = err
		return
	}
	
	kv.mu.Lock()
	if kv.cfg.Shards[shardId] != int64(kv.gid) {
		reply.Err = ErrWrongGroup
	} else if kv.shards[shardId].Data == nil {
		reply.Err = ShardNotArrived
	} else {
		reply.Err = OK
		reply.Value = kv.shards[shardId].Data[args.Key]
	}
	kv.mu.Unlock()
	
	return
	
}

func (kv *ShardKV) PutAppend(ctx context.Context, args *shardkvsvc.PutAppendArgs) (
	reply *shardkvsvc.PutAppendReply, err error,
) {
	reply = new(shardkvsvc.PutAppendReply)
	shardId := key2shard(args.Key)
	
	kv.mu.Lock()
	if kv.cfg.Shards[shardId] != int64(kv.gid) {
		reply.Err = ErrWrongGroup
	} else if kv.shards[shardId].Data == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	
	cmd := &Op{
		OpType:     args.Op,
		ClientId:   args.ClientId,
		SequenceId: args.SequenceId,
		Key:        args.Key,
		Value:      args.Value,
	}
	
	reply.Err = kv.startCmd(cmd)
	return
}

func (kv *ShardKV) AddShard(ctx context.Context, args *shardkvsvc.SendShardArg) (
	reply *shardkvsvc.SendShardReply, err error,
) {
	reply = new(shardkvsvc.SendShardReply)
	var cmd = &Op{
		OpType:     AddShard,
		ClientId:   args.ClientId,
		SequenceId: args.CfgNum,
		ShardId:    int(args.ShardId),
		Shard:      args.Shard,
		Duptable:   args.Duptable,
	}
	reply.Err = kv.startCmd(cmd)
	
	return
	
}

func StartServer(cfg *Config) *ShardKV {
	if cfg == nil {
		return nil
	}
	
	gob.Register(SnapshotStatus{})
	applych := make(chan raft.ApplyMsg)
	kv := &ShardKV{
		me:           cfg.Me,
		rf:           raft.Make(cfg.Servers, cfg.Me, cfg.Persister, applych),
		applyCh:      applych,
		makeEnd:      cfg.MakeEnd,
		gid:          cfg.Gid,
		masters:      cfg.Masters,
		maxRaftState: cfg.MaxRaftState,
		dead:         0,
		shards:       make([]*shardkvsvc.Shard, shardctrler.NShards),
		wakeClient:   make(map[int]chan OpReply),
		duptable:     make(map[int64]int),
		mck:          shardctrler.MakeClerk(cfg.Masters, int64(cfg.Gid)),
	}
	
	if snapshot := cfg.Persister.ReadSnapshot(); len(snapshot) > 0 {
		kv.decodeSnapShot(snapshot)
	}
	
	go kv.apply()
	go kv.watch()
	
	return kv
}

// apply 处理applyCh发送过来的ApplyMsg
func (kv *ShardKV) apply() {
	for !kv.stoped() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				opBs := msg.Command.([]byte)
				op := &Op{}
				op.Unmarshal(opBs)
				
				reply := OpReply{
					ClientId:   op.ClientId,
					SequenceId: op.SequenceId,
					Err:        OK,
				}
				
				if op.OpType == Put || op.OpType == Get || op.OpType == Append {
					shardId := key2shard(op.Key)
					
					if kv.cfg.Shards[shardId] != int64(kv.gid) /*当前组没有对应key*/ {
						reply.Err = ErrWrongGroup
					} else if kv.shards[shardId].Data == nil /*分片还没有到，需要等待到达*/ {
						reply.Err = ShardNotArrived
					} else /*当前组有对应key*/ {
						if !kv.isDuplicated(op.ClientId, int(op.SequenceId)) {
							kv.duptable[op.ClientId] = int(op.SequenceId)
							switch op.OpType {
							case Put:
								kv.shards[shardId].Data[op.Key] = op.Value
							case Append:
								data := kv.shards[shardId].Data
								builder := strings.Builder{}
								builder.WriteString(data[op.Key])
								builder.WriteString(op.Value)
								data[op.Key] = builder.String()
							case Get:
							default:
								log.Fatalf("invalid cmd type: %v.", op.OpType)
							}
						}
					}
				} else /*config or shard remove/add*/ {
					switch op.OpType {
					case UpgradeConfig:
						kv.upgradeConfig(op)
					case AddShard:
						if kv.cfg.Num < op.SequenceId /*不是最新的配置，等最新配置*/ {
							reply.Err = ConfigNotArrived
						} else /*否则就是最新配置，可以将分片接收*/ {
							kv.addShard(op)
						}
					case RemoveShard:
						kv.removeShard(op)
					default:
						log.Fatalf("invalid cmd type: %v.", op.OpType)
					}
				}
				
				// 判断快照
				if kv.maxRaftState != -1 && kv.rf.RaftStateSize() > kv.maxRaftState {
					snapshot := kv.encodeSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}
				
				ch := kv.waitCh(msg.CommandIndex)
				kv.mu.Unlock()
				ch <- reply
			} else if msg.SnapshotValid {
				// 读取快照的数据
				kv.mu.Lock()
				kv.decodeSnapShot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

// 配置监控
func (kv *ShardKV) watch() {
	kv.mu.Lock()
	curConfig := kv.cfg
	rf := kv.rf
	kv.mu.Unlock()
	
	for !kv.stoped() {
		// only leader needs to deal with configuration tasks
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpgradeConfigDuration)
			continue
		}
		kv.mu.Lock()
		
		// 判断是否把不属于自己的部分给分给别人了
		if !kv.isAllSent() {
			duptable := make(map[int64]int64)
			for k, v := range kv.duptable {
				duptable[k] = int64(v)
			}
			for shardId, gid := range kv.lastCfg.Shards {
				// 将最新配置里不属于自己的分片分给别人
				if gid == int64(kv.gid) && kv.cfg.Shards[shardId] != int64(kv.gid) &&
					kv.shards[shardId].ConfigNum < kv.cfg.Num {
					sendDate := kv.cloneShard(int(kv.cfg.Num), kv.shards[shardId].Data)
					args := &shardkvsvc.SendShardArg{
						Duptable: duptable,
						ShardId:  int64(shardId),
						Shard:    sendDate,
						ClientId: gid,
						CfgNum:   kv.cfg.Num,
					}
					
					// shardId -> gid -> server names
					serversList := kv.cfg.Groups[kv.cfg.Shards[shardId]].GetHosts()
					servers := make([]shardkvsvc.ShardKVClient, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.makeEnd(name)
					}
					
					// 开启协程对每个客户端发送切片(这里发送的应是别的组别，自身的共识组需要raft进行状态修改）
					go func(servers []shardkvsvc.ShardKVClient, args *shardkvsvc.SendShardArg) {
						index := 0
						start := time.Now()
						for {
							// 对自己的共识组内进行add
							reply, err := servers[index].AddShard(context.Background(), args)
							// for Challenge1:
							// 如果给予分片成功或者时间超时，这两种情况都需要进行GC掉不属于自己的分片
							// 成功：表示对端已经收到这个分片，那么这个分片的请求就一定会成功
							// 超时：保留一段时间的分片，以供客户端使用分片。这个时间一定要长于服务器崩溃恢复的时间
							if err == nil && reply.Err == OK || time.Now().Sub(start) >= 3*time.Second {
								// 如果成功
								kv.mu.Lock()
								cmd := &Op{
									OpType:     RemoveShard,
									ClientId:   int64(kv.gid),
									SequenceId: kv.cfg.Num,
									ShardId:    int(args.ShardId),
								}
								kv.mu.Unlock()
								kv.startCmd(cmd)
								break
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(UpgradeConfigDuration)
							}
						}
					}(servers, args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpgradeConfigDuration)
			continue
		}
		
		// 判断切片是否都收到了
		if !kv.isAllReceived() {
			kv.mu.Unlock()
			time.Sleep(UpgradeConfigDuration)
			continue
		}
		
		// 当前配置已配置，轮询下一个配置
		curConfig = kv.cfg
		mck := kv.mck
		kv.mu.Unlock()
		
		// 按照顺序一次处理一个重新配置请求，不能直接处理最新配置
		newConfig := mck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpgradeConfigDuration)
			continue
		}
		
		cmd := &Op{
			OpType:     UpgradeConfig,
			ClientId:   int64(kv.gid),
			SequenceId: newConfig.Num,
			UpgradeCfg: newConfig,
		}
		
		kv.startCmd(cmd)
	}
}

// 更新最新的config
func (kv *ShardKV) upgradeConfig(op *Op) {
	curConfig := shardctrler.CloneCfg(kv.cfg)
	upgradeCfg := op.UpgradeCfg
	
	if curConfig.Num >= upgradeCfg.Num {
		return
	}
	
	for shard, gid := range upgradeCfg.Shards {
		if gid == int64(kv.gid) && curConfig.Shards[shard] == 0 {
			// 如果更新的配置的gid与当前的配置的gid一样且分片为0(未分配）
			kv.shards[shard].Data = make(map[string]string)
			kv.shards[shard].ConfigNum = upgradeCfg.Num
		}
	}
	
	kv.lastCfg = curConfig
	kv.cfg = upgradeCfg
}

func (kv *ShardKV) addShard(op *Op) {
	// 此分片已添加或者它是过时的cmd
	if kv.shards[op.ShardId].Data != nil || op.Shard.ConfigNum < kv.cfg.Num {
		return
	}
	
	kv.shards[op.ShardId] = kv.cloneShard(int(op.Shard.ConfigNum), op.Shard.Data)
	
	// 查看发送分片过来的分组，更新seqid
	// 因为当前分片已被当前分组接管，其他附带的数据，诸如duptable也应该拿过来
	for clientId, seqId := range op.Duptable {
		if r, ok := kv.duptable[clientId]; !ok || int64(r) < seqId {
			kv.duptable[clientId] = int(seqId)
		}
	}
}

func (kv *ShardKV) removeShard(op *Op) {
	if op.SequenceId < kv.cfg.Num {
		return
	}
	kv.shards[op.ShardId].Data = nil
	kv.shards[op.ShardId].ConfigNum = op.SequenceId
	// lab tips: 服务器转移到新配置后，继续存储它不再拥有的分片是可以接受的（尽管这在实际系统中会令人遗憾）。这可能有助于简化您的服务器实现。
	// 这里无需移除duptable
}

func (kv *ShardKV) encodeSnapShot() []byte {
	w := new(bytes.Buffer)
	status := SnapshotStatus{
		Shards:       kv.shards,
		Duptable:     kv.duptable,
		MaxRaftState: kv.maxRaftState,
		Cfg:          kv.cfg,
		LastCfg:      kv.lastCfg,
	}
	if err := gob.NewEncoder(w).Encode(status); err != nil {
		log.Fatalf("[%d-%d] fails to take snapshot.", kv.gid, kv.me)
	}
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	var status SnapshotStatus
	if gob.NewDecoder(r).Decode(&status) != nil {
		log.Fatalf("[Server(%v)] Failed to decode snapshot！！！", kv.me)
	} else {
		kv.shards = status.Shards
		kv.duptable = status.Duptable
		kv.maxRaftState = status.MaxRaftState
		kv.cfg = status.Cfg
		kv.lastCfg = status.LastCfg
	}
}
func (kv *ShardKV) Stop() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Stop()
}

func (kv *ShardKV) stoped() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// 重复检测
func (kv *ShardKV) isDuplicated(clientId int64, seqId int) bool {
	lastSeqId, exist := kv.duptable[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardKV) waitCh(index int) chan OpReply {
	ch, exist := kv.wakeClient[index]
	if !exist {
		ch = make(chan OpReply, 1)
		kv.wakeClient[index] = ch
	}
	return ch
}

func (kv *ShardKV) isAllSent() bool {
	for shard, gid := range kv.lastCfg.Shards {
		// 如果当前配置中分片的信息不匹配，且持久化中的配置号更小，说明还未发送
		if gid == int64(kv.gid) && kv.cfg.Shards[shard] != int64(kv.gid) && kv.shards[shard].ConfigNum < kv.cfg.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) isAllReceived() bool {
	for shard, gid := range kv.lastCfg.Shards {
		// 判断切片是否都收到了
		if gid != int64(kv.gid) && kv.cfg.Shards[shard] == int64(kv.gid) && kv.shards[shard].ConfigNum < kv.cfg.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) startCmd(cmd *Op) string {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(cmd.Marshal())
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	
	ch := kv.waitCh(index)
	kv.mu.Unlock()
	
	select {
	case re := <-ch:
		kv.mu.Lock()
		kv.clean(index)
		if re.SequenceId != cmd.SequenceId || re.ClientId != cmd.ClientId {
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err
	case <-time.After(rpcTimeout):
		kv.mu.Lock()
		kv.clean(index)
		kv.mu.Unlock()
		return ErrOverTime
	}
}

func (kv *ShardKV) cloneShard(ConfigNum int, data map[string]string) *shardkvsvc.Shard {
	migrateShard := &shardkvsvc.Shard{
		Data:      make(map[string]string),
		ConfigNum: int64(ConfigNum),
	}
	
	for k, v := range data {
		migrateShard.Data[k] = v
	}
	
	return migrateShard
}

func (kv *ShardKV) clean(index int) {
	delete(kv.wakeClient, index)
}
