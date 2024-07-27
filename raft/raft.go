package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/cold-bin/dkv/internal/raftsvc"
	"github.com/cold-bin/dkv/pkg/logx"
)

type role uint8

const (
	leader role = iota + 1
	follower
	candidate
)

const (
	noVote = -1
)

func (r role) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	default:
		return fmt.Sprintf("UNKNOWN ROLE:%d", r)
	}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	raftsvc.UnimplementedRaftServer
	
	mu        sync.RWMutex
	peers     []raftsvc.RaftClient
	persister *Persister
	me        int
	dead      int32
	
	role role
	
	applyMsg chan ApplyMsg // 已提交日志需要被应用到状态机里
	cond     *sync.Cond
	
	// persistent states in every machine
	currentTerm       int                 // 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor          int                 // 当前任期内收到选票的 CandidateId，如果没有投给任何候选人 则为空
	log               []*raftsvc.LogEntry // 日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
	lastIncludedIndex int                 // 快照的最后一个日志条目索引
	
	snapshot []byte // 总是保存最新的快照
	
	// states of abnormal loss in every machine
	commitIndex int // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	
	// states of abnormal loss only in leader machine
	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增），作用就是ack掉成功调用日志复制的rpc
	
	electionTimer        *time.Timer   // random timer
	heartbeatTicker      *time.Ticker  // leader每隔至少100ms一次
	replicateSignal      chan struct{} // 新增的 leader log快速replicate的信号
	replicatePreSnapshot atomic.Bool   // 日志复制优先于快照安装进入applyCh
}

func withRandomElectionDuration() time.Duration {
	return time.Duration(300+(rand.Int63()%300)) * time.Millisecond
}

func withStableHeartbeatDuration() time.Duration {
	return 100 * time.Millisecond
}

// 论文里安全性的保证：参数的日志是否至少和自己一样新
func (rf *Raft) isLogUpToDate(lastLogTerm, lastLogIndex int) bool {
	return lastLogTerm > rf.lastLogTerm() || (lastLogTerm == rf.lastLogTerm() && lastLogIndex >= rf.lastLogIndex())
}

func (rf *Raft) firstLogIndex() int {
	if rf.lastIncludedIndex < 1 { /*没有快照*/
		return rf.lastIncludedIndex
	}
	return rf.lastIncludedIndex + 1 /*有快照*/
}

func (rf *Raft) lastLogIndex() int {
	// 快照：{nil 1 2 3 4 5 6 7 8 9} {nil,1}
	// lastIndex: 9+2-1=10
	// 无快照：{nil 1 2 3 4 5 6 7 8 9}
	// lastIndex: 10-1=9
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) < 2 /*只能从快照拿最后日志的term*/ {
		return int(rf.log[0].Term)
	}
	return int(rf.log[len(rf.log)-1].Term)
}

// 真实index在log中的索引
func (rf *Raft) logIndex(realIndex int) int {
	// 快照：{nil 1 2 3 4 5 6 7 8 9} {nil,10,11}
	// logIndex: 10-9 = 1
	// 无快照：{nil 1 2 3 4 5 6 7 8 9 10}
	// logIndex: 10-0=10
	return realIndex - rf.lastIncludedIndex
}

// log中的索引在log和快照里的索引
func (rf *Raft) realIndex(logIndex int) int {
	// 快照：{nil 1 2 3 4 5 6 7 8 9} {nil,10,11}
	// realIndex: 9+1 = 10
	// 无快照：{nil 1 2 3 4 5 6 7 8 9 10}
	// realIndex: 0+1=1
	return rf.lastIncludedIndex + logIndex
}

func (rf *Raft) realLogLen() int {
	// 快照：{nil 1 2 3 4 5 6 7 8 9} {nil,10,11}
	// realLogLen: 9+3=12
	// 无快照：{nil 1 2 3 4 5 6 7 8 9}
	// realLogLen: 0+10
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) Role() string {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.role.String()
}

func (rf *Raft) changeRole(r role) {
	preRole := rf.role
	rf.role = r
	logx.Debug(logx.DInfo, "S%d:%s->%s", rf.me, preRole.String(), r.String())
}

func (rf *Raft) GetState() (int, bool) {
	var (
		term     int
		isleader bool
	)
	// Your code here (2A).
	rf.mu.Lock()
	term, isleader = rf.currentTerm, rf.role == leader
	rf.mu.Unlock()
	
	return term, isleader
}

func (rf *Raft) Stop() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) stoped() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) electionEvent() {
	for !rf.stoped() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.changeRole(candidate)
			rf.startElection()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) logReplicateEvent() {
	for !rf.stoped() {
		select {
		case <-rf.replicateSignal:
			rf.mu.Lock()
			if rf.role == leader {
				rf.heartbeatBroadcast()
				rf.electionTimer.Reset(withRandomElectionDuration())
				// 新增log引起的快速log replicate算一次心跳，减少不必要的心跳发送
				rf.heartbeatTicker.Reset(withStableHeartbeatDuration())
			}
			rf.mu.Unlock()
		}
	}
}

// 将已提交的日志应用到状态机里。
// 注意：防止日志被应用状态机之前被裁减掉，也就是说，一定要等日志被应用过后才能被裁减掉。
func (rf *Raft) applierEvent() {
	for !rf.stoped() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied /*防止虚假唤醒*/ {
			rf.cond.Wait()
		}
		
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		msgs := make([]ApplyMsg, 0, commitIndex-lastApplied)
		for i := lastApplied + 1; i <= commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.logIndex(i)].Command,
				CommandIndex: i,
				CommandTerm:  int(rf.log[rf.logIndex(i)].Term),
			})
		}
		rf.mu.Unlock()
		
		logx.Debug(logx.DLog2, "S%d apply:%d,commit:%d", rf.me, commitIndex, lastApplied)
		
		for _, msg := range msgs {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 /*下一个apply的log一定是lastApplied+1，否则就是被快照按照替代了*/ {
				rf.mu.Unlock()
				continue
			}
			// 如果执行到这里了，说明下一个msg一定是日志复制，而不是快照安装.
			rf.replicatePreSnapshot.Store(true) // 快要执行快照安装的协程要先阻塞等待一会
			rf.mu.Unlock()
			rf.applyMsg <- msg
			rf.replicatePreSnapshot.Store(false) // 解除快照安装协程的等待
			
			rf.mu.Lock()
			rf.lastApplied = max(msg.CommandIndex, rf.lastApplied)
			
			logx.Debug(logx.DLog2, "S%d real apply:%d", rf.me, rf.lastApplied)
			
			rf.mu.Unlock()
		}
	}
}

func Make(peers []raftsvc.RaftClient, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	n := len(peers)
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		role:              follower,
		applyMsg:          applyCh,
		currentTerm:       0,
		votedFor:          noVote,
		log:               make([]*raftsvc.LogEntry, 1),
		lastIncludedIndex: 0,
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make([]int, n),
		matchIndex:        make([]int, n),
		electionTimer:     time.NewTimer(withRandomElectionDuration()),
		heartbeatTicker:   time.NewTicker(withStableHeartbeatDuration()),
		replicateSignal:   make(chan struct{}),
	}
	rf.readPersist(persister.ReadRaftState())
	rf.cond = sync.NewCond(&rf.mu)
	
	// 注册事件驱动并监听
	go rf.electionEvent()     // 选举协程
	go rf.heartbeatEvent()    // 心跳协程
	go rf.logReplicateEvent() // replicate协程
	go rf.applierEvent()      // apply协程
	
	return rf
}

func (rf *Raft) Start(command []byte) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.role != leader {
		return -1, -1, false
	}
	
	/* leader追加日志，长度一定大于等于2 */
	rf.log = append(rf.log, &raftsvc.LogEntry{
		Command: command,
		Term:    int64(rf.currentTerm),
	})
	rf.persist()
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = rf.lastLogIndex(), rf.realLogLen()
	index, term, isLeader = rf.lastLogIndex(), rf.currentTerm, true
	
	// 快速唤醒log replicate
	go func() {
		rf.replicateSignal <- struct{}{}
	}()
	
	logx.Debug(logx.DClient, "S%d Start cmd:%v,index:%d", rf.me, command, index)
	return index, term, isLeader
}
