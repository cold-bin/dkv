package raft

import (
	"context"
	"time"
	
	"github.com/cold-bin/dkv/internal/raftsvc"
	"github.com/cold-bin/dkv/pkg/logx"
)

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		logx.Debug(logx.DSnap,
			"S%d snaped: curTerm:%d,commit:%d,apply:%d,snapshot:%d,last:%d",
			rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, index, rf.lastIncludedIndex)
	}()
	
	if rf.commitIndex < index /*commit过后才能快照*/ ||
		rf.lastIncludedIndex >= index /*快照点如果小于前一次快照点，没有必要快照*/ {
		logx.Debug(logx.DSnap, "S%d failed: rf.lastApplied<index(%v) rf.lastIncludedIndex>=index(%v)",
			rf.me, rf.lastApplied < index, rf.lastIncludedIndex >= index)
		return
	}
	
	// 丢弃被快照了的日志，同时修改其他状态
	// last: snap{nil,1,2,3} {nil}
	// now:  snap{nil,1,2,3,4,5} {nil,4,5}
	split := rf.logIndex(index)
	rf.lastIncludedIndex = index
	rf.log = append([]*raftsvc.LogEntry{{Term: rf.log[split].Term}}, rf.log[split+1:]...)
	rf.snapshot = snapshot
	rf.persist()
}

func (rf *Raft) InstallSnapshot(ctx context.Context, args *raftsvc.InstallSnapshotArgs) (
	reply *raftsvc.InstallSnapshotReply, err error,
) {
	reply = new(raftsvc.InstallSnapshotReply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		logx.Debug(logx.DSnap,
			"InstallSnapshot S%d: curTerm:%d,commit:%d,apply:%d,last:%d",
			rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex)
	}()
	
	// 1. 如果`term < currentTerm`就立即回复
	if args.Term < int64(rf.currentTerm) /*请求的领导者过期了，不能安装过期leader的快照*/ {
		reply.Term = int64(rf.currentTerm)
		return
	}
	
	if args.Term > int64(rf.currentTerm) /*当前raft落后，可以接着安装快照*/ {
		rf.currentTerm, rf.votedFor = int(args.Term), noVote
	}
	
	rf.changeRole(follower)
	rf.electionTimer.Reset(withRandomElectionDuration())
	
	// 5. 保存快照文件，丢弃具有较小索引的任何现有或部分快照
	// 小于commitIndex一定小于lastIncludedIndex
	if args.LastIncludedIndex <= int64(rf.lastIncludedIndex) /*raft快照点要先于leader时，无需快照*/ {
		reply.Term = int64(rf.currentTerm)
		return
	}
	
	if args.LastIncludedIndex <= int64(rf.commitIndex) /*leader快照点小于当前提交点*/ {
		reply.Term = int64(rf.currentTerm)
		return
	}
	
	defer rf.persist()
	
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  int(args.LastIncludedTerm),
		SnapshotIndex: int(args.LastIncludedIndex),
	}
	// 6. 如果现存的日志条目与快照中最后包含的日志条目具有相同的索引值和任期号，则保留其后的日志条目并进行回复
	for i := 1; i < len(rf.log); i++ {
		if int64(rf.realIndex(i)) == args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm {
			rf.log = append([]*raftsvc.LogEntry{{Term: args.LastIncludedTerm}}, rf.log[i+1:]...)
			rf.lastIncludedIndex = int(args.LastIncludedIndex)
			rf.commitIndex = int(args.LastIncludedIndex)
			rf.lastApplied = int(args.LastIncludedIndex)
			rf.snapshot = args.Data
			go func() {
				// 等待日志复制的日志进入管道过后才能把快照塞入管道
				for rf.replicatePreSnapshot.Load() {
					time.Sleep(10 * time.Millisecond)
				}
				rf.applyMsg <- msg
			}()
			
			reply.Term = int64(rf.currentTerm)
			return
		}
	}
	// 7. 丢弃整个日志（因为整个log都是过期的）
	rf.log = []*raftsvc.LogEntry{{Term: args.LastIncludedTerm}}
	rf.lastIncludedIndex = int(args.LastIncludedIndex)
	rf.commitIndex = int(args.LastIncludedIndex)
	rf.lastApplied = int(args.LastIncludedIndex)
	rf.snapshot = args.Data
	// 8. 使用快照重置状态机（并加载快照的集群配置）
	go func() {
		// 等待日志复制的日志进入管道过后才能把快照塞入管道
		for rf.replicatePreSnapshot.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		rf.applyMsg <- msg
	}()
	
	reply.Term = int64(rf.currentTerm)
	return
}

func (rf *Raft) sendInstallSnapshot(
	server int, args *raftsvc.InstallSnapshotArgs, reply *raftsvc.InstallSnapshotReply,
) bool {
	client := rf.peers[server]
	rsp, err := client.InstallSnapshot(context.Background(), args)
	if err != nil {
		logx.Debug(logx.DError, err.Error())
		return false
	}
	reply = rsp
	return true
}
