package raft

import (
	"context"
	
	"github.com/cold-bin/dkv/internal/raftsvc"
	"github.com/cold-bin/dkv/pkg/logx"
)

func (rf *Raft) AppendEntries(ctx context.Context, args *raftsvc.AppendEntriesArgs) (
	reply *raftsvc.AppendEntriesReply, err error,
) {
	reply = new(raftsvc.AppendEntriesReply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		logx.Debug(logx.DLog1,
			"AppendEntries S%d: curTerm:%d,role:%s,commit:%d,apply:%d,last:%d",
			rf.me, rf.currentTerm, rf.role.String(), rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex)
	}()
	
	if args.Term < int64(rf.currentTerm) { /*请求的leader任期落后了，leader会变成follower，应该拒绝请求*/
		reply.Term, reply.Success = int64(rf.currentTerm), false
		return
	}
	
	defer rf.persist()
	
	// 刷新选举超时器
	rf.changeRole(follower)
	rf.electionTimer.Reset(withRandomElectionDuration())
	
	if args.Term > int64(rf.currentTerm) { /*请求的leader任期更大，那么rf的任期需要更新，并转化为follower,并且取消以前任期的无效投票*/
		rf.currentTerm, rf.votedFor = int(args.Term), noVote
	}
	
	if int64(rf.lastIncludedIndex) > args.PrevLogIndex /*对等点的快照点已经超过本次日志复制的点，没有必要接受此日志复制rpc了*/ {
		reply.Term, reply.Success = int64(rf.currentTerm), false
		return
	}
	
	if int64(rf.lastLogIndex()) < args.PrevLogIndex /*可能rf过期，领导者已经应用了很多日志*/ {
		// 这种情况下，该raft实例断网一段时间过后，日志落后。所以直接返回 XLen即可。
		// leader更新nextIndex为XLen即可，表示当前raft实例缺少XLen及后面的日志，leader在下次广播时带上这些日志
		// leader   0{0} 1{101 102 103} 5{104}	PrevLogIndex=3	nextIndex=4
		// follower 0{0} 1{101 102 103} 5{104}  PrevLogIndex=3  nextIndex=4
		// follower 0{0} 1{101} 5 				PrevLogIndex=1  nextIndex=1
		reply.XTerm, reply.XIndex, reply.XLen = -1, -1, int64(rf.realLogLen())
		reply.Term, reply.Success = int64(rf.currentTerm), false
		return
	}
	
	/*冲突：该条目的任期在 prevLogIndex，上不能和 prevLogTerm 匹配上，则返回假*/
	index := rf.logIndex(int(args.PrevLogIndex))
	if rf.log[index].Term != args.PrevLogTerm {
		// 从后往前找冲突条目，返回最小冲突条目的索引
		conflictIndex, conflictTerm := -1, rf.log[index].Term
		for i := int(args.PrevLogIndex); i > rf.commitIndex; i-- {
			if rf.log[rf.logIndex(i)].Term != conflictTerm {
				break
			}
			conflictIndex = i
		}
		
		reply.XTerm, reply.XIndex, reply.XLen = conflictTerm, int64(conflictIndex), int64(rf.realLogLen())
		reply.Term, reply.Success = int64(rf.currentTerm), false
		return
	}
	
	for i, entry := range args.Entries {
		index = rf.logIndex(int(args.PrevLogIndex) + 1 + i)
		if index < len(rf.log) { /*重叠*/
			if rf.log[index].Term != entry.Term { /*看是否发生冲突*/
				rf.log = rf.log[:index]        // 删除当前以及后续所有log
				rf.log = append(rf.log, entry) // 把新log加入进来
			}
			/*没有冲突，那么就不添加这个重复的log*/
		} else if index == len(rf.log) { /*没有重叠，且刚好在下一个位置*/
			rf.log = append(rf.log, entry)
		}
	}
	
	if args.LeaderCommit > int64(rf.commitIndex) {
		rf.commitIndex = int(min(args.LeaderCommit, int64(rf.lastLogIndex())))
		rf.cond.Signal()
	}
	
	reply.Term, reply.Success = int64(rf.currentTerm), true
	return
}

func (rf *Raft) sendAppendEntries(
	server int, args *raftsvc.AppendEntriesArgs, reply *raftsvc.AppendEntriesReply,
) bool {
	client := rf.peers[server]
	rsp, err := client.AppendEntries(context.Background(), args)
	if err != nil {
		logx.Debug(logx.DError, err.Error())
		return false
	}
	reply = rsp
	return true
}
