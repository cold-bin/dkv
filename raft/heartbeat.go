package raft

import (
	"fmt"
	
	"github.com/cold-bin/dkv/internal/raftsvc"
	"github.com/cold-bin/dkv/pkg/logx"
)

func (rf *Raft) heartbeatBroadcast() {
	logx.Debug(logx.DTimer, "S%d start broadcast", rf.me)
	
	n := len(rf.peers)
	for peer := 0; peer < n; peer++ {
		if peer == rf.me {
			continue
		}
		
		if rf.nextIndex[peer] <= rf.lastIncludedIndex /*存在于快照中，发送安装快照RPC*/ {
			args := &raftsvc.InstallSnapshotArgs{
				Term:              int64(rf.currentTerm),
				LeaderId:          int64(rf.me),
				LastIncludedIndex: int64(rf.lastIncludedIndex),
				LastIncludedTerm:  rf.log[0].Term,
				Data:              rf.snapshot,
			}
			
			logx.Debug(logx.DSnap, `sendInstallSnapshot S%d -> S%d, args{LastIncludedIndex:%d,LastIncludedTerm:%d}`,
				rf.me, peer, args.LastIncludedIndex, args.LastIncludedTerm)
			
			go rf.handleSendInstallSnapshot(peer, args)
		} else /*存在于未裁减的log中，发起日志复制rpc*/ {
			args := &raftsvc.AppendEntriesArgs{
				Term:         int64(rf.currentTerm),
				LeaderId:     int64(rf.me),
				Entries:      make([]*raftsvc.LogEntry, 0),
				PrevLogIndex: int64(rf.nextIndex[peer] - 1),
				LeaderCommit: int64(rf.commitIndex),
			}
			if args.PrevLogIndex > int64(rf.lastIncludedIndex) &&
				args.PrevLogIndex < int64(rf.lastIncludedIndex+len(rf.log)) /*下一个日志在leader log里，且前一个日志没在快照里，也在leader log里*/ {
				args.PrevLogTerm = rf.log[rf.logIndex(int(args.PrevLogIndex))].Term
			} else if args.PrevLogIndex == int64(rf.lastIncludedIndex) /*下一个日志在leader log里，但上一个日志在快照里，没在leader log里*/ {
				args.PrevLogTerm = rf.log[0].Term
			} else /*排除极端情况：PrevLogIndex>=rf.lastIncludedIndex+len(rf.log) */ {
				/*这种情况说明raft实现可能存在问题，因为leader的日志落后于follower了*/
				logx.Debug(logx.DError,
					fmt.Sprintf(`S%d -> S%d PrevLogIndex(%d)>={lastIncludedIndex+len(log)(%d)`,
						rf.me, peer, args.PrevLogIndex, rf.lastIncludedIndex+len(rf.log)))
			}
			
			// deep copy
			logts := rf.log[rf.logIndex(rf.nextIndex[peer]):]
			for i := 0; i < len(logts); i++ {
				cmd := make([]byte, len(logts[i].Command))
				copy(cmd, logts[i].Command)
				args.Entries = append(args.Entries, &raftsvc.LogEntry{
					Command: cmd,
					Term:    logts[i].Term,
				})
			}
			
			logx.Debug(logx.DLog1,
				`sendAppendEntries S%d -> S%d, args{PrevLogIndex:%d,PrevLogTerm:%d,LeaderCommit:%d,entries_len:%d}`,
				rf.me, peer, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
			
			go rf.handleSendAppendEntries(peer, args)
		}
	}
}

func (rf *Raft) handleSendAppendEntries(peer int, args *raftsvc.AppendEntriesArgs) {
	reply := &raftsvc.AppendEntriesReply{}
	if ok := rf.sendAppendEntries(peer, args, reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer func() {
			logx.Debug(logx.DLog1, `sendAppendEntries S%d's reply, nextIndex:%d matchIndex:%d`, peer,
				rf.nextIndex[peer],
				rf.matchIndex[peer])
		}()
		
		if int64(rf.currentTerm) != args.Term || rf.role != leader { /*不是leader，没有必要在进行广播*/
			return
		}
		
		if reply.Term > int64(rf.currentTerm) { /*过期该返回*/
			rf.changeRole(follower)
			rf.currentTerm = int(reply.Term)
			rf.persist()
			return
		}
		
		if reply.Success { /*心跳成功或日志复制成功*/
			// 可能快照和日志复制reply同一时间到达，需要取两者的最大值
			rf.matchIndex[peer] = max(rf.matchIndex[peer], int(args.PrevLogIndex)+len(args.Entries))
			rf.nextIndex[peer] = max(rf.nextIndex[peer], int(args.PrevLogIndex)+len(args.Entries)+1)
			/*超过半数节点追加成功，也就是已提交，并且还是leader，那么就可以应用当前任期里的日志到状态机里。找到共识N：遍历对等点，找到相同的N*/
			rf.checkAndCommitLogs()
		} else {
			// 快速定位nextIndex
			rf.findNextIndex(peer, reply)
		}
	}
}

func (rf *Raft) handleSendInstallSnapshot(peer int, args *raftsvc.InstallSnapshotArgs) {
	reply := &raftsvc.InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(peer, args, reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer func() {
			logx.Debug(logx.DSnap,
				`sendInstallSnapshot S%d: next:%d,match:%d,LastIndex:%d,LastTerm:%d}`,
				peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.lastIncludedIndex, rf.log[0].Term)
		}()
		
		if int64(rf.currentTerm) != args.Term || rf.role != leader {
			return
		}
		
		if reply.Term > int64(rf.currentTerm) {
			rf.changeRole(follower)
			rf.currentTerm = int(reply.Term)
			rf.persist()
			return
		}
		
		// 可能快照和日志复制reply同一时间到达，需要取两者的最大值
		rf.matchIndex[peer] = max(rf.matchIndex[peer], int(args.LastIncludedIndex))
		rf.nextIndex[peer] = max(rf.nextIndex[peer], int(args.LastIncludedIndex+1))
	}
}

func (rf *Raft) checkAndCommitLogs() {
	n := len(rf.peers)
	N := rf.commitIndex
	for _N := rf.commitIndex + 1; _N < rf.realLogLen(); _N++ {
		succeedNum := 0
		for peer := 0; peer < n; peer++ {
			if _N <= rf.matchIndex[peer] && rf.log[rf.logIndex(_N)].Term == int64(rf.currentTerm) {
				succeedNum++
			}
		}
		if succeedNum > n/2 { /*继续找更大的共识N*/
			N = _N
		}
	}
	
	if N > rf.commitIndex { /*leader可以提交了*/
		logx.Debug(logx.DLog1, `S%d commit:%d, last:%d`, rf.me, N, rf.lastIncludedIndex)
		rf.commitIndex = N
		rf.cond.Signal()
	}
}

func (rf *Raft) findNextIndex(peer int, reply *raftsvc.AppendEntriesReply) {
	if reply.XTerm == -1 && reply.XIndex == -1 { /*Case 3: follower's log is too short*/
		rf.nextIndex[peer] = int(reply.XLen)
		return
	}
	
	ok := false
	for i, entry := range rf.log { /*Case 2: leader has XTerm*/
		if entry.Term == reply.XTerm {
			ok = true
			rf.nextIndex[peer] = rf.realIndex(i)
		}
	}
	
	if !ok { /*Case 1: leader doesn't have XTerm*/
		rf.nextIndex[peer] = int(reply.XIndex)
	}
}

func (rf *Raft) heartbeatEvent() {
	for !rf.stoped() {
		select {
		case <-rf.heartbeatTicker.C:
			rf.mu.Lock()
			if rf.role == leader {
				rf.heartbeatBroadcast()
				rf.electionTimer.Reset(withRandomElectionDuration()) // leader广播完毕时，也应该把自己的选举超时器刷新一下
			}
			rf.mu.Unlock()
		}
	}
}
