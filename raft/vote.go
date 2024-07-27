package raft

import (
	"context"
	
	"github.com/cold-bin/dkv/internal/raftsvc"
	"github.com/cold-bin/dkv/pkg/logx"
)

func (rf *Raft) RequestVote(ctx context.Context, args *raftsvc.RequestVoteArgs) (
	reply *raftsvc.RequestVoteReply, err error,
) {
	reply = new(raftsvc.RequestVoteReply)
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		logx.Debug(logx.DVote, "RequestVote S%d: votedFor:%d,role:%s,curTerm:%d",
			rf.me, rf.votedFor, rf.role.String(), rf.currentTerm)
	}()
	
	if args.Term < int64(rf.currentTerm) { /*请求者任期较小，拒绝请求*/
		reply.Term, reply.VoteGranted = int64(rf.currentTerm), false
		return
	}
	
	defer rf.persist()
	
	if args.Term > int64(rf.currentTerm) { /*还可以投票*/
		rf.changeRole(follower)
		rf.currentTerm, rf.votedFor = int(args.Term), noVote
	}
	
	if (rf.votedFor == noVote || rf.votedFor == int(args.CandidateId)) &&
		rf.isLogUpToDate(int(args.LastLogTerm), int(args.LastLogIndex)) { /*日志至少和自己一样新，才能投票，否则不能投票*/
		rf.votedFor = int(args.CandidateId)
		rf.electionTimer.Reset(withRandomElectionDuration())
		logx.Debug(logx.DVote, "S%d vote to S%d", rf.me, args.CandidateId)
		reply.Term, reply.VoteGranted = int64(rf.currentTerm), true
		return
	}
	
	reply.Term, reply.VoteGranted = int64(rf.currentTerm), false
	return
}

func (rf *Raft) sendRequestVote(
	server int, args *raftsvc.RequestVoteArgs, reply *raftsvc.RequestVoteReply,
) bool {
	client := rf.peers[server]
	rsp, err := client.RequestVote(context.Background(), args)
	if err != nil {
		logx.Debug(logx.DError, err.Error())
		return false
	}
	reply = rsp
	return true
}

// for candidate
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	approvedNum := 1 // 先给自己投一票
	rf.electionTimer.Reset(withRandomElectionDuration())
	logx.Debug(logx.DTimer, "S%d start election", rf.me)
	n := len(rf.peers)
	
	// 向其他对等点并发发送投票请求
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		args := &raftsvc.RequestVoteArgs{
			Term:         int64(rf.currentTerm),
			CandidateId:  int64(rf.me),
			LastLogIndex: int64(rf.lastLogIndex()),
			LastLogTerm:  int64(rf.lastLogTerm()),
		}
		logx.Debug(logx.DVote, "sendRequestVote S%d -> S%d, args{%+v}", rf.me, i, args)
		go func(peer int) {
			reply := new(raftsvc.RequestVoteReply)
			if ok := rf.sendRequestVote(peer, args, reply); !ok {
				return
			}
			
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer func() {
				logx.Debug(logx.DVote, "sendRequestVote S%d: granted:%v,term:%d", peer, reply.VoteGranted,
					reply.Term)
			}()
			
			if int64(rf.currentTerm) != args.Term || rf.role != candidate /*提前结束*/ {
				return
			}
			
			if reply.Term > int64(rf.currentTerm) {
				rf.currentTerm, rf.votedFor = int(reply.Term), noVote
				rf.persist()
				rf.changeRole(follower)
			} else if reply.Term == int64(rf.currentTerm) && rf.role == candidate /*我们需要确认此刻仍然是candidate，没有发生状态变化*/ {
				if reply.VoteGranted {
					approvedNum++
				}
				if approvedNum > n/2 { /*找到leader了，需要及时广播，防止选举超时*/
					rf.changeRole(leader)
					rf.initializeLeaderEasilyLostState() /*领导人（服务器）上的易失性状态(选举后已经重新初始化)*/
					rf.heartbeatBroadcast()
				}
			}
		}(i)
	}
}

// 选举后，领导人的易失状态需要重新初始化
func (rf *Raft) initializeLeaderEasilyLostState() {
	defer func() {
		logx.Debug(logx.DLeader, "S%d is leader, nextIndex:%v,matchIndex:%v", rf.me, rf.nextIndex, rf.matchIndex)
	}()
	
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.realLogLen()
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
	
	// 领导人的nextIndex和matchIndex是确定的
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.nextIndex[rf.me] = rf.lastLogIndex() + 1
}
