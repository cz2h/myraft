package raft

import (
	"log"
)

func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// Can consider it as invoking the corresponding Raft.RequestVote
	// log.Printf("%d send request vote to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	if rf.role != Candidate {
		log.Printf("%d receive outdated request vote reply but %d is now %d\n", rf.me, rf.me, rf.role)
		rf.mu.Unlock()
		return false
	}
	if reply.Term > rf.currentTerm {
		log.Printf("%d with term %d change from candidate to follower on reciving higher term %d from %d after receving rquest vote response\n",
			 rf.me,
			 rf.currentTerm,
			 reply.Term,
			 server)
		rf.SetTermTo(reply.Term)
		defer rf.ChangeToFollower()		
	} else if reply.Term < rf.currentTerm{
		// abandon this response
	} else {
		if reply.VoteGranted {
			log.Printf("%d vote for %d, current vote count is %d, term is %d\n", server, rf.me, rf.voteCounts, rf.currentTerm)
			rf.VoteCountIncrBy(1)
		}
	} 
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// reply false if term < currentTerm 
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm;
		return
	} else if args.Term  > rf.currentTerm {
		log.Printf("%d with term %d change to follower on reciving higher term %d from %d on reciving request vote request\n",
			rf.me,
			rf.currentTerm,
			args.Term,
			args.CandidateId)
		rf.SetTermTo(args.Term)
		rf.ChangeToFollower()
	} 
	rf.mu.Lock()
	// if votedFor is null or candidateId, and candidate's log is at least as 
	// up-to-date as receivers log, grant vote.
	if rf.commitIndex <= args.LastLogIndex && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		log.Printf("%d grant vote for %d with votedFor%d\n", rf.me, args.CandidateId, rf.votedFor)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) HeartBeat(args *HeartBeatArgs, reply *HeartBeatRes){
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		log.Printf("%d change to follower due to heart beat of term %d not uptodate with term %d\n", rf.me, args.Term, rf.currentTerm)
		rf.raftTimer.SetHeartBeatTo(true)
		rf.SetTermTo(args.Term)
		defer rf.ChangeToFollower()
	} else if args.Term == rf.currentTerm {
		rf.raftTimer.SetHeartBeatTo(true)
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
}


func (rf *Raft) sendHeartBeat(server int, args *HeartBeatArgs, reply *HeartBeatRes) bool {
	// Can consider it as invoking the corresponding Raft.RequestVote
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		log.Printf("%d with term %d change from leader to follower on reciving higher term %d from on recving heartbeat response\n", rf.me, rf.currentTerm, reply.Term)
		rf.SetTermTo(reply.Term)
		defer rf.ChangeToFollower()
	}
	rf.mu.Unlock()
	return ok
}