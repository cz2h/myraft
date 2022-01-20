package raft

import (
	"fmt"
)

func (rf *Raft) SendRequestVote(server int, args *RequestVoteRequest, reply *RequestVoteReply) bool {
	// Can consider it as invoking the corresponding Raft.RequestVote
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	if rf.role != Candidate {
		rf.mu.Unlock()
		return false
	}
	if reply.Term > rf.currentTerm {

		rf.SetTermTo(reply.Term)
		defer rf.ChangeToFollower()		
	} else if reply.Term < rf.currentTerm{
		// abandon this response
	} else {
		if reply.VoteGranted {
			rf.VoteCountIncrBy(1)
		}
	} 
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteRequest, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// reply false if term < currentTerm 
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm;
		return
	} else if args.Term  > rf.currentTerm {

		rf.SetTermTo(args.Term)
		rf.ChangeToFollower()
	} 
	rf.mu.Lock()
	// if votedFor is null or candidateId, and candidate's log is at least as 
	// up-to-date as receivers log, grant vote.
	if rf.commitIndex <= args.LastLogIndex && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) HeartBeat(args *AppendEntriesRequest, reply *AppendEntriesReply){
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		rf.raftTimer.SetHeartBeatTo(true)
		rf.SetTermTo(args.Term)
		defer rf.ChangeToFollower()
	} else if args.Term == rf.currentTerm {
		rf.raftTimer.SetHeartBeatTo(true)
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
}


func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	// Can consider it as invoking the corresponding Raft.RequestVote
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.SetTermTo(reply.Term)
		defer rf.ChangeToFollower()
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) StartAgreementWithPeer(server int, newEntryIndex int) {
	if rf.nextIndex[server] > newEntryIndex {
		return
	}
	
	fmt.Println(rf.nextIndex)
	fmt.Println(rf.logs)
	nextIndex := rf.nextIndex[server]

	args := &AppendEntriesRequest{
		Term : rf.currentTerm,
		LeaderId : rf.me,
		PrevLogTerm : nil,
		PrevLogIndex : nextIndex - 1,
		Entries : rf.logs[ : nextIndex + 1] ,
		LeaderCommit : rf.commitIndex,
	}

	if nextIndex - 1 >= 0 {
		args.PrevLogTerm = rf.logs[nextIndex - 1].Term
	}

	reply := &AppendEntriesReply{}

	
	rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !reply.Success && rf.role == Leader {

		nextIndex -= 1
		rf.nextIndex[server] = nextIndex
		if nextIndex - 1 < 0 {
			fmt.Println("nextIndex : %d decreases to  sth bad !!!\n", nextIndex)
			return
		}
		args.PrevLogTerm = rf.logs[nextIndex - 1].Term
		args.PrevLogIndex = nextIndex - 1
		args.Entries = rf.logs[ : nextIndex + 1]

		rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	if reply.Success {
		rf.nextIndex[server] += 1
		rf.matchIndex[server] = newEntryIndex	
		fmt.Println("Server ", server)
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply)  {
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return 
	}

	if args.PrevLogIndex >= 0  && ! LogsContainsEntry(rf.logs, args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		return 
	}

	nextAppendIndex := rf.DeleteConflictEntries(args.Entries)
	indexOfLastNewEntry := rf.AppendNewEntriesFromIndex(args.Entries, nextAppendIndex)

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > indexOfLastNewEntry {
			rf.commitIndex = indexOfLastNewEntry
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	fmt.Println("Server " , rf.me , " logs: ", rf.logs, " commitI:", rf.commitIndex)
	return 
}