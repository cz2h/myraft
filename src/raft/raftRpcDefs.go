package raft

// Definitions of rpc requests
type RequestVoteArgs struct {
	Term int // candidate's term
	CandidateId int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry.
	LastLogTerm string // term of candidate's last log entry.
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term int // currentTerm, for candidate to update itself.
	VoteGranted bool // true means candiate received vote.
}