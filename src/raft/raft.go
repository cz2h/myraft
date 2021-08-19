package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"

import "log"
import "math/rand"
import "time"
// import "bytes"
// import "../labgob"


// Role of raft instance
const (
	NotAssigned int = -1
	Follower int = 0
	Candidate int = 1
	Leader int = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Role of a raft server, can only be one of three.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role int // role of this raft server.
	currentTerm int // term used for proposing a election.
	votedFor int // candidateId that received vote in current term.
	logs []LogEntry // log entries, each entry contains command and term when it was received.
	raftTimer *RaftTime

	// volatile state on all servers.
	commitIndex int // index of highest log entry
	lastApplied int // index of highest log entry applied to state machine.

	// volatile state on leaders.


	// Variables for leader election.
	// vote counts.
	voteCounts int
	electionTimeoutMillSec int


}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm;
	var isleader bool = rf.role == Leader
	// Your code here (2A).
	if isleader {
		log.Printf("%d is leader with term %d!\n", rf.me, rf.currentTerm)
	} else {
		log.Printf("%d is now %d\n", rf.me, rf.role)
	}
	return term, isleader
}

func (rf *Raft) ChangeRoleTo(role int) {
	log.Printf("%d %d -> %d with term %d", rf.me, rf.role, role, rf.currentTerm)
	rf.role = role
}

func (rf *Raft) SetTermTo(term int) {
	rf.currentTerm = term
}

func (rf *Raft) VoteCountIncrBy(num int) {
	rf.voteCounts += num
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}



type HeartBeatArgs struct {
	Term int // term of the Leader
}

type HeartBeatRes struct {
	Term int // term from the response
	Success bool
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start thera
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := rf.commitIndex
	term := rf.currentTerm
	isLeader := rf.role == Leader

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Follower related functions
// State change for followers.
func (rf *Raft) ChangeToFollower() {
	if rf.role == Follower {
		return
	}
	rf.ChangeRoleTo(Follower)
	rf.votedFor = -1
	rf.raftTimer.SetHeartBeatTo(false)
	// Start a electionTimeout trigger.
	go rf.FollowerWaitTimeOut()
}

// Follower wait for timeout.
func (rf *Raft) FollowerWaitTimeOut() {
	rf.raftTimer.SetHeartBeatTo(false) // initialize.
	for rf.role == Follower{
		// sleep
		log.Printf("%d sleep for heartbeat \n", rf.me)
		rf.raftTimer.WaitHeartBeat()
		// if timeout and not granting vote to candidate
		// (if follower receives no communication over a period of time)
		rf.mu.Lock()
		if (! rf.raftTimer.HasReceivedHeartBeat()) && (rf.votedFor == -1 && rf.role == Follower){
			log.Printf("%d wants to timeout\n", rf.me)
			defer rf.ChangeToCandidate() 
			rf.mu.Unlock()
			break
		}
		// will not start an election on either or both
		// 1. receive heartbeat 
		// 2. has granted vote for someone.
		// Reset the timer.
		log.Printf("%d with role:%d heartbeat:%d votedfor %d\n", rf.me, rf.role, rf.raftTimer.HasReceivedHeartBeat(), rf.votedFor)
		
		rf.raftTimer.SetHeartBeatTo(false)
		rf.votedFor = -1
		rf.mu.Unlock()
		rf.raftTimer.SleepFor(1)
	}
	log.Printf("%d outside follower timeout\n", rf.me)
}

func (rf *Raft) ChangeToCandidate() {

	if rf.role == Candidate || rf.role == Leader{
		return
	}
	rf.ChangeRoleTo(Candidate)
	rf.votedFor = -1
	rf.voteCounts = 0
	rf.raftTimer.SetHeartBeatTo(false)

	go rf.StartElection()
}

func (rf *Raft) StartElection() {
	// While there is no leader, keep waiting for election
	for rf.role == Candidate && rf.voteCounts < len(rf.peers)/2 + 1 {
		rf.mu.Lock()
		// log.Printf("%d start election with term %d\n", rf.me, rf.currentTerm)
		rf.currentTerm += 1
		rf.voteCounts = 1
		rf.votedFor = rf.me
		_, lastIndex := GetLastLogEntryAndIndex(rf.logs)
		LastLogTerm := "WHO?"
		args :=  RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm: LastLogTerm,
		}

		reply := RequestVoteReply {
			Term : -1,
			VoteGranted : false,
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers) && rf.role == Candidate; i ++ {
			if i != rf.me {
				// should be multi-thread or it will be blocked?
				go rf.SendRequestVote(i, &args, &reply)
			}
		}
		// Wait for any response or someone with higher amount of 
		randTimeOutInMillSec := rf.electionTimeoutMillSec + rand.Intn(50)

		time.Sleep(time.Duration(randTimeOutInMillSec))
		rf.mu.Lock()
		if rf.voteCounts >= (len(rf.peers)/2 + 1) {
			break
		} else if rf.raftTimer.HasReceivedHeartBeat() {
			break
		}
		rf.mu.Unlock()
	}
	
	if rf.raftTimer.HasReceivedHeartBeat() {
		defer rf.ChangeToFollower()
		rf.mu.Unlock()

	} else if rf.voteCounts >= (len(rf.peers))/2 + 1 {
		defer rf.ChangeToLeader() // Change to leader only when no heart beat and recevie from majority.
		rf.mu.Unlock()
	}
}

func (rf *Raft) startHeartBeat(){
	log.Printf("%d Broadcast heartbeat with term: %d\n", rf.me, rf.currentTerm)
	for rf.role == Leader {
		for i  := 0; i < len(rf.peers); i ++ {
			if i != rf.me {
				args := HeartBeatArgs{}
				args.Term = rf.currentTerm
				res := HeartBeatRes{}
				go rf.sendHeartBeat(i, &args, &res)
			}
		}
	}
	log.Printf("%d is now %d instead of a leader\n", rf.me, rf.role)
}

func (rf *Raft) ChangeToLeader() {	

	if rf.role == Leader || rf.role == Follower{
		return
	}
	rf.ChangeRoleTo(Leader)
	rf.raftTimer.SetHeartBeatTo(false)
	go rf.startHeartBeat()
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = NotAssigned
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.raftTimer = NewRaftTime(300,200,500) // follower should receive heart beat within 80 millsec
	rf.electionTimeoutMillSec = 100
	rf.ChangeToFollower()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
