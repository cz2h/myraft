package raft 

import (
	"math/rand"
    "sync"
	"time"
)

type RaftTime struct {
	mu        sync.Mutex
	heartBeatWaitSeconds int
	requestVoteRandMax int

	hasReceivedHeartBeat bool
}

func (rt *RaftTime) WaitHeartBeat() {
	time.Sleep(time.Millisecond * time.Duration(rt.heartBeatWaitSeconds))
}

func (rt *RaftTime) WaitRandomRequestVote() {
	time.Sleep(time.Duration(rand.Intn(rt.requestVoteRandMax)))
}

func (rt *RaftTime) HasReceivedHeartBeat() bool {
	return rt.hasReceivedHeartBeat
}

func (rt *RaftTime) SetHeartBeatTo(value bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.hasReceivedHeartBeat = value
}

func (rt *RaftTime) SleepFor(millsec int) {
	time.Sleep(time.Millisecond * time.Duration(millsec))
}

func NewRaftTime(hbWaitSec int, electionRandMax int, hbWaitMax int) *RaftTime{
	return & RaftTime{
		heartBeatWaitSeconds: hbWaitSec,
		requestVoteRandMax: electionRandMax,
		hasReceivedHeartBeat: false}
}