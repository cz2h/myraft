package raft

import (
	"fmt"
)

func (rf *Raft) DeleteConflictEntries(leaderEntries []LogEntry) (nextAppendIndex int) {
	i := 0
	for i < len(rf.logs) && i < len(leaderEntries) && rf.logs[i].Equals(leaderEntries[i]) {
		i ++
	}

	if i < len(rf.logs) - 1 {
		rf.logs = rf.logs[:i]
	}

	return i
}

func (rf *Raft) AppendNewEntriesFromIndex(leaderEntries []LogEntry, index int) int {
	if len(leaderEntries) == len(rf.logs) || index > len(leaderEntries) - 1{
		return -1
	}

	rf.logs = append(rf.logs[ : len(rf.logs)], leaderEntries[index:]...)
	return GetLastIndex(rf.logs)
}

func (rf *Raft) GetNextCommitIndex() int {
	countMap := make(map[int]int)
	for _, i := range rf.matchIndex {
		_, contains := countMap[i]
		if ! contains {
			countMap[i] = 1
		} else {
			countMap[i] += 1
		}
	}

	for Ncandidate, count := range countMap {
		if count >= len(rf.matchIndex) / 2 {
			fmt.Println(Ncandidate , " rf.commitI ", rf.commitIndex,  " TermReceived ", rf.logs[Ncandidate].TermReceived, " currentTerm ", rf.currentTerm)
			if Ncandidate > rf.commitIndex && rf.logs[Ncandidate].TermReceived == rf.currentTerm {
				return Ncandidate
			}
		}
	}	

	return -1
}