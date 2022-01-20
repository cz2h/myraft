package raft

import "log"

// Status of log entries.
type LogEntry struct {
	Term interface{} // command for state machine.
	TermReceived int // Term when this log is received.
}

// Debugging
const Debug = 0

func (entry *LogEntry) Equals(otherEntry LogEntry) bool {
	return entry.Term == otherEntry.Term && entry.TermReceived == otherEntry.TermReceived
}

func GetLastLogEntryAndIndex(logs []LogEntry) (log LogEntry, n int){
	if len(logs) == 0 {
		return LogEntry{}, 0
	}
	size := len(logs)
	return logs[size - 1], size
}


func GetLastIndex(logs []LogEntry) (n int) {
	if len(logs) == 0 {
		return -1
	}
	return len(logs) - 1
}

func LogsContainsEntry(logs []LogEntry, targetIndex int, targetTerm interface{}) bool {
	if len(logs) <= targetIndex {
		return false	
	}

	for _, log := range logs {
		if log.Term == targetTerm && log.TermReceived == targetIndex {
			return true
		}
	}

	return false
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func SetAllValueInArrayTo(myArray []int, value int) {
	for i := 0; i < len(myArray); i ++ {
		myArray[i] = value
	}
}