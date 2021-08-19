package raft

import "log"

// Status of log entries.
type LogEntry struct {
	term string // command for state machine.
	termReceived int // term when this log is received.
}

// Debugging
const Debug = 0

func GetLastLogEntryAndIndex(logs []LogEntry) (log LogEntry, n int){
	if len(logs) == 0 {
		return LogEntry{}, 0
	}
	size := len(logs)
	return logs[size - 1], size
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
