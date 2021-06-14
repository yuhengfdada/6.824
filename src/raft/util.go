package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Timer
func (rf *Raft) resetElectionTimer() {
	rf.timer.Stop()
	rf.timer.Reset(time.Duration(rf.timeout+rand.Intn(300)) * time.Millisecond)
}
