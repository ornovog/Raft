package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	heartbeatIntervalMS   = time.Millisecond * 200
	appendEntriesTimoutMS = 50 * heartbeatIntervalMS
	minElectionTimeoutMS  = 3 * heartbeatIntervalMS
	maxElectionTimeoutMS  = 5 * heartbeatIntervalMS
	intervals             = 10
	intervalMS            = (maxElectionTimeoutMS - minElectionTimeoutMS) / time.Duration(intervals)
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) toString() string {
	return fmt.Sprintf("me: %d state: %v term: %d log: %v votedFor: %d",
		rf.me, rf.state, rf.currentTerm, rf.log, rf.votedFor)
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		electionTimeout := randElectionTimeout()
		if waitForConditionWithTimeout(&rf.heartbeatCond, electionTimeout) {
			voteArgs := rf.beCandidateAndGetRequestVoteArgsAtomic()
			go rf.tryToWinElection(voteArgs)
		}
	}
}

func (rf *Raft) seenBiggerTermAtomic(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.beFollower()
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) getLastLogIndex() int {
	return int(len(rf.log) - 1) + rf.startingLogIndex
}

func (rf *Raft) getLastLogIndexAtomic() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLastLogIndex()
}

func (rf *Raft) getLogAtIndex(index int) LogEntry {
	return rf.log[index]
}

func (rf *Raft) getLogAtIndexAtomic(index int) LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLogAtIndex(index)
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) getTermAtIndexAtomic(index int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getTermAtIndex(index)
}

func (rf *Raft) getTermAtIndex(index int) int {
	if index >= 0 {
		return rf.log[index].Term
	}
	return 0
}

func (rf *Raft) getCurrentTermAtomic() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

//returns true if got timeout
func waitForConditionWithTimeout(cond *sync.Cond, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		cond.L.Lock()
		defer cond.L.Unlock()
		cond.Wait()
		close(done)
	}()
	select {
	case <-time.After(timeout):
		return true
	case <-done:
		return false
	}
}

func (rf *Raft) getLastLogTerm() int {
	var lastLogTerm int
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex >= 0 {
		lastLogTerm = rf.getLogAtIndex(lastLogIndex).Term
	}
	return lastLogTerm
}

func (rf *Raft) getTermAtomic() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getStateAtomic() ServerState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) setStateAtomic(state ServerState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) getCommitIndexAtomic() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) setCommitIndexAtomic(commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = commitIndex
}
