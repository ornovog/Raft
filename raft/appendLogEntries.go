package raft

import (
	"math"
	"sync"
	"sync/atomic"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntryMsg struct {
	IsHeartBeat  bool
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryMsgReply struct {
	Term    int
	Success bool
	Retry   bool
}

/* Leader side */
func (rf *Raft) waitForSendAppendEntries(term int) {
	for !rf.killed() && rf.getTermAtomic() == term {
		waitForConditionWithTimeout(&rf.sendAppendEntriesCond, appendEntriesTimoutMS)
		rf.sendAppendEntriesAndCommitLogs(term)
	}
}

func (rf *Raft) appendCommandToLog(command interface{}) {
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, logEntry)
	rf.persist()
}

func (rf *Raft) sendAppendEntriesAndCommitLogs(term int) {
	lastLogIndex := rf.getLastLogIndexAtomic()
	log := rf.getSliceToLogLock()
	var started, finished, agreed int32
	majorityOrFinished := *sync.NewCond(&sync.Mutex{})
	commitIndex := rf.getCommitIndexAtomic()

	for peer := range rf.peers {
		if peer != rf.me && lastLogIndex >= rf.getNextIndexAtomic(peer){
			started++
			go func(server int) {
				defer majorityOrFinished.Broadcast()
				defer atomic.AddInt32(&finished, 1)

				ni := rf.getNextIndexAtomic(server)
				appendEntryMsg := AppendEntryMsg{
					Term:         term,
					LeaderCommit: commitIndex,
					LeaderId:     rf.me,
					PrevLogIndex: ni - 1,
					PrevLogTerm:  rf.getTermAtIndexAtomic(ni - 1),
					Entries:      log[ni:],
				}

				reply := AppendEntryMsgReply{}
				rf.sendAppendEntry(server, &appendEntryMsg, &reply)
				for !reply.Success && reply.Retry {
					appendEntryMsg.Entries = log[appendEntryMsg.PrevLogIndex:]
					appendEntryMsg.PrevLogIndex--
					appendEntryMsg.PrevLogTerm = rf.getTermAtIndexAtomic(appendEntryMsg.PrevLogIndex)
					reply.Retry = false
					rf.sendAppendEntry(server, &appendEntryMsg, &reply)
					appendEntryMsg.PrevLogIndex /= 2
				}

				if !reply.Success {
					return
				}
				rf.setNextIndexAtomic(server, lastLogIndex+1)
				rf.setMathIndexForFollowerLock(server, lastLogIndex)
				atomic.AddInt32(&agreed, 1)
			}(peer)
		} else {
			atomic.AddInt32(&agreed, 1)
		}
	}

	rf.waitForMajorityToAgreeOrAllToFinish(&majorityOrFinished, &started, &finished, &agreed)

	rf.commitLogs()
}

func (rf *Raft) sendAppendEntry(server int, appendEntryMsg *AppendEntryMsg, reply *AppendEntryMsgReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", appendEntryMsg, reply)
	if ok && reply.Term > rf.getTermAtomic() {
		rf.seenBiggerTermAtomic(reply.Term)
	}
	return ok
}

func (rf *Raft) getSliceToLogLock() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

func (rf *Raft) setMathIndexForFollowerLock(server, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[server] = index
}

func (rf *Raft) waitForMajorityToAgreeOrAllToFinish(majorityOrFinished *sync.Cond, started, finished, agreed *int32) {
	finishedVal := atomic.LoadInt32(finished)
	agreedVal := atomic.LoadInt32(agreed)
	majority := int32(rf.majority())
	majorityOrFinished.L.Lock()
	for finishedVal < *started && agreedVal < majority {
		majorityOrFinished.Wait()
		finishedVal = atomic.LoadInt32(finished)
		agreedVal = atomic.LoadInt32(agreed)
	}
	majorityOrFinished.L.Unlock()
}

func (rf *Raft) commitLogs() {
	nextCommitIndex := rf.getNextCommitIndexForLeader()
	if nextCommitIndex > rf.commitIndex {
		rf.applyCommittedMessagesWithSliceToLog(nextCommitIndex)
		rf.setCommitIndexAtomic(nextCommitIndex)
	}
}

func (rf *Raft) getNextCommitIndexForLeader() int {
	majority := rf.majority()
	nextCommitIndex := rf.getLastLogIndexAtomic()
	currentTerm := rf.getCurrentTermAtomic()
	for ; nextCommitIndex > rf.getCommitIndexAtomic()  && rf.getLogAtIndexAtomic(nextCommitIndex).Term == currentTerm; nextCommitIndex-- {
		followersCommitted := 0
		for follower := range rf.matchIndex {
			if rf.getMatchIndexForFollowerLock(follower) >= nextCommitIndex || follower == rf.me {
				followersCommitted++
			}
		}

		if followersCommitted >= majority {
			return nextCommitIndex
		}
	}

	return rf.commitIndex
}

func (rf *Raft) getMatchIndexForFollowerLock(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[server]
}

func (rf *Raft) setNextIndexAtomic(server, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = index
}

func (rf *Raft) applyCommittedMessages(nextCommitIndex int) {
	for i, logEntry := range rf.log[rf.commitIndex+1 : nextCommitIndex+1] {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: int(rf.commitIndex) + i + 1,
		}
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) applyCommittedMessagesWithSliceToLog(nextCommitIndex int) {
	log := rf.getSliceToLogLock()
	for i, logEntry := range log[rf.commitIndex+1 : nextCommitIndex+1] {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: int(rf.commitIndex) + i + 1,
		}
		rf.applyCh <- applyMsg
	}
}

/* Follower side */

func (rf *Raft) AppendEntry(args *AppendEntryMsg, reply *AppendEntryMsgReply) {
	reply.Term = rf.getTermAtomic()
	if reply.Term > args.Term {
		return
	} else if reply.Term < args.Term {
		rf.seenBiggerTermAtomic(args.Term)
	}

	rf.heartbeatCond.Broadcast()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !args.IsHeartBeat {
		if !rf.hasLeadersPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
			reply.Retry = true
			return
		}
		reply.Success = true
		rf.appendNewLogs(args.PrevLogIndex, args.Entries)
	}

	if rf.commitIndex < args.LeaderCommit && rf.getLastLogTerm() == args.Term {
		nextCommitIndex := rf.getNextCommitIndexForFollower(args.LeaderCommit)
		rf.applyCommittedMessages(nextCommitIndex)
		rf.commitIndex = nextCommitIndex
	}
}

func (rf *Raft) hasLeadersPrevLog(leaderPrevLogIndex, leaderPrevLogTerm int) bool {
	lastLogIndex := rf.getLastLogIndex()
	return lastLogIndex >= leaderPrevLogIndex &&
		(leaderPrevLogIndex == -1 || rf.getLogAtIndex(leaderPrevLogIndex).Term == leaderPrevLogTerm)
}

func (rf *Raft) appendNewLogs(leaderPrevLogIndex int, entries []LogEntry) {
	newLogIndex := leaderPrevLogIndex + 1
	i := 0
	for ; newLogIndex+int(i) < rf.getLastLogIndex()+1 && i < len(entries); i++ {
		if rf.getTermAtIndex(newLogIndex+int(i)) != entries[i].Term {
			break
		}
	}

	if i == len(entries) {
		return
	}

	indexToCutFrom := newLogIndex + int(i)
	rf.log = append(rf.log[:indexToCutFrom], entries[i:]...)
	//fmt.Printf("server %d logs %d \n", rf.me, entries[i:])
	//fmt.Printf("server %d logs %v \n", rf.me, rf.log)
	rf.persist()
}

func (rf *Raft) getNextCommitIndexForFollower(leaderCommit int) int {
	currentLogIndex := rf.getLastLogIndex()
	return int(math.Min(float64(leaderCommit), float64(currentLogIndex)))

}