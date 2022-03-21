package Raft

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

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"Raft/labgob"
	"Raft/labrpc"
)

var (
	heartbeatIntervalMS   = time.Millisecond * 200
	appendEntriesTimoutMS = 10 * heartbeatIntervalMS
	minElectionTimeoutMS  = 3 * heartbeatIntervalMS
	maxElectionTimeoutMS  = 5 * heartbeatIntervalMS
	intervals             = 10
	intervalMS            = (maxElectionTimeoutMS - minElectionTimeoutMS) / time.Duration(intervals)
)

type ServerState int64

const (
	Follower ServerState = iota
	Leader
	Candidate
)

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term           int32
	CandidateIndex int32
	LastLogIndex   int32
	LastLogTerm    int32
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type LogEntry struct {
	// Your data here (2A, 2B).
	Term    int32
	Command interface{}
}

type AppendEntryMsg struct {
	IsHeartBeat  bool
	Term         int32
	LeaderId     int
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int32
}

type AppendEntryMsgReply struct {
	Term    int32
	Success bool
	Retry   bool
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	//Persistent state on all servers
	currentTerm int32
	votedFor    int32
	log         []LogEntry

	//Volatile state on all servers
	commitIndex int32
	lastApplied int32
	state       ServerState

	//Volatile state on leaders
	nextIndex  []int32
	matchIndex []int32

	heartbeatCond         sync.Cond
	sendAppendEntriesCond sync.Cond
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (state ServerState) String() string {
	if state == Follower {
		return "Follower"
	} else if state == Candidate {
		return "Candidate"
	} else {
		return "Leader"
	}
}

func (rf *Raft) toString() string {
	return fmt.Sprintf("me: %d state: %v term: %d log: %v votedFor: %d",
		rf.me, rf.state, rf.currentTerm, rf.log, rf.votedFor)
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.currentTerm)
	isleader = rf.state == Leader
	return term, isleader
}



/*---Initializations stuff---*/

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeatCond = *sync.NewCond(&sync.Mutex{})
	rf.sendAppendEntriesCond = *sync.NewCond(&sync.Mutex{})
	rf.nextIndex = make([]int32, len(peers))
	rf.matchIndex = make([]int32, len(peers))
	rf.commitIndex = -1
	rf.votedFor = -1
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()

	return rf
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



/*---Election stuff---*/

/*Candidate side*/

func randElectionTimeout() time.Duration {
	timeoutMS := time.Duration(rand.Intn(intervals+1))*intervalMS + minElectionTimeoutMS
	return timeoutMS
}

func (rf *Raft) beCandidateAndGetRequestVoteArgsAtomic() RequestVoteArgs {
	rf.mu.Lock()
	rf.beCandidate()
	args := RequestVoteArgs{
		Term:           rf.currentTerm,
		LastLogIndex:   rf.getLastLogIndex(),
		LastLogTerm:    rf.getLastLogTerm(),
		CandidateIndex: int32(rf.me),
	}
	rf.mu.Unlock()
	return args
}

func (rf *Raft) beCandidate() {
	rf.currentTerm++
	rf.votedFor = int32(rf.me)
	rf.state = Candidate
	rf.persist()
}

func (rf *Raft) beLeader(term int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm {
		return
	}
	rf.state = Leader
	lastLogIndex := rf.getLastLogIndex() + 1
	for server := range rf.peers {
		atomic.StoreInt32(&rf.nextIndex[server], lastLogIndex)
		rf.matchIndex[server] = -1
	}
	go rf.sendHeartbeats(term)
	go rf.waitForSendAppendEntries(term)
}

func (rf *Raft) beFollower() {
	rf.state = Follower
}

func(rf *Raft) tryToWinElection(args RequestVoteArgs) { // start election process on this 'timer' args
	if rf.startElections(args) {
		rf.beLeader(args.Term) // won election for a specific term so be a leader for that specific term
	}
}

func (rf *Raft) startElections(args RequestVoteArgs) bool {
	if rf.getStateLock() != Candidate {
		return false
	}

	voted, finished := 1, 1
	voteCh := make(chan bool, len(rf.peers)-1)
	// try to get votes
	for server := range rf.peers {
		if server != rf.me {
			go func(s int) {
				var reply RequestVoteReply
				rf.sendRequestVote(s, &args, &reply)
				voteCh <- reply.VoteGranted
			}(server)
		}
	}

	majority := rf.majority()
	var voteGranted bool
	readVoteGranted := func() bool {
		voteGranted = <-voteCh
		return true
	}

	for voted < majority && finished < len(rf.peers) && readVoteGranted() {
		finished++
		if voteGranted {
			voted++
		}
	}

	return rf.getStateLock() == Candidate && voted >= majority
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok && reply.Term > rf.getTermLock() {
		rf.seenBiggerTermLock(reply.Term)
	}
	return ok
}

func (rf *Raft) sendHeartbeats(term int32) {
	for !rf.killed() && rf.getTermLock() == term {
		time.Sleep(heartbeatIntervalMS)
		//the actual send sendHeartbeat
		commitIndex := rf.getCommitIndexLock()
		for server := range rf.peers {
			if server != rf.me {
				go func(s int) {
					heartbeatMsg := &AppendEntryMsg{
						IsHeartBeat:  true,
						Term:         term,
						LeaderCommit: commitIndex,
					}
					reply := &AppendEntryMsgReply{}
					rf.sendAppendEntry(s, heartbeatMsg, reply)
				}(server)
			} else {
				rf.heartbeatCond.Broadcast()
			}
		}
	}
}

func (rf *Raft) waitForSendAppendEntries(term int32) {
	for !rf.killed() && rf.getTermLock() == term {
		waitForConditionWithTimeout(&rf.sendAppendEntriesCond, appendEntriesTimoutMS)
		rf.sendAppendEntriesAndCommitLogs(term)
	}
}

/*Electorate side*/

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.getTermLock() < args.Term {
		rf.seenBiggerTermLock(args.Term)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = rf.shouldVoteForCandidate(args)

	if reply.VoteGranted {
		rf.grantVote(args.CandidateIndex, args.Term)
	}
}

func (rf *Raft) shouldVoteForCandidate(args *RequestVoteArgs) bool {
	if rf.currentTerm < args.Term {
		return rf.isCandidateHasNewerLogEntry(args.LastLogTerm, args.LastLogIndex)
	} else if rf.currentTerm == args.Term {
		return rf.didntVotedForOtherCandidate(args.CandidateIndex) &&
			rf.isCandidateHasNewerLogEntry(args.LastLogTerm, args.LastLogIndex)
	} else { // rf.currentTerm > args.Term
		return false
	}
}

func (rf *Raft) isCandidateHasNewerLogEntry(candidateLastLogTerm, candidateLastLogIndex int32) bool {
	lastLogTerm := rf.getLastLogTerm()
	return lastLogTerm < candidateLastLogTerm ||
		(lastLogTerm == candidateLastLogTerm && rf.getLastLogIndex() <= candidateLastLogIndex)
}

func (rf *Raft) didntVotedForOtherCandidate(candidateIndex int32) bool {
	return rf.votedFor == -1 || rf.votedFor == candidateIndex
}

func (rf *Raft) grantVote(candidateIndex, candidateTerm int32) {
	if rf.currentTerm != candidateTerm {
		panic(fmt.Sprintf("Own term %d differ from candidate term %d, cannot grant vote for the term the candidate wants",
			rf.currentTerm,
			candidateTerm))
	}
	rf.heartbeatCond.Broadcast()
	rf.votedFor = candidateIndex
	rf.persist()
}


/*---Backup stuff---*/
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int32
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Println("error in readPersist")
	} else if currentTerm != 0 {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}



/*---Snapshot stuff---*/

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}



/* Handle commands stuff */

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	rf.appendCommandToLog(command)
	newIndex := int(rf.getLastLogIndex())
	term := int(rf.currentTerm)
	rf.sendAppendEntriesCond.Broadcast()

	return newIndex, term, true
}

func (rf *Raft) appendCommandToLog(command interface{}) {
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, logEntry)
	rf.persist()
}

func (rf *Raft) sendAppendEntriesAndCommitLogs(term int32) {
	lastLogIndex := rf.getLastLogIndexLock()
	log := rf.getSliceToLogLock()
	var started, finished, agreed int32
	majorityOrFinished := *sync.NewCond(&sync.Mutex{})
	commitIndex := rf.getCommitIndexLock()

	for peer := range rf.peers {
		if peer != rf.me && lastLogIndex >= atomic.LoadInt32(&rf.nextIndex[peer]) {
			started++
			go func(server int) {
				defer majorityOrFinished.Broadcast()
				defer atomic.AddInt32(&finished, 1)

				ni := atomic.LoadInt32(&rf.nextIndex[server])
				appendEntryMsg := AppendEntryMsg{
					Term:         term,
					LeaderCommit: commitIndex,
					LeaderId:     rf.me,
					PrevLogIndex: ni - 1,
					PrevLogTerm:  rf.getTermAtIndexLock(ni - 1),
					Entries:      log[ni:],
				}

				reply := AppendEntryMsgReply{}
				rf.sendAppendEntry(server, &appendEntryMsg, &reply)
				for !reply.Success && reply.Retry {
					logEntry := log[appendEntryMsg.PrevLogIndex]
					appendEntryMsg.Entries = append([]LogEntry{logEntry}, appendEntryMsg.Entries...)
					appendEntryMsg.PrevLogIndex--
					appendEntryMsg.PrevLogTerm = rf.getTermAtIndexLock(appendEntryMsg.PrevLogIndex)
					reply.Retry = false
					rf.sendAppendEntry(server, &appendEntryMsg, &reply)
				}

				if !reply.Success {
					return
				}
				atomic.StoreInt32(&rf.nextIndex[server], lastLogIndex+1)
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
	if ok && reply.Term > rf.getTermLock() {
		rf.seenBiggerTermLock(reply.Term)
	}
	return ok
}

func (rf *Raft) getSliceToLogLock() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

func (rf *Raft) setMathIndexForFollowerLock(server int, index int32) {
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
		rf.applyCommittedMessages(nextCommitIndex)
		rf.setCommitIndexLock(nextCommitIndex)
	}
}

func (rf *Raft) getNextCommitIndexForLeader() int32 {
	majority := rf.majority()
	nextCommitIndex := rf.getLastLogIndexLock()
	currentTerm := rf.getCurrentTermLock()
	for ; nextCommitIndex > atomic.LoadInt32(&rf.commitIndex) && rf.getLogAtIndex(nextCommitIndex).Term == currentTerm;
	nextCommitIndex-- {
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

func (rf *Raft) getMatchIndexForFollowerLock(server int) int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[server]
}

func (rf *Raft) applyCommittedMessages(nextCommitIndex int32) {
	for i, logEntry := range rf.log[rf.commitIndex+1 : nextCommitIndex+1] {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: int(rf.commitIndex) + i + 1,
		}
		rf.applyCh <- applyMsg
	}
}

// AppendEntry
// example AppendEntryMsg RPC handler.
//
func (rf *Raft) AppendEntry(args *AppendEntryMsg, reply *AppendEntryMsgReply) {
	reply.Term = rf.getTermLock()
	if reply.Term > args.Term {
		return
	} else if reply.Term < args.Term {
		rf.seenBiggerTermLock(args.Term)
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

	if atomic.LoadInt32(&rf.commitIndex) < args.LeaderCommit && rf.getLastLogTerm() == args.Term {
		nextCommitIndex := rf.getNextCommitIndexForFollower(args.LeaderCommit)
		rf.applyCommittedMessages(nextCommitIndex)
		atomic.StoreInt32(&rf.commitIndex, nextCommitIndex)
	}
}

func (rf *Raft) hasLeadersPrevLog(leaderPrevLogIndex, leaderPrevLogTerm int32) bool {
	lastLogIndex := rf.getLastLogIndex()
	return lastLogIndex >= leaderPrevLogIndex &&
		(leaderPrevLogIndex == -1 || rf.getLogAtIndex(leaderPrevLogIndex).Term == leaderPrevLogTerm)
}

func (rf *Raft) appendNewLogs(leaderPrevLogIndex int32, entries []LogEntry) {
	newLogIndex := leaderPrevLogIndex + 1
	i := 0
	for ; newLogIndex+int32(i) < rf.getLastLogIndex()+1 && i < len(entries); i++ {
		if rf.getTermAtIndex(newLogIndex+int32(i)) != entries[i].Term {
			break
		}
	}

	if i == len(entries) {
		return
	}

	indexToCutFrom := newLogIndex + int32(i)
	rf.log = append(rf.log[:indexToCutFrom], entries[i:]...)
	rf.persist()
}

func (rf *Raft) getNextCommitIndexForFollower(leaderCommit int32) int32 {
	currentLogIndex := rf.getLastLogIndex()
	return int32(math.Min(float64(leaderCommit), float64(currentLogIndex)))

}



/*---General---*/
func (rf *Raft) seenBiggerTermLock(term int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.beFollower()
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) getLastLogIndex() int32 {
	return int32(len(rf.log) - 1)
	//return rf.nextIndex[rf.me] - 1
}

func (rf *Raft) getLastLogIndexLock() int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLastLogIndex()
}

func (rf *Raft) getLogAtIndex(index int32) LogEntry {
	return rf.log[index]
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) getTermAtIndexLock(index int32) int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getTermAtIndex(index)
}

func (rf *Raft) getTermAtIndex(index int32) int32 {
	if index >= 0 {
		return rf.log[index].Term
	}
	return 0
}

func (rf *Raft) getCurrentTermLock() int32 {
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

func (rf *Raft) getLastLogTerm() int32 {
	var lastLogTerm int32
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex >= 0 {
		lastLogTerm = rf.getLogAtIndex(lastLogIndex).Term
	}
	return lastLogTerm
}

func (rf *Raft) getTermLock() int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getStateLock() ServerState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) setStateLock(state ServerState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) getCommitIndexLock() int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) setCommitIndexLock(commitIndex int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = commitIndex
}






