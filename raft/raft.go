package raft

import (
	"Raft/labrpc"
	"sync"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	//Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	startingLogIndex int

	//Volatile state on all servers
	commitIndex int
	lastApplied int
	state       ServerState

	//Volatile state on leaders
	nextIndex         []int
	matchIndex        []int
	lastIncludedIndex int
	lastIncludedTerm  int

	heartbeatCond         sync.Cond
	sendAppendEntriesCond sync.Cond
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.heartbeatCond = *sync.NewCond(&sync.Mutex{})
	rf.sendAppendEntriesCond = *sync.NewCond(&sync.Mutex{})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = -1
	rf.votedFor = -1
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()

	return rf
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool
	term = int(rf.currentTerm)
	isLeader = rf.state == Leader
	return term, isLeader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
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

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	/*if rf.getCommitIndexAtomic() <= lastIncludedTerm && rf.lastIncludedTerm <= lastIncludedIndex {
		return true
	}*/
	return  false
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) getNextIndexAtomic(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[server]
}





