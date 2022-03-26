package raft

import (
	"fmt"
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	Term           int
	CandidateIndex int
	LastLogIndex   int
	LastLogTerm    int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type ServerState int64

const (
	Follower ServerState = iota
	Leader
	Candidate
)

func (state ServerState) String() string {
	if state == Follower {
		return "Follower"
	} else if state == Candidate {
		return "Candidate"
	} else {
		return "Leader"
	}
}

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
		CandidateIndex: rf.me,
	}
	rf.mu.Unlock()
	return args
}

func (rf *Raft) beCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
}

func (rf *Raft) beLeader(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm {
		return
	}
	rf.state = Leader
	lastLogIndex := rf.getLastLogIndex() + 1
	for server := range rf.peers {
		rf.nextIndex[server] = lastLogIndex
		rf.matchIndex[server] = -1
	}
	go rf.sendHeartbeats(term)
	go rf.waitForSendAppendEntries(term)
}

func (rf *Raft) beFollower() {
	rf.state = Follower
}

func (rf *Raft) tryToWinElection(args RequestVoteArgs) { // start election process on this 'timer' args
	if rf.startElections(args) {
		rf.beLeader(args.Term) // won election for a specific term so be a leader for that specific term
	}
}

func (rf *Raft) startElections(args RequestVoteArgs) bool {
	if rf.getStateAtomic() != Candidate {
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

	return rf.getStateAtomic() == Candidate && voted >= majority
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok && reply.Term > rf.getTermAtomic() {
		rf.seenBiggerTermAtomic(reply.Term)
	}
	return ok
}

func (rf *Raft) sendHeartbeats(term int) {
	for !rf.killed() && rf.getTermAtomic() == term {
		time.Sleep(heartbeatIntervalMS)
		//the actual send sendHeartbeat
		commitIndex := rf.getCommitIndexAtomic()
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


/*Electorate side*/

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.getTermAtomic() < args.Term {
		rf.seenBiggerTermAtomic(args.Term)
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

func (rf *Raft) isCandidateHasNewerLogEntry(candidateLastLogTerm, candidateLastLogIndex int) bool {
	lastLogTerm := rf.getLastLogTerm()
	return lastLogTerm < candidateLastLogTerm ||
		(lastLogTerm == candidateLastLogTerm && rf.getLastLogIndex() <= candidateLastLogIndex)
}

func (rf *Raft) didntVotedForOtherCandidate(candidateIndex int) bool {
	return rf.votedFor == -1 || rf.votedFor == candidateIndex
}

func (rf *Raft) grantVote(candidateIndex, candidateTerm int) {
	if rf.currentTerm != candidateTerm {
		panic(fmt.Sprintf("Own term %d differ from candidate term %d, cannot grant vote for the term the candidate wants",
			rf.currentTerm,
			candidateTerm))
	}
	rf.heartbeatCond.Broadcast()
	rf.votedFor = candidateIndex
	rf.persist()
}


