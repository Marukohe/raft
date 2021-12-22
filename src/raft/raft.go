package raft

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
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

type RFState int

const (
	Follower RFState = iota
	Candidate
	Leader
)

const ElectionTimeOut = 150 * time.Millisecond
const HeartBeatTimeInterval = 50 * time.Millisecond

func (rf *Raft) getRandElectionTimeout() time.Duration {
	return ElectionTimeOut + (time.Duration(rand.Int63()) % ElectionTimeOut)
}

const DEBUG = 0

func (rf *Raft) d(format string, args ...interface{}) {
	if DEBUG > 0 {
		format = fmt.Sprintf("[%d] ", rf.me) + format
		log.Printf(format, args...)
	}
}

func IntMin(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Log struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyChan chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state RFState

	// Persistent state
	currentTerm int
	votedFor    int
	logs        []Log

	// Volatile state
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// Timers
	electionTimer *time.Timer
	hearBeatTimer *time.Ticker
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	_ = d.Decode(&rf.currentTerm)
	_ = d.Decode(&rf.votedFor)
	_ = d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rf.d("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, rf.currentTerm, rf.votedFor)

	lastIndex, lastTerm := rf.getLogIndexAndTerm()
	//rf.d("VoteFor %d with lastIndex: %d, lastTerm :%d, args.Term: %d, args.LastIndex: %d",
	//	args.CandidateId, lastIndex, lastTerm, args.LastLogTerm)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if rf.currentTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.electionTimer.Reset(rf.getRandElectionTimeout())
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.d("Received AppendEntries args: %+v in term: %d", args, rf.currentTerm)
	rf.d("Now rf.logs are : %+v", rf.logs)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.state != Follower {
			rf.becomeFollower(args.Term)
		}
		rf.electionTimer.Reset(rf.getRandElectionTimeout())
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rf.logs) && args.PrevLogTerm == rf.logs[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			entriesIndex := 0

			for {
				if logInsertIndex >= len(rf.logs) || entriesIndex >= len(args.Entries) {
					break
				}
				if rf.logs[logInsertIndex].Term != args.Entries[entriesIndex].Term {
					break
				}
				logInsertIndex++
				entriesIndex++
			}

			if entriesIndex < len(args.Entries) {
				rf.logs = append(rf.logs[:logInsertIndex], args.Entries[entriesIndex:]...)
			}

			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = IntMin(args.LeaderCommit, len(rf.logs)-1)
				go rf.Apply()
			}
		}
	}
	rf.persist()
	reply.Term = rf.currentTerm
}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.d("SendAppendEntries : %+v", args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := false

	if rf.state != Leader {
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, Log{command, rf.currentTerm})
	rf.nextIndex[rf.me] = len(rf.logs)
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	index = len(rf.logs)
	term = rf.currentTerm
	isLeader = true
	rf.persist()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	//rf.d("%+v", rf.nextIndex)
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimer = time.NewTimer(rf.getRandElectionTimeout())
	rf.hearBeatTimer = time.NewTicker(HeartBeatTimeInterval)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	rf.d("Make Raft")
	go func(rf *Raft) {
		for {
			select {
			case <-rf.electionTimer.C:
				go rf.startElection()
			}
		}
	}(rf)

	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.d("Start Election")
	rf.becomeCandidate()
	savedCandidateTerm := rf.currentTerm
	voteCount := 0
	for peer := range rf.peers {
		go func(i int, rf *Raft) {
			rf.mu.Lock()
			lastIndex, lastTerm := rf.getLogIndexAndTerm()
			rf.mu.Unlock()
			args := RequestVoteArgs{
				savedCandidateTerm,
				rf.me,
				lastIndex,
				lastTerm,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Candidate {
					return
				}

				if reply.Term > savedCandidateTerm {
					rf.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCandidateTerm {
					if reply.VoteGranted {
						//rf.d("Grantee: %d", i)
						voteCount++
						if voteCount*2 >= len(rf.peers)+1 {
							rf.becomeLeader()
							return
						}
					}
				}
			}
		}(peer, rf)
	}
	rf.mu.Unlock()
}

func (rf *Raft) getLogIndexAndTerm() (int, int) {
	lastIndex := -1
	lastTerm := -1
	if len(rf.logs) > 0 {
		lastIndex = len(rf.logs) - 1
		lastTerm = rf.logs[lastIndex].Term
	}
	return lastIndex, lastTerm
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(id int) {
			rf.mu.Lock()
			nextIndex := rf.nextIndex[id]
			prevLogIndex := nextIndex - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = rf.logs[prevLogIndex].Term
			}
			entries := rf.logs[nextIndex:]
			args := AppendEntriesArgs{
				currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()
			ok := rf.SendAppendEntries(id, args, reply)

			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			if rf.state == Leader && currentTerm == rf.currentTerm {
				if reply.Success {
					rf.nextIndex[id] = nextIndex + len(entries)
					rf.matchIndex[id] = rf.nextIndex[id] - 1

					for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
						if rf.logs[N].Term != rf.currentTerm {
							continue
						}
						count := 0
						for peerId := range rf.peers {
							if rf.matchIndex[peerId] >= N {
								rf.d("Agree From : %d for N : %d in Term : %d", peerId, N, rf.currentTerm)
								count++
							}
						}
						if count*2 >= len(rf.peers)+1 {
							rf.commitIndex = N
							go rf.Apply()
							break
						}
					}
				} else {
					rf.nextIndex[id] = nextIndex - 1
				}
			}
		}(peer)
	}
}

func (rf *Raft) becomeLeader() {
	rf.d("Become Leader with term: %d", rf.currentTerm)
	rf.state = Leader
	for peerId := range rf.peers {
		rf.nextIndex[peerId] = len(rf.logs)
		rf.matchIndex[peerId] = -1
	}
	go func() {
		for {
			rf.sendHeartBeat()
			<-rf.hearBeatTimer.C
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()
}

func (rf *Raft) becomeCandidate() {
	rf.d("Become Candidate with term: %d", rf.currentTerm)
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.electionTimer.Reset(rf.getRandElectionTimeout())
}

func (rf *Raft) becomeFollower(term int) {
	rf.d("Become Follower: Current Term: %d, term : %d", rf.currentTerm, term)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionTimer.Reset(rf.getRandElectionTimeout())
}

func (rf *Raft) Apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{}
			applyMsg.Index = i + 1
			applyMsg.Command = rf.logs[i].Command
			rf.d("Raft Commit Index: %d, Command: %+v", applyMsg.Index, applyMsg.Command)
			rf.applyChan <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
	}
}
