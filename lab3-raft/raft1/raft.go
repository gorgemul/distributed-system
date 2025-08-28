package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"fmt"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type logEntry struct {
	term int
	cmd  any
}

type state int

const (
	follower state = iota
	candidate
	leader
)

const (
	ElectionMinTimeMs  = 600
	HeartbeatMinTimeMs = 100
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// You data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTime       time.Time
	endHeartbeatTicker chan bool
	currentTerm        int
	state              state
	votedFor           *int
	// TODO: maybe change to map[int]logEntry
	logs        map[int]any
	commitIndex int
	lastApplied int
	// only exist in leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.broadcastHeartbeat()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	FollowerTerm int
	VoteGranted  bool
}

type AppendEntriesArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      map[int]any
	LeaderCommit int
}

type AppendEntriesReply struct {
	FollowerTerm int
	Success      bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintln1(who{follower, rf.me, rf.currentTerm}, RECV, who{candidate, args.CandidateId,args.CandidateTerm}, "RequestVote")
	reply.FollowerTerm = rf.currentTerm
	if rf.currentTerm > args.CandidateTerm {
		reply.VoteGranted = false
		return
	}
	if args.CandidateTerm > rf.currentTerm {
		if rf.state == leader {
			rf.endHeartbeatTicker <- true
			rf.state = follower
			rf.electionTime = time.Now()
		}
		rf.votedFor = nil
		rf.currentTerm = args.CandidateTerm
	}
	lastLogIndex, lastLogTerm := rf.latestLog()
	candidateLogUpToDate := log1MoreUpToDate(args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm)
	if (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && candidateLogUpToDate {
		DPrintln1(who{follower, rf.me, rf.currentTerm}, REPLY, who{candidate, args.CandidateId,args.CandidateTerm}, "RequestVote")
		rf.votedFor = &args.CandidateId
		reply.VoteGranted = true
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintln1(who{follower, rf.me, rf.currentTerm}, RECV, who{leader, args.LeaderId, args.LeaderTerm}, "AppendEntries")
	reply.FollowerTerm = rf.currentTerm
	if rf.currentTerm > args.LeaderTerm {
		reply.Success = false
		return
	}
	rf.electionTime = time.Now()
	rf.votedFor = nil
	if args.LeaderTerm >= rf.currentTerm {
		if rf.state == candidate {
			DPrintln2(who{candidate, rf.me, rf.currentTerm}, "step down from AppendEntries")
			rf.state = follower
			rf.votedFor = nil
		}
		if rf.state == leader && args.LeaderTerm > rf.currentTerm {
			DPrintln2(who{leader, rf.me, rf.currentTerm}, "step down from AppendEntries")
			rf.endHeartbeatTicker <- true
			rf.state = follower
		}
		rf.currentTerm = args.LeaderTerm
	}
	_, lastLogTerm := rf.latestLog()
	_, ok := rf.logs[args.PrevLogIndex]
	if lastLogTerm == args.PrevLogTerm && !ok {
		reply.Success = false
		return
	}
	if len(args.Entries) > 0 {
		for i := range rf.logs {
			followerEntry := rf.logs[i].(logEntry)
			leaderEntry := args.Entries[i].(logEntry)
			if followerEntry.term != leaderEntry.term {
				// TODO: delete follower entry and all follow it && Append any entries not already in the log
				panic("TODO")
			}
		}
	}
	// TODO: handle args.leaderCommit and rf.commitIndex
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		rf.mu.Lock()
		electionTimeout := time.Since(rf.electionTime) >= electionTime()
		firstElection := rf.state == follower && rf.votedFor == nil
		reElection := rf.state == candidate && (rf.votedFor != nil && *rf.votedFor == rf.me)
		rf.mu.Unlock()
		if electionTimeout && (firstElection || reElection) {
			rf.launchElection()
		}
	}
}

func (rf *Raft) launchElection() {
	var wg sync.WaitGroup
	var votes int64
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.state = candidate
	rf.votedFor = &rf.me
	atomic.AddInt64(&votes, 1)
	rf.mu.Unlock()
	DPrintln2(who{candidate, rf.me, rf.currentTerm}, "launch election")
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			rf.mu.Lock()
			if rf.state != candidate {
				rf.mu.Unlock()
				return
			}
			lastLogIndex, lastLogTerm := rf.latestLog()
			args := RequestVoteArgs{
				CandidateTerm: rf.currentTerm,
				CandidateId:   rf.me,
				LastLogIndex:  lastLogIndex,
				LastLogTerm:   lastLogTerm,
			}
			var reply RequestVoteReply
			rf.mu.Unlock()
			DPrintln1(who{candidate, rf.me, rf.currentTerm}, SEND, who{follower, peer, NO_TERM}, "RequestVote")
			done := make(chan bool)
			go func() {
				ok := rf.sendRequestVote(peer, &args, &reply)
				if ok {
					if reply.VoteGranted {
						atomic.AddInt64(&votes, 1)
					}
					rf.mu.Lock()
					if reply.FollowerTerm > rf.currentTerm {
						rf.currentTerm = reply.FollowerTerm
						rf.state = follower
						rf.votedFor = nil
						rf.electionTime = time.Now()
					}
					rf.mu.Unlock()
				}
				done <- true
			}()
			select {
			case <-done:
				return
			case <-time.After((ElectionMinTimeMs - 100) * time.Millisecond):
				return
			}
		}(i)
	}
	wg.Wait()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == candidate && votes >= rf.majority() {
		DPrintln2(who{candidate, rf.me, rf.currentTerm}, fmt.Sprintf("Getting %d votes, become leader", votes))
		rf.state = leader
		rf.votedFor = nil
		go rf.broadcastHeartbeatTicker()
	}
}

// caller must hold rf.mu
func (rf *Raft) latestLog() (index, term int) {
	keys := slices.Sorted(maps.Keys(rf.logs))
	if len(keys) == 0 {
		return
	}
	index = keys[len(keys)-1]
	term = rf.logs[index].(logEntry).term
	return
}

func (rf *Raft) majority() int64 {
	return int64((len(rf.peers) + 1) / 2)
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	isLeader := rf.state == leader
	rf.mu.Unlock()
	if !isLeader {
		return
	}
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				return
			}
			prevLogIndex, prevLogTerm := rf.latestLog()
			args := AppendEntriesArgs{
				LeaderTerm:   rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      make(map[int]any),
				LeaderCommit: rf.commitIndex,
			}
			var reply AppendEntriesReply
			rf.mu.Unlock()
			DPrintln1(who{leader, rf.me, rf.currentTerm}, SEND, who{follower, peer, NO_TERM}, "heartbeat")
			done := make(chan bool)
			go func() {
				ok := rf.sendAppendEntries(peer, &args, &reply)
				rf.mu.Lock()
				if ok && reply.FollowerTerm > rf.currentTerm {
					DPrintln2(who{leader, rf.me, rf.currentTerm}, "step down from heartbeat")
					rf.endHeartbeatTicker <- true
					rf.state = follower
					rf.electionTime = time.Now()
					rf.currentTerm = reply.FollowerTerm
				}
				rf.mu.Unlock()
				done<-true
			}()
			select {
			case <-done:
				return
			case <-time.After((HeartbeatMinTimeMs - 10) * time.Millisecond):
				return
			}
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) broadcastHeartbeatTicker() {
	for {
		select {
		case <-rf.endHeartbeatTicker:
			return
		default:
			rf.broadcastHeartbeat()
			time.Sleep(HeartbeatMinTimeMs * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.endHeartbeatTicker = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func log1MoreUpToDate(log1Index, log1Term, log2Index, log2Term int) bool {
	if log1Term < log2Term {
		return false
	}
	if log1Term > log2Term {
		return true
	}
	return log1Index >= log2Index
}

// 600 - 800 ms
func electionTime() time.Duration {
	return time.Duration(ElectionMinTimeMs+(rand.Int63()%200)) * time.Millisecond
}
