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
	"math/rand"
	"sync"
	"time"

	"lab2raft/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // Reserved for a later lab; ignore in Lab 2.
	Snapshot    []byte // Reserved for a later lab; ignore in Lab 2.
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type serverRole int

const (
	follower serverRole = iota
	candidate
	leader
)

const (
	heartbeatInterval  = 100 * time.Millisecond
	minElectionTimeout = 400 * time.Millisecond
	maxElectionTimeout = 800 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Protected by mu after initialization.
	role          serverRole
	currentTerm   int        // Latest term this peer has seen; starts at 0.
	votedFor      int        // Peer ID voted for in currentTerm; -1 means no vote.
	log           []LogEntry // Index 0 is a dummy entry; real entries start at 1.
	votesReceived int        // Votes in the current election, including this peer's vote.

	// Starting an election or granting a vote restarts the election clock.
	electionResetTime time.Time
	electionTimeout   time.Duration
	lastHeartbeat     time.Time
	dead              bool
	stopCh            chan struct{}
	tickerDone        chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == leader && !rf.dead
}

// The caller must hold rf.mu, unless the peer has not been published yet.
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionResetTime = time.Now()
	rf.electionTimeout = minElectionTimeout +
		time.Duration(rand.Int63n(int64(maxElectionTimeout-minElectionTimeout)))
}

func (rf *Raft) becomeLeaderLocked() {
	rf.role = leader
	rf.lastHeartbeat = time.Time{} // Send the first heartbeat immediately.
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// adoptHigherTermLocked discards election state from an older term.
// The caller must hold rf.mu.
func (rf *Raft) adoptHigherTermLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.role = follower
		rf.votedFor = -1
		rf.votesReceived = 0
	}
}

// startElection begins one election round.
// The caller must not hold rf.mu.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	args, started := rf.beginElectionLocked()
	rf.mu.Unlock()
	if started {
		rf.requestVotes(args)
		rf.broadcastHeartbeats()
	}
}

// Keeping the timeout check and this transition under one lock prevents a
// heartbeat from resetting the clock just before an election starts.
func (rf *Raft) beginElectionLocked() (RequestVoteArgs, bool) {
	if rf.dead || rf.role == leader {
		return RequestVoteArgs{}, false
	}
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.resetElectionTimerLocked()

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}
	// A single-server cluster wins with its own vote.
	if rf.votesReceived >= len(rf.peers)/2+1 {
		rf.becomeLeaderLocked()
	}
	return args, true
}

func (rf *Raft) requestVotes(args RequestVoteArgs) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(server, &args, &reply) {
				return // No usable reply; do not count a vote.
			}
			rf.handleRequestVoteReply(args.Term, reply)
		}(server)
	}
}

// Each peer is contacted once per election, so its vote can be counted once.
func (rf *Raft) handleRequestVoteReply(electionTerm int, reply RequestVoteReply) {
	rf.mu.Lock()
	if rf.dead {
		rf.mu.Unlock()
		return
	}

	// Even a reply to an old request can reveal a newer term.
	if reply.Term > rf.currentTerm {
		rf.adoptHigherTermLocked(reply.Term)
		rf.resetElectionTimerLocked()
		rf.mu.Unlock()
		return
	}
	if rf.role != candidate || rf.currentTerm != electionTerm || reply.Term != electionTerm {
		rf.mu.Unlock()
		return
	}
	if !reply.VoteGranted {
		rf.mu.Unlock()
		return
	}

	rf.votesReceived++
	won := rf.votesReceived >= len(rf.peers)/2+1
	if won {
		rf.becomeLeaderLocked()
	}
	rf.mu.Unlock()
	if won {
		rf.broadcastHeartbeats()
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.dead {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Adopt a newer term even if we later reject the candidate's log.
	rf.adoptHigherTermLocked(args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	logUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if !logUpToDate {
		return
	}

	rf.votedFor = args.CandidateId
	rf.resetElectionTimerLocked()
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // Empty for a heartbeat.
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.dead || args.Term < rf.currentTerm {
		return
	}
	rf.adoptHigherTermLocked(args.Term)
	reply.Term = rf.currentTerm
	// A same-term leader also makes a candidate step down. Keep its vote.
	rf.role = follower
	rf.votesReceived = 0
	rf.resetElectionTimerLocked()

	// A leader is still alive even if the consistency check fails.
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}
	if len(args.Entries) != 0 {
		return // Log replication and commitment will be implemented in 3B.
	}
	reply.Success = true
}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	if rf.dead || rf.role != leader || time.Since(rf.lastHeartbeat) < heartbeatInterval {
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeat = time.Now()
	// All logs contain the dummy entry at index 0. Per-follower log positions
	// and entries will be added when we implement replication.
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			var reply AppendEntriesReply
			if !rf.sendAppendEntries(server, &args, &reply) {
				return
			}
			rf.handleAppendEntriesReply(reply)
		}(server)
	}
}

func (rf *Raft) handleAppendEntriesReply(reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.dead && reply.Term > rf.currentTerm {
		rf.adoptHigherTermLocked(reply.Term)
		rf.resetElectionTimerLocked()
	}
}

func (rf *Raft) ticker() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	defer close(rf.tickerDone)
	for {
		select {
		case <-rf.stopCh:
			return
		case <-ticker.C:
			rf.mu.Lock()
			if rf.dead {
				rf.mu.Unlock()
				return
			}
			if rf.role == leader {
				rf.mu.Unlock()
				rf.broadcastHeartbeats()
				continue
			}
			if time.Since(rf.electionResetTime) >= rf.electionTimeout {
				args, started := rf.beginElectionLocked()
				rf.mu.Unlock()
				if started {
					rf.requestVotes(args)
					rf.broadcastHeartbeats()
				}
				continue
			}
			rf.mu.Unlock()
		}
	}
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
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.dead {
		rf.dead = true
		close(rf.stopCh)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := newRaft(peers, me)
	go rf.ticker()
	return rf
}

// Construct the state separately so unit tests can drive protocol events
// without a background timer changing their manually prepared state.
func newRaft(peers []*labrpc.ClientEnd, me int) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	rf.role = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.resetElectionTimerLocked()
	rf.stopCh = make(chan struct{})
	rf.tickerDone = make(chan struct{})

	return rf
}
