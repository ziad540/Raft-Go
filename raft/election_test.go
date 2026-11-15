package raft

import (
	"sync"
	"testing"
	"time"

	"lab2raft/labrpc"
)

func TestStartElectionSinglePeer(t *testing.T) {
	rf := newTestRaft(1, 0)
	rf.currentTerm = 3
	rf.electionResetTime = time.Unix(1, 0)
	rf.startElection()

	if term, isLeader := rf.GetState(); term != 4 || !isLeader {
		t.Fatalf("state = (term %d, leader %v); want (4, true)", term, isLeader)
	}
	if rf.votedFor != 0 || rf.votesReceived != 1 {
		t.Fatalf("self vote missing: votedFor %d, votesReceived %d", rf.votedFor, rf.votesReceived)
	}
	if !rf.electionResetTime.After(time.Unix(1, 0)) {
		t.Fatal("election did not reset the election clock")
	}

	// An existing leader must not start a new election.
	rf.startElection()
	if term, isLeader := rf.GetState(); term != 4 || !isLeader {
		t.Fatalf("leader restarted election: term %d, leader %v", term, isLeader)
	}
}

func TestStartElectionParallelRPCs(t *testing.T) {
	net := labrpc.MakeNetwork()
	const servers = 5
	ends := make([]*labrpc.ClientEnd, servers)
	for i := range ends {
		ends[i] = net.MakeEnd(i)
		net.Connect(i, i)
		net.Enable(i, true)
	}
	peers := make([]*Raft, servers)
	for i := range peers {
		peers[i] = newRaft(ends, i)
		peers[i].log = append(peers[i].log, LogEntry{Term: 1}, LogEntry{Term: 2})
		server := labrpc.MakeServer()
		server.AddService(labrpc.MakeService(peers[i]))
		net.AddServer(i, server)
	}

	// Peer 0 cannot answer yet. Other peers can still form a majority.
	peers[0].mu.Lock()
	defer peers[0].mu.Unlock()
	rf := peers[3]
	rf.currentTerm = 2
	started := make(chan struct{})
	go func() {
		rf.startElection()
		close(started)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("starting an election blocked on a remote peer")
	}

	// Acquiring the mutex also verifies RPCs do not run while holding it.
	elected := make(chan int, 1)
	go func() {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if term, isLeader := rf.GetState(); isLeader {
				elected <- term
				return
			}
			time.Sleep(time.Millisecond)
		}
		elected <- -1
	}()
	select {
	case term := <-elected:
		if term != 3 {
			t.Fatalf("elected term = %d; want 3", term)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow peer prevented election or blocked local state access")
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.votedFor != 3 || rf.votesReceived != 3 {
		t.Fatalf("elected without the expected majority: self vote %d, votes %d", rf.votedFor, rf.votesReceived)
	}
}

func TestElectionReplyRules(t *testing.T) {
	tests := []struct {
		name         string
		role         serverRole
		currentTerm  int
		electionTerm int
		reply        RequestVoteReply
		wantTerm     int
		wantRole     serverRole
		wantVotes    int
		wantVote     int
	}{
		{
			name: "grant without majority stays candidate", role: candidate, currentTerm: 4, electionTerm: 4,
			reply:    RequestVoteReply{Term: 4, VoteGranted: true},
			wantTerm: 4, wantRole: candidate, wantVotes: 2,
		},
		{
			name: "denial stays candidate", role: candidate, currentTerm: 4, electionTerm: 4,
			reply:    RequestVoteReply{Term: 4},
			wantTerm: 4, wantRole: candidate, wantVotes: 1,
		},
		{
			name: "old election grant cannot count in newer election", role: candidate, currentTerm: 5, electionTerm: 4,
			reply:    RequestVoteReply{Term: 4, VoteGranted: true},
			wantTerm: 5, wantRole: candidate, wantVotes: 1,
		},
		{
			name: "reply term must match election", role: candidate, currentTerm: 4, electionTerm: 4,
			reply:    RequestVoteReply{Term: 3, VoteGranted: true},
			wantTerm: 4, wantRole: candidate, wantVotes: 1,
		},
		{
			name: "follower ignores grant", role: follower, currentTerm: 4, electionTerm: 4,
			reply:    RequestVoteReply{Term: 4, VoteGranted: true},
			wantTerm: 4, wantRole: follower, wantVotes: 1,
		},
		{
			name: "leader ignores further grants", role: leader, currentTerm: 4, electionTerm: 4,
			reply:    RequestVoteReply{Term: 4, VoteGranted: true},
			wantTerm: 4, wantRole: leader, wantVotes: 1,
		},
		{
			name: "higher term denial forces step down", role: candidate, currentTerm: 4, electionTerm: 4,
			reply:    RequestVoteReply{Term: 6},
			wantTerm: 6, wantRole: follower, wantVote: -1,
		},
		{
			name: "old request still reveals higher term", role: candidate, currentTerm: 5, electionTerm: 4,
			reply:    RequestVoteReply{Term: 6},
			wantTerm: 6, wantRole: follower, wantVote: -1,
		},
		{
			name: "higher term reply makes leader step down", role: leader, currentTerm: 4, electionTerm: 4,
			reply:    RequestVoteReply{Term: 6, VoteGranted: true},
			wantTerm: 6, wantRole: follower, wantVote: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := newTestRaft(5, 0)
			rf.role = tt.role
			rf.currentTerm = tt.currentTerm
			rf.votedFor = 0
			rf.votesReceived = 1
			rf.handleRequestVoteReply(tt.electionTerm, tt.reply)
			if rf.currentTerm != tt.wantTerm || rf.role != tt.wantRole ||
				rf.votesReceived != tt.wantVotes || rf.votedFor != tt.wantVote {
				t.Fatalf("state = (term %d, role %d, votes %d, vote %d); want (%d, %d, %d, %d)",
					rf.currentTerm, rf.role, rf.votesReceived, rf.votedFor,
					tt.wantTerm, tt.wantRole, tt.wantVotes, tt.wantVote)
			}
		})
	}
}

func TestElectionConcurrentMajority(t *testing.T) {
	rf := newTestRaft(5, 0)
	rf.role = candidate
	rf.currentTerm = 4
	rf.votedFor = 0
	rf.votesReceived = 1
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rf.handleRequestVoteReply(4, RequestVoteReply{Term: 4, VoteGranted: true})
		}()
	}
	wg.Wait()
	if term, isLeader := rf.GetState(); term != 4 || !isLeader {
		t.Fatalf("state = (term %d, leader %v); want (4, true)", term, isLeader)
	}
	if rf.votesReceived != 3 {
		t.Fatalf("votes = %d; want majority of 3 before ignoring further replies", rf.votesReceived)
	}
}
