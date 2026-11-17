package raft

import (
	"testing"
	"time"

	"lab2raft/labrpc"
)

// Unit tests drive events manually; disconnected endpoints allow any outgoing
// heartbeats to fail normally without starting election timers.
func newTestRaft(servers, me int) *Raft {
	net := labrpc.MakeNetwork()
	ends := make([]*labrpc.ClientEnd, servers)
	for i := range ends {
		ends[i] = net.MakeEnd(i)
	}
	return newRaft(ends, me)
}

func TestHeartbeatReceiverRules(t *testing.T) {
	tests := []struct {
		name        string
		role        serverRole
		args        AppendEntriesArgs
		wantTerm    int
		wantRole    serverRole
		wantVote    int
		wantSuccess bool
		wantReset   bool
	}{
		{
			name: "stale leader does not postpone election", role: candidate,
			args:     AppendEntriesArgs{Term: 2, LeaderId: 1},
			wantTerm: 3, wantRole: candidate, wantVote: 0,
		},
		{
			name: "same term leader makes candidate follow without clearing vote", role: candidate,
			args:     AppendEntriesArgs{Term: 3, LeaderId: 1},
			wantTerm: 3, wantRole: follower, wantVote: 0, wantSuccess: true, wantReset: true,
		},
		{
			name: "higher term makes old leader follow and clears vote", role: leader,
			args:     AppendEntriesArgs{Term: 4, LeaderId: 1},
			wantTerm: 4, wantRole: follower, wantVote: -1, wantSuccess: true, wantReset: true,
		},
		{
			name: "missing preceding entry still resets election clock", role: candidate,
			args:     AppendEntriesArgs{Term: 3, LeaderId: 1, PrevLogIndex: 2},
			wantTerm: 3, wantRole: follower, wantVote: 0, wantReset: true,
		},
		{
			name: "mismatched preceding term still resets election clock", role: candidate,
			args:     AppendEntriesArgs{Term: 3, LeaderId: 1, PrevLogTerm: 1},
			wantTerm: 3, wantRole: follower, wantVote: 0, wantReset: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := newRaft(nil, 0)
			rf.currentTerm = 3
			rf.role = tt.role
			rf.votedFor = 0
			previousReset := time.Unix(1, 0)
			rf.electionResetTime = previousReset
			reply := AppendEntriesReply{Term: -1, Success: true}
			rf.AppendEntries(&tt.args, &reply)
			if reply.Term != tt.wantTerm || reply.Success != tt.wantSuccess ||
				rf.currentTerm != tt.wantTerm || rf.role != tt.wantRole || rf.votedFor != tt.wantVote {
				t.Fatalf("reply %+v, state (term %d, role %d, vote %d); want term %d, success %v, role %d, vote %d",
					reply, rf.currentTerm, rf.role, rf.votedFor,
					tt.wantTerm, tt.wantSuccess, tt.wantRole, tt.wantVote)
			}
			if reset := !rf.electionResetTime.Equal(previousReset); reset != tt.wantReset {
				t.Fatalf("reset clock = %v; want %v", reset, tt.wantReset)
			}
		})
	}
}

func TestHeartbeatHigherTermReply(t *testing.T) {
	rf := newRaft(nil, 0)
	rf.role = leader
	rf.currentTerm = 3
	rf.votedFor = 0
	rf.handleAppendEntriesReply(1, AppendEntriesArgs{Term: 3}, AppendEntriesReply{Term: 5})
	if term, isLeader := rf.GetState(); term != 5 || isLeader || rf.votedFor != -1 {
		t.Fatalf("after higher term reply: term %d, leader %v, vote %d", term, isLeader, rf.votedFor)
	}
}

func TestHeartbeatStability(t *testing.T) {
	cfg := make_config(t, 3, false)
	defer cfg.cleanup()
	leader := cfg.checkOneLeader()
	term, _ := cfg.rafts[leader].GetState()
	rpcs := func() int {
		total := 0
		for i := 0; i < cfg.n; i++ {
			total += cfg.rpcCount(i)
		}
		return total
	}
	before := rpcs()
	time.Sleep(time.Second)
	if sent := rpcs() - before; sent < 2 || sent > 24 {
		t.Fatalf("idle cluster received %d RPCs in one second; want periodic, bounded heartbeats", sent)
	}
	for i, rf := range cfg.rafts {
		currentTerm, isLeader := rf.GetState()
		if currentTerm != term || isLeader != (i == leader) {
			t.Fatalf("peer %d changed stable leadership: term %d, leader %v", i, currentTerm, isLeader)
		}
	}
}

func TestHeartbeatUnreliableElection(t *testing.T) {
	cfg := make_config(t, 5, true)
	defer cfg.cleanup()
	cfg.checkOneLeader()
}

func TestKillStopsElectionTicker(t *testing.T) {
	rf := Make(make([]*labrpc.ClientEnd, 1), 0, nil)
	defer rf.Kill()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, leader := rf.GetState(); leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("single peer did not automatically elect itself")
		}
		time.Sleep(5 * time.Millisecond)
	}
	rf.Kill()
	rf.Kill() // Shutdown is idempotent.
	select {
	case <-rf.tickerDone:
	case <-time.After(time.Second):
		t.Fatal("ticker did not stop")
	}
	select {
	case <-rf.applierDone:
	case <-time.After(time.Second):
		t.Fatal("applier did not stop")
	}
	term, _ := rf.GetState()
	rf.startElection()
	rf.handleRequestVoteReply(term, RequestVoteReply{Term: term + 1, VoteGranted: true})
	rf.handleAppendEntriesReply(0, AppendEntriesArgs{Term: term}, AppendEntriesReply{Term: term + 1})
	var voteReply RequestVoteReply
	rf.RequestVote(&RequestVoteArgs{Term: term + 1, CandidateId: 1}, &voteReply)
	var heartbeatReply AppendEntriesReply
	rf.AppendEntries(&AppendEntriesArgs{Term: term + 1, LeaderId: 1}, &heartbeatReply)
	if currentTerm, leader := rf.GetState(); currentTerm != term || leader ||
		voteReply.VoteGranted || heartbeatReply.Success {
		t.Fatal("killed peer continued participating in the protocol")
	}
}
