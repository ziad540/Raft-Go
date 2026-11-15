package raft

import (
	"sync"
	"testing"
	"time"
)

func TestRequestVoteRules(t *testing.T) {
	tests := []struct {
		name        string
		role        serverRole
		votedFor    int
		logTerms    []int
		args        RequestVoteArgs
		wantGranted bool
		wantTerm    int
		wantVote    int
		wantRole    serverRole
	}{
		{
			name: "reject older election term", votedFor: -1,
			args:     RequestVoteArgs{Term: 2, CandidateId: 1, LastLogTerm: 2, LastLogIndex: 10},
			wantTerm: 3, wantVote: -1,
		},
		{
			name: "grant first vote with empty logs", votedFor: -1,
			args:        RequestVoteArgs{Term: 3, CandidateId: 1},
			wantGranted: true, wantTerm: 3, wantVote: 1,
		},
		{
			name: "grant repeated request from same candidate", votedFor: 1,
			args:        RequestVoteArgs{Term: 3, CandidateId: 1},
			wantGranted: true, wantTerm: 3, wantVote: 1,
		},
		{
			name: "reject second candidate in same term", votedFor: 1,
			args:     RequestVoteArgs{Term: 3, CandidateId: 2},
			wantTerm: 3, wantVote: 1,
		},
		{
			name: "candidate keeps its self vote", role: candidate, votedFor: 0,
			args:     RequestVoteArgs{Term: 3, CandidateId: 1},
			wantTerm: 3, wantVote: 0, wantRole: candidate,
		},
		{
			name: "reject longer log with older last entry term", votedFor: -1, logTerms: []int{1, 2},
			args:     RequestVoteArgs{Term: 3, CandidateId: 1, LastLogTerm: 1, LastLogIndex: 10},
			wantTerm: 3, wantVote: -1,
		},
		{
			name: "grant shorter log with newer last entry term", votedFor: -1, logTerms: []int{1, 2, 2},
			args:        RequestVoteArgs{Term: 3, CandidateId: 1, LastLogTerm: 3, LastLogIndex: 1},
			wantGranted: true, wantTerm: 3, wantVote: 1,
		},
		{
			name: "reject shorter log with equal last entry term", votedFor: -1, logTerms: []int{1, 2},
			args:     RequestVoteArgs{Term: 3, CandidateId: 1, LastLogTerm: 2, LastLogIndex: 1},
			wantTerm: 3, wantVote: -1,
		},
		{
			name: "grant equal log", votedFor: -1, logTerms: []int{1, 2},
			args:        RequestVoteArgs{Term: 3, CandidateId: 1, LastLogTerm: 2, LastLogIndex: 2},
			wantGranted: true, wantTerm: 3, wantVote: 1,
		},
		{
			name: "grant longer log with equal last entry term", votedFor: -1, logTerms: []int{1, 2},
			args:        RequestVoteArgs{Term: 3, CandidateId: 1, LastLogTerm: 2, LastLogIndex: 3},
			wantGranted: true, wantTerm: 3, wantVote: 1,
		},
		{
			name: "newer term replaces leader and old vote", role: leader, votedFor: 0,
			args:        RequestVoteArgs{Term: 4, CandidateId: 1},
			wantGranted: true, wantTerm: 4, wantVote: 1,
		},
		{
			name: "newer term adopted even when candidate log is stale", role: leader, votedFor: 0, logTerms: []int{3},
			args:     RequestVoteArgs{Term: 4, CandidateId: 1, LastLogTerm: 2, LastLogIndex: 10},
			wantTerm: 4, wantVote: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := newRaft(nil, 0)
			rf.currentTerm = 3
			rf.role = tt.role
			rf.votedFor = tt.votedFor
			for _, term := range tt.logTerms {
				rf.log = append(rf.log, LogEntry{Term: term})
			}
			previousReset := time.Unix(1, 0)
			rf.electionResetTime = previousReset

			// Start with a nonzero reply to verify rejection overwrites it.
			reply := RequestVoteReply{Term: -1, VoteGranted: true}
			rf.RequestVote(&tt.args, &reply)

			if reply.Term != tt.wantTerm || reply.VoteGranted != tt.wantGranted {
				t.Fatalf("reply = %+v; want term %d, granted %v", reply, tt.wantTerm, tt.wantGranted)
			}
			if rf.currentTerm != tt.wantTerm || rf.votedFor != tt.wantVote || rf.role != tt.wantRole {
				t.Fatalf("state = (term %d, vote %d, role %d); want (%d, %d, %d)",
					rf.currentTerm, rf.votedFor, rf.role, tt.wantTerm, tt.wantVote, tt.wantRole)
			}
			if tt.wantGranted {
				if !rf.electionResetTime.After(previousReset) {
					t.Fatal("granting a vote did not reset the election clock")
				}
			} else if !rf.electionResetTime.Equal(previousReset) {
				t.Fatal("rejecting a vote reset the election clock")
			}
		})
	}
}

func TestRequestVoteConcurrentCandidates(t *testing.T) {
	rf := newRaft(nil, 0)
	const candidates = 8
	replies := make([]RequestVoteReply, candidates)
	var wg sync.WaitGroup
	for i := range replies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := RequestVoteArgs{Term: 1, CandidateId: i + 1}
			rf.RequestVote(&args, &replies[i])
		}(i)
	}
	wg.Wait()

	granted := 0
	for i, reply := range replies {
		if reply.Term != 1 {
			t.Fatalf("candidate %d received term %d; want 1", i+1, reply.Term)
		}
		if reply.VoteGranted {
			granted++
			if rf.votedFor != i+1 {
				t.Fatalf("granted candidate %d but recorded vote for %d", i+1, rf.votedFor)
			}
		}
	}
	if granted != 1 {
		t.Fatalf("granted %d competing candidates; want exactly 1", granted)
	}
}
