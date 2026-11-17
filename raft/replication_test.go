package raft

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"lab2raft/labrpc"
)

func logForTerms(terms ...int) []LogEntry {
	entries := []LogEntry{{Term: 0}}
	for i, term := range terms {
		entries = append(entries, LogEntry{Term: term, Command: fmt.Sprintf("entry-%d-term-%d", i+1, term)})
	}
	return entries
}

func TestLogReplicationFollowerUpdates(t *testing.T) {
	tests := []struct {
		name      string
		existing  []int
		incoming  []int
		prevIndex int
		want      []int
	}{
		{"append missing entries", []int{1}, []int{1, 2, 2}, 1, []int{1, 2, 2}},
		{"keep matching entries and extra suffix", []int{1, 2, 3}, []int{1, 2}, 1, []int{1, 2, 3}},
		{"empty heartbeat preserves extra suffix", []int{1, 2, 3}, []int{1}, 1, []int{1, 2, 3}},
		{"replace only from first conflict", []int{1, 2, 2, 3}, []int{1, 2, 4, 4}, 1, []int{1, 2, 4, 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := newRaft(nil, 0)
			rf.currentTerm = 5
			rf.log = logForTerms(tt.existing...)
			incoming := logForTerms(tt.incoming...)
			args := AppendEntriesArgs{
				Term: 5, LeaderId: 1, PrevLogIndex: tt.prevIndex,
				PrevLogTerm: incoming[tt.prevIndex].Term, Entries: incoming[tt.prevIndex+1:],
			}
			for attempt := 0; attempt < 2; attempt++ {
				var reply AppendEntriesReply
				rf.AppendEntries(&args, &reply)
				if !reply.Success || !reflect.DeepEqual(rf.log, logForTerms(tt.want...)) {
					t.Fatalf("attempt %d: reply %+v, log %+v; want %+v", attempt, reply, rf.log, logForTerms(tt.want...))
				}
			}
		})
	}

	rf := newRaft(nil, 0)
	rf.log = logForTerms(1, 2, 3)
	before := append([]LogEntry(nil), rf.log...)
	var reply AppendEntriesReply
	rf.AppendEntries(&AppendEntriesArgs{Term: 5, PrevLogIndex: 2, PrevLogTerm: 4, Entries: []LogEntry{{Term: 5}}}, &reply)
	if reply.Success || !reflect.DeepEqual(rf.log, before) {
		t.Fatal("a failed preceding-entry check changed the log")
	}
}

func makeReplicationPeers(t *testing.T, count int) []*Raft {
	t.Helper()
	net := labrpc.MakeNetwork()
	peers := make([]*Raft, count)
	for i := range peers {
		ends := make([]*labrpc.ClientEnd, count)
		for j := range ends {
			name := [2]int{i, j}
			ends[j] = net.MakeEnd(name)
			net.Connect(name, j)
			net.Enable(name, true)
		}
		peers[i] = newRaft(ends, i)
		server := labrpc.MakeServer()
		server.AddService(labrpc.MakeService(peers[i]))
		net.AddServer(i, server)
	}
	t.Cleanup(func() {
		for _, rf := range peers {
			rf.Kill()
		}
	})
	return peers
}

func waitForReplication(t *testing.T, leader *Raft, follower, index int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leader.mu.Lock()
		matched := leader.matchIndex[follower]
		leader.mu.Unlock()
		if matched >= index {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("follower %d did not confirm replication through index %d", follower, index)
}

func copyRaftLog(rf *Raft) []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return append([]LogEntry(nil), rf.log...)
}

func TestLogReplicationPaperScenarios(t *testing.T) {
	leaderTerms := []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6}
	tests := []struct {
		name  string
		terms []int
	}{
		{"a missing last entry", []int{1, 1, 1, 4, 4, 5, 5, 6, 6}},
		{"b missing long suffix", []int{1, 1, 1, 4}},
		{"c extra term 6 entry", []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}},
		{"d extra term 7 entries", []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}},
		{"e conflicting short suffix", []int{1, 1, 1, 4, 4, 4, 4}},
		{"f conflicting long suffix", []int{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peers := makeReplicationPeers(t, 2)
			rf, follower := peers[0], peers[1]
			rf.log = logForTerms(leaderTerms...)
			rf.currentTerm = 8
			rf.becomeLeaderLocked()
			follower.log = logForTerms(tt.terms...)
			rf.broadcastAppendEntries()
			waitForReplication(t, rf, 1, 10)

			want := logForTerms(leaderTerms...)
			original := logForTerms(tt.terms...)
			if len(tt.terms) > 10 && reflect.DeepEqual(original[:11], want) {
				// c and d keep their extra suffix until a new entry conflicts.
				want = logForTerms(tt.terms...)
			}
			if got := copyRaftLog(follower); !reflect.DeepEqual(got, want) {
				t.Fatalf("after initial repair: log %+v; want %+v", got, want)
			}

			index, term, accepted := rf.Start("new command")
			if !accepted || index != 11 || term != 8 {
				t.Fatalf("Start returned (%d, %d, %v); want (11, 8, true)", index, term, accepted)
			}
			waitForReplication(t, rf, 1, 11)
			if got, want := copyRaftLog(follower), copyRaftLog(rf); !reflect.DeepEqual(got, want) {
				t.Fatalf("after new entry: follower %+v; leader %+v", got, want)
			}
			rf.mu.Lock()
			next := rf.nextIndex[1]
			rf.mu.Unlock()
			if next != 12 {
				t.Fatalf("nextIndex = %d; want 12", next)
			}
		})
	}
}

func TestLogReplicationStartReturnsBeforeRPC(t *testing.T) {
	peers := makeReplicationPeers(t, 2)
	rf := peers[0]
	if index, term, accepted := rf.Start("reject"); accepted || index != -1 || term != 0 || len(rf.log) != 1 {
		t.Fatal("follower accepted a command")
	}
	rf.currentTerm = 3
	rf.becomeLeaderLocked()
	peers[1].mu.Lock()
	defer peers[1].mu.Unlock()
	result := make(chan bool, 1)
	go func() {
		index, term, accepted := rf.Start("set x = 10")
		result <- accepted && index == 1 && term == 3
	}()
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("leader did not accept command at index 1, term 3")
		}
	case <-time.After(time.Second):
		t.Fatal("Start waited for the blocked follower")
	}
	if got := copyRaftLog(rf); len(got) != 2 || got[1].Command != "set x = 10" {
		t.Fatalf("leader did not store the command: %+v", got)
	}
}

func TestLogReplicationConcurrentCommands(t *testing.T) {
	peers := makeReplicationPeers(t, 3)
	rf := peers[0]
	rf.currentTerm = 3
	rf.becomeLeaderLocked()
	const commands = 16
	type result struct {
		index    int
		command  string
		accepted bool
	}
	results := make(chan result, commands)
	for i := 0; i < commands; i++ {
		go func(i int) {
			command := fmt.Sprintf("command-%d", i)
			index, _, accepted := rf.Start(command)
			results <- result{index, command, accepted}
		}(i)
	}
	seen := map[int]bool{}
	for i := 0; i < commands; i++ {
		select {
		case result := <-results:
			if !result.accepted || result.index < 1 || result.index > commands || seen[result.index] {
				t.Fatalf("invalid command result: %+v", result)
			}
			seen[result.index] = true
			if got := copyRaftLog(rf)[result.index].Command; got != result.command {
				t.Fatalf("index %d contains %v; want %s", result.index, got, result.command)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent Start calls blocked")
		}
	}
	for follower := 1; follower < len(peers); follower++ {
		waitForReplication(t, rf, follower, commands)
		if got, want := copyRaftLog(peers[follower]), copyRaftLog(rf); !reflect.DeepEqual(got, want) {
			t.Fatalf("follower %d has different commands: %+v; want %+v", follower, got, want)
		}
	}
}

func TestLogReplicationDelayedReplies(t *testing.T) {
	rf := newTestRaft(2, 0)
	rf.currentTerm = 5
	rf.log = logForTerms(1, 2, 3, 4, 4)
	rf.becomeLeaderLocked()
	success := AppendEntriesReply{Term: 5, Success: true}
	rf.handleAppendEntriesReply(1, AppendEntriesArgs{Term: 5, PrevLogIndex: 5}, success)
	// Old failures and successes must not undo the confirmed prefix.
	rf.handleAppendEntriesReply(1, AppendEntriesArgs{Term: 5, PrevLogIndex: 5}, AppendEntriesReply{Term: 5})
	rf.handleAppendEntriesReply(1, AppendEntriesArgs{Term: 5, PrevLogIndex: 2}, success)
	if rf.matchIndex[1] != 5 || rf.nextIndex[1] != 6 {
		t.Fatalf("delayed reply regressed progress: match %d, next %d", rf.matchIndex[1], rf.nextIndex[1])
	}
	rf.handleAppendEntriesReply(1, AppendEntriesArgs{Term: 4, PrevLogIndex: 5}, AppendEntriesReply{Term: 5})
	if rf.nextIndex[1] != 6 {
		t.Fatal("reply from older leadership changed progress")
	}
	rf.handleAppendEntriesReply(1, AppendEntriesArgs{Term: 4}, AppendEntriesReply{Term: 6})
	if term, leader := rf.GetState(); term != 6 || leader {
		t.Fatal("higher term in an old reply did not force step down")
	}
}
