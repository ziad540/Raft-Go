package raft

import (
	"testing"
	"time"
)

func TestCommitRequiresCurrentTermMajority(t *testing.T) {
	rf := newTestRaft(5, 0)
	rf.log = logForTerms(1, 2, 2)
	rf.currentTerm = 3
	rf.becomeLeaderLocked()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 3
	}
	rf.advanceCommitLocked()
	if rf.commitIndex != 0 {
		t.Fatal("counting replicas committed an older-term entry")
	}

	rf.log = append(rf.log, LogEntry{Term: 3, Command: "new term command"})
	rf.matchIndex[0], rf.matchIndex[1] = 4, 4
	rf.advanceCommitLocked()
	if rf.commitIndex != 0 {
		t.Fatal("two of five replicas committed an entry")
	}
	rf.matchIndex[2] = 4
	rf.advanceCommitLocked()
	if rf.commitIndex != 4 {
		t.Fatalf("current-term majority did not commit the whole prefix: %d", rf.commitIndex)
	}
}

func TestCommitFollowerBounds(t *testing.T) {
	rf := newRaft(nil, 0)
	rf.currentTerm = 5
	rf.log = logForTerms(1, 2, 3, 4, 4)
	rf.commitIndex = 2
	args := AppendEntriesArgs{
		Term: 5, PrevLogIndex: 2, PrevLogTerm: 2,
		Entries: []LogEntry{rf.log[3]}, LeaderCommit: 5,
	}
	var reply AppendEntriesReply
	rf.AppendEntries(&args, &reply)
	if !reply.Success || rf.commitIndex != 3 {
		t.Fatalf("committed beyond the RPC's matching prefix: success %v, commit %d", reply.Success, rf.commitIndex)
	}

	// A delayed heartbeat covering a shorter prefix must not move backward.
	rf.AppendEntries(&AppendEntriesArgs{Term: 5, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 5}, &reply)
	if !reply.Success || rf.commitIndex != 3 {
		t.Fatal("shorter heartbeat reduced the committed index")
	}
	rf.AppendEntries(&AppendEntriesArgs{Term: 5, PrevLogIndex: 3, PrevLogTerm: 2, LeaderCommit: 5}, &reply)
	if reply.Success || rf.commitIndex != 3 {
		t.Fatal("a rejected consistency check advanced commitment")
	}
	rf.AppendEntries(&AppendEntriesArgs{
		Term: 5, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 5, Command: "conflict"}}, LeaderCommit: 5,
	}, &reply)
	if reply.Success || rf.log[2].Term != 2 || rf.commitIndex != 3 {
		t.Fatal("a conflicting request overwrote a committed entry")
	}
	rf.AppendEntries(&AppendEntriesArgs{Term: 5, PrevLogIndex: 5, PrevLogTerm: 4, LeaderCommit: 5}, &reply)
	if !reply.Success || rf.commitIndex != 5 {
		t.Fatal("a matching heartbeat did not announce commitment")
	}
}

func waitForApplied(t *testing.T, rf *Raft, index int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rf.mu.Lock()
		applied := rf.lastApplied
		rf.mu.Unlock()
		if applied == index {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("lastApplied did not reach %d", index)
}

func TestCommitSingleServerAppliesCommand(t *testing.T) {
	rf := newTestRaft(1, 0)
	rf.currentTerm = 3
	rf.becomeLeaderLocked()
	rf.applyCh = make(chan ApplyMsg, 1)
	go rf.applier()
	defer rf.Kill()
	index, term, accepted := rf.Start("set x = 10")
	if !accepted || index != 1 || term != 3 {
		t.Fatalf("Start returned (%d, %d, %v)", index, term, accepted)
	}
	select {
	case msg := <-rf.applyCh:
		if msg.Index != 1 || msg.Command != "set x = 10" || msg.UseSnapshot {
			t.Fatalf("unexpected applied command: %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("single server waited for nonexistent peer replies to commit")
	}
	waitForApplied(t, rf, 1)
}

func TestCommitApplierOrderAndBlockedConsumer(t *testing.T) {
	rf := newRaft(nil, 0)
	rf.log = logForTerms(1, 1, 1)
	rf.currentTerm = 1
	rf.commitIndex = 3
	rf.applyCh = make(chan ApplyMsg, 2)
	go rf.applier()
	defer rf.Kill()
	deadline := time.Now().Add(time.Second)
	for len(rf.applyCh) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("applier did not fill the consumer's buffer")
		}
		time.Sleep(time.Millisecond)
	}
	// With two messages buffered, sending entry 3 must wait for the consumer.
	// The RPC must still append and commit entry 4 while delivery is blocked.
	done := make(chan AppendEntriesReply, 1)
	go func() {
		args := AppendEntriesArgs{
			Term: 2, PrevLogIndex: 3, PrevLogTerm: 1,
			Entries: []LogEntry{{Term: 2, Command: "fourth"}}, LeaderCommit: 4,
		}
		var reply AppendEntriesReply
		rf.AppendEntries(&args, &reply)
		done <- reply
	}()
	select {
	case reply := <-done:
		if !reply.Success {
			t.Fatal("RPC rejected a valid log extension")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked applyCh prevented RPC handling")
	}
	for index := 1; index <= 4; index++ {
		select {
		case msg := <-rf.applyCh:
			wantCommand := "fourth"
			if index <= 3 {
				wantCommand = logForTerms(1, 1, 1)[index].Command.(string)
			}
			if msg.Index != index || msg.Command != wantCommand {
				t.Fatalf("applied %+v; want index %d, command %s", msg, index, wantCommand)
			}
		case <-time.After(time.Second):
			t.Fatalf("entry %d was not applied", index)
		}
	}
	waitForApplied(t, rf, 4)
	var reply AppendEntriesReply
	rf.AppendEntries(&AppendEntriesArgs{Term: 2, PrevLogIndex: 4, PrevLogTerm: 2, LeaderCommit: 4}, &reply)
	select {
	case msg := <-rf.applyCh:
		t.Fatalf("repeated commitment applied an entry twice: %+v", msg)
	case <-time.After(50 * time.Millisecond):
	}
	rf.Kill()
	select {
	case <-rf.applierDone:
	case <-time.After(time.Second):
		t.Fatal("idle applier did not stop")
	}
}

func TestCommitKillUnblocksDelivery(t *testing.T) {
	rf := newRaft(nil, 0)
	rf.log = logForTerms(1, 1)
	rf.commitIndex = 2
	rf.applyCh = make(chan ApplyMsg, 1)
	go rf.applier()
	defer rf.Kill()
	deadline := time.Now().Add(time.Second)
	for len(rf.applyCh) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("applier did not begin delivery")
		}
		time.Sleep(time.Millisecond)
	}
	rf.Kill()
	select {
	case <-rf.applierDone:
	case <-time.After(time.Second):
		t.Fatal("Kill did not stop an applier with a blocked consumer")
	}
}

func TestCommitUnreliableAgreement(t *testing.T) {
	cfg := make_config(t, 5, true)
	defer cfg.cleanup()
	for command := 201; command <= 203; command++ {
		cfg.one(command, 5)
	}
}
