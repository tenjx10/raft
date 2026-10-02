package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/tenjx10/raft/internal/cluster"
)

func newCluster(t *testing.T, n int) *cluster.Cluster {
	t.Helper()
	c, err := cluster.New(n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Shutdown)
	return c
}

func TestElectsOneLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader, term, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// with no failures, the leader should stay put
	time.Sleep(1 * time.Second)
	leader2, term2, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if leader2 != leader || term2 != term {
		t.Fatalf("leader changed from %d (term %d) to %d (term %d) with no failures", leader, term, leader2, term2)
	}
}

func TestReplicatesInOrder(t *testing.T) {
	c := newCluster(t, 5)
	for i := 1; i <= 10; i++ {
		index, err := c.Replicate(fmt.Sprintf("cmd-%d", i), 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if index != i {
			t.Fatalf("command %d committed at index %d", i, index)
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 5)
	if _, err := c.Replicate("before", 10*time.Second); err != nil {
		t.Fatal(err)
	}

	leader, _, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Kill(leader)

	newLeader, _, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLeader == leader {
		t.Fatal("dead server is still leader")
	}

	// the entry committed under the old leader must survive
	if _, err := c.Replicate("after", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < c.Size(); i++ {
		if !c.Alive(i) {
			continue
		}
		got := c.Applied(i)
		if len(got) != 2 || got[0] != "before" || got[1] != "after" {
			t.Fatalf("server %d applied %v, want [before after]", i, got)
		}
	}
}

func TestNoProgressWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	leader, _, err := c.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < c.Size(); i++ {
		if i != leader {
			c.Kill(i)
		}
	}

	_, index, err := c.Submit("lonely")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if len(c.Applied(leader)) >= index {
		t.Fatal("leader committed an entry without a majority")
	}
}
