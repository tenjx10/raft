// Package cluster runs several Raft servers inside one process, talking to
// each other over real TCP connections on localhost. It is used by the demo
// and the tests.
package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/tenjx10/raft/raft"
)

type node struct {
	rf     *raft.Raft
	server *raft.Server

	mu      sync.Mutex
	applied []interface{} // committed commands, in log order
	alive   bool
}

// Cluster is a set of Raft servers on localhost.
type Cluster struct {
	nodes []*node
	addrs []string
}

// New starts n Raft servers on free localhost ports.
func New(n int) (*Cluster, error) {
	c := &Cluster{nodes: make([]*node, n), addrs: make([]string, n)}

	// bind every port first so each server knows all addresses
	servers := make([]*raft.Server, n)
	for i := range servers {
		s, err := raft.Listen("127.0.0.1:0")
		if err != nil {
			for _, prev := range servers[:i] {
				prev.Close()
			}
			return nil, err
		}
		servers[i] = s
		c.addrs[i] = s.Addr()
	}

	for i := 0; i < n; i++ {
		peers := make([]raft.Peer, n)
		for j := range peers {
			if j != i {
				peers[j] = raft.NewPeer(c.addrs[j])
			}
		}

		applyCh := make(chan raft.ApplyMsg)
		nd := &node{server: servers[i], alive: true}
		nd.rf = raft.Make(peers, i, applyCh)
		nd.server.Serve(nd.rf)
		c.nodes[i] = nd

		go func() {
			for msg := range applyCh {
				if msg.CommandValid {
					nd.mu.Lock()
					nd.applied = append(nd.applied, msg.Command)
					nd.mu.Unlock()
				}
			}
		}()
	}
	return c, nil
}

// Size returns the number of servers, alive or not.
func (c *Cluster) Size() int { return len(c.nodes) }

// Addr returns server i's address.
func (c *Cluster) Addr(i int) string { return c.addrs[i] }

// Alive reports whether server i is still running.
func (c *Cluster) Alive(i int) bool {
	nd := c.nodes[i]
	nd.mu.Lock()
	defer nd.mu.Unlock()
	return nd.alive
}

// Kill crashes server i: it stops answering RPCs and its goroutines exit.
func (c *Cluster) Kill(i int) {
	nd := c.nodes[i]
	nd.mu.Lock()
	nd.alive = false
	nd.mu.Unlock()
	nd.server.Close()
	nd.rf.Kill()
}

// Shutdown kills every server.
func (c *Cluster) Shutdown() {
	for i := range c.nodes {
		c.Kill(i)
	}
}

// Applied returns a copy of the commands server i has applied so far.
func (c *Cluster) Applied(i int) []interface{} {
	nd := c.nodes[i]
	nd.mu.Lock()
	defer nd.mu.Unlock()
	return append([]interface{}(nil), nd.applied...)
}

// State returns server i's current term and whether it thinks it is leader.
func (c *Cluster) State(i int) (int, bool) {
	return c.nodes[i].rf.GetState()
}

// WaitForLeader waits until exactly one live server is leader in the
// highest term any live server has seen, and returns its index and term.
func (c *Cluster) WaitForLeader(timeout time.Duration) (int, int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := map[int][]int{} // term -> leader indices
		maxTerm := 0
		for i := range c.nodes {
			if !c.Alive(i) {
				continue
			}
			term, isLeader := c.State(i)
			if term > maxTerm {
				maxTerm = term
			}
			if isLeader {
				leaders[term] = append(leaders[term], i)
			}
		}
		if l := leaders[maxTerm]; len(l) == 1 {
			return l[0], maxTerm, nil
		}
		if len(leaders[maxTerm]) > 1 {
			return -1, 0, fmt.Errorf("term %d has %d leaders", maxTerm, len(leaders[maxTerm]))
		}
		time.Sleep(50 * time.Millisecond)
	}
	return -1, 0, fmt.Errorf("no leader elected within %v", timeout)
}

// Replicate submits cmd to the leader and waits until every live server has
// applied it, retrying if leadership changes along the way. It returns the
// log index the command was committed at.
func (c *Cluster) Replicate(cmd interface{}, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader, _, err := c.WaitForLeader(time.Until(deadline))
		if err != nil {
			return -1, err
		}
		index, _, ok := c.nodes[leader].rf.Start(cmd)
		if !ok {
			continue
		}

		// give this attempt up to 2 seconds to commit everywhere
		attemptEnd := time.Now().Add(2 * time.Second)
		for time.Now().Before(attemptEnd) {
			if c.allApplied(index, cmd) {
				return index, nil
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return -1, fmt.Errorf("command %v was not committed within %v", cmd, timeout)
}

// allApplied reports whether every live server applied cmd at index.
func (c *Cluster) allApplied(index int, cmd interface{}) bool {
	for i := range c.nodes {
		if !c.Alive(i) {
			continue
		}
		applied := c.Applied(i)
		if len(applied) < index || applied[index-1] != cmd {
			return false
		}
	}
	return true
}

// Submit hands cmd to the current leader without waiting for it to commit.
// It returns the leader used and the index the command was placed at.
func (c *Cluster) Submit(cmd interface{}) (leader int, index int, err error) {
	leader, _, err = c.WaitForLeader(2 * time.Second)
	if err != nil {
		return -1, -1, err
	}
	index, _, ok := c.nodes[leader].rf.Start(cmd)
	if !ok {
		return -1, -1, fmt.Errorf("server %d is no longer leader", leader)
	}
	return leader, index, nil
}
