// Command demo starts a Raft cluster on localhost and walks through leader
// election, log replication, leader failover, and loss of a majority.
//
//	go run ./cmd/demo            # 5 servers
//	go run ./cmd/demo -n 3       # 3 servers
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tenjx10/raft/internal/cluster"
)

func main() {
	n := flag.Int("n", 5, "number of Raft servers (at least 3)")
	flag.Parse()
	if *n < 3 {
		fmt.Fprintln(os.Stderr, "need at least 3 servers to survive a failure")
		os.Exit(2)
	}

	c, err := cluster.New(*n)
	check(err)
	defer c.Shutdown()

	step("Starting %d Raft servers", *n)
	for i := 0; i < c.Size(); i++ {
		fmt.Printf("  server %d listening on %s\n", i, c.Addr(i))
	}

	step("Leader election")
	leader, term, err := c.WaitForLeader(5 * time.Second)
	check(err)
	fmt.Printf("  server %d was elected leader for term %d\n", leader, term)

	step("Log replication")
	for _, cmd := range []string{"set x = 1", "set y = 2", "set x = 3"} {
		index, err := c.Replicate(cmd, 10*time.Second)
		check(err)
		fmt.Printf("  committed %-12q at index %d on every server\n", cmd, index)
	}
	printLogs(c)

	step("Leader failure")
	fmt.Printf("  crashing leader (server %d)...\n", leader)
	c.Kill(leader)
	start := time.Now()
	newLeader, term, err := c.WaitForLeader(5 * time.Second)
	check(err)
	fmt.Printf("  server %d took over as leader for term %d after %v\n",
		newLeader, term, time.Since(start).Round(time.Millisecond))

	for _, cmd := range []string{"set z = 4", "del y"} {
		index, err := c.Replicate(cmd, 10*time.Second)
		check(err)
		fmt.Printf("  committed %-12q at index %d on every live server\n", cmd, index)
	}
	printLogs(c)

	step("Losing the majority")
	majority := c.Size()/2 + 1
	for i := 0; i < c.Size() && liveCount(c) >= majority; i++ {
		if i != newLeader && c.Alive(i) {
			fmt.Printf("  crashing server %d\n", i)
			c.Kill(i)
		}
	}
	fmt.Printf("  %d of %d servers are left, but a majority is %d\n", liveCount(c), c.Size(), majority)

	_, index, err := c.Submit("set x = 99")
	check(err)
	fmt.Printf("  leader accepted \"set x = 99\" at index %d; waiting 2s to see if it commits...\n", index)
	time.Sleep(2 * time.Second)
	if len(c.Applied(newLeader)) >= index {
		fmt.Println("  ERROR: entry committed without a majority!")
		os.Exit(1)
	}
	fmt.Println("  not committed: without a majority Raft makes no progress (as it should)")
	printLogs(c)

	step("Done")
}

func liveCount(c *cluster.Cluster) int {
	live := 0
	for i := 0; i < c.Size(); i++ {
		if c.Alive(i) {
			live++
		}
	}
	return live
}

func printLogs(c *cluster.Cluster) {
	fmt.Println("  applied commands per server:")
	for i := 0; i < c.Size(); i++ {
		status := ""
		if !c.Alive(i) {
			status = "  (crashed)"
		}
		fmt.Printf("    server %d: %q%s\n", i, c.Applied(i), status)
	}
}

func step(format string, args ...interface{}) {
	fmt.Printf("\n== "+format+" ==\n", args...)
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
