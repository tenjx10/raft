// Command raftnode runs a single Raft server as its own process, so a cluster
// can be spread across terminals (or machines). Lines typed on stdin are
// submitted to the log when this server is the leader.
//
// Start three servers in three terminals:
//
//	go run ./cmd/raftnode -id 0 -peers localhost:8000,localhost:8001,localhost:8002
//	go run ./cmd/raftnode -id 1 -peers localhost:8000,localhost:8001,localhost:8002
//	go run ./cmd/raftnode -id 2 -peers localhost:8000,localhost:8001,localhost:8002
//
// Then type a command into the leader's terminal, or Ctrl-C the leader and
// watch another server take over.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tenjx10/raft/raft"
)

func main() {
	id := flag.Int("id", -1, "this server's index into -peers")
	peerList := flag.String("peers", "", "comma-separated host:port of every server, in the same order on all servers")
	flag.Parse()

	addrs := strings.Split(*peerList, ",")
	if *peerList == "" || *id < 0 || *id >= len(addrs) {
		flag.Usage()
		os.Exit(2)
	}

	server, err := raft.Listen(addrs[*id])
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	peers := make([]raft.Peer, len(addrs))
	for i, addr := range addrs {
		if i != *id {
			peers[i] = raft.NewPeer(addr)
		}
	}

	applyCh := make(chan raft.ApplyMsg)
	rf := raft.Make(peers, *id, applyCh)
	server.Serve(rf)
	logf("server %d listening on %s (cluster of %d)", *id, server.Addr(), len(addrs))

	// print committed entries
	go func() {
		for msg := range applyCh {
			if msg.CommandValid {
				logf("applied index %d: %v", msg.CommandIndex, msg.Command)
			}
		}
	}()

	// print role and term changes
	go func() {
		lastTerm, wasLeader := -1, false
		for {
			term, isLeader := rf.GetState()
			if term != lastTerm || isLeader != wasLeader {
				role := "follower"
				if isLeader {
					role = "LEADER"
				}
				logf("term %d: %s", term, role)
				lastTerm, wasLeader = term, isLeader
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// submit stdin lines as commands
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmd := strings.TrimSpace(scanner.Text())
		if cmd == "" {
			continue
		}
		index, term, isLeader := rf.Start(cmd)
		if !isLeader {
			logf("not the leader; type commands into the leader's terminal")
			continue
		}
		logf("submitted %q at index %d (term %d)", cmd, index, term)
	}

	// stdin closed: keep serving until killed
	select {}
}

func logf(format string, args ...interface{}) {
	fmt.Printf("%s  %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}
