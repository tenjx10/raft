# Raft in Go

An implementation of the [Raft consensus protocol](https://raft.github.io/raft.pdf) in Go, with a TCP transport and a demo that runs a live cluster.

Raft keeps a log of commands replicated across a group of servers. A command is committed once a majority of servers store it. After that, every server applies committed commands in the same order. The cluster keeps working as long as a majority of servers are up and can reach each other.

## What's implemented

Following Figure 2 of the extended Raft paper:

- **Leader election:** randomized election timeouts (300–600 ms), `RequestVote` RPCs, at most one vote per term, and the "candidate's log is at least as up-to-date" check (§5.2, §5.4.1).
- **Log replication:** `AppendEntries` RPCs that double as heartbeats (every 100 ms). Followers that fall behind are repaired by backing up `nextIndex` until the logs match (§5.3).
- **Commitment:** the leader advances `commitIndex` once a majority's `matchIndex` reaches an entry. It only counts entries from its own term (§5.4.2).
- **Term handling:** any RPC with a higher term turns the server back into a follower (§5.1).
- **Applying:** committed entries go to the application in order on a channel.

Not implemented: persistence to disk, log compaction/snapshots, and cluster membership changes.

## Layout

```
raft/raft.go              the Raft protocol
raft/transport.go         TCP transport (net/rpc): Server and Peer
raft/raft_test.go         tests that run real clusters on localhost
cmd/demo/                 one-process demo: election, replication, failover
cmd/raftnode/             run each server as its own process
internal/cluster/         helper that runs N servers in one process
```

## Running it

Requires Go 1.22+.

### Demo

```sh
go run ./cmd/demo          # 5 servers (use -n 3 for 3)
```

It starts a cluster on localhost and then:

1. waits for a leader to be elected
2. commits some commands and shows every server's applied log
3. crashes the leader, waits for a new one, and commits more commands
4. crashes servers until there's no majority left, and shows that new commands no longer commit

Sample output:

```
== Leader failure ==
  crashing leader (server 0)...
  server 1 took over as leader for term 2 after 405ms
  committed "set z = 4"  at index 4 on every live server
  committed "del y"      at index 5 on every live server
```

### One process per server

Open three terminals:

```sh
go run ./cmd/raftnode -id 0 -peers localhost:8000,localhost:8001,localhost:8002
go run ./cmd/raftnode -id 1 -peers localhost:8000,localhost:8001,localhost:8002
go run ./cmd/raftnode -id 2 -peers localhost:8000,localhost:8001,localhost:8002
```

Each server prints its role and term. Type a line into the leader's terminal and every server prints it once it commits. Press Ctrl-C in the leader's terminal to watch the others elect a new one.

### Tests

```sh
go test -race ./...
```

## Using the package

```go
peers := []raft.Peer{nil, raft.NewPeer("host1:8000"), raft.NewPeer("host2:8000")} // nil = this server
server, _ := raft.Listen("host0:8000")
applyCh := make(chan raft.ApplyMsg)
rf := raft.Make(peers, 0, applyCh)
server.Serve(rf)

index, term, isLeader := rf.Start("some command") // only succeeds on the leader
for msg := range applyCh {
	// msg.Command is committed at msg.CommandIndex; apply it to your state machine
}
```

Commands are sent with `encoding/gob`, so custom command types need a `gob.Register` call first.
