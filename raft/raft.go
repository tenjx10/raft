package raft

// Package raft implements the Raft consensus protocol (leader election and
// log replication) as described in Figure 2 of the extended Raft paper.
//
//	rf := Make(peers, me, applyCh)
//	  Create a new Raft server.
//	rf.Start(command) (index, term, isLeader)
//	  Start agreement on a new log entry.
//	rf.GetState() (term, isLeader)
//	  Ask a Raft peer for its current term, and whether it thinks it is leader.
//	ApplyMsg
//	  Each time a new entry is committed to the log, each Raft peer
//	  sends an ApplyMsg on applyCh.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Peer is an RPC endpoint for another Raft server. Call invokes the named
// method (e.g. "Raft.RequestVote") and returns false if the request or reply
// was lost, or the server could not be reached. See transport.go for a TCP
// implementation built on net/rpc.
type Peer interface {
	Call(method string, args interface{}, reply interface{}) bool
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer sends an ApplyMsg to the service on the same
// server, via the applyCh passed to Make(). CommandValid is true when
// the ApplyMsg contains a newly committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// creating a struct for the log entry that will house a command and term
type LogEntry struct {
	Command interface{}
	Term    int
}

// using information from piazza post
// creating a type for state
type State int

// the three types of state
const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex // Lock to protect shared access to this peer's state
	peers []Peer     // RPC end points of all peers
	me    int        // This peer's index into peers[]
	dead  int32      // Set by Kill()

	// the arguments in the state section
	currentTerm           int
	votedFor              int
	log                   []LogEntry // created a struct for this as per hint
	commitIndex           int
	lastApplied           int
	nextIndex             []int
	matchIndex            []int
	state                 State
	lastContactFromLeader time.Time
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// add locks to prevent data race
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// get the current term
	term = rf.currentTerm

	// check if current server is leader
	isleader = (rf.state == Leader) //(no if statement; cleaner)

	return term, isleader
}

// RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// the arguments in the requestVotes RPC section under arguments
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// same as above but now under Results
	Term        int
	VoteGranted bool
}

// creating the AppendEntriesArgs struct
type AppendEntriesArgs struct {
	// the argument ing the append entries rpc section
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// creating the AppendEntriesReply struct
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// locking
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rule: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		// set the current term to args term
		rf.currentTerm = args.Term

		// convert the state to follower
		rf.state = Follower

		// set voted for to null (-1)
		rf.votedFor = -1
	}

	// rule 1: Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		// set the replay term to the current term
		reply.Term = rf.currentTerm

		// set vote granted to false
		reply.VoteGranted = false

		// just return
		return
	}

	// figure out this server's current last log index
	lastIndex := len(rf.log)

	// set current last log term to 0, and keep as 0 if length of log is 0
	lastTerm := 0

	// the log is not 0
	if lastIndex > 0 {
		// set term to the term of last index
		lastTerm = rf.log[lastIndex-1].Term
	}

	// rule 2: If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote(§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && ((args.LastLogTerm > lastTerm) || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) {
		// set vote to true
		reply.VoteGranted = true

		// store who server voted for
		rf.votedFor = args.CandidateId

		// update the term
		reply.Term = rf.currentTerm

		// reset timer
		rf.lastContactFromLeader = time.Now()

	} else {
		// otw set vote to false
		reply.VoteGranted = false

		// update the term
		reply.Term = rf.currentTerm
	}
}

// create AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// create a lock for data race prevention
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// all server rule: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		// set the current term to args term
		rf.currentTerm = args.Term

		// convert the state to follower
		rf.state = Follower

		// set voted for to null (-1)
		rf.votedFor = -1
	}

	// rule 1: Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {

		// set reply term to current term
		reply.Term = rf.currentTerm

		// set success to false
		reply.Success = false

		return
	}

	// candidate rule: f AppendEntries RPC received from new leader: convert to follower
	rf.state = Follower

	// reset the timer
	rf.lastContactFromLeader = time.Now()

	// rule 2: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// check if we even have an entry at prevLogIndex then check if the term matches
	if args.PrevLogIndex > len(rf.log) || (args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {

		// chnage the current term
		reply.Term = rf.currentTerm

		// set success to false
		reply.Success = false

		return
	}

	// rule 3: If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// check if entries is not empty
	if len(args.Entries) > 0 {

		// for each entry
		for i := 0; i < len(args.Entries); i++ {

			// figure out where this entry should go in the log
			idx := args.PrevLogIndex + 1 + i

			// see if we have an entry at this position with a conflicting term
			if idx <= len(rf.log) && rf.log[idx-1].Term != args.Entries[i].Term {

				// delete this entry and all that follow
				rf.log = rf.log[:idx-1]

				// break out the loop
				break
			} else if idx > len(rf.log) { // no more entrices to check so break out
				break
			}

		}

	}

	// rule 4: Append any new entries not already in the log
	// check if entries is not empty
	if len(args.Entries) > 0 {

		// for each entry
		for i := 0; i < len(args.Entries); i++ {

			// if this entry is beyond our current log (we don't have it yet)
			if args.PrevLogIndex+1+i > len(rf.log) {

				// append entry to log
				rf.log = append(rf.log, args.Entries[i])
			}
		}
	}

	// rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		// set the commit index to the minimum between leader commit and index of last new entry
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}

	// upadte the term
	reply.Term = rf.currentTerm

	// reply is a success
	reply.Success = true
}

// send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Call() returns false if the server is dead or unreachable, or if the
// request or reply was lost; it always returns after a timeout.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// send a AppenEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// create and set the lock and unlock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if this server is not a leader
	if rf.state != Leader {

		// set isLeader to false
		isLeader = false

		// return
		return index, term, isLeader
	}

	// otw

	// figure out index, which length of log plus 1
	index = len(rf.log) + 1

	//get current term
	term = rf.currentTerm

	// create an log entry and append that into the log
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})

	return index, term, isLeader
}

// Kill stops this peer's background goroutines (ticker, heartbeats, apply).
// Long-running loops check killed() to know when to exit. The use of atomic
// avoids the need for a lock.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// applying the commited entries to the state machine through apply channel
func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		// sleep for a bit
		time.Sleep(10 * time.Millisecond)

		// lock
		rf.mu.Lock()

		// iterates entries if multiple instead of waiting 10 seconds
		// rules for servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
		for rf.lastApplied < rf.commitIndex {
			// increment lastApplied
			rf.lastApplied++

			// make a message
			msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-1].Command, CommandIndex: rf.lastApplied}

			// send message through channel without a lock
			rf.mu.Unlock()

			// pass the message through the apply message channel
			applyCh <- msg

			// without this lock, the script is not ending, I'm assuming it is deadlocking without it
			rf.mu.Lock()
		}

		// unlock
		rf.mu.Unlock()
	}
}

// function to replicate the logs in the followers
func (rf *Raft) sendEntries(followerIdx int, currentTerm int, currentCommitIndex int) {
	// create a lock
	rf.mu.Lock()

	// figure out the index of the next entry I need to send to follower
	idx := rf.nextIndex[followerIdx]

	// keep track of the previous log index
	prevLogIndex := idx - 1

	//fmt.Printf("Server %d sending to follower %d: nextIdx=%d, prevLogIdx=%d\n", rf.me, followerIdx, idx, prevLogIndex)

	// set previous log term to 0
	prevLogTerm := 0

	// if the previous log index is not 0 and is within the log
	if prevLogIndex > 0 && prevLogIndex <= len(rf.log) {
		// set the previous term to the term of the entry at prevLogIndex
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}

	// initial an []LogEntry variable for entries
	var entries []LogEntry

	// leader rule: If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex

	// if follower is behind, send them the missing entries, otw it will be nil and send empty heartbeat
	if idx > 0 && idx <= len(rf.log) {
		// loop through the missing entries
		for i := idx - 1; i < len(rf.log); i++ {
			// append to entries
			entries = append(entries, rf.log[i])
		}
	}

	// now pass in the arguments in append entries arguemtns function
	args := AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: currentCommitIndex}

	//fmt.Printf("Server %d sending %d entries to follower %d\n", rf.me, len(entries), followerIdx)

	// unlock
	rf.mu.Unlock()

	// create a reply append entries
	reply := AppendEntriesReply{}

	// make append entrices
	err := rf.sendAppendEntries(followerIdx, &args, &reply)

	// check for issues
	if !err {
		//fmt.Printf("Server %d: RPC to follower %d FAILED\n", rf.me, followerIdx)
		return
	}

	//fmt.Printf("Server %d: Reply from %d - success=%v, term=%d\n", rf.me, followerIdx, reply.Success, reply.Term)

	// lock
	rf.mu.Lock()

	// if the reply has higher term, become follower
	if reply.Term > rf.currentTerm {

		// set term to reply term
		rf.currentTerm = reply.Term

		// set state to follower
		rf.state = Follower

		// set voted for to null (-1)
		rf.votedFor = -1

		// unlcok
		rf.mu.Unlock()

		return
	}

	// if not the leader anymore or the term is different, jsut return
	if rf.state != Leader || rf.currentTerm != currentTerm {
		// deadlocking here, adding unlocking here
		rf.mu.Unlock()

		return
	}

	// If successful: update nextIndex and matchIndex for follower (§5.3)
	if reply.Success {
		// calculate the index of the last entry the follower now has
		temp := prevLogIndex + len(entries)

		//fmt.Printf("RACE CHECK: Server %d - UPDATING nextIndex[%d] from %d to %d\n", rf.me, followerIdx, rf.nextIndex[followerIdx], temp + 1)

		// if this is new information, we update
		if temp > rf.matchIndex[followerIdx] {
			// store that the follower has entries up to index newMatchIndex
			rf.matchIndex[followerIdx] = temp

			// update nextIndex, so we know which entry to send next time
			rf.nextIndex[followerIdx] = temp + 1
		}

		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).

		// go through all the commit indices to find such an N exists
		for i := rf.commitIndex + 1; i <= len(rf.log); i++ {
			// see if the terms match; only commmit entries from the current term (safety requirement from §5.4.2)
			if rf.log[i-1].Term == rf.currentTerm {
				// figure out how many servers have replicated this entry
				cnt := 1 // leader already up to date

				// loop through the followers
				for j := range rf.peers {
					// if you are not a leader and server j has entry i
					if j != rf.me && rf.matchIndex[j] >= i {
						// increment count
						cnt++
					}
				}

				//fmt.Printf("Count is %d", cnt)

				// if there is a majority
				if cnt > len(rf.peers)/2 {
					// set commit index to i
					rf.commitIndex = i
				}
			}
		}
	} else { // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
		// making sure nextIndex is not 1
		if rf.nextIndex[followerIdx] > 1 {
			// decrement nextIndex (leader will try again after)
			rf.nextIndex[followerIdx]--

			//fmt.Printf("The new next index is %d", rf.nextIndex[followerIdx])
		}
	}

	// unlock
	rf.mu.Unlock()
	//fmt.Printf("Server %d: sendEntries DONE for follower %d\n", rf.me, followerIdx)
}

// creating a function for heartbeat, listening to raft structure advice
func (rf *Raft) heartBeat() {
	for rf.killed() == false {
		// send 10 heartbeats per second
		time.Sleep(100 * time.Millisecond)

		// lock for data race prevention
		rf.mu.Lock()

		// check if you are the leader
		isLeader := (rf.state == Leader)

		// get the current term
		currentTerm := rf.currentTerm

		// get the current commit index and lock it too
		currentCommitIndex := rf.commitIndex

		// unlcok
		rf.mu.Unlock()

		// if we are not leader then we don't send heart beat
		if !isLeader {
			continue
		}

		// send heartbeats to the followers
		for i := range rf.peers {
			// check if you are you this iteration, if so, skip
			if i == rf.me {
				continue
			}

			//fmt.Printf("HEARTBEAT: Launching sendEntries to follower %d at time %v\n", i, time.Now())

			// call helper function to send apprend entries to followers
			go rf.sendEntries(i, currentTerm, currentCommitIndex)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// be started and to randomize sleeping time using
		// time.Sleep().

		// sleep for a bit
		time.Sleep(10 * time.Millisecond)

		// lock here, shared variabels
		rf.mu.Lock()

		// random election timeout between the doubled time range from the raft paper recommendation
		electionTimeout := time.Duration(300+rand.Intn(300)) * time.Millisecond

		// check if you are not a leader and election timeout has elapsed (using Since now)
		needElection := (rf.state != Leader && time.Since(rf.lastContactFromLeader) > electionTimeout)

		// unlcok
		rf.mu.Unlock()

		// if we don't need an election just skip
		if !needElection {
			continue
		}

		// lock the shared vars in the election
		rf.mu.Lock()

		// increase term by 1
		rf.currentTerm++

		// server becomes a candidate
		rf.state = Candidate

		// vote for yourself
		rf.votedFor = rf.me

		// reset the timer
		rf.lastContactFromLeader = time.Now()

		// get and store the current term value
		currentTerm := rf.currentTerm

		// unlock
		rf.mu.Unlock()

		// create a lock for the votes, running into issues without it
		var vMu sync.Mutex

		// create a votes variabkle and set to 1
		votes := 1

		// loop through all peers, so we can send vote requests
		for i := range rf.peers {
			// if it is itself, just continue
			if i == rf.me {
				continue
			}

			// create go routine to send request Votes
			go func(idx int) {
				// lock
				rf.mu.Lock()

				// get the last log index
				lastLogIndex := len(rf.log)

				// set previous term to 0, and change it if last log index is not 0
				lastLogTerm := 0

				// if last log index not 0
				if lastLogIndex > 0 {
					// set the term to the term of the index
					lastLogTerm = rf.log[lastLogIndex-1].Term
				}

				// unlock
				rf.mu.Unlock()

				// create the argument for request vote
				args := RequestVoteArgs{Term: currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}

				// create the reply for request vote
				reply := RequestVoteReply{}

				// make a requestVote
				err := rf.sendRequestVote(idx, &args, &reply)

				// check if nothing went wrong
				if !err {
					// not sure; might have to change this**
					return
				}

				// lock the check to see if we are in right term
				rf.mu.Lock()
				inCorrectTerm := (rf.currentTerm == currentTerm)
				rf.mu.Unlock()

				// check that the term hasn't changed since sending the RPC
				if !inCorrectTerm {
					return
				}

				// if reply has higher term, become follower
				if reply.Term > currentTerm {
					// put a lock on the shared states
					rf.mu.Lock()

					// set the current term to the reply
					rf.currentTerm = reply.Term

					// chnage the state to a follower
					rf.state = Follower

					// set voted for to -1 meaning null
					rf.votedFor = -1

					// unlock the lcok
					rf.mu.Unlock()

					return
				}

				// if we got a vote, increase the number of votes
				if reply.VoteGranted {
					// use a lock on teh votes so we don't have any potential data race here
					vMu.Lock()
					// increment by 1
					votes++

					// the current total number of votes
					totalVotes := votes

					// unlock
					vMu.Unlock()

					// candidate rule: If votes received from majority of servers: become leader
					if totalVotes >= len(rf.peers)/2+1 {
						// locking the state
						rf.mu.Lock()

						// check if it is still candidate
						if rf.state == Candidate {
							// promote to leader
							rf.state = Leader

							// leader rule: Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
							for x := range rf.nextIndex {
								// set to length of log
								rf.nextIndex[x] = len(rf.log) + 1 // plus one sence we want avaliable index

								// set to 0
								rf.matchIndex[x] = 0
							}
						}

						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
}

// Make creates a Raft server. The endpoints of all the Raft servers
// (including this one) are in peers[]; this server is peers[me], which is
// never called and may be nil. All the servers' peers[] arrays have the same
// order. Committed entries are delivered as ApplyMsg values on applyCh.
// Make returns quickly; long-running work happens in goroutines.
func Make(peers []Peer, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// recommended by the paper
	// initialized to 0 on first boot
	rf.currentTerm = 0

	// null if none
	rf.votedFor = -1

	// empty log
	rf.log = make([]LogEntry, 0)

	// initialized to 0
	rf.commitIndex = 0

	// initialized to 0
	rf.lastApplied = 0

	// initalize next index and match index just in case if server becomes a leader
	// we'll have to reinitialize them when an election starts
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize state as follower
	rf.state = Follower
	rf.lastContactFromLeader = time.Now()

	// start ticker goroutine to start elections.
	go rf.ticker()

	// start heartbeats
	go rf.heartBeat()

	// start applying committed entries
	go rf.apply(applyCh)

	return rf
}
