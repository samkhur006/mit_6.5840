package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int
	log         []int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	status string

	count int

	electionTimer    time.Duration
	lastElectionTime time.Time

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.status == "leader"
	fmt.Printf("GETSTATE,me: %d, term: %d, isleader: %t\n", rf.me, term, isleader)
	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("[%d]AppendEntries from %d at %v\n", rf.me, args.LeaderId, time.Now())

	rf.lastElectionTime = time.Now()
	rf.electionTimer = time.Duration(rand.Int63()%7+10) * time.Millisecond

	if rf.currentTerm < args.Term {
		rf.votedFor = -1
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term
}

func (rf *Raft) Heartbeat() {
	fmt.Printf("!!!!!! [%d]Heartbeat started at %v\n", rf.me, time.Now())
	for rf.status == "leader" {
		count := 1
		if rf.killed() {
			fmt.Printf("!!!!!! [%d]Heartbeat killed at %v\n", rf.me, time.Now())
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(idx int, count *int) {
					reply := &AppendEntriesReply{}
					args := &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					ok := rf.peers[idx].Call("Raft.AppendEntries", args, reply)
					if !ok {
						return
					}
					if reply.Term > 0 {
						*count++
					}
				}(i, &count)
			}
		}
		time.Sleep(2 * time.Millisecond)
		if count <= len(rf.peers)/2 {
			fmt.Printf("!!!!!! [%d]Heartbeat failed at %v\n", rf.me, time.Now())
			rf.status = "candidate"
			return
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		fmt.Printf("%d is not1 giving vote to %d\n", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId || rf.currentTerm < args.Term {
		fmt.Printf("%d is giving vote to %d at %v\n", rf.me, args.CandidateId, time.Now())
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastElectionTime = time.Now()
		return
	}
	fmt.Printf("%d is not giving vote to %d\n", rf.me, args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	initTime := time.Now()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}
	fmt.Printf("[%d]RequestVote to %d received at %v, took %v\n", rf.me, server, time.Now(), time.Since(initTime))
	return reply.VoteGranted
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	rf.electionTimer = time.Duration(rand.Int63()%7+10) * time.Millisecond
	// if rf.votedFor != -1 {
	// 	rf.mu.Unlock()
	// 	return
	// }
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.status = "candidate"
	rf.count = 1
	fmt.Printf("New term for %d is %d\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  -1,
	}
	if len(rf.log) > 0 {
		args.LastLogTerm = rf.log[len(rf.log)-1]
	}
	// todo(sambhav): parallelize
	//todo(sambhav): shayad issue is here. might be waiting for crashed servers
	//add conditional var maybe
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(idx int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(idx, args, reply) {
					fmt.Printf("[%d]      Appending count to %d\n", rf.me, rf.count+1)
					rf.count++
				}
			}(i)
		}
	}
	time.Sleep(2 * time.Millisecond)
	rf.lastElectionTime = time.Now()
	fmt.Printf("[%d] GOT %d votes\n", rf.me, rf.count)
	if rf.count > len(rf.peers)/2 {
		rf.mu.Lock()
		rf.status = "leader"
		fmt.Printf("!!!!!! [%d]BECAME LEADER at %v\n", rf.me, time.Now())
		rf.mu.Unlock()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			rf.Heartbeat()
		}()
		wg.Wait()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		fmt.Printf("[%d] TICKER at time %v\n", rf.me, time.Now())

		// Your code here (3A)
		// Check if a leader election should be started.
		if time.Since(rf.lastElectionTime) > rf.electionTimer {
			fmt.Printf("		STARTING ELECTION, me: %d, status: %s\n, lastElectionTime: %v, electionTimer: %v\n", rf.me, rf.status, rf.lastElectionTime, rf.electionTimer)
			rf.startElection()
		} else {
			fmt.Printf("[%d]Not starting election, time since last election: %v\n", rf.me, time.Since(rf.lastElectionTime))
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{votedFor: -1, currentTerm: 0, log: make([]int, 0), commitIndex: 0, lastApplied: 0, nextIndex: make([]int, len(peers)), matchIndex: make([]int, len(peers)),
		status:           "candidate",
		electionTimer:    time.Duration(rand.Int63()%7+10) * time.Millisecond,
		lastElectionTime: time.Now()}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// if me == 0 {
	// 	rf.status = "leader"
	// }
	fmt.Printf("INIT, me: %d, status: %s\n, lastElectionTime: %v, electionTimer: %v\n", me, rf.status, rf.lastElectionTime, rf.electionTimer)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
