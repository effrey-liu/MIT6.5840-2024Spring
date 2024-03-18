package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	// "log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

type Entry struct {
	Term int
	Cmd  interface{}
}

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)
const HeartBeatTime int = 113

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	// (Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	// for requestVote & heartbeats
	role      Role
	voteCnt   int
	timeStamp time.Time  // Record the time when the request was last received. when this time out, re-elect
	voteMutex sync.Mutex //protect when vote

	// for log(lab3B)
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// // Your code here (3A).
	term = rf.currentTerm
	isleader = (rf.role == Leader)
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
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry (§5.4)
	LastLogTerm  int // term of candidate's last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// Invoked by candidates to gather votes (§5.2).

	// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log,
	// 		grant vote (§5.2, §5.4)
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		// 1. Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = false
		DPrintf("server %d refuced vote for Candidate %d because its term less than mine", rf.me, args.CandidateId)
		return
	}
	if args.Term > rf.currentTerm {
		// if Candidate's term is larger than mine, then previous voteFor is no longer counting
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log) - 1].Term ||
			(args.LastLogIndex >= len(rf.log)-1 && args.LastLogTerm >= rf.log[len(rf.log)-1].Term) {
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.role = Follower
			rf.timeStamp = time.Now()

			rf.mu.Unlock()
			reply.VoteGranted = true
			DPrintf("server %d voted for Candidate %d", rf.me, args.CandidateId)
			return
		}

	} else {
		DPrintf("server %d refused to vote for Candidate %d", rf.me, args.CandidateId)
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntryArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader's commitIndex
}

type AppendEntryReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	rf.timeStamp = time.Now()
	// rf.log[args.leaderCommit].Term = args.lead
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	if args.Entries == nil { // empty for heartbeat;
		DPrintf("server %d received leader %d heartbeats\n", rf.me, args.LeaderId)
	} else {
		DPrintf("server %d received leader %d AppendEntries\n", rf.me, args.LeaderId)
	}
	
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// 	  delete the existing entry and all that follow it (§5.3)
	if len(args.Entries) != 0 && len(rf.log) > args.PrevLogIndex+1 && rf.log[args.PrevLogIndex + 1].Term != args.Entries[0].Term {
		/*
			leader: 1 2 5
			s1	  : 1 2 3 4
			make s1 beacome: 1 2
			and then in rule 4, append 5 into s1
		*/
		DPrintf("violate the rule3, delete the existing entry and all that follow it\n")
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	// 4. Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)

	reply.Success = true
	reply.Term = rf.currentTerm

	// To implement heartbeats, define an AppendEntries RPC struct (though you may not need all the arguments yet),
	// and have the leader send them out periodically. Write an AppendEntries RPC handler method.

	if args.LeaderCommit > rf.commitIndex { // If leaderCommit > commitIndex,
		// set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(sendTo int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[sendTo].Call("Raft.AppendEntries", args, reply)
	return ok
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
	// index := -1
	// term := -1
	// isLeader := false

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		// log.Printf("I'm not Leader!\n")
		return -1, -1, false
	}
	newEntry := Entry{Term: rf.currentTerm, Cmd: command}
	rf.log = append(rf.log, newEntry)
	// log.Printf("I'm Leader, but len(rf.log) = %d\n", len(rf.log))
	return len(rf.log) - 1, rf.currentTerm, true
}

func (rf *Raft) CommitChecker() {
	// check if there are new Msg
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- msg
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
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

func (rf *Raft) getVoteAnswer(sendTo int, args *RequestVoteArgs) bool {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(sendTo, args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		// if other server send requestVote before mine, return false
		return false
	}

	if reply.Term > rf.currentTerm {
		// old term
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	return reply.VoteGranted
}

func (rf *Raft) handleAppendEntries(sendTo int, args *AppendEntryArgs) {
	appendReply := &AppendEntryReply{}
	ok := rf.sendAppendEntries(sendTo, args, appendReply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.currentTerm != appendReply.Term {
		// if other server send appendReply before mine, return false
		return
	}

	if appendReply.Success { // update nextIndex and matchIndex
		rf.matchIndex[sendTo] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[sendTo] = rf.matchIndex[sendTo] + 1

		// If there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4).

		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			cnt := 1
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
					cnt++
				}
			}

			if cnt > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
		}

		return
	}

	
	if rf.currentTerm < appendReply.Term {
		// become old leader, update and become to Follower
		rf.currentTerm = appendReply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.timeStamp = time.Now()
		return
	}

	// if get a rejection as leader.
	if rf.currentTerm == args.Term && rf.role == Leader {
		// After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
		// this can be optimized as paper said:
		// If desired, the protocol can be optimized to reduce the number of rejected AppendEntries RPCs.
		// For example, when rejecting an AppendEntries request,
		// the follower can include the term of the conflicting entry and the first index it stores for that term.
		// With this information, the leader can decrement nextIndex to bypass all of the conflicting entries in that term;
		// one AppendEntries RPC will be required for each term with conflicting entries, rather than one RPC per entry.
		// In practice, we doubt this optimization is necessary,
		// since failures happen infrequently and it is unlikely that there will be many inconsistent entries.
		rf.nextIndex[sendTo]--
		return
	}
}

func (rf *Raft) SendHeartBeats() {
	DPrintf("server %v start sending heartbeats\n", rf.me)

	for !rf.killed() {
		rf.mu.Lock()
		// if the server is dead or is not the leader, just return
		if rf.role != Leader {
			rf.mu.Unlock()
			// if not leader any more, stop send heartbeats
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := &AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i] - 1].Term,
				LeaderCommit: rf.commitIndex,
			}
			if len(rf.log)-1 >= rf.nextIndex[i] {
				args.Entries = rf.log[rf.nextIndex[i]:]
				DPrintf("detect entries need to be appended\n")
			} else {
				args.Entries = make([]Entry, 0)
				// args.Entries = nil
				DPrintf("Just for Heartbeat\n")
			}

			go rf.handleAppendEntries(i, args)
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartBeatTime) * time.Millisecond)
	}
}

func (rf *Raft) collectVotes(sendTo int, args *RequestVoteArgs) {
	voteAnswer := rf.getVoteAnswer(sendTo, args)
	if !voteAnswer {
		return
	}
	rf.voteMutex.Lock()
	if rf.voteCnt > len(rf.peers)/2 {
		rf.voteMutex.Unlock()
		return
	}
	rf.voteCnt++
	if rf.voteCnt > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.role == Follower {
			// there is another voting goroutine received updated term and changed this server to Follower
			rf.mu.Unlock()
			rf.voteMutex.Unlock()
			return
		}
		rf.role = Leader
		// if be elected as leader after election, reinitialized nextIndex&matchIndex
		// (Reinitialized after election) in figure 2.
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		go rf.SendHeartBeats()
	}
	rf.voteMutex.Unlock()
}

func (rf *Raft) Election() {
	rf.mu.Lock()

	rf.currentTerm += 1
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.voteCnt = 1
	rf.timeStamp = time.Now()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			DPrintf("Raft %d: vote myself", server)
			continue
		}
		go rf.collectVotes(server, args)
	}
}

func (rf *Raft) ticker() {
	// rd := rand.New(rand.NewSource(int64(rf.me)))
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// If a follower receives no communication over a period of time called the **election timeout**,
		// then it assumes there is no viable leader and begins an election to choose a new leader.
		randomElectionTimeout := rand.Int63()%51 + 250
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.timeStamp) > time.Duration(randomElectionTimeout)*time.Millisecond {
			// start Election
			go rf.Election()
		}
		rf.mu.Unlock()
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timeStamp = time.Now()
	rf.role = Follower
	rf.applyCh = applyCh

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.CommitChecker()

	return rf
}
