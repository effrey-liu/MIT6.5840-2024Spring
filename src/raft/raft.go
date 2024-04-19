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
	"bytes"
	// "log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
const HeartbeatTime int = 113

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	voteTimer      *time.Timer	// for lab4A time test
	heartbeatTimer *time.Timer

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
	applyCond *sync.Cond

	// for compaction(lab3D)
	// Even when the log is trimmed, your implemention still needs to properly send the term and index of the entry prior to new entries in AppendEntries RPCs;
	// this may require saving and referencing the latest snapshot's lastIncludedTerm/lastIncludedIndex (consider whether this should be persisted).
	snapshot          []byte
	lastIncludedIndex int // highest index of log
	lastIncludedTerm  int // highest Term of log
}

func (rf *Raft) ResetVoteTimer() {
	randomElectionTimeout := rand.Int63()%51 + 250
	rf.voteTimer.Reset(time.Duration(randomElectionTimeout) * time.Millisecond)
}

func (rf *Raft) ResetHeartbeatTimer(timeStamp int) {
	rf.heartbeatTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}


// accounding to global Index is increasing,
// get Real log Index by using this func.
func (rf *Raft) RealLogIndex(gIdx int) int {
	return gIdx - rf.lastIncludedIndex
}

// use real log index to get Global Index by using this func.
func (rf *Raft) GlobalIndex(rIdx int) int {
	return rIdx + rf.lastIncludedIndex
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// lab3C
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	// lab3D
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var votedFor int
	var currentTerm int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("readPersist failed\n")
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		DPrintf("server %v refuesd Snapshot request, its index=%v, self commitIndex=%v, lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
		return
	}

	DPrintf("server %v agreed Snapshot request, its index=%v, self commitIndex=%v, original lastIncludedIndex=%v,  after Snapshot, lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex, index)

	rf.snapshot = snapshot
	rf.lastIncludedTerm = rf.log[rf.RealLogIndex(index)].Term
	// Then make Snapshot(index) discard the log before index,
	rf.log = rf.log[rf.RealLogIndex(index):]
	// and set X equal to index
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.persist()
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	rf.snapshot = data
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
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// 1. Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("server %d refuced vote for Candidate %d because its term less than mine", rf.me, args.CandidateId)
		return
	}
	if args.Term > rf.currentTerm {
		// if Candidate's term is larger than mine, then previous voteFor is no longer counting
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogIndex >= rf.GlobalIndex(len(rf.log)-1) && args.LastLogTerm >= rf.log[len(rf.log)-1].Term) {
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.role = Follower
			// rf.timeStamp = time.Now()
			rf.ResetVoteTimer()
			rf.persist()
			reply.VoteGranted = true
			DPrintf("server %d voted for Candidate %d", rf.me, args.CandidateId)
			return
		}
	} else {
		DPrintf("server %d refused to vote for Candidate %d", rf.me, args.CandidateId)
	}

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
	XTerm   int  // term in the conflicting entry (if any)
	XIndex  int  // index of first entry with that term (if any)
	XLen    int  // log length
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.ResetVoteTimer()
	// rf.timeStamp = time.Now()
	// rf.log[args.leaderCommit].Term = args.lead
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	isConflict := false

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// outdated RPC, its's PrevLogIndex < lastIncludedIndex
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	} else if args.PrevLogIndex >= rf.GlobalIndex(len(rf.log)) {
		reply.XTerm = -1
		reply.XLen = rf.GlobalIndex(len(rf.log))
		isConflict = true
		// DPrintf("server%d's log doesn't exist log entry in PrevLogIndex %d, LogLen is %d\n", rf.me, args.PrevLogIndex, reply.LogLen)
	} else if rf.log[rf.RealLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.RealLogIndex(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex && rf.log[rf.RealLogIndex(i)].Term == reply.XTerm {
			i--
		}
		reply.XIndex = i + 1
		reply.XLen = rf.GlobalIndex(len(rf.log))
		isConflict = true
	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// 	  delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	for idx, log := range args.Entries {
		ridx := rf.RealLogIndex(args.PrevLogIndex) + 1 + idx
		if ridx < len(rf.log) && rf.log[ridx].Term != log.Term {
			// some positions in log conflict, cover all log entries from this ridx.
			DPrintf("violate the rule3, delete the existing entry and all that follow it\n")
			rf.log = rf.log[:ridx]
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		} else if ridx == len(rf.log) {
			// no conflicts, but log's len is bigger, put together.
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}

	rf.persist()

	reply.Success = true
	reply.Term = rf.currentTerm

	// To implement heartbeats, define an AppendEntries RPC struct (though you may not need all the arguments yet),
	// and have the leader send them out periodically. Write an AppendEntries RPC handler method.

	if args.LeaderCommit > rf.commitIndex { // If leaderCommit > commitIndex,
		// set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.GlobalIndex(len(rf.log)-1))))
		rf.applyCond.Signal()
	}
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
	rf.persist()
	// rf.timeStamp = time.Now()

	defer func() {
		rf.ResetHeartbeatTimer(10)
	}()

	return rf.GlobalIndex(len(rf.log) - 1), rf.currentTerm, true
}

func (rf *Raft) CommitChecker() {
	// check if there are new Msg
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		msgBuf := make([]*ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		tmpApplied := rf.lastApplied
		for rf.commitIndex > tmpApplied {
			tmpApplied += 1
			if tmpApplied <= rf.lastIncludedIndex {
				// tmpApplied may be log entries that have been truncated in snapShot,
				// and these log entries do not need to be sent again.
				continue
			}
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.RealLogIndex(tmpApplied)].Cmd,
				CommandIndex: tmpApplied,
				SnapshotTerm: rf.log[rf.RealLogIndex(tmpApplied)].Term,
			}

			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()
		// time.Sleep(time.Duration(10) * time.Millisecond)

		// Note that InstallSnapShot may appear again after unlocking and then modify rf.lastApplied
		for _, msg := range msgBuf {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}

			rf.mu.Unlock()
			// Note that InstallSnapShot may appear again after unlocking and then modify rf.lastApplied
			rf.applyCh <- *msg

			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}
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

type InstallSnapshotArgs struct {
	Term              int         // leader's termleaderId so follower can redirect clients
	LeaderId          int         // so follower can redirect clients
	LastIncludedIndex int         // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int         // term of lastIncludedIndexoffset byte offset where chunk is positioned in the snapshot file
	Data              []byte      // raw bytes of the snapshot chunk, starting at offset
	Done              bool        // true if this is the last chunk
	LastIncludedCmd   interface{} // added for Snapshot.
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// Invoked by leader to send chunks of a snapshot to a follower.
// Leaders always send chunks in order.
func (rf *Raft) sendInstallSnapshot(sendTo int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[sendTo].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer func() {
		// rf.timeStamp = time.Now()
		rf.mu.Unlock()
	}()
	// 1. Reply immediately if term < currentTerm(old leader, refuse)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.role = Follower
	rf.ResetVoteTimer()
	// rf.timeStamp = time.Now()

	if args.LastIncludedIndex < rf.lastIncludedIndex || args.LastIncludedIndex < rf.commitIndex {
		// 1. 快照反而比当前的 lastIncludedIndex 更旧, 不需要快照
		// 2. 快照比当前的 commitIndex 更旧, 不能安装快照
		reply.Term = rf.currentTerm
		return
	}

	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index

	// 6. If existing log entry has same index and term as snapshot's last included entry,
	//	  retain log entries following it and reply.
	hasEntry := false
	rIdx := 0
	for ; rIdx < len(rf.log); rIdx++ {
		if rf.GlobalIndex(rIdx) == args.LastIncludedIndex && rf.log[rIdx].Term == args.LastIncludedTerm {
			hasEntry = true
			break
		}
	}

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if hasEntry {
		rf.log = rf.log[rIdx:]
	} else {
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{Term: rf.lastIncludedTerm, Cmd: args.LastIncludedCmd})
	}
	// 7. Discard the entire log
	// 8. Reset state machine using snapshot contents (and load snapshot's cluster configuration)
	rf.snapshot = args.Data
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	reply.Term = rf.currentTerm
	rf.applyCh <- msg
	rf.persist()
}

func (rf *Raft) handleInstallSnapshot(sendTo int) {
	reply := InstallSnapshotReply{}
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
		LastIncludedCmd:   rf.log[0].Cmd,
	}
	rf.mu.Unlock()
	// After the parameters are filled in determination, there is no need to lock when sending RPC
	ok := rf.sendInstallSnapshot(sendTo, &args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		// rf.timeStamp = time.Now()
		rf.ResetVoteTimer()
		rf.persist()
		return
	}

	// rf.nextIndex[sendTo] = rf.GlobalIndex(1)
	// LastIncludedIndex may include log entries that have not been copied yet, which do not need to be copied.
	if rf.matchIndex[sendTo] < args.LastIncludedIndex {
		rf.matchIndex[sendTo] = args.LastIncludedIndex
	}
	rf.nextIndex[sendTo] = rf.matchIndex[sendTo] + 1
}

func (rf *Raft) handleAppendEntries(sendTo int, args *AppendEntryArgs) {
	appendReply := &AppendEntryReply{}
	ok := rf.sendAppendEntries(sendTo, args, appendReply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != appendReply.Term || rf.role != Leader {
		// add role judgement in lab4 becasue `test_test.go:382: history is not linearizable` error

		// if other server send appendReply before mine, return false
		return
	}

	if appendReply.Success { // update nextIndex and matchIndex
		// rf.matchIndex[sendTo] = args.PrevLogIndex + len(args.Entries)
		// rf.nextIndex[sendTo] = rf.matchIndex[sendTo] + 1

		// if be elected as leader after election, reinitialized nextIndex&matchIndex.
		// plus: because of Lab3D, if InstallSnapshot happened before, rf.matchIndex&NextIndex could be set larger.
		newMatchIdx := args.PrevLogIndex + len(args.Entries)
		if newMatchIdx > rf.matchIndex[sendTo] {
			rf.matchIndex[sendTo] = newMatchIdx
		}
		newNextIdx := args.PrevLogIndex + len(args.Entries) + 1
		if newNextIdx > rf.nextIndex[sendTo] {
			rf.nextIndex[sendTo] = newNextIdx
		}
		// If there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4).

		for N := rf.GlobalIndex(len(rf.log) - 1); N > rf.commitIndex; N-- {
			cnt := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[rf.RealLogIndex(N)].Term == rf.currentTerm {
					cnt++
				}
			}

			if cnt > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
		}

		rf.applyCond.Signal()
		return
	}

	if rf.currentTerm < appendReply.Term {
		// become old leader, update and become to Follower
		rf.currentTerm = appendReply.Term
		rf.votedFor = -1
		rf.role = Follower
		// rf.timeStamp = time.Now()
		rf.ResetVoteTimer()
		rf.persist()
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

		// handle conflict in AppendEntries
		if appendReply.XTerm == -1 {
			if rf.lastIncludedIndex >= appendReply.XLen {
				// go rf.handleInstallSnapshot(sendTo)
				rf.nextIndex[sendTo] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[sendTo] = appendReply.XLen
			}
			return
		}

		i := rf.nextIndex[sendTo] - 1
		if i < rf.lastIncludedIndex {
			i = rf.lastIncludedIndex
		}
		for i > rf.lastIncludedIndex && rf.log[rf.RealLogIndex(i)].Term > appendReply.XTerm {
			i--
		}
		if i == rf.lastIncludedIndex && rf.log[rf.RealLogIndex(i)].Term > appendReply.XTerm {
			// still not found in current log, sned InstallSnapshot RPC to get earlier log and find.
			// go rf.handleInstallSnapshot(sendTo)
			rf.nextIndex[sendTo] = rf.lastIncludedIndex
		} else if rf.log[rf.RealLogIndex(i)].Term == appendReply.XTerm {
			rf.nextIndex[sendTo] = i + 1
		} else {
			// now: i != rf.lastIncludedIndex || rf.log[rf.RealLogIndex(i)].Term < appendReply.Xterm
			if appendReply.XIndex <= rf.lastIncludedIndex {
				// indicate that Prev Snapshot logs may have PrevLogIndex.
				// go rf.handleInstallSnapshot(sendTo)
				rf.nextIndex[sendTo] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[sendTo] = appendReply.XIndex
			}
		}
		return
	}
}

func (rf *Raft) SendHeartBeats() {
	DPrintf("server %v start sending heartbeats\n", rf.me)

	for !rf.killed() {
		<-rf.heartbeatTimer.C
		rf.mu.Lock()
		// if the server is dead or is not the leader, just return
		if rf.role != Leader {
			rf.mu.Unlock()
			// if not leader any more, stop send heartbeats
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me { // avoid commiting to self in starting one-by-one.
				continue
			}
			args := &AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				// PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			sendInstallSnapshotRPC := false
			if args.PrevLogIndex < rf.lastIncludedIndex {
				// When the log request for retreat has been intercepted by Snapshot, send InsatllSnapshot RPC
				sendInstallSnapshotRPC = true
			} else if rf.GlobalIndex(len(rf.log)-1) > args.PrevLogIndex {
				args.Entries = rf.log[rf.RealLogIndex(args.PrevLogIndex+1):]
				DPrintf("detect entries need to be appended\n")
			} else {
				args.Entries = make([]Entry, 0)
				// args.Entries = nil
				DPrintf("Just for Heartbeat\n")
			}

			if sendInstallSnapshotRPC {
				go rf.handleInstallSnapshot(i)
			} else {
				args.PrevLogTerm = rf.log[rf.RealLogIndex(args.PrevLogIndex)].Term
				go rf.handleAppendEntries(i, args)
			}

		}

		rf.mu.Unlock()
		// time.Sleep(time.Duration(HeartbeatTime) * time.Millisecond)
		rf.ResetHeartbeatTimer(HeartbeatTime)
	}
}

func (rf *Raft) getVoteAnswer(sendTo int, args *RequestVoteArgs) bool {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(sendTo, args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate || args.Term != rf.currentTerm {
		// if other server send requestVote before mine, return false
		return false
	}

	if reply.Term > rf.currentTerm {
		// old term
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}
	return reply.VoteGranted
}

func (rf *Raft) collectVotes(sendTo int, args *RequestVoteArgs, voteMutex *sync.Mutex, voteCnt *int) {
	voteAnswer := rf.getVoteAnswer(sendTo, args)
	if !voteAnswer {
		return
	}
	voteMutex.Lock()
	if *voteCnt > len(rf.peers)/2 {
		voteMutex.Unlock()
		return
	}
	(*voteCnt)++

	if *voteCnt > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.role != Candidate || rf.currentTerm != args.Term {
			// there is another voting goroutine received updated term and changed this server to Follower
			rf.mu.Unlock()
			voteMutex.Unlock()
			return
		}
		rf.role = Leader
		// if be elected as leader after election, reinitialized nextIndex&matchIndex
		// (Reinitialized after election) in figure 2.
		for i := 0; i < len(rf.nextIndex); i++ {
			// rf.nextIndex[i] = len(rf.log)
			rf.nextIndex[i] = rf.GlobalIndex(len(rf.log))
			rf.matchIndex[i] = rf.lastIncludedIndex
		}
		rf.mu.Unlock()
		go rf.SendHeartBeats()
	}
	voteMutex.Unlock()
}

func (rf *Raft) Election() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.role = Candidate
	rf.votedFor = rf.me
	// rf.voteCnt = 1
	// rf.timeStamp = time.Now()

	voteCnt := 1
	var muVote sync.Mutex

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		// LastLogIndex: len(rf.log) - 1,
		LastLogIndex: rf.GlobalIndex(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for server := range rf.peers {
		if server == rf.me {
			DPrintf("Raft %d: vote myself", server)
			continue
		}
		go rf.collectVotes(server, args, &muVote, &voteCnt)
	}
}

func (rf *Raft) ticker() {
	// rd := rand.New(rand.NewSource(int64(rf.me)))
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// If a follower receives no communication over a period of time called the **election timeout**,
		// then it assumes there is no viable leader and begins an election to choose a new leader.
		// randomElectionTimeout := rand.Int63()%51 + 250
		<- rf.voteTimer.C
		rf.mu.Lock()
		// if rf.role != Leader && time.Since(rf.timeStamp) > time.Duration(randomElectionTimeout)*time.Millisecond {
		if rf.role != Leader {
			// start Election
			go rf.Election()
		}
		rf.ResetVoteTimer()
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
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
	// rf.timeStamp = time.Now()
	rf.role = Follower
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.voteTimer = time.NewTimer(0)
	rf.heartbeatTimer = time.NewTimer(0)
	rf.ResetVoteTimer()

	// initialize from state persisted before a crash
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		// rf.nextIndex[i] = len(rf.log)
		rf.nextIndex[i] = rf.GlobalIndex(len(rf.log))
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.CommitChecker()

	return rf
}
