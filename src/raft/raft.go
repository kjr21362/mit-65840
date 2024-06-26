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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

const (
	LEADER    = iota
	FOLLOWER
	CANDIDATE
)

type Entry struct {
	Command interface{}
	Term    int
}

const HEARTBEAT_INTERVAL = 30 * time.Millisecond

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
	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	lastApplied int

	identity    int
	nextIndex   []int
	matchIndex  []int
	lastHeartBeat time.Time
	heartBeatTimeout int64
	votesReceived int
	applyCh     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = !rf.killed() && (rf.identity == LEADER)
	rf.mu.Unlock()

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	state := w.Bytes()
	rf.persister.Save(state, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, votedFor int
	log := make([]Entry, 0)

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		Debug(dError, "S%d readPersist error", rf.me)
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
	}
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
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Ack     int

	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	rf.mu.Lock()
	Debug(dLog2, "S%d respond to AppendEntries from S%d", rf.me, args.LeaderId)
	rf.lastHeartBeat = time.Now()
	if args.LeaderTerm > rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
		rf.persist()
	}
	if args.LeaderTerm == rf.currentTerm {
		Debug(dLeader, "S%d is follower", rf.me)
		rf.identity = FOLLOWER
	}
	
	logOK := (len(rf.log) >= args.PrevLogIndex) && (args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm)
	Debug(dLog2, "S%d len(rf.log): %d, args.PrevLogIndex %d, logOK %t", rf.me , len(rf.log) , args.PrevLogIndex, logOK)
	
	if args.LeaderTerm == rf.currentTerm && logOK {
		
		suffix := args.Entries
		Debug(dLog2, "S%d got suffix, length %d from leaderId %d", rf.me, len(suffix), args.LeaderId)
		if len(suffix) > 0 && len(rf.log) > args.PrevLogIndex {
			index := len(rf.log) - 1
			if args.PrevLogIndex + len(suffix) - 1 < index {
				index = args.PrevLogIndex + len(suffix) - 1
			}
			if rf.log[index].Term != suffix[index - args.PrevLogIndex].Term {
				Debug(dLog2, "S%d rf.log[%d].Term %d != suffix[%d].Term %d", rf.me, index, rf.log[index].Term, index - args.PrevLogIndex, suffix[index - args.PrevLogIndex].Term)
				rf.log = rf.log[:args.PrevLogIndex]
				rf.persist()
			}
		}
		if args.PrevLogIndex + len(suffix) > len(rf.log) {
			Debug(dLog2, "S%d appending log from suffix[%d:]", rf.me, len(rf.log) - args.PrevLogIndex)
			toAppend := suffix[len(rf.log) - args.PrevLogIndex:]
			rf.log = append(rf.log, toAppend...)
			rf.persist()
		}
		//if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.lastApplied {
			// deliver msg to application
			rf.commitIndex = args.LeaderCommit
			go rf.applyLogs()
		}

		reply.Term = rf.currentTerm
		reply.Success = true
		reply.Ack = args.PrevLogIndex + len(suffix)

		rf.debugPrintLog()

	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Ack = 0

		reply.XTerm = -1
		if len(rf.log) < args.PrevLogIndex {
			reply.XIndex = len(rf.log)
			reply.XTerm = -1
		} else if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.XTerm = rf.log[args.PrevLogIndex-1].Term
			i := args.PrevLogIndex - 1
			for i >= 0 && rf.log[i].Term == reply.XTerm {
				i --
			}
			reply.XIndex = i
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied; i < rf.commitIndex; i++ {
		Debug(dCommit, "S%d deliver log[%d] to applyCh", rf.me, i)
		msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
		rf.applyCh <- msg
	}

	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeat = time.Now()
	Debug(dLog2, "S%d respond to RequestVote from S%d", rf.me, args.CandidateId)
	if args.CandidateTerm > rf.currentTerm {
		Debug(dLeader, "S%d change to follower because CandidateTerm %d > currentTerm %d", rf.me, args.CandidateTerm, rf.currentTerm)
		rf.currentTerm = args.CandidateTerm
		rf.identity = FOLLOWER
		rf.votedFor = -1
		rf.votesReceived = 0
		rf.persist()
	}

	lastTerm := rf.log[len(rf.log)-1].Term
	logOK := (args.LastLogTerm > lastTerm) || (args.LastLogTerm == lastTerm && args.LastLogIndex >= len(rf.log)-1)
	
	if rf.currentTerm == args.CandidateTerm && logOK && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		//rf.lastHeartBeat = time.Now()
		Debug(dLog2, "S%d grant vote to %d, term %d, votedFor %d", rf.me, args.CandidateId , rf.currentTerm, rf.votedFor)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.persist()
	} else {
		Debug(dLog2, "S%d not grant vote to %d, currentTerm %d, candidateTerm %d, logOK %t, votedFor %d", rf.me, args.CandidateId, rf.currentTerm, args.CandidateTerm, logOK, rf.votedFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.persist()
	}
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
	rf.mu.Lock()
	Debug(dClient, "S%d start got command %v, currentTerm %d", rf.me, command, rf.currentTerm)
	if rf.identity != LEADER {
		Debug(dClient, "S%d not a leader return", rf.me)
		rf.mu.Unlock()
		return index, term, false
	}

	newEntry := Entry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, newEntry)
	rf.matchIndex[rf.me] = len(rf.log)
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.debugPrintLog()
	rf.persist()
	rf.mu.Unlock()

	//go rf.sendHeartBeat()

	return index, term, isLeader
}

func (rf *Raft) debugPrintLog() {
	//Debug(dCommit, "S%d log length %d", rf.me, len(rf.log))
	for i := 0; i < len(rf.log); i++ {
		Debug(dCommit, "S%d log[%d] command %v term %d", rf.me, i, rf.log[i].Command , rf.log[i].Term)
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
	Debug(dError, "S%d kill", rf.me)

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	
	rf.mu.Lock()
	
	rf.currentTerm ++
	rf.identity = CANDIDATE
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.lastHeartBeat = time.Now()
	rf.heartBeatTimeout = 250 + (rand.Int63() % 300)
	rf.persist()
	Debug(dVote, "S%d start election with Timeout %d", rf.me, rf.heartBeatTimeout)

	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me, 
		len(rf.log) - 1, 
		rf.log[len(rf.log)-1].Term }

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			go func (i int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					Debug(dVote, "S%d got higher term %d when checking votes, change to follower", rf.me, reply.Term)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.votesReceived = 0
					rf.identity = FOLLOWER
					rf.heartBeatTimeout = 200
					rf.lastHeartBeat = time.Now()
					rf.persist()
					return
				}
				if reply.VoteGranted {
					rf.votesReceived ++
					Debug(dVote, "S%d got vote from %d, votesReceived %d", rf.me, i, rf.votesReceived)
				}

				if args.CandidateTerm != rf.currentTerm || rf.identity != CANDIDATE {
					Debug(dVote, "S%d identity changed from candidate to %d after checking votes", rf.me, rf.identity)
					return
				}

				if rf.votesReceived >= len(rf.peers)/2 + 1 {
					Debug(dVote, "S%d become leader", rf.me)
					rf.identity = LEADER
					rf.heartBeatTimeout = 200

					for i := 0; i < len(rf.peers); i++ {
						if rf.me != i {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 1
							go rf.sendHeartBeatTo(i)
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) sendHeartBeatTo(follower int) {
	rf.mu.Lock()
	if rf.identity != LEADER {
		Debug(dLeader, "S%d not a leader, cannot send heartbeat to S%d", rf.me, follower)
		rf.mu.Unlock()
		return
	}
	if rf.me == follower {
		Debug(dLeader, "S%d cannot send heartbeat to itself", rf.me)
		rf.mu.Unlock()
		return
	}
	rf.lastHeartBeat = time.Now()
	//rf.mu.Unlock()

	prefixLen := rf.nextIndex[follower]
	prefixTerm := 0
	if prefixLen > 0 {
		prefixTerm = rf.log[prefixLen-1].Term
	}
	Debug(dLeader, "S%d sending heartbeat to S%d, prefixLen %d", rf.me, follower, prefixLen)
	args := AppendEntriesArgs{
		rf.currentTerm, 
		rf.me, 
		prefixLen,
		prefixTerm,
		rf.log[prefixLen:], 
		rf.commitIndex}
	
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(follower, &args, &reply)
	if !ok {
		return
	}
	
	rf.mu.Lock()
	if reply.Term == rf.currentTerm && rf.identity == LEADER {
		//ack := len(rf.log)
		Debug(dLeader, "S%d currentTerm: %d, reply.Success: %t, ack: %d, rf.matchIndex[%d]: %d", rf.me, rf.currentTerm, reply.Success, reply.Ack, follower, rf.matchIndex[follower])
		if reply.Success && reply.Ack >= rf.matchIndex[follower] {
			rf.nextIndex[follower] = reply.Ack
			rf.matchIndex[follower] = reply.Ack
			// commit log
			go rf.commitLogEntries()
		} else if rf.nextIndex[follower] > 0 {
			Debug(dLeader, "S%d rf.nextIndex[%d]: %d, try sending heartbeat again", rf.me, follower, rf.nextIndex[follower])
			//rf.nextIndex[follower] --
			//if reply.XTerm == -1 {
				rf.nextIndex[follower] = reply.XIndex
			//} else {

			//}
			//go rf.sendHeartBeatTo(follower)
		}
	} else if reply.Term > rf.currentTerm {
		Debug(dLeader, "S%d got higher term %d when sending heartbeat, change to follower", rf.me, reply.Term)
		rf.currentTerm = reply.Term
		rf.identity = FOLLOWER
		rf.votedFor = -1
		rf.heartBeatTimeout = 200
		rf.lastHeartBeat = time.Now()
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	if rf.identity != LEADER {
		Debug(dLeader, "S%d not a leader, cannot send heartbeat", rf.me)
		rf.mu.Unlock()
		return
	}
	rf.lastHeartBeat = time.Now()
	Debug(dLeader, "S%d sending heartbeat", rf.me)
	rf.debugPrintLog()
	rf.mu.Unlock()
	
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			go rf.sendHeartBeatTo(i)
		}
	}
}

func (rf *Raft) acksCount(length int) int {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	count := 0
	for i := 0; i < len(rf.peers); i++ {
		if rf.matchIndex[i] >= length {
			count ++
		}
	}
	return count
}

func (rf *Raft) commitLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dCommit, "S%d commitLogEntries, rf.log length %d", rf.me, len(rf.log))
	minAcks := len(rf.peers)/2 + 1
	ready := 0
	for leng := 1; leng <= len(rf.log); leng++ {
		ct := rf.acksCount(leng)
		Debug(dCommit, "S%d length: %d, acksCount: %d", rf.me, leng, ct)
		if ct >= minAcks {
			ready = leng
		}
	}
	if ready > 0 && ready > rf.commitIndex && rf.log[ready-1].Term == rf.currentTerm {
		Debug(dCommit, "S%d commitEntries ready %d, rf.commitIndex %d, rf.log[%d].Term %d, currentTerm %d", rf.me, ready, rf.commitIndex, ready-1 , rf.log[ready-1].Term, rf.currentTerm)
		// deliver log msg to application
		Debug(dCommit, "S%d deliver message, ready %d", rf.me, ready)
		rf.commitIndex = ready
		go rf.applyLogs()
	}
}

func (rf *Raft) isHeartBeatTimedOut() bool {
	return time.Since(rf.lastHeartBeat) > time.Duration(rf.heartBeatTimeout) * time.Millisecond
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		identity := rf.identity
		heartBeatTimedOut := rf.isHeartBeatTimedOut()
		rf.mu.Unlock()
		
		switch identity {
		case LEADER:
			rf.sendHeartBeat()
		default:
			if heartBeatTimedOut {
				Debug(dLog, "S%d HeartBeatTimedOut %t", rf.me, heartBeatTimedOut)
				rf.startElection()
			}
		}
		
		time.Sleep(HEARTBEAT_INTERVAL)
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
	rf.identity = FOLLOWER
	rf.votedFor = -1
	rf.votesReceived = 0
	rf.currentTerm = 0
	rf.log = append(rf.log, Entry{Term: 0})
	rf.heartBeatTimeout = 200
	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
