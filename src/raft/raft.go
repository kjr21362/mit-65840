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

const (
	LEADER    = iota
	FOLLOWER
	CANDIDATE
)

type Entry struct {
	Command interface{}
	Term    int
}

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
	//electionTimeout int64
	//lastElectionStart time.Time
	lastHeartBeat time.Time
	heartBeatTimeout int64
	votesReceived int
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	rf.mu.Lock()
	Debug(dLog2, "S%d respond to AppendEntries from S%d", rf.me, args.LeaderId)
	rf.lastHeartBeat = time.Now()
	if args.LeaderTerm > rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
	}
	if args.LeaderTerm == rf.currentTerm {
		Debug(dLeader, "S%d is follower", rf.me)
		rf.identity = FOLLOWER
	}

	logOK := true
	if args.LeaderTerm == rf.currentTerm && logOK {
		reply.Term = rf.currentTerm
		reply.Success = true
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	rf.mu.Unlock()
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
	Debug(dLog2, "S%d respond to RequestVote from S%d", rf.me, args.CandidateId)
	if args.CandidateTerm > rf.currentTerm {
		Debug(dLeader, "S%d receive more up-to-date RequestVote, change to follower", rf.me)
		rf.currentTerm = args.CandidateTerm
		rf.identity = FOLLOWER
		rf.votedFor = -1
		rf.votesReceived = 0
	}

	lastTerm := rf.log[len(rf.log)-1].Term
	logOK := (args.CandidateTerm > lastTerm) || (args.CandidateTerm == lastTerm && args.LastLogIndex >= len(rf.log))
	
	if rf.currentTerm == args.CandidateTerm && logOK && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		//rf.lastHeartBeat = time.Now()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
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
	Debug(dVote, "S%d start election with Timeout %d", rf.me, rf.heartBeatTimeout)

	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me, 
		len(rf.log), 
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
				}
			}(i)
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	if rf.identity != LEADER {
		Debug(dLeader, "S%d not a leader, cannot send heartbeat", rf.me)
		rf.mu.Unlock()
		return
	}
	rf.lastHeartBeat = time.Now()

	args := AppendEntriesArgs{
		rf.currentTerm, 
		rf.me, 
		len(rf.log), 
		rf.log[len(rf.log)-1].Term, 
		[]Entry{}, 
		rf.commitIndex}
	rf.mu.Unlock()
	
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			go func(i int) {
				Debug(dLeader, "S%d sending heartbeat to %d", rf.me, i)
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					return
				}
				
				rf.mu.Lock()
				if reply.Term == rf.currentTerm && rf.identity == LEADER {

				} else if reply.Term > rf.currentTerm {
					Debug(dLeader, "S%d got higher term %d when sending heartbeat, change to follower", rf.me, reply.Term)
					rf.currentTerm = reply.Term
					rf.identity = FOLLOWER
					rf.votedFor = -1
					rf.heartBeatTimeout = 200
					rf.lastHeartBeat = time.Now()
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}(i)
		}
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
		
		time.Sleep(time.Duration(100) * time.Millisecond)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
