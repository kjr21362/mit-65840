package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false
const SERVER_WAIT_INTERVAL = 10 * time.Millisecond
const APPLY_INTERVAL = 5 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string
	Key       string
	Value     string
	RequestID int64
	ClientID  int64
}

type RequestResult struct {
	RequestID int64
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store       map[string]string
	processedCID map[int64]RequestResult
}

func (kv *KVServer) SendOp(op Op) Err {
	_, term, _ := kv.rf.Start(op)

	for {
		if kv.killed() {
			return ErrWrongLeader
		}

		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			return ErrWrongLeader
		}

		kv.mu.Lock()
		requestResult, ok := kv.processedCID[op.ClientID]
		kv.mu.Unlock()
		if ok && requestResult.RequestID == op.RequestID {
			return OK
		}

		time.Sleep(SERVER_WAIT_INTERVAL)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	result, ok := kv.processedCID[args.ClientID]
	if ok && args.RequestID <= result.RequestID {
		reply.Err = OK
		reply.Value = result.Value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{Command: CommandGet, Key: args.Key, RequestID: args.RequestID, ClientID: args.ClientID}
	err := kv.SendOp(op)
	reply.Err = err
	if err == OK {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.processedCID[op.ClientID].Value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	result, ok := kv.processedCID[args.ClientID]
	if ok && args.RequestID <= result.RequestID {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{Command: CommandPut, Key: args.Key, Value: args.Value , RequestID: args.RequestID, ClientID: args.ClientID}
	err := kv.SendOp(op)
	reply.Err = err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	result, ok := kv.processedCID[args.ClientID]
	if ok && args.RequestID <= result.RequestID {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{Command: CommandAppend, Key: args.Key, Value: args.Value , RequestID: args.RequestID, ClientID: args.ClientID}
	err := kv.SendOp(op)
	reply.Err = err
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		msg := <- kv.applyCh

		kv.mu.Lock()
		if msg.CommandValid && msg.Command != nil {
			op := msg.Command.(Op)
			requestResult, ok := kv.processedCID[op.ClientID]
			if !ok || (ok && requestResult.RequestID < op.RequestID) {
				kv.processedCID[op.ClientID] = RequestResult{RequestID: op.RequestID, Value: kv.store[op.Key]}

				if op.Command == CommandPut {
					kv.store[op.Key] = op.Value
				} else if op.Command == CommandAppend {
					kv.store[op.Key] = kv.store[op.Key] + op.Value
				}
			}
		}

		kv.mu.Unlock()

		time.Sleep(APPLY_INTERVAL)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.processedCID = make(map[int64]RequestResult)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
