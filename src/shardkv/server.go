package shardkv


import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "time"
import "6.5840/labgob"
import "6.5840/shardctrler"


const SERVER_WAIT_INTERVAL = 10 * time.Millisecond
const APPLY_INTERVAL = 5 * time.Millisecond
const CONFIG_FETCH_INTERVAL = 50 * time.Millisecond

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	mck          *shardctrler.Clerk
	config       shardctrler.Config
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store       map[string]string
	processedCID map[int64]RequestResult
}

func (kv *ShardKV) SendOp(op Op) Err {
	_, term, _ := kv.rf.Start(op)

	for {
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


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)
	kv.mu.Lock()
	gid := kv.config.Shards[shard]
	kv.mu.Unlock()
	if gid != args.GID {
		reply.Err = ErrWrongGroup
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

	op := Op{Command: "Get", Key: args.Key, RequestID: args.RequestID, ClientID: args.ClientID}
	err := kv.SendOp(op)
	reply.Err = err
	if err == OK {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.processedCID[op.ClientID].Value
	}

	
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)
	kv.mu.Lock()
	gid := kv.config.Shards[shard]
	kv.mu.Unlock()
	if gid != args.GID {
		reply.Err = ErrWrongGroup
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

	op := Op{Command: args.Op, Key: args.Key, Value: args.Value , RequestID: args.RequestID, ClientID: args.ClientID}
	err := kv.SendOp(op)
	reply.Err = err
	
}

func (kv *ShardKV) applier() {
	
		for msg := range kv.applyCh {

		kv.mu.Lock()
		if msg.CommandValid && msg.Command != nil {
			op := msg.Command.(Op)
			requestResult, ok := kv.processedCID[op.ClientID]
			if !ok || (ok && requestResult.RequestID < op.RequestID) {
				kv.processedCID[op.ClientID] = RequestResult{RequestID: op.RequestID, Value: kv.store[op.Key]}

				if op.Command == "Put" {
					kv.store[op.Key] = op.Value
				} else if op.Command == "Append" {
					kv.store[op.Key] = kv.store[op.Key] + op.Value
				}
			}
		}

		kv.mu.Unlock()

		time.Sleep(APPLY_INTERVAL)
	}
}

func (kv *ShardKV) configFetcher() {
	for {
		kv.mu.Lock()
		kv.config = kv.mck.Query(-1)
		kv.mu.Unlock()

		time.Sleep(CONFIG_FETCH_INTERVAL)	
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.store = make(map[string]string)
	kv.processedCID = make(map[int64]RequestResult)

	go kv.configFetcher()
	go kv.applier()

	return kv
}
