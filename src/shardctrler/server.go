package shardctrler

import (
	"log"
	"sync"
	"time"
	"sort"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const SERVER_WAIT_INTERVAL = 10 * time.Millisecond
const APPLY_INTERVAL = 5 * time.Millisecond

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	processedQueryRequests map[int64]QueryResult
	processedMoveRequests map[int64]MoveResult
	processedLeaveRequests map[int64]LeaveResult
	processedJoinRequests  map[int64]JoinResult

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	Command   string
	ConfigNum int
	Config    Config
	Shard     int
	GID       int
	GIDs      []int
	Servers   map[int][]string
	RequestID int64
	ClientID  int64

}

type QueryResult struct {
	requestID int64
	config    Config
}

type MoveResult struct {
	requestID int64
}

type LeaveResult struct {
	requestID int64
}

type JoinResult struct {
	requestID int64
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	result, ok := sc.processedJoinRequests[args.ClientID]
	if ok && args.RequestID <= result.requestID {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{Command: CommandJoin, Servers: args.Servers, RequestID: args.RequestID, ClientID: args.ClientID}
	
	err := sc.SendOp(op)
	reply.Err = err
	if err == OK {
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	result, ok := sc.processedLeaveRequests[args.ClientID]
	if ok && args.RequestID <= result.requestID {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{Command: CommandLeave, GIDs: args.GIDs, RequestID: args.RequestID, ClientID: args.ClientID}
	
	err := sc.SendOp(op)
	reply.Err = err
	if err == OK {
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	result, ok := sc.processedMoveRequests[args.ClientID]
	if ok && args.RequestID <= result.requestID {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{Command: CommandMove, Shard: args.Shard, GID: args.GID, RequestID: args.RequestID, ClientID: args.ClientID}

	err := sc.SendOp(op)
	reply.Err = err
	if err == OK {
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	result, ok := sc.processedQueryRequests[args.ClientID]
	if ok && args.RequestID <= result.requestID {
		reply.WrongLeader = false
		reply.Err = OK
		reply.Config = result.config
		sc.mu.Unlock()
		return
	}
	
	op := Op{Command: CommandQuery, ConfigNum: args.Num, RequestID: args.RequestID, ClientID: args.ClientID}
	if op.ConfigNum == -1 || op.ConfigNum >= len(sc.configs) {
		op.ConfigNum = len(sc.configs) - 1
	}
	sc.mu.Unlock()

	err := sc.SendOp(op)
	reply.Err = err
	if err == OK {
		sc.mu.Lock()
		reply.WrongLeader = false
		reply.Config = sc.processedQueryRequests[op.ClientID].config
		sc.mu.Unlock()
	}

}

func (sc *ShardCtrler) SendOp(op Op) Err {
	_, term, _ := sc.rf.Start(op)
	for {
		currentTerm, isLeader := sc.rf.GetState()
		if !isLeader || currentTerm != term {
			return ErrWrongLeader
		}

		if op.Command == CommandQuery {
			sc.mu.Lock()
			result, ok := sc.processedQueryRequests[op.ClientID]
			sc.mu.Unlock()
			if ok && result.requestID == op.RequestID {
				return OK
			}
		} else if op.Command == CommandMove {
			sc.mu.Lock()
			result, ok := sc.processedMoveRequests[op.ClientID]
			sc.mu.Unlock()
			if ok && result.requestID == op.RequestID {
				return OK
			}
		} else if op.Command == CommandJoin {
			sc.mu.Lock()
			result, ok := sc.processedJoinRequests[op.ClientID]
			sc.mu.Unlock()
			if ok && result.requestID == op.RequestID {
				return OK
			}
		} else if op.Command == CommandLeave {
			sc.mu.Lock()
			result, ok := sc.processedLeaveRequests[op.ClientID]
			sc.mu.Unlock()
			if ok && result.requestID == op.RequestID {
				return OK
			}
		}
		
		time.Sleep(SERVER_WAIT_INTERVAL)
	}
}

func (sc *ShardCtrler) copyConfig(config Config) Config {
	var newShards [NShards]int
	for i := 0; i < NShards; i++ {
		newShards[i] = config.Shards[i]
	}

	newGroups := make(map[int][]string, len(config.Groups))
	for gid, servers := range config.Groups {
		newGroups[gid] = make([]string, len(servers))
		copy(newGroups[gid], servers)
	}

	return Config{Num: config.Num+1, Shards: newShards, Groups: newGroups}
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		sc.mu.Lock()
		if msg.CommandValid && msg.Command != nil {
			op := msg.Command.(Op)
			if op.Command == CommandJoin {
				result, ok := sc.processedJoinRequests[op.ClientID]
				if !ok || (ok && result.requestID < op.RequestID) {
					oldConfig := sc.copyConfig(sc.configs[len(sc.configs)-1])
					for gid, servers := range op.Servers {
						oldConfig.Groups[gid] = make([]string, len(servers))
						copy(oldConfig.Groups[gid], servers)
					}

					// rebalance
					newConfig := sc.Rebalance(oldConfig)

					sc.configs = append(sc.configs, newConfig)
					sc.processedJoinRequests[op.ClientID] = JoinResult{requestID: op.RequestID}
				}
			} else if op.Command == CommandLeave {
				result, ok := sc.processedLeaveRequests[op.ClientID]
				if !ok || (ok && result.requestID < op.RequestID) {
					oldConfig := sc.copyConfig(sc.configs[len(sc.configs)-1])
					
					for _, deletedGroup := range op.GIDs {
						for i := 0; i < NShards; i++ {
							if oldConfig.Shards[i] == deletedGroup {
								oldConfig.Shards[i] = 0
							}
						}
						_, gidExists := oldConfig.Groups[deletedGroup]
						if gidExists {
							delete(oldConfig.Groups, deletedGroup)
						}
					}
					// rebalance
					newConfig := sc.Rebalance(oldConfig)

					sc.configs = append(sc.configs, newConfig)
					sc.processedLeaveRequests[op.ClientID] = LeaveResult{requestID: op.RequestID}

				}
			} else if op.Command == CommandMove {
				result, ok := sc.processedMoveRequests[op.ClientID]
				if !ok || (ok && result.requestID < op.RequestID) {
					oldConfig := sc.configs[len(sc.configs)-1]
					var newShards [NShards]int
					for i := 0; i < NShards; i++ {
						newShards[i] = oldConfig.Shards[i]
					}
					newShards[op.Shard] = op.GID

					newGroups := make(map[int][]string)
					for gid, servers := range oldConfig.Groups {
						newGroups[gid] = make([]string, len(servers))
						copy(newGroups[gid], servers)
					}
					
					newConfig := Config{Num: len(sc.configs), Shards: newShards, Groups: newGroups}
					sc.configs = append(sc.configs, newConfig)
					sc.processedMoveRequests[op.ClientID] = MoveResult{requestID: op.RequestID}

				}
			} else if op.Command == CommandQuery {
				result, ok := sc.processedQueryRequests[op.ClientID]
				if !ok || (ok && result.requestID < op.RequestID) {
					config := sc.configs[op.ConfigNum]
					sc.processedQueryRequests[op.ClientID] = QueryResult{requestID: op.RequestID, config: config}
				}
			}
		}
		sc.mu.Unlock()
		time.Sleep(APPLY_INTERVAL)
	}
}

func (sc *ShardCtrler) Rebalance(oldConfig Config) Config {

	shardsInGroup := map[int][]int{} // gid -> [shards]
	//lastConfig := sc.configs[len(sc.configs)-1]
	availableGroups := make([]int, 0)
	for gid, _ := range oldConfig.Groups {
		availableGroups = append(availableGroups, gid)
	}
	var newShards [NShards]int
	for i := 0; i < NShards; i++ {
		newShards[i] = oldConfig.Shards[i]
	}

	newGroups := make(map[int][]string, len(oldConfig.Groups))
	for gid, servers := range oldConfig.Groups {
		newGroups[gid] = make([]string, len(servers))
		copy(newGroups[gid], servers)
	}

	if len(availableGroups) == 0 {
		return Config{Num: len(sc.configs), Shards: newShards, Groups: newGroups}
	} else if len(availableGroups) == 1 {
		for i := 0; i < NShards; i++ {
			newShards[i] = availableGroups[0]
		}
		return Config{Num: len(sc.configs), Shards: newShards, Groups: newGroups}
	}

	sort.Ints(availableGroups)

	for _, gid := range availableGroups {
		shardsInGroup[gid] = make([]int, 0)
	}

	for shard, gid := range newShards {
		if gid > 0 {
			shardsInGroup[gid] = append(shardsInGroup[gid], shard)
		}
	}

	for i := 0; i < NShards; i++ {
		if newShards[i] == 0 {
			_, minG, _, _ := sc.findMinMaxGroup(shardsInGroup, availableGroups)
			newShards[i] = minG
			shardsInGroup[minG] = append(shardsInGroup[minG], i)
		}
	}

	for {
		min, minG, max, maxG := sc.findMinMaxGroup(shardsInGroup, availableGroups)
		if max - min > 1 {
			shard := shardsInGroup[maxG][0]
			shardsInGroup[minG] = append(shardsInGroup[minG], shard)
			shardsInGroup[maxG] = shardsInGroup[maxG][1:]
		} else {
			break
		}
	}

	//for gid, shards := range shardsInGroup {
	for _, gid := range availableGroups {
		shards := shardsInGroup[gid]
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	return Config{Num: len(sc.configs), Shards: newShards, Groups: newGroups}
}

func (sc *ShardCtrler) findMinMaxGroup(shardsInGroup map[int][]int, availableGroups []int) (int, int, int, int) {
	min := 257
	max := 0
	minG := 0
	maxG := 0
	//for gid, shards := range shardsInGroup {
	for _, gid := range availableGroups {
		shards := shardsInGroup[gid]
		if len(shards) > max {
			max = len(shards)
			maxG = gid
		}
		if len(shards) < min {
			min = len(shards)
			minG = gid
		}
	}

	return min, minG, max, maxG
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.processedJoinRequests = make(map[int64]JoinResult)
	sc.processedLeaveRequests = make(map[int64]LeaveResult)
	sc.processedMoveRequests = make(map[int64]MoveResult)
	sc.processedQueryRequests = make(map[int64]QueryResult)

	go sc.applier()

	return sc
}
