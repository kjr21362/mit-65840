package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const CLIENT_WAIT_INTERVAL = 10 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID     int64
	requestID    int64
	leaderIndex  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.requestID = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{key, ck.requestID, ck.clientID}
	ck.requestID ++
	i := ck.leaderIndex

	for {
		reply := GetReply{}
		ok := ck.servers[i%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.leaderIndex = i % len(ck.servers)
			return reply.Value
		}

		time.Sleep(CLIENT_WAIT_INTERVAL)
		i ++
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	
	args := PutAppendArgs{key, value, op, ck.requestID, ck.clientID}
	ck.requestID ++
	i := ck.leaderIndex

	for {
		reply := PutAppendReply{}
		ok := ck.servers[i%len(ck.servers)].Call("KVServer." + op, &args, &reply)
		if ok && reply.Err == OK {
			ck.leaderIndex = i % len(ck.servers)
			break
		}

		time.Sleep(CLIENT_WAIT_INTERVAL)
		i ++
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
