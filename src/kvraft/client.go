package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64  // client invoking request, client's unique identifier
	leaderId int    // The current known leader id, update from the reply after requesting the non-leader server.
	sequenceNum uint64 // to eliminate duplicates
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
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.sequenceNum = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.sequenceNum++
	args := GetArgs{key, ck.sequenceNum, ck.clientId}

	// You will have to modify this function.
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("resend Get Operation request\n")
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(20 * time.Millisecond)
			continue
		}

		switch reply.Err {
		case OK:
			return reply.Value
		case ErrWrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(20 * time.Millisecond)
			continue
		case ErrTimeout:
			time.Sleep(20 * time.Millisecond)
			continue
		case ErrHandleOpTimeout:
			time.Sleep(20 * time.Millisecond)
			continue
		case ErrChanClosed:
			time.Sleep(20 * time.Millisecond)
			continue
		case ErrNoKey:
			return reply.Value
		default:
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sequenceNum++
	args := PutAppendArgs{key, value, ck.clientId, ck.sequenceNum, op}

	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer." + op, &args, &reply)
		if !ok {
			DPrintf("resend PutAppend Request\n")
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(20 * time.Millisecond)
			continue
		}

		switch reply.Err {
		case OK:
			return
		case ErrWrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(20 * time.Millisecond)
			continue
		case ErrTimeout:
			time.Sleep(20 * time.Millisecond)
			continue
		case ErrHandleOpTimeout:
			time.Sleep(20 * time.Millisecond)
			continue
		case ErrChanClosed:
			time.Sleep(20 * time.Millisecond)
			continue
		case ErrNoKey:
			return
		default:
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
