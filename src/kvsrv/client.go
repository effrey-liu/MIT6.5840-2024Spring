package kvsrv

import (
	"crypto/rand"
	// "log"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	id     int64
	req_id int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.id = nrand()
	ck.req_id = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	reply := GetReply{}
	args := GetArgs{key, ck.id, ck.req_id}
	// ok := ck.server.Call("KVServer.Get", &args, reply)

	for !ck.server.Call("KVServer.Get", &args, &reply) {
		DPrintf("resend Get Operation command\n")
	}

	// You will have to modify this function.
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	var targetOp string
	if op == "Put" {
		targetOp = "KVServer.Put"
	} else if op == "Append" {
		targetOp = "KVServer.Append"
	} else {
		return "Invalid Op"
	}
	ck.req_id++

	args := PutAppendArgs{key, value, ck.id, ck.req_id}
	reply := PutAppendReply{value}

	for !ck.server.Call(targetOp, &args, &reply) {
		DPrintf("resend %v command\n", targetOp)
	}
	
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
