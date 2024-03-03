package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClientRecord struct {
	value  string
	req_id int64
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	kv_pairs      map[string]string
	clientRecords map[int64]ClientRecord
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.kv_pairs[args.Key]
	if !ok {
		val = ""
	}
	reply.Value = val
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, value, c_id, r_id := args.Key, args.Value, args.Clerk_Id, args.Req_Id
	reply.Value = kv.kv_pairs[key]
	kv.clientRecords[c_id] = ClientRecord{value, r_id}
	kv.kv_pairs[key] = value
	
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, val, c_id, r_id := args.Key, args.Value, args.Clerk_Id, args.Req_Id
	
	last_record, ok := kv.clientRecords[c_id]

	var last_val string

	if !ok || last_record.req_id != r_id {
		last_val = ""
	}
	last_val = last_record.value
	
	kv.clientRecords[c_id] = ClientRecord{value: args.Value, req_id: r_id}
	if ok {
		kv.kv_pairs[key] = last_val + val
		reply.Value = last_val
	} else {
		kv.kv_pairs[key] = val
		reply.Value = ""
	}

	
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	// c.Cond = sync.NewCond(c.Mutex)
	kv.kv_pairs = make(map[string]string, 0)
	kv.clientRecords = make(map[int64]ClientRecord, 0)

	return kv
}
