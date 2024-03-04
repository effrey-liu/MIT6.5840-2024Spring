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
	req_id uint32
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

	var isDuplicate bool
	var last_val string
	last_record, ok := kv.clientRecords[c_id]

	if !ok || last_record.req_id != r_id {
		last_val, isDuplicate = "", false
	}
	last_val, isDuplicate = last_record.value, true
	
	if isDuplicate {
		reply.Value = last_val
		return
	}

	kv.kv_pairs[key] = value

	kv.clientRecords[c_id] = ClientRecord{value: "", req_id: r_id}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, val, c_id, r_id := args.Key, args.Value, args.Clerk_Id, args.Req_Id

	var last_val string
	var isDuplicate bool
	last_record, record_ok := kv.clientRecords[c_id]

	if !record_ok || last_record.req_id != r_id {
		last_val, isDuplicate = "", false
	}
	last_val, isDuplicate = last_record.value, true

	if isDuplicate {
		reply.Value = last_val
		return
	}

	cur_val, kv_ok := kv.kv_pairs[key]

	if kv_ok {
		kv.kv_pairs[key] = cur_val + val
		reply.Value = cur_val
	} else {
		kv.kv_pairs[key] = val
		reply.Value = ""
	}

	kv.clientRecords[c_id] = ClientRecord{value: cur_val, req_id: r_id}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	// c.Cond = sync.NewCond(c.Mutex)
	kv.kv_pairs = make(map[string]string, 0)
	kv.clientRecords = make(map[int64]ClientRecord, 0)

	return kv
}
