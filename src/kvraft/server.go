package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"

	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const HandleOpTimeout = time.Millisecond * 500

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64 // identify clerk
	SequenceNum uint64
	OpType      string // Operation typ: put/append/get
	Key         string
	Value       string // for PutAppend func
}

type Result struct {
	LastSeqNum uint64
	Err        Err
	Value      string
	ApplyTerm  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB           map[string]string // Database on KVServer, storing key-value pairs.
	logLastApplied int               // 此kvserver apply的上一个日志的index
	maxMapLen      int
	waiCh          map[int]*chan Result
	historyMap     map[int64]*Result
}

func (kv *KVServer) handleOp(opArgs *Op) (res Result) {
	startIndex, startTerm, isleader := kv.rf.Start(opArgs)
	if !isleader {
		return Result{Err: ErrWrongLeader, Value: ""}
	}
	kv.mu.Lock()

	newCh := make(chan Result)
	kv.waiCh[startIndex] = &newCh
	DPrintf("leader %v identifier %v SeqNum %v, create new Channel: %p\n", kv.me, opArgs.ClientId, opArgs.SequenceNum, &newCh)

	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waiCh, startIndex)
		close(newCh)
		kv.mu.Unlock()
	}()

	// wait message and judge
	select {
	case <-time.After(HandleOpTimeout):
		res.Err = ErrHandleOpTimeout
		DPrintf("server %v identifier %v SeqNum %v: HandleOpTimeout", kv.me, opArgs.ClientId, opArgs.SequenceNum)
		return
	case msg, ok := <-newCh:
		if ok && msg.ApplyTerm == startTerm {
			res = msg
			return
		} else if !ok {
			res.Err = ErrChanClosed
			DPrintf("server %v identifier %v SeqNum %v: channel closed", kv.me, opArgs.ClientId, opArgs.SequenceNum)
			return
		} else {
			res.Err = ErrWrongLeader
			res.Value = ""
			return
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	opArgs := &Op{OpType: "Get", SequenceNum: args.Seq_Num, Key: args.Key, ClientId: args.Clerk_Id}

	res := kv.handleOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value

}

// unlike in lab 2, neither Put nor Append should return a value.
// this is already reflected in the PutAppendReply struct.
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	opArgs := &Op{OpType: args.Op, SequenceNum: args.Seq_Num, Key: args.Key, Value: args.Value, ClientId: args.Clerk_Id}

	res := kv.handleOp(opArgs)
	reply.Err = res.Err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	opArgs := &Op{OpType: args.Op, SequenceNum: args.Seq_Num, Key: args.Key, Value: args.Value, ClientId: args.Clerk_Id}

	res := kv.handleOp(opArgs)
	reply.Err = res.Err
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

func (kv *KVServer) DBExecute(op *Op, isleader bool) (res Result) {
	res.LastSeqNum = op.SequenceNum
	switch op.OpType {
	case "Get":
		val, exist := kv.kvDB[op.Key]
		if exist {
			// kv.LogInfoDBExecute(op, "", val, isLeader)
			res.Value = val
			return
		} else {
			res.Err = ErrNoKey
			res.Value = ""
			// kv.LogInfoDBExecute(op, "", ErrKeyNotExist, isLeader)
			return
		}
	case "Put":
		kv.kvDB[op.Key] = op.Value
		// kv.LogInfoDBExecute(op, "", kv.db[op.Key], isLeader)
		return
	case "Append":
		val, exist := kv.kvDB[op.Key]
		if exist {
			kv.kvDB[op.Key] = val + op.Value
			// kv.LogInfoDBExecute(op, "", kv.db[op.Key], isLeader)
			return
		} else {
			kv.kvDB[op.Key] = op.Value
			// kv.LogInfoDBExecute(op, "", kv.db[op.Key], isLeader)
			return
		}
	}
	return
}

func (kv *KVServer) ApplyChanHandler() {
	for !kv.killed() {
		log := <-kv.applyCh

		if log.CommandValid {
			var op Op
			if cmd, ok := log.Command.(Op); ok {
				// 如果是 Op 类型，直接使用
				op = cmd
			} else if cmdPtr, ok := log.Command.(*Op); ok {
				// 如果是 *Op 类型，需要解引用后使用
				op = *cmdPtr
			}
			// if !ok {
			// 	DPrintf("interface convertion failure: interface {} is *kvraft.Op, not kvraft.Op")
			// 	continue
			// }
			kv.mu.Lock()
			var res Result
			needApply := false

			if historyMap, exist := kv.historyMap[op.ClientId]; exist {
				if historyMap.LastSeqNum == op.SequenceNum {
					res = *historyMap
				} else if historyMap.LastSeqNum < op.SequenceNum {
					needApply = true
				}
			} else {
				needApply = true
			}
			_, isleader := kv.rf.GetState()

			if needApply {
				res = kv.DBExecute(&op, isleader)
				res.ApplyTerm = log.SnapshotTerm

				kv.historyMap[op.ClientId] = &res
			}

			if !isleader {
				kv.mu.Unlock()
				continue
			}
			ch, exist := kv.waiCh[log.CommandIndex]

			if !exist {
				// receiver closed corresponding channel, which means this is a repeat request.
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()

			res.ApplyTerm = log.SnapshotTerm
			*ch <- res	// maybe incur panic if this channel is closed. use below to detect panic:
			/*
			func() {
				defer func() {
					if recover() != nil {
						// if occurs panic，because the channel is closed
						DPrintf("channel closed.")
					}
				}()
				res.ApplyTerm = log.SnapshotTerm
				*ch <- res
			}()
			*/
		}
	}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.historyMap = make(map[int64]*Result)
	kv.kvDB = make(map[string]string)
	kv.waiCh = make(map[int]*chan Result)

	go kv.ApplyChanHandler()
	return kv
}
