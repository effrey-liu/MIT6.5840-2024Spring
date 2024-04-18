package kvraft

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrHandleOpTimeout = "ErrHandleOpTimeout"
	ErrChanClosed      = "ErrChanClosed"
	ErrTimeout         = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Clerk_Id int64
	Seq_Num  uint64
	Op       string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq_Num  uint64
	Clerk_Id int64
}

type GetReply struct {
	Err   Err
	Value string
}
