package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string

	ClientId int64
	ReqId    int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
