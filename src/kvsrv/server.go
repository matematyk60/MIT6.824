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

type KVServer struct {
	mu    sync.Mutex
	store map[string]string

	log map[int64]string

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value := kv.store[args.Key]
	reply.Value = value
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, alreadyCompleted := kv.log[args.ReqId]
	if alreadyCompleted {

		//skip
	} else {
		kv.store[args.Key] = args.Value
		kv.log[args.ReqId] = args.Value
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, alreadyCompleted := kv.log[args.ReqId]
	if alreadyCompleted {
		reply.Value = value
	} else {
		previousValue := kv.store[args.Key]
		kv.store[args.Key] = previousValue + args.Value
		kv.log[args.ReqId] = previousValue

		reply.Value = previousValue
	}

	// Your code here.
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.log = make(map[int64]string)

	return kv
}
