package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// record is a key's stored value and its version number.
type record struct {
	value string
	ver   rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	kv map[string]*record
}

func MakeKVServer() *KVServer {
	kv := &KVServer{kv: map[string]*record{}}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if rec, exists := kv.kv[args.Key]; exists {
		reply.Err = rpc.OK
		reply.Value = rec.value
		reply.Version = rec.ver
		return
	}
	reply.Err = rpc.ErrNoKey
	reply.Value = ""
	reply.Version = 0
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rec, exists := kv.kv[args.Key]
	if !exists {
		if args.Version == 0 {
			kv.kv[args.Key] = &record{value: args.Value, ver: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	}
	if args.Version != rec.ver {
		reply.Err = rpc.ErrVersion
		return
	}
	rec.value = args.Value
	rec.ver++
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}

func TestKVServer(tc *tester.TesterClnt) []any {
	return []any{false}
}
