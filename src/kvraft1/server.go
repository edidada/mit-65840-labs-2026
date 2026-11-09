package kvraft

import (
	"bytes"
	"sync"

	kvrpc "6.5840/kvraft1/rpc"
	"6.5840/kvraft1/rsm"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type kvRecord struct {
	value string
	ver   kvrpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	mu sync.Mutex
	kv map[string]*kvRecord
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case *kvrpc.TGetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		rec, exists := kv.kv[args.Key]
		if !exists {
			return kvrpc.TGetReply{Err: kvrpc.ErrNoKey}
		}
		return kvrpc.TGetReply{Value: rec.value, Version: rec.ver, Err: kvrpc.OK}
	case *kvrpc.TPutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		rec, exists := kv.kv[args.Key]
		if !exists {
			if args.Version == 0 {
				kv.kv[args.Key] = &kvRecord{value: args.Value, ver: 1}
				return kvrpc.TPutReply{Err: kvrpc.OK}
			}
			return kvrpc.TPutReply{Err: kvrpc.ErrNoKey}
		}
		if args.Version != rec.ver {
			return kvrpc.TPutReply{Err: kvrpc.ErrVersion}
		}
		rec.value = args.Value
		rec.ver++
		return kvrpc.TPutReply{Err: kvrpc.OK}
	default:
		panic("kvraft DoOp: unknown request type")
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kv)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[string]*kvRecord
	if d.Decode(&table) != nil {
		return
	}
	kv.mu.Lock()
	if kv.kv == nil {
		kv.kv = map[string]*kvRecord{}
	}
	for k, v := range table {
		if v != nil {
			kv.kv[k] = v
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *kvrpc.TGetArgs, reply *kvrpc.TGetReply) {
	err, rep := kv.rsm.Submit(args)
	if err == kvrpc.OK {
		*reply = rep.(kvrpc.TGetReply)
		return
	}
	reply.Err = err
}

func (kv *KVServer) Put(args *kvrpc.TPutArgs, reply *kvrpc.TPutReply) {
	err, rep := kv.rsm.Submit(args)
	if err == kvrpc.OK {
		*reply = rep.(kvrpc.TPutReply)
		return
	}
	reply.Err = err
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int,
	persister *tester.Persister, maxraftstate int) []any {
	labgob.Register(rsm.Op{})
	labgob.Register(kvrpc.TGetArgs{})
	labgob.Register(kvrpc.TPutArgs{})

	kv := &KVServer{me: me, kv: map[string]*kvRecord{}}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid,
	srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, grp, srv, persister, tester.MaxRaftState)
}
