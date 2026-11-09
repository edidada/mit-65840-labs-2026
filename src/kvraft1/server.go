package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rpc"
	"6.5840/kvraft1/rsm"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case *rpc.TGetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		rec, exists := kv.kv[args.Key]
		if !exists {
			return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: rec.value, Version: rec.ver, Err: rpc.OK}
	case *rpc.TPutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		rec, exists := kv.kv[args.Key]
		if !exists {
			if args.Version == 0 {
				kv.kv[args.Key] = &kvRecord{value: args.Value, ver: 1}
				return rpc.PutReply{Err: rpc.OK}
			}
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		if args.Version != rec.ver {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		rec.value = args.Value
		rec.ver++
		return rpc.PutReply{Err: rpc.OK}
	default:
		panic("kvraft DoOp unknown request type")
	}
}

func (kv *KVServer) Snapshot() []byte {
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
	kv.kv = table
}

func (kv *KVServer) Get(args *rpc.TGetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(args.ClientID, args.Seq, args)
	if err == rpc.OK {
		*reply = rep.(rpc.GetReply)
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) Put(args *rpc.TPutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(args.ClientID, args.Seq, args)
	if err == rpc.OK {
		*reply = rep.(rpc.PutReply)
	} else {
		reply.Err = err
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.TPutArgs{})
	labgob.Register(rpc.TGetArgs{})

	kv := &KVServer{me: me, kv: map[string]*kvRecord{}}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

// NewServer boots the tester harness view of this KVServer.
func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
