package shardgrp

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type kvRecord struct {
	Val string
	Ver rpc.Tversion
}

// shard is one shard's FSM state on this group: the data and the
// ownership marker. State is "active" (this group serves it),
// "frozen" (this group stopped serving it and awaits Delete), or the
// shard is absent (never owned/given away).
type shard struct {
	State string // "active" | "frozen"
	Num   shardcfg.Tnum
	KV    map[string]*kvRecord
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	mu     sync.Mutex
	shards map[shardcfg.Tshid]*shard
}

func (kv *KVServer) DoOp(req any) any {
	switch a := req.(type) {
	case *rpc.GetArgs:
		return kv.doGet(a.Key)
	case rpc.GetArgs:
		return kv.doGet(a.Key)
	case *rpc.PutArgs:
		return kv.doPut(a.Key, a.Value, a.Version)
	case rpc.PutArgs:
		return kv.doPut(a.Key, a.Value, a.Version)
	case *shardrpc.FreezeShardArgs:
		return kv.doFreeze(a)
	case shardrpc.FreezeShardArgs:
		return kv.doFreeze(&a)
	case *shardrpc.InstallShardArgs:
		return kv.doInstall(a)
	case shardrpc.InstallShardArgs:
		return kv.doInstall(&a)
	case *shardrpc.DeleteShardArgs:
		return kv.doDelete(a)
	case shardrpc.DeleteShardArgs:
		return kv.doDelete(&a)
	default:
		panic("shardgrp DoOp: unknown request type")
	}
}

func (kv *KVServer) owns(key string) *shard {
	s := shardcfg.Key2Shard(key)
	sh, ok := kv.shards[s]
	if !ok || sh.State != "active" {
		return nil
	}
	return sh
}

func (kv *KVServer) doGet(key string) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sh := kv.owns(key)
	if sh == nil {
		return rpc.GetReply{Err: rpc.ErrWrongGroup}
	}
	rec, exists := sh.KV[key]
	if !exists {
		return rpc.GetReply{Err: rpc.ErrNoKey}
	}
	return rpc.GetReply{Value: rec.Val, Version: rec.Ver, Err: rpc.OK}
}

func (kv *KVServer) doPut(key string, value string, version rpc.Tversion) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sh := kv.owns(key)
	if sh == nil {
		return rpc.PutReply{Err: rpc.ErrWrongGroup}
	}
	rec, exists := sh.KV[key]
	if !exists {
		if version == 0 {
			sh.KV[key] = &kvRecord{Val: value, Ver: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrNoKey}
	}
	if version != rec.Ver {
		return rpc.PutReply{Err: rpc.ErrVersion}
	}
	rec.Val = value
	rec.Ver++
	return rpc.PutReply{Err: rpc.OK}
}

// doFreeze stops serving the shard as of Num and returns its state.
// Repeated freezes at the remembered Num hand back the same frozen
// state (how a recovered controller finds an interrupted install), and
// a freeze at a newer Num re-freezes whatever the group still holds
// (an older freeze that never got installed over).
func (kv *KVServer) doFreeze(args *shardrpc.FreezeShardArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sh, ok := kv.shards[args.Shard]
	switch {
	case !ok, sh.State == "deleted", args.Num < sh.Num, args.Num == sh.Num && sh.State == "active":
		// we don't own the shard as of this config number
		return shardrpc.FreezeShardReply{Num: sh.frozenNum(ok, args.Num), Err: rpc.ErrWrongGroup}
	default:
		// freeze (or re-freeze) at args.Num and hand back the state
		sh.State = "frozen"
		sh.Num = args.Num
		return shardrpc.FreezeShardReply{State: encodeKV(sh.KV), Num: sh.Num, Err: rpc.OK}
	}
}

func (sh *shard) frozenNum(ok bool, num shardcfg.Tnum) shardcfg.Tnum {
	if !ok {
		return 0
	}
	return sh.Num
}

// doInstall adopts the shard as of Num. Installing over means: newer
// number wins (replaces the state), equal number is idempotent (keep
// whatever data is already here unless the fresh state is non-empty).
func (kv *KVServer) doInstall(args *shardrpc.InstallShardArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sh, ok := kv.shards[args.Shard]
	if ok && args.Num < sh.Num {
		return shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
	}
	if ok && args.Num == sh.Num {
		// An install retry must never overwrite newer writes or revive a
		// shard that has since been frozen/deleted at the same fence.
		return shardrpc.InstallShardReply{Err: rpc.OK}
	}
	nsh := &shard{State: "active", Num: args.Num, KV: decodeKV(args.State)}
	if ok && len(args.State) == 0 {
		// keep previously adopted data, bump the fence
		nsh.KV = sh.KV
	}
	kv.shards[args.Shard] = nsh
	return shardrpc.InstallShardReply{Err: rpc.OK}
}

// doDelete drops the frozen shard's data to keep the snapshot small.
func (kv *KVServer) doDelete(args *shardrpc.DeleteShardArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sh, ok := kv.shards[args.Shard]
	switch {
	case !ok:
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	case args.Num < sh.Num:
		return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
	case sh.State != "frozen":
		return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
	default:
		// Retain the fence so a delayed Install cannot resurrect old data.
		sh.State = "deleted"
		sh.Num = args.Num
		sh.KV = nil
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var m map[shardcfg.Tshid]*shard
	if d.Decode(&m) != nil {
		return
	}
	kv.mu.Lock()
	kv.shards = m
	kv.mu.Unlock()
}

func encodeKV(kvMap map[string]*kvRecord) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kvMap)
	return w.Bytes()
}

func decodeKV(state []byte) map[string]*kvRecord {
	if len(state) < 1 {
		return map[string]*kvRecord{}
	}
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var m map[string]*kvRecord
	if d.Decode(&m) != nil {
		return map[string]*kvRecord{}
	}
	return m
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(args)
	log.Printf("[shard g%v/%v] Get %v -> %v", kv.gid, kv.me, args.Key, err)
	if err == rpc.OK {
		*reply = rep.(rpc.GetReply)
		return
	}
	reply.Err = err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(args)
	log.Printf("[shard g%v/%v] Put %v -> %v", kv.gid, kv.me, args.Key, err)
	if err == rpc.OK {
		*reply = rep.(rpc.PutReply)
		return
	}
	reply.Err = err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(args)
	log.Printf("[shard g%v/%v] FreezeShard shard=%v num=%v -> err=%v", kv.gid, kv.me, args.Shard, args.Num, err)
	if err == rpc.OK {
		*reply = rep.(shardrpc.FreezeShardReply)
		return
	}
	reply.Err = err
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(args)
	log.Printf("[shard g%v/%v] InstallShard shard=%v num=%v -> err=%v", kv.gid, kv.me, args.Shard, args.Num, err)
	if err == rpc.OK {
		*reply = rep.(shardrpc.InstallShardReply)
		return
	}
	reply.Err = err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(args)
	log.Printf("[shard g%v/%v] DeleteShard shard=%v num=%v -> err=%v", kv.gid, kv.me, args.Shard, args.Num, err)
	if err == rpc.OK {
		*reply = rep.(shardrpc.DeleteShardReply)
		return
	}
	reply.Err = err
}

// StartShardServerGrp starts a server for shardgrp `gid`.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int,
	persister *tester.Persister, maxraftstate int) []any {
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(kvRecord{})
	labgob.Register(shard{})
	labgob.Register(map[string]*kvRecord{})

	kv := &KVServer{me: me, gid: gid, shards: map[shardcfg.Tshid]*shard{}}
	// Recreate the first group's initial ownership before replaying its log.
	// A restored snapshot replaces this state before later commands apply.
	if gid == shardcfg.Gid1 {
		for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
			kv.shards[s] = &shard{
				State: "active",
				Num:   shardcfg.NumFirst,
				KV:    map[string]*kvRecord{},
			}
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.rsm.SetTag(fmt.Sprintf("g%d", gid))
	log.Printf("[shard g%v/%v] started raftlen=%v snaplen=%v", gid, me, persister.RaftStateSize(), persister.SnapshotSize())
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid,
	srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
