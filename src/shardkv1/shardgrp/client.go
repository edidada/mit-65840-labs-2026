package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	return &Clerk{
		Clnt:    clnt,
		servers: servers,
		leader:  0,
	}
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for attempts := 0; attempts < len(ck.servers); attempts++ {
		args := rpc.GetArgs{Key: key}
		var reply rpc.GetReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.Get",
			&args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey, rpc.ErrWrongGroup:
				return reply.Value, reply.Version, reply.Err
			}
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
	return "", 0, rpc.ErrWrongLeader
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	first := true
	for attempts := 0; attempts < len(ck.servers); attempts++ {
		args := rpc.PutArgs{Key: key, Value: value, Version: version}
		var reply rpc.PutReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.Put",
			&args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey:
				return reply.Err
			case rpc.ErrWrongGroup:
				if !first {
					// An unanswered Put may have committed before the shard
					// moved. Do not discard that ambiguity on a new group.
					return rpc.ErrMaybe
				}
				return rpc.ErrWrongGroup
			case rpc.ErrVersion:
				if first {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
		}
		first = false
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
	return rpc.ErrWrongLeader
}

// FreezeShard stops the shard at the source group and returns its
// state. ErrMaybe is not needed here: repeats are fenced by Num.
func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	for attempts := 0; attempts < len(ck.servers); attempts++ {
		args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
		var reply shardrpc.FreezeShardReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup) {
			return reply.State, reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
	return nil, rpc.ErrWrongLeader
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	for attempts := 0; attempts < len(ck.servers); attempts++ {
		args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
		var reply shardrpc.InstallShardReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup) {
			return reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	for attempts := 0; attempts < len(ck.servers); attempts++ {
		args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
		var reply shardrpc.DeleteShardReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup) {
			return reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
	return rpc.ErrWrongLeader
}
