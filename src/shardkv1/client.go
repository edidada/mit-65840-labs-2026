package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler

	num  shardcfg.Tnum            // config num the rcks cache belongs to
	rcks map[tester.Tgid]*shardgrp.Clerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
		rcks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// refresh looks up the group responsible for the key in the current
// configuration, (re)building the per-group clerk cache when the
// configuration number changed.
func (ck *Clerk) refresh(key string) (tester.Tgid, *shardgrp.Clerk) {
	s := shardcfg.Key2Shard(key)
	for {
		cfg := ck.sck.Query()
		gid := cfg.Shards[s]
		if _, ok := cfg.Groups[gid]; !ok {
			continue
		}
		if ck.num != cfg.Num {
			ck.num = cfg.Num
			ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
			for g, srvs := range cfg.Groups {
				ck.rcks[g] = shardgrp.MakeClerk(ck.clnt, srvs)
			}
		}
		return gid, ck.rcks[gid]
	}
}

// Get a key from a shardgrp. Retries until it gets a definitive
// answer; ErrWrongGroup means the configuration has moved under us,
// so re-query and try again.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		_, rck := ck.refresh(key)
		value, ver, err := rck.Get(key)
		if err == rpc.ErrWrongGroup {
			ck.num = 0 // force refresh next round
			continue
		}
		return value, ver, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		_, rck := ck.refresh(key)
		err := rck.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			ck.num = 0
			continue
		}
		return err
	}
}
