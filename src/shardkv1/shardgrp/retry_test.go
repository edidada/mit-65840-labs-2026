package shardgrp

import (
	"testing"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

// Replayed migration commands can arrive after a client write, deletion,
// or a later handoff. Their fence must survive both replay and snapshots.
func TestMigrationReplayFences(t *testing.T) {
	key := "replay-key"
	s := shardcfg.Key2Shard(key)
	kv := &KVServer{shards: map[shardcfg.Tshid]*shard{}}
	state := encodeKV(map[string]*kvRecord{key: {Val: "before", Ver: 1}})
	install := &shardrpc.InstallShardArgs{Shard: s, State: state, Num: 2}
	if r := kv.doInstall(install).(shardrpc.InstallShardReply); r.Err != rpc.OK {
		t.Fatal(r)
	}
	if r := kv.doPut(key, "after", 1).(rpc.PutReply); r.Err != rpc.OK {
		t.Fatal(r)
	}
	kv.doInstall(install)
	if r := kv.doGet(key).(rpc.GetReply); r.Value != "after" || r.Version != 2 {
		t.Fatal(r)
	}
	kv.doFreeze(&shardrpc.FreezeShardArgs{Shard: s, Num: 3})
	kv.doDelete(&shardrpc.DeleteShardArgs{Shard: s, Num: 3})
	restored := &KVServer{}
	restored.Restore(kv.Snapshot())
	restored.doInstall(install)
	restored.doInstall(&shardrpc.InstallShardArgs{Shard: s, State: state, Num: 3})
	if r := restored.doGet(key).(rpc.GetReply); r.Err != rpc.ErrWrongGroup {
		t.Fatal(r)
	}
	if r := restored.doFreeze(&shardrpc.FreezeShardArgs{Shard: s, Num: 3}).(shardrpc.FreezeShardReply); r.Err != rpc.ErrWrongGroup {
		t.Fatal(r)
	}
	// A genuinely later handoff may reacquire the shard.
	restored.doInstall(&shardrpc.InstallShardArgs{Shard: s, State: state, Num: 4})
	if r := restored.doGet(key).(rpc.GetReply); r.Value != "before" || r.Err != rpc.OK {
		t.Fatal(r)
	}
}
