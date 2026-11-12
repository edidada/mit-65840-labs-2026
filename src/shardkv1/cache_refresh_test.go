package shardkv

import (
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
)

// The cache must not pin a clerk to an old group after a completed leave
// physically stops that group's servers. No ErrWrongGroup reply can then
// arrive to invalidate the cached route.
func TestRetiredGroupCacheRefresh(t *testing.T) {
	ts := MakeTest(t, "retired group cache refresh", true)
	defer ts.Cleanup()
	ts.setupKVService()
	ck := ts.MakeClerk()
	keys, values := ts.SpreadPuts(ck, NKEYS)
	gid := ts.newGid()
	if !ts.joinGroups(ts.ShardCtrler(), []tester.Tgid{gid}) {
		t.Fatal("join failed")
	}
	cfg := ts.ShardCtrler().Query()
	index := -1
	for i, key := range keys {
		if cfg.Shards[shardcfg.Key2Shard(key)] == gid {
			index = i
			break
		}
	}
	if index < 0 {
		t.Fatal("no key moved to joined group")
	}
	ts.CheckGet(ck, keys[index], values[index], 1)
	if !ts.leaveGroups(ts.ShardCtrler(), []tester.Tgid{gid}) {
		t.Fatal("leave failed")
	}
	type result struct {
		value   string
		version rpc.Tversion
		err     rpc.Err
	}
	done := make(chan result, 1)
	go func() {
		v, ver, err := ck.Get(keys[index])
		done <- result{v, ver, err}
	}()
	select {
	case r := <-done:
		if r.value != values[index] || r.version != 1 || r.err != rpc.OK {
			t.Fatal(r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("clerk remained on retired group")
	}
}
