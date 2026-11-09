package kvraft

import (
	crand "crypto/rand"
	"math/big"

	kvrpc "6.5840/kvraft1/rpc"
	rpc "6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

// Clerk talks to the group. Every call owns a (Id, Seq) pair so
// retries replay the same command for at-most-once handling.
type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int

	id  int64
	seq int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:    clnt,
		servers: servers,
		leader:  0,
		id:      nrand(),
		seq:     0,
	}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key. Each Get uses a
// fresh Seq so the duplicate table never replays a stale value.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		ck.seq++
		args := &kvrpc.TGetArgs{Key: key, Id: ck.id, Seq: ck.seq}
		reply := &kvrpc.TGetReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", args, reply)
		if ok && reply.Err == kvrpc.OK {
			return reply.Value, reply.Version, rpc.OK
		}
		if ok && reply.Err == kvrpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

// Put updates key with value only if the version matches. A
// ErrVersion seen on the first attempt is definitive; the same
// ErrVersion after a failed attempt means the earlier try may have
// gone through, so the caller sees ErrMaybe.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	first := true
	for {
		args := &kvrpc.TPutArgs{Key: key, Value: value, Version: version,
			Id: ck.id, Seq: ck.seq}
		reply := &kvrpc.TPutReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", args, reply)
		if ok {
			switch reply.Err {
			case kvrpc.OK, kvrpc.ErrNoKey:
				ck.seq++
				return rpc.Err(reply.Err)
			case kvrpc.ErrVersion:
				ck.seq++
				if first {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			case kvrpc.ErrWrongLeader:
				// same Seq stays in flight; follow the leader
			}
		}
		first = false
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func nrand() int64 {
	max := new(big.Int)
	max.SetString("9223372036854775807", 10)
	val, err := crand.Int(crand.Reader, max)
	if err != nil {
		panic(err)
	}
	return val.Int64()
}
