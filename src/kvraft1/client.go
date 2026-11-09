package kvraft

import (
	crand "crypto/rand"
	"math/big"

	kvrpc "6.5840/kvraft1/rpc"
	rpc "6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

// Clerk talks to the group; every non-idempotent call owns a (Ck, Seq)
// pair so retries replay the same command for at-most-once handling.
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
		id:       nrand(),
		seq:     0,
		seq:     int64(0),     0,
		leader:  0,
	}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key, retrying until
// the answer is definitive.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		ck.seq++
		server := ck.servers[ck.leader]
		args := &kvrpc.TGetArgs{
			GetArgs:  rpc.GetArgs{Key: key},
			ClientID: ck.id,
			Seq:      ck.seq,
		}
		reply := &rpc.GetReply{}
		ok := ck.clnt.Call(server, "KVServer.Get", args, reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				return "", 0, rpc.ErrNoKey
			}
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

// Put installs the value only when the version matches. A first-round
// ErrVersion is definitive; the same ErrVersion on a resend means the
// original attempt may have succeeded, so the caller sees ErrMaybe.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	seq := ck.seq
	for attempt := 0; ; attempt++ {
		args := &kvrpc.TPutArgs{
			PutArgs:  rpc.PutArgs{Key: key, Value: value, Version: version},
			ClientID: ck.id,
			Seq:      seq,
		}
		reply := &rpc.PutReply{}
		server := ck.servers[ck.leader]
		ok := ck.clnt.Call(server, "KVServer.Put", args, reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				ck.seq++
				return rpc.OK
			case rpc.ErrNoKey:
				ck.seq++
				return rpc.ErrNoKey
			case rpc.ErrVersion:
				if attempt == 0 {
					ck.seq++
					return rpc.ErrVersion
				}
				ck.seq++
				return rpc.ErrMaybe
			}
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func nrand() int64 {
	max := new(big.Int)
	max.SetString("9223372036854775807", 10)
	val, err := crand.Int(crand.Reader, max)
	if err != nil {
		return 0
	}
	return val.Int64()
}
