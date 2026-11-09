package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &rpc.GetArgs{Key: key}, &reply)
		if !ok {
			// Lost or undelivered request/response: keep trying.
			continue
		}
		return reply.Value, reply.Version, reply.Err
	}
}

// Put installs the value if the version matches. On the unreliable network a
// lost OK answer makes the retry look like a duplicate: a second ErrVersion
// can then mean the first attempt actually succeeded, which the tester wants
// surfaced as ErrMaybe.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	reply := rpc.PutReply{}
	ok := ck.clnt.Call(ck.server, "KVServer.Put", &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}, &reply)
	if !ok {
		// The request wasn't answered at all; retry once from scratch.
		if err := ck.RetryPut(key, value, version); err != rpc.OK {
			if err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			return err
		}
		return rpc.OK
	}
	return reply.Err
}

// RetryPut repeats a Put after an unanswered attempt. The first duplicate
// rejection must map to ErrMaybe, later direct answers pass through.
func (ck *Clerk) RetryPut(key, value string, version rpc.Tversion) rpc.Err {
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &rpc.PutArgs{
			Key:     key,
			Value:   value,
			Version: version,
		}, &reply)
		if !ok {
			continue
		}
		return reply.Err
	}
}
