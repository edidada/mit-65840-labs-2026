package rsm

import (
	"bytes"
	crand "crypto/rand"
	"log"
	"math/big"
	"strconv"
	"sync"
	"time"

	kvrpc "6.5840/kvraft1/rpc"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// Op is one command flowing through Raft. Requests that carry a clerk
// identity (lab 4B get/put) dedup on (Ck, Seq); other requests (lab 4A
// harness ops such as Inc/Null/Dec) dedup on a random Id issued per
// Submit call.
type Op struct {
	Me     int
	Id     int64
	Ck     int64
	Seq    int64
	Cloned bool // true when Ck/Seq carry a clerk identity
	Req    any
}

// A server that wants to replicate itself calls MakeRSM and must
// implement the StateMachine interface: DoOp executes an operation,
// Snapshot/Restore move the server's state in and out of Raft.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	// duplicate table and waiters, keyed by op identity
	results     map[string]any
	waitCh      map[string]chan any
	lastApplied    int
	lastSnapIndex  int
}

// servers[] contains the ports of the servers cooperating via Raft.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		results:      map[string]any{},
		waitCh:       map[string]chan any{},
	}
	labgob.Register(Op{})
	labgob.Register(kvrpc.TGetArgs{})
	labgob.Register(kvrpc.TPutArgs{})
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applier()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func nrand() int64 {
	max := new(big.Int)
	max.SetString("9223372036854775807", 10)
	val, err := crand.Int(crand.Reader, max)
	if err != nil {
		return time.Now().UnixNano()
	}
	return val.Int64()
}

// newOp wraps req in an Op carrying its dedup identity.
func (rsm *RSM) newOp(req any) Op {
	op := Op{Me: rsm.me, Id: nrand(), Req: req}
	switch r := req.(type) {
	case *kvrpc.TGetArgs:
		op.Ck, op.Seq, op.Cloned = r.Id, r.Seq, true
	case *kvrpc.TPutArgs:
		op.Ck, op.Seq, op.Cloned = r.Id, r.Seq, true
	}
	return op
}

func opKey(op Op) string {
	var b bytes.Buffer
	if op.Cloned {
		b.WriteString("c:")
		b.WriteString(strconv.FormatInt(op.Ck, 10))
		b.WriteString(":")
		b.WriteString(strconv.FormatInt(op.Seq, 10))
	} else {
		b.WriteString("h:")
		b.WriteString(strconv.FormatInt(op.Id, 10))
	}
	return b.String()
}

// Submit a command to Raft, and wait for it to commit. It returns
// ErrWrongLeader if the caller should find a new leader and try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	op := rsm.newOp(req)
	key := opKey(op)

	for noLeader := 0; ; {
		rsm.mu.Lock()
		if rep, ok := rsm.results[key]; ok {
			rsm.mu.Unlock()
			return rpc.OK, rep
		}
		ch := rsm.waitCh[key]
		if ch == nil {
			ch = make(chan any, 1)
			rsm.waitCh[key] = ch
		}
		rsm.mu.Unlock()

		_, _, isLeader := rsm.rf.Start(op)
		if !isLeader {
			// The op may still be committed by whoever replicated it;
			// wait briefly for the applier before giving up this term.
			noLeader++
			if noLeader > 10 {
				return rpc.ErrWrongLeader, nil
			}
			select {
			case rep := <-ch:
				return rpc.OK, rep
			case <-time.After(60 * time.Millisecond):
			}
			continue
		}
		noLeader = 0
		select {
		case rep := <-ch:
			return rpc.OK, rep
		case <-time.After(200 * time.Millisecond):
			// Re-check leadership below via the following Start call.
		}
	}
}

// applier consumes committed commands in order, executes unknown ones
// exactly once, resolves waiters, and snapshots the raft log when it
// grows past maxraftstate.
func (rsm *RSM) applier() {
	for msg := range rsm.applyCh {
		if msg.SnapshotValid {
			rsm.mu.Lock()
			rsm.sm.Restore(msg.Snapshot)
			rsm.lastApplied = msg.SnapshotIndex
			rsm.mu.Unlock()
			continue
		}
		if !msg.CommandValid {
			continue
		}
		rsm.mu.Lock()
		if msg.CommandIndex <= rsm.lastApplied {
			rsm.mu.Unlock()
			continue
		}
		op, isOp := msg.Command.(Op)
		if !isOp {
			log.Printf("applier: non-Op command at index %v", msg.CommandIndex)
			rsm.lastApplied = msg.CommandIndex
			rsm.mu.Unlock()
			continue
		}
		key := opKey(op)
		rep, seen := rsm.results[key]
		if !seen {
			rep = rsm.sm.DoOp(op.Req)
			rsm.results[key] = rep
		}
		rsm.lastApplied = msg.CommandIndex
		snap := rsm.maxraftstate > 0 && rsm.rf.PersistBytes() > rsm.maxraftstate
		rsm.lastSnapIndex = msg.CommandIndex
		waiter := rsm.waitCh[key]
		rsm.mu.Unlock()

		if waiter != nil {
			select {
			case waiter <- rep:
			default:
			}
		}
		if snap {
			rsm.mu.Lock()
			blob := rsm.sm.Snapshot()
			idx := rsm.lastSnapIndex
			rsm.mu.Unlock()
			rsm.rf.Snapshot(idx, blob)
		}
	}
}
