package rsm

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// Op is one FSM command flowing through Raft. The (Ck, Seq) pair comes
// from the clerk's wire call, so exactly-once replay is decidable across
// retries and restarts.
type Op struct {
	Srv int   // submitting rsm's index into peers[]
	Ck  int64 // clerk ID supplied by the wire wrapper
	Seq int64 // per-clerk serial number
	Req any
}

// A server that wants to replicate itself calls MakeRSM and must
// implement the StateMachine interface.
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

	// duplicate table and waiters
	results        map[string]any  // (Ck:Seq) -> DoOp result
	waitCh         map[string]chan any
	lastApplied    int
	applyEpoch     int
}

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
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applier(0)
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func dedupKey(srv int, ck int64, seq int64) string {
	var b bytes.Buffer
	b.WriteString(strconv.Itoa(srv))
	b.WriteString(":")
	b.WriteString(strconv.FormatInt(ck, 10))
	b.WriteString(":")
	b.WriteString(strconv.FormatInt(seq, 10))
	return b.String()
}

// Submit a command carrying its clerk identity and wait for it to be
// committed exactly once. Term changes hand ErrWrongLeader to the clerk.
func (rsm *RSM) Submit(clk int64, seq int64, req any) (rpc.Err, any) {
	key := dedupKey(rsm.me, clk, seq)
	for {
		rsm.mu.Lock()
		if rep, ok := rsm.results[key]; ok {
			rsm.mu.Unlock()
			return rpc.OK, rep
		}
		_, isLeader := rsm.rf.GetState()
		if !isLeader {
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
		ch, known := rsm.waitCh[key]
		if !known {
			ch = make(chan any, 1)
			rsm.waitCh[key] = ch
			go rsm.launch(Op{Srv: rsm.me, Ck: clk, Seq: seq, Req: req})
		}
		rsm.mu.Unlock()

		select {
		case rep := <-ch:
			return rpc.OK, rep
		case <-time.After(200 * time.Millisecond):
			// loop around: the StateMachine may have a new leader now
		}
	}
}

// launch keeps re-floating one op through raft while leadership holds;
// repeated Start() calls of the same op are safe because the dedup table
// only lets the first apply reach the FSM.
func (rsm *RSM) launch(op Op) {
	for attempt := 0; attempt < 12; attempt++ {
		rsm.mu.Lock()
		if _, seen := rsm.results[dedupKey(op.Srv, op.Ck, op.Seq)]; seen {
			rsm.mu.Unlock()
			return
		}
		_, isLeader := rsm.rf.GetState()
		rsm.mu.Unlock()
		if !isLeader {
			return
		}
		_, _, ok := rsm.rf.Start(op)
		if !ok {
			return
		}
		time.Sleep(80 * time.Millisecond)
	}
}

// applier consumes committed commands in order, executes unknown ones
// exactly once, resolves waiters, and snapshots past maxraftstate.
func (rsm *RSM) applier(epoch int) {
	for msg := range rsm.applyCh {
		if msg.SnapshotValid {
			rsm.mu.Lock()
			rsm.lastApplied = msg.SnapshotIndex
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Unlock()
			continue
		}
		if !msg.CommandValid {
			continue
		}
		op, good := msg.Command.(Op)
		if !good {
			log.Printf("applier: non-Op command at index %v", msg.CommandIndex)
			continue
		}
		key := dedupKey(op.Srv, op.Ck, op.Seq)
		rsm.mu.Lock()
		rep, seen := rsm.results[key]
		if !seen {
			rep = rsm.sm.DoOp(op.Req)
			rsm.results[key] = rep
			rsm.lastApplied = msg.CommandIndex
		}
		waiter := rsm.waitCh[key]
		delete(rsm.waitCh, key)
		snap := rsm.maxraftstate > 0 && rsm.lastApplied%10 == 0 &&
			rsm.rf.PersistBytes() > rsm.maxraftstate
		rsm.mu.Unlock()
		if waiter != nil {
			waiter <- rep
		}
		if snap {
			blob := rsm.sm.Snapshot()
			rsm.rf.Snapshot(rsm.lastApplied, blob)
		}
	}
}
