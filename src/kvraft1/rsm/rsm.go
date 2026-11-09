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

// An Op flows through Raft. Its (Clk, Seq) pair comes from the clerk's
// wire call, so exactly-once replay is decidable across retries.
type Op struct {
	Srv  int
	Ck   int64
	Seq  int64
	Req  any
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	// duplicate table and waiters
	results    map[string]any   // (Ck:Seq) -> DoOp result
	fate       map[string]rpc.Err
	waitCh     map[string]chan any
	lastApplied int
	applyEpoch  int
}

//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		results:      map[string]any{},
		fate:         map[string]rpc.Err{},
		waitCh:       map[string]chan any{},
	}
	labgob.Register(Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applier()
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

// Submit a command to Raft and wait for it to be committed. The caller
// has already attached the (Ck, Seq) identity to the request so replays
// can be answered from the duplicate table instead of re-executing.
func (rsm *RSM) Submit(clk int64, seq int64, req any) (rpc.Err, any) {
	key := dedupKey(rsm.me, clk, seq)
	for {
		rsm.mu.Lock()
		if rep, ok := rsm.results[key]; ok {
			rsm.mu.Unlock()
			return rpc.OK, rep
		}
		term, isLeader := rsm.rf.GetState()
		if !isLeader {
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
		ch, chOk := rsm.waitCh[key]
		if !chOk {
			ch = make(chan any, 1)
			rsm.waitCh[key] = ch
			go func(op Op, term int) {
				for attempt := 0; attempt < 8; attempt++ {
					rsm.mu.Lock()
					pending := rsm.results[dedupKey(op.Srv, op.Ck, op.Seq)] != nil
					_, leaderNow := rsm.rf.GetState()
					alive := leaderNow
					rsm.mu.Unlock()
					if pending || !alive {
						return
					}
					startTerm := 0
					rsm.mu.Lock()
					startTerm = rsm.applyEpoch
					rsm.mu.Unlock()
					_ = startTerm
					_, _, ok := rsm.rf.Start(op)
					if !ok {
						return
					}
					time.Sleep(60 * time.Millisecond)
				}
			}(Op{Srv: rsm.me, Ck: clk, Seq: seq, Req: req}, term)
		}
		rsm.mu.Unlock()

		select {
		case rep := <-ch:
			return rpc.OK, rep
		case <-time.After(120 * time.Millisecond):
			// keep polling: term refresh or re-submit from the new leader
		}
	}
}



type anyCache struct {
	op Op
}

// applier consumes committed commands in order, executes unknown ones
// exactly once, resolves waiters, and snapshots past maxraftstate.
func (rsm *RSM) applier() {
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
			rsm.fate[key] = rpc.OK
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
