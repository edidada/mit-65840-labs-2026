package rsm

import (
	"bytes"
	crand "crypto/rand"
	"log"
	"math/big"
	"os"
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

var traceRequests = os.Getenv("MIT65840_TRACE_REQUESTS") == "1"

// Op is one command flowing through Raft. Requests that carry a clerk
// identity (lab 4B get/put) dedup on (Ck, Seq); other requests (lab 4A
// harness ops such as Inc/Null/Dec) are identified by a random Id per
// Submit call and always execute.
type Op struct {
	Me     int
	Id     int64
	Ck     int64
	Seq    int64
	Cloned bool // Ck/Seq carry a clerk identity
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

// dupEntry holds one clerk's identity: only the latest applied Seq and
// its reply; earlier entries are superseded because a clerk issues ops
// sequentially.
type dupEntry struct {
	Seq int64
	Rep any
}

// waiter is registered by Submit and resolved by the applier.
type waiter struct {
	seq int64
	ch  chan any
}

type RSM struct {
	mu           sync.Mutex
	me           int
	Tag          string // optional group prefix for tracing
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	dup           map[int64]dupEntry // clerk id -> latest seq + reply
	waiters       map[string]*waiter // op key -> submitter
	lastApplied   int
	lastSnapIndex int
}

// servers[] contains the ports of the servers cooperating via Raft.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		dup:          map[int64]dupEntry{},
		waiters:      map[string]*waiter{},
	}
	labgob.Register(Op{})
	labgob.Register(kvrpc.TGetArgs{})
	labgob.Register(kvrpc.TPutArgs{})
	labgob.Register(kvrpc.TGetReply{})
	labgob.Register(kvrpc.TPutReply{})
	labgob.Register(dupEntry{})
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applier()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// SetTag sets this server's tracing label and propagates it to raft.
func (rsm *RSM) SetTag(tag string) {
	rsm.Tag = tag
	if rsm.rf != nil {
		rsm.rf.TraceTag(tag)
	}
}

// who returns the tracing label for this server.
func (rsm *RSM) who() string {
	if rsm.Tag == "" {
		return strconv.Itoa(rsm.me)
	}
	return rsm.Tag + "/" + strconv.Itoa(rsm.me)
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
	case kvrpc.TGetArgs:
		op.Ck, op.Seq, op.Cloned = r.Id, r.Seq, true
		op.Req = r
	case kvrpc.TPutArgs:
		op.Ck, op.Seq, op.Cloned = r.Id, r.Seq, true
		op.Req = r
	}
	return op
}

func opKey(op Op) string {
	if op.Cloned {
		return "c:" + strconv.FormatInt(op.Ck, 10)
	}
	return "h:" + strconv.FormatInt(op.Id, 10)
}

// Submit a command to Raft, and wait for it to commit. It returns
// ErrWrongLeader if the caller should find a new leader and try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	return rsm.submit(req, 0)
}

// SubmitWithTimeout lets an RPC handler bound a minority leader's response
// time. Submit itself retains the Lab4A contract of waiting for leadership
// to change or the command to commit.
func (rsm *RSM) SubmitWithTimeout(req any, timeout time.Duration) (rpc.Err, any) {
	return rsm.submit(req, timeout)
}

func (rsm *RSM) submit(req any, timeout time.Duration) (rpc.Err, any) {
	op := rsm.newOp(req)
	key := opKey(op)
	if traceRequests {
		log.Printf("[rsm %v] Submit %v cloned=%v key=%v", rsm.who(), op.Id, op.Cloned, key)
	}

	w := &waiter{seq: op.Seq, ch: make(chan any, 1)}
	rsm.mu.Lock()
	if op.Cloned {
		if entry, ok := rsm.dup[op.Ck]; ok && entry.Seq == op.Seq {
			rsm.mu.Unlock()
			return rpc.OK, entry.Rep
		}
	}
	rsm.waiters[key] = w
	rsm.mu.Unlock()
	defer func() {
		rsm.mu.Lock()
		if rsm.waiters[key] == w {
			delete(rsm.waiters, key)
		}
		rsm.mu.Unlock()
	}()
	_, term, leader := rsm.rf.Start(op)
	if !leader {
		return rpc.ErrWrongLeader, nil
	}
	// Bound waits on a partitioned leader. Never re-append a request
	// here: the client retries, and the service's version/fence makes
	// a late commit safe. Release the waiter on every return path.
	var deadline <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		deadline = timer.C
	}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case rep := <-w.ch:
			return rpc.OK, rep
		case <-deadline:
			return rpc.ErrWrongLeader, nil
		case <-tick.C:
			nowTerm, nowLeader := rsm.rf.GetState()
			if !nowLeader || nowTerm != term {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

// applier consumes committed commands in order, executes unknown ones
// exactly once, resolves waiters, and snapshots the raft log when it
// grows past maxraftstate.
func (rsm *RSM) applier() {
	var lastRecv time.Time
	for msg := range rsm.applyCh {
		now := time.Now()
		if !lastRecv.IsZero() && now.Sub(lastRecv) > 300*time.Millisecond {
			log.Printf("[rsm %v] apply feed gap %v before idx=%v", rsm.who(), now.Sub(lastRecv), msg.CommandIndex)
		}
		lastRecv = now
		t0 := time.Now()
		if msg.SnapshotValid {
			rsm.installSnapshot(msg.Snapshot, msg.SnapshotIndex)
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
		rsm.lastApplied = msg.CommandIndex
		rsm.lastSnapIndex = msg.CommandIndex
		if !isOp {
			rsm.mu.Unlock()
			log.Printf("applier: non-Op command at index %v", msg.CommandIndex)
			continue
		}
		if traceRequests {
			log.Printf("[rsm %v] applied idx=%v id=%v cloned=%v", rsm.who(), msg.CommandIndex, op.Id, op.Cloned)
		}
		key := opKey(op)
		var rep any
		if op.Cloned {
			entry, exists := rsm.dup[op.Ck]
			switch {
			case exists && entry.Seq == op.Seq:
				rep = entry.Rep // replay: nothing to execute
			case exists && entry.Seq > op.Seq:
				rep = nil // superseded stale replay
			default:
				rep = rsm.sm.DoOp(op.Req)
				rsm.dup[op.Ck] = dupEntry{Seq: op.Seq, Rep: rep}
			}
		} else {
			rep = rsm.sm.DoOp(op.Req)
		}
		snap := rsm.maxraftstate > 0 && rsm.rf.PersistBytes() > rsm.maxraftstate
		// hand over the waiter that matches this seq
		var notify *waiter
		if w, ok := rsm.waiters[key]; ok && w.seq == op.Seq {
			notify = w
			delete(rsm.waiters, key)
		}
		rsm.mu.Unlock()

		if notify != nil && rep != nil {
			select {
			case notify.ch <- rep:
			default:
			}
		}
		if d := time.Since(t0); d > 100*time.Millisecond {
			log.Printf("[rsm %v] slow apply idx=%v took %v", rsm.who(), msg.CommandIndex, d)
		}
		if snap {
			rsm.takeSnapshot()
		}
	}
}

// installSnapshot feeds a leader-provided or persisted snapshot into
// both the duplicate table and the service state machine.
func (rsm *RSM) installSnapshot(blob []byte, index int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	d := labgob.NewDecoder(bytes.NewBuffer(blob))
	var dup map[int64]dupEntry
	var kvBlob []byte
	if d.Decode(&dup) == nil && d.Decode(&kvBlob) == nil {
		rsm.dup = dup
		rsm.sm.Restore(kvBlob)
	} else {
		rsm.sm.Restore(blob)
	}
	rsm.lastApplied = index
}

// takeSnapshot writes {dup table, kv state} through raft so a restart
// or an install reproduces both halves.
func (rsm *RSM) takeSnapshot() {
	rsm.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rsm.dup)
	e.Encode(rsm.sm.Snapshot())
	blob := w.Bytes()
	idx := rsm.lastSnapIndex
	rsm.mu.Unlock()
	rsm.rf.Snapshot(idx, blob)
}
