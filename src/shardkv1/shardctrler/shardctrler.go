package shardctrler

// Shardctrler with InitConfig, Query, and ChangeConfigTo methods.
//
// Config state lives in the lab2 kvsrv under two keys:
//
//	KEYCUR  -> current ShardConfig (JSON)
//	KEYNEXT -> pending ShardConfig (JSON) or "" when no change is in
//	           flight
//
// Controllers cooperate through the shared NEXT slot: a controller
// claims an empty slot with a versioned CAS put, runs the shard
// moves (FreezeShard/InstallShard/DeleteShard, all fenced by the
// config number inside the shardgrps), promotes the slot into CUR
// with a versioned CAS, and clears the slot. Any controller that
// finds a stuck NEXT (Num > cur.Num) first completes it (Part B/C
// recovery): the number fences inside the shardgrps make re-running
// an interrupted move idempotent.

import (
	"log"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const (
	KEYCUR  = "shardctrlerCur"
	KEYNEXT = "shardctrlerNext"

	slotPoll = 20 * time.Millisecond
	movePoll = 10 * time.Millisecond
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()
	id     string
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, id: tester.Randstring(6)}
	log.Printf("[ctl new] %v", sck.id)
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	return sck
}

// The tester calls InitController() before starting a new controller.
// In part A this has nothing to finish; in B and C it completes an
// interrupted ChangeConfigTo recorded in KEYNEXT.
func (sck *ShardCtrler) InitController() {
	sck.finishPending()
}

// Called once by the tester to supply the first configuration. The
// initial configuration lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	for {
		_, _, err := sck.IKVClerk.Get(KEYCUR)
		switch err {
		case rpc.OK:
			return // already initialized
		case rpc.ErrNoKey:
			if err := sck.IKVClerk.Put(KEYCUR, cfg.String(), 0); err == rpc.OK {
				return
			}
		}
		time.Sleep(slotPoll)
	}
}

// Return the current configuration.
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	for {
		value, _, err := sck.IKVClerk.Get(KEYCUR)
		if err == rpc.OK {
			return shardcfg.FromString(value)
		}
		if err == rpc.ErrNoKey {
			return shardcfg.MakeShardConfig()
		}
		time.Sleep(slotPoll)
	}
}

// readNext returns the pending config in the NEXT slot (nil when the
// slot is free) together with the slot's kvsrv version.
func (sck *ShardCtrler) readNext() (*shardcfg.ShardConfig, rpc.Tversion) {
	value, ver, err := sck.IKVClerk.Get(KEYNEXT)
	if err != rpc.OK || value == "" {
		return nil, ver
	}
	return shardcfg.FromString(value), ver
}

// casNext writes val into the NEXT slot with the version of a fresh
// read, retrying on races. val == "" clears the slot.
// Claim only an empty slot at the version we actually observed. A failed
// CAS is returned to the caller, which first helps the winning transition.
func (sck *ShardCtrler) casNext(val string) rpc.Err {
	value, ver, err := sck.IKVClerk.Get(KEYNEXT)
	if err != rpc.OK && err != rpc.ErrNoKey {
		return err
	}
	if value != "" {
		return rpc.ErrVersion
	}
	return sck.IKVClerk.Put(KEYNEXT, val, ver)
}

// commitCur publishes pending into CUR with a versioned CAS. It
// returns true when CUR is at pending.Num or beyond (someone else
// committed it, or the CAS hit).
func (sck *ShardCtrler) commitCur(pending *shardcfg.ShardConfig) bool {
	for {
		cur, ver, err := sck.IKVClerk.Get(KEYCUR)
		if err == rpc.OK {
			c := shardcfg.FromString(cur)
			if c.Num >= pending.Num {
				return true
			}
			if p := sck.IKVClerk.Put(KEYCUR, pending.String(), ver); p == rpc.OK {
				return true
			}
			time.Sleep(slotPoll)
			continue
		}
		if err == rpc.ErrNoKey {
			if p := sck.IKVClerk.Put(KEYCUR, pending.String(), 0); p == rpc.OK {
				return true
			}
			time.Sleep(slotPoll)
			continue
		}
	}
}

// clearNext drops the pending slot when it still holds pending (a
// newer pending value stands and must survive).
func (sck *ShardCtrler) clearNext(pending *shardcfg.ShardConfig) {
	for {
		value, ver, err := sck.IKVClerk.Get(KEYNEXT)
		if err == rpc.ErrNoKey || value == "" {
			return
		}
		if err != rpc.OK {
			time.Sleep(slotPoll)
			continue
		}
		if value != pending.String() {
			return
		}
		if sck.IKVClerk.Put(KEYNEXT, "", ver) == rpc.OK {
			return
		}
		// ErrMaybe needs a new read too: another helper may have advanced
		// both CUR and NEXT. Never CAS using a newly read slot blindly.
	}
}

// doMoves executes the Freeze/Install/Delete moves that bring cur's
// shard owners up to pending's, all fenced at pending.Num inside the
// shardgrps. It returns without error; every outage o wait is handled
// by the group clerks' retry loops.
func (sck *ShardCtrler) doMoves(cur *shardcfg.ShardConfig, pending *shardcfg.ShardConfig) {
	num := pending.Num
	for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
		sg, srcServers, srcOK := cur.GidServers(s)
		dg, dstServers, dstOK := pending.GidServers(s)
		if dg == sg {
			continue
		}
		for {
			// re-check the slot per attempt: a newer pending means the
			// newer controller redoes the remaining moves.
			if slot, _ := sck.readNext(); slot == nil || slot.Num != pending.Num {
				return
			}
			if dg == 0 || !dstOK {
				// shard released with no owner: freeze and drop it
				if !srcOK {
					break
				}
				switch _, ferr0 := freezeServer(sck.clnt, srcServers, s, num); ferr0 {
				case rpc.OK:
					if err := deleteServer(sck.clnt, srcServers, s, num); err == rpc.ErrWrongLeader {
						time.Sleep(movePoll)
						continue
					}
				case rpc.ErrWrongGroup:
					// nothing left there to freeze
				default:
					time.Sleep(movePoll)
					continue
				}
				break
			}
			if !srcOK {
				// stale cur after recovery: the shard's frozen state
				// already sits somewhere reachable; skip and re-check
				// via the outer finishPending loop.
				break
			}
			// join/move: freeze at source, install at destination,
			// delete at source (delete only after a confirmed install)
			state, ferr := freezeServer(sck.clnt, srcServers, s, num)
			log.Printf("[ctl %v] shard %d %v->%v freeze err=%v len=%d", sck.id, s, sg, dg, ferr, len(state))
			if ferr == rpc.ErrWrongLeader {
				time.Sleep(movePoll)
				continue
			}
			if ferr == rpc.OK {
				ierr := installServer(sck.clnt, dstServers, s, state, num)
				if ierr == rpc.ErrWrongLeader {
					time.Sleep(movePoll)
					continue
				}
				if ierr == rpc.OK {
					if deleteServer(sck.clnt, srcServers, s, num) == rpc.ErrWrongLeader {
						time.Sleep(movePoll)
						continue
					}
				}
			}
			break
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new. While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		cur := sck.Query()
		if cur.Num >= new.Num {
			log.Printf("[ctl %v] done cur=%v target=%v", sck.id, cur.Num, new.Num)
			return // superseded or already done
		}
		log.Printf("[ctl %v] ChangeConfigTo cur=%v target=%v", sck.id, cur.Num, new.Num)
		if sck.finishPending() {
			continue
		}
		// slot is empty: claim it for our candidate
		if e := sck.casNext(new.String()); e == rpc.OK {
			sck.finishPending()
		}
		// loop: re-check whether cur advanced (ours or a competitor)
	}
}

// finishPending drives the CURRENT pending slot to completion: runs
// its moves, promotes it into CUR, and clears the slot. It returns
// true when it observed a pending slot (so the caller re-checks cur),
// and false when the slot was empty.
func (sck *ShardCtrler) finishPending() bool {
	for {
		pending, _ := sck.readNext()
		log.Printf("[ctl %v] finishPending slot=%v", sck.id, pending != nil)
		if pending == nil {
			return false
		}
		cur := sck.Query()
		if cur.Num >= pending.Num {
			// already finished by another controller: clear the slot
			sck.clearNext(pending)
			continue
		}
		sck.doMoves(cur, pending)
		// re-read slot (it may have been replaced by a newer config)
		slot, _ := sck.readNext()
		if slot == nil || slot.Num > pending.Num {
			// superseded while moving: leave newer slot's work to it
			return true
		}
		if slot.Num == pending.Num {
			if sck.commitCur(pending) {
				sck.clearNext(pending)
			}
		}
	}
}

// The recovery entry point (init controller): finish what a previous
// controller left in the slot, repeatedly, since new pendings may be
// waiting.
func (sck *ShardCtrler) recoverNext() {
	for sck.finishPending() {
	}
}

// freezeServer freeze's the shard at its source group and returns the
// frozen state (nil state/ErrWrongGroup if that group doesn't own it).
func freezeServer(clnt *tester.Clnt, servers []string, s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	rck := shardgrp.MakeClerk(clnt, servers)
	return rck.FreezeShard(s, num)
}

func installServer(clnt *tester.Clnt, servers []string, s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	rck := shardgrp.MakeClerk(clnt, servers)
	return rck.InstallShard(s, state, num)
}

func deleteServer(clnt *tester.Clnt, servers []string, s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	rck := shardgrp.MakeClerk(clnt, servers)
	return rck.DeleteShard(s, num)
}
