package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// LogRecord is an entry of the replicated log.
type LogRecord struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// 3A: persistent state (Figure 2 "Persistent state on all servers")
	currentTerm int   // latest term server has seen
	votedFor    int   // candidateId that received vote in current term (-1 none)
	log         []LogRecord // entry 0 is a sentinel covering the snapshot prefix

	// snapshot prefix (3D)
	lastIncludedIndex int
	lastIncludedTerm  int

	// 3A: volatile state
	commitIndex int
	lastApplied int
	state       int // 0 follower, 1 candidate, 2 leader

	// 3B: leaders only (reinitialized after election)
	nextIndex  []int
	matchIndex []int

	applyCh     chan raftapi.ApplyMsg
	applyInt    chan raftapi.ApplyMsg
	electionEnd time.Time // reset whenever a valid message arrives
	granted     int       // votes received in this term
	leaderID    int

	}

const (
	follower  = 0
	candidate = 1
	leader    = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// lastLogIndex is the index of the newest entry in the log (0 if the
// log only covers a snapshot prefix).
func (rf *Raft) lastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

// lastLogTerm mirrors that index's term (0 if sentinel-only).
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// entryAt returns the record at index, or nil when the index is inside
// the snapshot prefix or beyond the end.
func (rf *Raft) entryAt(index int) *LogRecord {
	off := index - rf.lastIncludedIndex
	if off < 1 || off > len(rf.log) {
		return nil
	}
	return &rf.log[off-1]
}

// trimLogPrefix drops every local record at or below `index`, keeping
// the function of the in-memory log aligned with the snapshot.
func (rf *Raft) trimLogPrefix(index int) {
	off := index - rf.lastIncludedIndex
	if off < 0 || off > len(rf.log) {
		return
	}
	// Compute the boundary term while the records are still present.
	term := rf.entryTermInternal(index)
	if off == len(rf.log) {
		rf.log = nil
	} else {
		rf.log = append([]LogRecord{}, rf.log[off:]...)
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
}

// entryTermInternal returns the term of the entry at index, taking the
// snapshot prefix into account.
func (rf *Raft) entryTermInternal(index int) int {
	rec := rf.entryAt(index)
	if rec != nil {
		return rec.Term
	}
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return 0
}

// save Raft's persistent state to stable storage.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(data, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var lastIndex int
	var lastTerm int
	var log []LogRecord
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIndex) != nil ||
		d.Decode(&lastTerm) != nil ||
		d.Decode(&log) != nil {
		return
	}
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIndex
	rf.lastIncludedTerm = lastTerm
	rf.log = log
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}
	snapTerm := rf.entryTermInternal(index)
	rf.trimLogPrefix(index)
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.lastIncludedTerm = snapTerm
	// The service just wrote this snapshot itself; retire any queued
	// entries at or below index before they can double-deliver.
	rf.resetApplyQueue()
	// Persist the raft state with the service's snapshot bytes stored in
	// the persister's snapshot slot so a crash restores both halves.
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	rf.persister.Save(w.Bytes(), snapshot)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// candidateId re-answering with our known fact about the log.
func (rf *Raft) upToDate(lastLogIndex, lastLogTerm int) bool {
	lTerm, lIndex := rf.lastLogTerm(), rf.lastLogIndex()
	if lastLogTerm != lTerm {
		return lastLogTerm > lTerm
	}
	return lastLogIndex >= lIndex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
	}
	reply.VoteGranted = false
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if rf.upToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.resetElectionLocked()
		}
	}
	rf.persist()
}

// example AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogRecord
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool
	// conflict acceleration so a lagging leader backs up in one shot
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.ConflictIndex = rf.lastIncludedIndex + 1
	reply.ConflictTerm = 0
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = follower
	rf.leaderID = args.LeaderID
	rf.resetElectionLocked()

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}
	if args.PrevLogIndex >= rf.lastIncludedIndex &&
		rf.entryTermInternal(args.PrevLogIndex) != args.PrevLogTerm {
		// conflicts are accelerated: describe the term boundary.
		hole := args.PrevLogIndex
		if rec := rf.entryAt(args.PrevLogIndex); rec != nil {
			hole = rec.Index
		}
		reply.ConflictTerm = rf.entryTermInternal(hole)
		idx := hole
		for idx > rf.lastIncludedIndex+1 &&
			rf.entryTermInternal(idx-1) == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// Everything inside the snapshot prefix is correct on this peer
		// by construction; normalise the reference point to the boundary
		// and continue with entries beyond it (they arrive with indexes
		// above lastIncludedIndex, so skip any that overlap).
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
	}

	for _, entry := range args.Entries {
		// The record carries its own authoritative index, which keeps the
		// mapping valid when PrevLogIndex was normalised into the snapshot
		// prefix (see the branch above).
		ind := entry.Index
		if ind <= rf.lastIncludedIndex {
			continue
		}
		rec := rf.entryAt(ind)
		if rec == nil {
			rf.log = append(rf.log, LogRecord{
				Index: ind, Term: entry.Term, Command: entry.Command,
			})
			continue
		}
		if rec.Term != entry.Term {
			rf.log = append([]LogRecord{}, rf.log[:ind-rf.lastIncludedIndex-1]...)
			rf.log = append(rf.log, LogRecord{
				Index: ind, Term: entry.Term, Command: entry.Command,
			})
		}
	}
	reply.Success = true
	reply.ConflictIndex = rf.lastLogIndex() + 1
	rf.commitIndexAtLeast(args.LeaderCommit)
	rf.persist()
	rf.applyCommitted()
}

// commitIndexAtLeast advances commitIndex using information about the
// leader's commit point and unapplies nothing; used from AppendEntries.
func (rf *Raft) commitIndexAtLeast(commit int) {
	if commit > rf.commitIndex {
		if commit > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
		} else {
			rf.commitIndex = commit
		}
	}
}

// example InstallSnapshot RPC arguments structure (3D).
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// example InstallSnapshot RPC reply structure (3D).
type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC handler (3D).
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = follower
	rf.leaderID = args.LeaderID
	rf.resetElectionLocked()

	if args.LastIncludedIndex < rf.lastIncludedIndex {
		return
	}
	if args.LastIncludedIndex == rf.lastIncludedIndex &&
		args.LastIncludedTerm != rf.lastIncludedTerm {
		return
	}
	// 3D semantics: the installed snapshot covers everything through
	// LastIncludedIndex, so a follower whose log came back shorter or
	// clashed simply adopts the leader's prefix wholesale.
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	// Reset lastApplied to the installed boundary so the service sees a
	// single reference point: it will ingest the SnapshotValid message and
	// expect CommandValid messages from lastIncludedIndex+1 onward. A
	// max() here would leave the raft thinking it already applied entries
	// the queue had delivered before the install, causing "apply out of
	// order" at the service.
	rf.lastApplied = args.LastIncludedIndex
	// The follower's log tail beyond the installed snapshot belongs to
	// earlier, possibly conflicting leaders: discard it and let the
	// leader resend authoritative entries from lastIncludedIndex+1.
	rf.log = nil
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// The received blob becomes this peer's own snapshot state; persist
	// both halves atomically (the service will be told via applyCh).
	rf.persister.Save(func() []byte {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.lastIncludedIndex)
		e.Encode(rf.lastIncludedTerm)
		e.Encode(rf.log)
		return w.Bytes()
	}(), args.Data)
	// Tell the service and keep running.
	rf.applyInt <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

// backupNextIndex repositions a follower's nextIndex after a rejection.
// The next index can never fall below the snapshot prefix (lastIncluded+1)
// nor exceed the leader's own end of log, which is how a leader backs up
// quickly past a follower whose log was longer.
func (rf *Raft) backupNextIndex(reply *AppendEntriesReply, server int) {
	confl := reply.ConflictIndex
	if reply.ConflictTerm > 0 {
		first := confl
		for idx := confl; idx >= rf.lastIncludedIndex+1; idx-- {
			if rf.entryTermInternalClamp(idx) == reply.ConflictTerm {
				first = idx
			}
		}
		if rf.entryTermInternalClamp(first) == reply.ConflictTerm {
			confl = first
		}
	}
	rf.nextIndex[server] = max(rf.lastIncludedIndex+1,
		min(rf.lastLogIndex()+1, confl))
}

// Log entries are sent to the leader's peers asynchronously here.
func (rf *Raft) leaderAppend(server int) {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		// The follower needs a snapshot: entries below the local snapshot
		// prefix can never be sent as AppendEntries deltas.
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		var reply InstallSnapshotReply
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
		if !ok {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != leader || rf.currentTerm != term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = follower
			rf.votedFor = -1
			rf.persist()
			return
		}
		if rf.nextIndex[server] < args.LastIncludedIndex+1 {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
		if rf.matchIndex[server] < args.LastIncludedIndex {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		return
	}
	nextIndex := rf.nextIndex[server]
	prevIndex := nextIndex - 1

	prevTerm := rf.entryTermInternalClamp(prevIndex)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: rf.commitIndex,
	}
	if rec := rf.entryAt(nextIndex); rec != nil {
		off := nextIndex - rf.lastIncludedIndex - 1
		entries := make([]LogRecord, len(rf.log)-off)
		copy(entries, rf.log[off:])
		args.Entries = entries
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader || rf.currentTerm != term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = follower
		rf.votedFor = -1
		rf.persist()
		return
	}
	if reply.Success {
		if args.Entries != nil && len(args.Entries) > 0 {
			last := args.Entries[len(args.Entries)-1].Index
			if rf.matchIndex[server] < last {
				rf.matchIndex[server] = last
			}
			if rf.nextIndex[server] < last+1 {
				rf.nextIndex[server] = last + 1
			}
		} else {
			if rf.matchIndex[server] < args.PrevLogIndex {
				rf.matchIndex[server] = args.PrevLogIndex
			}
		}
		rf.leaderAdvanceCommit()
		return
	}
	rf.backupNextIndex(&reply, server)
}

// electionEndFromTick bumps the timer whenever someone needs their
// chance to win an election.
func (rf *Raft) resetElectionLocked() {
	rf.electionEnd = time.Now().
		Add(time.Duration(+(rand.Int63()%150)+300) * time.Millisecond)
}

// leaderAdvanceCommit only ever moves the commit index forward when a
// majority of peers confirm they've replicated the entries.
func (rf *Raft) leaderAdvanceCommit() {
	if rf.state != leader {
		return
	}
	mid := rf.commitIndex
	for idx := rf.lastLogIndex(); idx > rf.commitIndex; idx-- {
		count := 1
		rec := rf.entryAt(idx)
		if rec == nil || rec.Term != rf.currentTerm {
			continue
		}
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			if rf.matchIndex[server] >= idx {
				count++
			}
		}
		if count*2 > len(rf.peers) {
			mid = idx
			break
		}
	}
	if mid > rf.commitIndex {
		rf.commitIndex = mid
		rf.applyCommitted()
	}
}

// entryTermInternalClamp prevents an out of bounds query at index 0.
func (rf *Raft) entryTermInternalClamp(index int) int {
	if index <= rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.entryTermInternal(index)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft wants to start agreement on the next command.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return 0, rf.currentTerm, false
	}
	index := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogRecord{
		Index: index, Term: rf.currentTerm, Command: command,
	})
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()
	go rf.dispatchAppendAll(rf.currentTerm)
	return index, rf.currentTerm, true
}

// dispatchAppendAll kicks one append outbound pass.
func (rf *Raft) dispatchAppendAll(term int) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.leaderAppend(server)
	}
}

// the ticker drives election and heartbeat.
func (rf *Raft) ticker() {
	for true {
		rf.mu.Lock()
		state := rf.state
		if state != leader && time.Now().After(rf.electionEnd) {
			rf.startElectionLocked()
		}
		term := rf.currentTerm
		isLeader := rf.state == leader
		rf.mu.Unlock()

		if isLeader {
			rf.dispatchAppendAll(term)
		}

		ms := 25 + (rand.Int63() % 25)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// startElectionLocked transitions to candidate and sends vote requests
// synchronously by launching outbound goroutines.
func (rf *Raft) startElectionLocked() {
	rf.state = candidate
	rf.currentTerm++
	rf.granted = 1
	rf.votedFor = rf.me
	lastIdx := rf.lastLogIndex()
	lastTrm := rf.lastLogTerm()
	rf.persist()
	timeNow := time.Now()
	rf.electionEnd = timeNow.
		Add(time.Duration(+(rand.Int63()%150)+300) * time.Millisecond)
	term := rf.currentTerm
	rf.resetElectionLocked()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendVote(server, term, lastIdx, lastTrm)
	}
}

// sendVote sends one RequestVote and handles its reply.
func (rf *Raft) sendVote(server, term, lastIdx, lastTrm int) {
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTrm,
	}
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != candidate || rf.currentTerm != term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = follower
		rf.votedFor = -1
		rf.persist()
		return
	}
	if reply.VoteGranted {
		rf.granted++
	}
	if rf.granted*2 > len(rf.peers) {
		rf.state = leader
			rf.leaderID = rf.me
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.lastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = rf.lastLogIndex()
		// An immediate heartbeat fixes slow followers.
		rf.dispatchAppendAll(rf.currentTerm)
	}
}

// applyCommitted queues every committed-but-unapplied entry for the
// dedicated applier goroutine, so the raft mutex is never held while a
// possibly slow consumer drains applyCh.
func (rf *Raft) applyCommitted() {
	for rf.lastApplied < rf.commitIndex {
		rec := rf.entryAt(rf.lastApplied + 1)
		if rec == nil {
			break
		}
		rf.lastApplied++
		rf.applyInt <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rec.Command,
			CommandIndex: rec.Index,
		}
	}
}

// applier forwards queued messages onto the service channel without
// holding rf.mu.
func (rf *Raft) applier() {
	for msg := range rf.applyInt {
		rf.applyCh <- msg
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// the service or tester wants to create a Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.state = follower
	rf.votedFor = -1
		rf.leaderID = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.granted = 0
	rf.applyInt = make(chan raftapi.ApplyMsg, 8192)
	rf.resetElectionLocked()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}
