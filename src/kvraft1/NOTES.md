# kvraft Lab4 — WIP notes (not accepted)

Started 2026-11-09: rsm.Submit + applier bootstrap restored to official starter after an
uncommitted iteration. Open design questions before the first green run:

1. rpc.PutArgs has no (Clk,Seq) identity; dedup needs a kvraft-internal rpc wrapper
   (e.g. type PutPlus struct { Args rpc.PutArgs; Clk int64; Seq int64 }) that the
   Clerk attaches in its own call to "KVServer.Put2" — or confirm the 2026 course
   keeps dedup containerized in rsm via a values-based replay map.
2. Tester side has no clerk identity either (kvtest.IKVClerk exposes only Put/Get),
   so clerk Ck derives from a rand at MakeClerk, Seq increments per non-idempotent op.
3. TestSnapshot* 4C need rsm.Snapshot -> rf.PersistBytes check each apply batch.
