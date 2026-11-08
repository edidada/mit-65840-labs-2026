# 6.5840/6.824 Lab3 Raft（edidada/raft 分支，进行中—不作为验收）

## 状态（2026-11-09 快照）

HEAD a0eb15c4eca9bb214b9f8fd2cf9258bd2669db30 的 CI（run 见 Actions）28 项官方 raft 测试中 **23 PASS / 5 FAIL**：

- 全部 3A/3B（12 项）与全部 3C（9 项）通过：TestInitialElection3A、TestReElection3A、TestManyElections3A、TestBasicAgree3B、TestRPCBytes3B、TestFollowerFailure3B、TestLeaderFailure3B、TestFailAgree3B、TestFailNoAgree3B、TestConcurrentStarts3B、TestRejoin3B、TestBackup3B、TestCount3B、TestPersist1/2/3C、TestFigure83C、TestUnreliableAgree3C、TestFigure8Unreliable3C、TestReliableChurn3C、TestUnreliableChurn3C。
- 3D 仅 TestSnapshotBasic3D、TestSnapshotInit3D 通过；TestSnapshotInstall3D/Unreliable3D/Crash3D/UnCrash3D/AllCrash3D 失败（one(...) failed to reach agreement / apply out of order 混杂）。

## 已实现与修复史

-raft.go 全 3A-D：leader dispatch、election、AppendEntries、InstallSnapshot、persist（labgob）、Snapshot、conflict-index 加速、committed apply queue（8192 缓冲 applyInt + applier goroutine）。
- 修复次数 continuous：term mismatch 功能、commit majority 计数（排除 matchIndex 自 reputations）、backupNextIndex bounded by leader last log、InitSnapshot 电报 path、InstallSnapshot blob 持久化（r13 里 nil snapshot 问题的根源—service 的 snapshot blob 未出现在 persister.Save 中）。

## 下一断点

TestSnapshotInstall* 失败原因：在 disconnection 后 a victim 重连，rf.InstallSnapshot 走 applyCh 交付 SnapshotValid blob。3D `apply out of order, expected #N? got #M` 表染 tester log-server rs.lastApplied 与 rf.lastApplied 在 after-install 时以某种顺序 drift。下一个迭代应该：一方面确认 rf.InstallSnapshot 交付 blob *before* trimming 但 applicator goroutine 顺序约束；另一方面是 ts.g.RaftSize 检查 (MAXLOGSIZE=2000)——rf.persister.RaftStateSize 仍包含 rf.log beyond the prefix…

本 WIP 未进入台账，未宣称为完成。
