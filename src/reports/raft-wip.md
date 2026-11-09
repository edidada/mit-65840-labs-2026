# 6.5840/6.824 Lab3 Raft（edidada/raft 分支，进行中 — 不验收）

## 状态（2026-11-09 03:21 UTC 快照，HEAD 见 git log）

官方 28 项 raft 测试 **25 PASS / 3 FAIL**：

- PASS：全部 3A（3）、全部 3B（12）、全部 3C（9）、TestSnapshotBasic3D、TestSnapshotInstallCrash3D、TestSnapshotAllCrash3D、TestSnapshotInit3D（共 25）。
- FAIL：TestSnapshotInstall3D、TestSnapshotInstallUnreliable3D、TestSnapshotInstallUnCrash3D —— 断点日志显示 'one(...) failed to reach agreement' 与 'commit index=N server=X ... != server=Y'（各服务器同一下标上有不同已提交值，属 install-snapshot 后 follower 尾巴未被斩断/参数边界分支仍有一个残留竞态）。

## 修复史（本分支累计）

1. ticker 里 leader 不启动新选举；
2. leaderAppend 先取 rf.mu 锁（race 修复）；
3. leaderAdvanceCommit 只在多数时推进 commitIndex（TestFailNoAgree3B）；
4. leader 自身 apply 已提交项（TestBasicAgree3B 一类通吃）；
5. backupNextIndex 边界 bounded 于 leader last+1（TestBackup3B）；
6. 专用 applier goroutine（applyInt 队列）避免 applyCh 慢消费时锁住 raft；
7. AppendEntries 里 prev <= lastIncludedIndex 时把双方归一化到快照边界；
8. InstallSnapshot blob 持久化 + SnapshotValid 经 applyInt 顺序化交付（TestSnapshotInit3D/AllCrash3D 通过）；
9. 装 snapshot 后尾日志全弃（此轮改动）——未彻底解 Install3D / Unreliable3D / UnCrash3D。

## 下一断点

对照官方 3D solution 复查 InstallSnapshot + leaderAppend snapshot 分支，重点：a) rf.InstallSnapshot 后 nextIndex[server] 与 leader rf.applyInt 队列中残留 CommandValid 的交序；b) repeated InstallSnapshot IDs 的 ConflictIndex 加速竞态；c) fence post at `lastIncludedIndex+1`。
