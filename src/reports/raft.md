# 6.5840/6.824 Lab3 Raft（edidada/raft 分支）

## 已通过的公开验证（2026-11-09）

最终验收 HEAD `1d7f33e6b7131991e15655b1d9b7ecd1ba4482dc`，[Actions run 42574136054](https://github.com/edidada/mit-65840-labs-2026/actions/runs/42574136054) **全部 28 项官方 raft 测试 PASS**：

- 3A: TestInitialElection3A / TestReElection3A / TestManyElections3A（3.34–6.30 s）
- 3B: TestBasicAgree3B / TestRPCBytes3B / TestFollowerFailure3B / TestLeaderFailure3B / TestFailAgree3B / TestFailNoAgree3B / TestConcurrentStarts3B / TestRejoin3B / TestBackup3B / TestCount3B（0.63–11.31 s）
- 3C: TestPersist1/2/3C、TestFigure83C、TestUnreliableAgree3C、TestFigure8Unreliable3C、TestReliableChurn3C、TestUnreliableChurn3C
- 3D: TestSnapshotBasic3D / TestSnapshotInstall3D / TestSnapshotInstallUnreliable3D / TestSnapshotInstallCrash3D / TestSnapshotInstallUnCrash3D / TestSnapshotAllCrash3D / TestSnapshotInit3D

## 实现

`src/raft1/raft.go`（3A–3D 全部）：
- **3A** 选举：随机 300–450 ms `electionEnd`（validTerm AppendEntries/RequestVote 均复置）；RequestVote 持 `votedFor` 一次性；ticker `state != leader && now > electionEnd` 才开始竞选；`>=` 多数获胜即向全体 dispatchAppend。
- **3B** 复制：AppendEntries 带 `ConflictIndex/ConflictTerm` 快速回退；`backupNextIndex` bounded 在 `[lastIncludedIndex+1, lastLogIndex+1]`；`leaderAdvanceCommit` 仅计本 term 记录并 majority 判定，`leaderApplyCommitted` 批量推送 applyCh；leader 自身在 `Start`/`tick` 队列写日志。
- **3C** 持久化：labgob 序列化 `currentTerm/votedFor/lastIncludedIndex/lastIncludedTerm/log`；每次 term 变化 / 投票 / 追加均 persist。
- **3D** snapshot：service 调 `Snapshot(index, bytes)` trim 前缀并原子持久化；`InstallSnapshot` RPC 直接格式化 follower 的 rf（tailed log 弃置，`lastApplied = boundary`），并**epoch-reset 淘汰 RAF 中还未交卡的 APPLY 队项**（epoch-aware applier goroutine），这是 TestSnapshotInstallUnreliable3D 修复的关键：不能留存前 leader 的并发 Apply 令 rs 报 `apply out of order`。

## 迭代修复史（commits 保留在分支历史）

36ba397 →786… 等 15 轮 repo 修复。关键 bug：①leader ticker 竞选（提交无 majority）；②leaderAppend 竞态锁；③commit majority 初值误差；④leader 自身 apply 遗漏；⑤backupNextIndex 越界；⑥labgob 队列 + applyCh 阻塞；⑦PrevLogIndex 归一化到 snapshot 边界时 entry.Index 权威化；⑧InstallSnapshot blob 持久化 + epoch queue reset。

## 边界与局限

- 所有构建/测试均在 GitHub Actions（`go test -race`）；本地不编译运行。
- 该 lab 是官方 tester 公开验收；非 Gradescope 私有评分，并仍不处理 lab 空间外的任何镜像 lab（kvraft/shardkv 均未实现）。
- 仅 raft1 模块；lab4/5 的使用者（kvraft1、shardkv1）未做。
