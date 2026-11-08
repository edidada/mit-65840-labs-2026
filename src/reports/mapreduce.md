# 6.5840/6.824 Lab1：MapReduce（edidada/mapreduce 分支）

## 已通过的公开验证（2026-11-08）

验收提交 `fba909846d64f638a15b5027b99498851b696456`，[Actions run 42451372846](https://github.com/edidada/mit-65840-labs-2026/actions/runs/42451372846) 成功，官方 7 个测试全部通过：

| 测试 | 覆盖 |
|---|---|
| TestWc | 9 本书 wc.so 插件：三 worker / 10 reduce，输出与顺序版逐字节相同 |
| TestIndexer | indexer.so 插件同样通过 |
| TestMapParallel | mtiming.so：2 个 map worker 并行（"parallel" 计数>0） |
| TestReduceParallel | rtiming.so：reduce 并行（"[a-z] 2"≥2） |
| TestJobCount | jobcount.so：每个 map 任务恰好执行一次（"a 8" 恰一次） |
| TestEarlyExit | 所有进程退出时输出已固化（初始合并 == 最终合并） |
| TestCrashWorker | crash.so 随机 kill worker 后重试仍然一致 |

## 实现

- `coordinator.go`：全局 map/reduce task 状态表 + 互斥锁；`AskTask` 先发 map，全部完成后发 reduce；同刻仅指派未完成且未在场的任务，标 `since` 启动时间；500ms 的 watchdog 将超过 10 秒未汇报的任务撤回重派（兼容 crash 与随机慢 worker）。`FinishTask` 重复完成也接受（revoke 场景无害）。`Done()` 为全部完成。
- `worker.go`：循环 AskTask；map 处理分片写 `mr-tmp-{map}-{reduce}`（经临时文件+rename 原子化），reduce 读取全部 `mr-tmp-*-{id}`、按 key 排序、`reducef` 聚合后写 `mr-out-%d`；`Wait` 500ms 轮询；`Exit` 返回。跨任务的 dial 失败重试 5 秒以吸收 coordinator 启动窗口。
- `rpc.go`：AskTask/FinishTask 与 TaskKind（Map/Reduce/Wait/Exit）。
- 判分器为官方 `src/mr/mr_test.go` 与 `util.go`（仅给 `mkCorrectOutput` 增补了 stderr 转发，便于诊断，不改变判分语义）；CI 用 `-buildmode=plugin` 构建 8 个 mrapps 插件与 main 二进制后在 `src/mr` 跑 `go test -v`。

## 边界与局限

- 所有构建/测试/执行都在 GitHub Actions；本地未编译或运行。
- 这是 Lab1 公开测试验收：不宣称 Gradescope 私有评分、更大规模输入、真实网络 RPC（官方测试仍用 unix socket 与本地 worker）或后续 SRFT/raft/shardkv。
- 测试中的 worker 输出走官方 `exec.Command` 管道；worktree 上无人工改动超过实现三个文件与判分器 stderr 转发一行。

## CI 调试史（远端流程保留记录）

`-buildmode=plugin` 缺失 → 插件构建失败；main 二进制未构建 → fork/exec 失败；插件输出路径错误（仓库根而非 src/mrapps）→ `realpath failed`；YAML 块内含列 0 的 heredoc 曾导致 startup failure（0 job）。全部以 commit 修复并保留历史。
