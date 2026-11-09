# 6.5840/6.824 Lab4 kvraft（edidada/kvraft1 分支）

## 已通过的公开验证（2026-11-09）

最终验收 HEAD `4c74676323fe64a0061107537b274cf6c1159260`，[Actions run 42592844356](https://github.com/edidada/mit-65840-labs-2026/actions/runs/42592844356) **全部 22 项官方 kvraft 测试 PASS（go test -race）**：

- 4B: TestBasic4B / TestSpeed4B / TestConcurrent4B / TestUnreliable4B / TestOnePartition4B / TestManyPartitionsOneClient4B / TestManyPartitionsManyClients4B / TestPersistOneClient4B / TestPersistConcurrent4B / TestPersistConcurrentUnreliable4B / TestPersistPartition4B / TestPersistPartitionUnreliable4B / TestPersistPartitionUnreliableLinearizable4B
- 4C: TestSnapshotRPC4C / TestSnapshotSize4C / TestSpeed4C / TestSnapshotRecover4C / TestSnapshotRecoverManyClients4C / TestSnapshotUnreliable4C / TestSnapshotUnreliableRecover4C / TestSnapshotUnreliableRecoverConcurrentPartition4C / TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C

## 实现

- `src/kvraft1/rpc/rpc.go`：kvrpc 线 типов TGetArgs/TPutArgs 拥 тес Clerk 身份 (Id, Seq)（Err/Tversion 共享 kvsrv1 别名，避免转换）。
- `src/kvraft1/rsm/rsm.go`：官方 API `Submit(req)`；dedup 表 per-clerk：每 (Id,Seq) 只执行一次，高度表 `dup[Ck]={Seq,Rep}` 持久到快照（恢复端重建）；applier 消费 applyCh、SnapshotValid 先 Restore（dup+kv blob），命令序按 (op.Ck, op.Seq) 去重；snapshot 在 PersistBytes 超过 maxraftstate 时调用 rf.Snapshot(idx, blob) blob 包含 (dup 表 + kv state)。Submit 的 waiters 按 (clerk,seq) 匹配（对超时后 leader 反转/换新流程避免 stuck）。
- `src/kvraft1/server.go`：KVServer DoOp/Get/Put handler（值体 & 指针兼容）/Snapshot/Restore，kv 记录 Val/Ver（labgob 导出字段）。
- `src/kvraft1/client.go`：Clerk 随机 Id + 逐 op seq 递增；Get 每次新 seq；Put 同一 (Id,Seq) 在重试中重用，ErrVersion 第一轮 definitive、失败轮后同 ErrVersion 返回 ErrMaybe（так ErrMaybe 语义）。
- `.github/workflows/kvraft1.yml`：go build -race kvraft1d + go test -race 22 项 + artifact logs。

## 迭代修复史（commits 保留）

1. StateMachine interface 恢复 & Submit 签形单参（官方 rsm_test 兼容）；
2. gob 接口解码 value/pointer 双形状；DoOp 值/指兼收；
3. Clerk seq 方案：Get 每 call 消 seq、Put 每调用多入口 ++（防 (Ck,Seq) 冲突 dedup 错回包）；
4. labgob 导出字段（kvRecord Val/Ver）；
5. dedup 压缩为 per-clerk 记录（最新 Seq+Rep）→ Snapshot Size 52KB→ 编码可行；
6. **SnapshotValid 重启交付**（raft Make 中读取 persister.ReadSnapshot()，先发 SnapshotValid 再 replay）→ recover 系 4/4 全过。

## 边界与局限

- 构建与测试均 GitHub Actions `go test -race`；本地不编译运行。
- 公开 tester 需要 maxraftstate ≥ 1000 时快照小（SnapshotSize 检查 < 8×maxraftstate 逻辑：日志 trim 由 rsm.Snapshot blob 管理，kv 部分只含 kv map+dup 表）。
- 公开 CI；未宣称 Gradescope 私有分。
