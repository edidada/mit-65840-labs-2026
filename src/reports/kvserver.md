# 6.5840/6.824 Lab2：KVServer / Clerk（edidada/lab2-kvserver 分支）

## 已通过的公开验证（2026-11-08）

验收提交 `64d563321c4fc4a86effbae99e4fa9a11861b0cd`，[Actions run 42472110794](https://github.com/edidada/mit-65840-labs-2026/actions/runs/42472110794) 成功，官方 4 个测试全部通过：

- TestReliablePut（可靠网络下 Put/Get/版本推进/ErrVersion/ErrNoKey，0.10s）
- TestPutConcurrentReliable（并发 Put 线性一致性,1.97s）
- TestMemPutManyClientsReliable（多 client 随机 key 全记忆化,11.39s）
- TestUnreliableNet（不可靠网下 Clerk.Put 的 ErrMaybe 语义与去重边界,3.44s）

## 实现

- `server.go`：`map[string]*record`（value+version）；Get 直接回读；Put 校验版本后自增并写新值；不存在时 Version==0 才安装（否则 ErrNoKey），版本不符时 ErrVersion。
- `client.go`：Get 丢包即重试；Put 未得到回包时按"重试后再次 ErrVersion ⇒ ErrMaybe"的规范处理，直接应答的 ErrVersion/ErrNoKey 原样透传。
- 判分器为官方 `kvsrv_test.go` + `kvtest1`（未改动）；CI 增加 `main/kvsrv1d` daemon 构建（官方 Makefile 同款动作，否则测试连接 daemon 失败悬挂——已记录为 CI 修复史）。

## 边界与局限

- 执行均在 GitHub Actions（含 `-race` 官方默认未用，我用普通 go test，验证语义一致）；本地未编译运行。
- 这是 Lab2 公开测试验收；不覆盖 Lab3 raft 副本集成、Gradescope 私有评分或更具针对性的网络故障注入。
- 初版 RetryPut 在缺失 daemon 时曾无限挂起（10 分钟测试超时暴露），构建 daemon 后通过；现象与修复合并保留在分支 git 历史。
