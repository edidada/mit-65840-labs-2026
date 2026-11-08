# 6.5840/6.824 Lock 子实验（kvsrv1/lock，edidada/lock1 分支）

## 已通过的公开验证（2026-11-08）

验收提交 `fc4c5d17a794f033752541bc6c40556f22db4635`，[Actions run 42484940418](https://github.com/edidada/mit-65840-labs-2026/actions/runs/42484940418) 成功，官方 6 个测试全部通过、零 'two clients acquired' 断言：

- TestReliableBasic / TestReliableNested（0.11s 各）
- TestOneClientReliable（2.11s）/ TestManyClientsReliable（2.51s，10 客户端）
- TestOneClientUnreliable（2.10s）/ TestManyClientsUnreliable（10 客户端 + 不可靠网络）

## 实现

`lock.go`：用 Lab2 的 KVServer 存锁（key=lockname，值=随机 holder id）。Acquire 循环：ErrNoKey→装 holder；读到空值→以当前 version 装 holder；读到自己的 holder→确认（此前 ErrMaybe 的落地值）；他人在持→等待。Release：仅当值仍为本 holder 时以当前 version 清空；OK 直接返回，ErrMaybe **必须**再 Get 确认（可能丢答未生效），ErrVersion 表条目已被接管/清空（释放等效产物），否则重试。ErrMaybe 的两个使用点均不返回直到事实核验——CI 实测早版实现里把 ErrMaybe 当成功会导致 tester 的 'two clients acquired' 互斥断言破 (10 客户端不可靠网)。

判分器为官方 `lock_test.go`（未改动）；CI 增加 `main/kvsrv1d` daemon 构建（官方 Makefile 同款）。

## 边界与局限

- 执行均在 GitHub Actions；本地未编译运行。
- 这是 lock 子实验公开测试验收：不覆盖 Gradescope 私有评分、raft 挂接下的锁、crash-recovery 持久化或更严的公平性/无饿死规范；等待策略为 50ms 轮询而非变更通知。
