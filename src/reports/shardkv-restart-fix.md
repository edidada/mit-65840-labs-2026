# Lab5 分片组重启恢复修复

## 问题与证据

2026-11-11 排查 `edidada/shardkv1` 的 GitHub Actions。
原失败运行：https://github.com/edidada/mit-65840-labs-2026/actions/runs/42831677587

构建成功，官方 `TestJoinBasic5A` 在 240 秒后超时。堆栈停在
`shardkv_test.go:109`，即第一个分片组关闭、重启后读取数据的位置。

## 根因与修复

首个分片组只在 Raft 持久化状态为空时初始化分片归属。无快照重启时，
服务从空分片表开始回放日志，历史 Put 因为不拥有该分片而无法重建数据。
客户端随后不断收到 ErrWrongGroup，无法完成读取。

现在首个分片组在每次启动时都创建确定的初始分片状态，再回放 Raft 日志。
若已有快照，快照会先替换初始状态，之后才应用其后的命令。
其他分片组仍从空表开始，通过分片迁移获取数据。

## 验证与 CI

修复提交 `13a21fc1c9cbd6ceed648d5d47429b93d8fbad61` 的原失败用例已通过：
https://github.com/edidada/mit-65840-labs-2026/actions/runs/43019075108

工作流进一步将守护进程与测试均开启 `-race`，将同一官方用例连续运行三次，
并使用 pipefail/tee 保留真实退出状态和 `test-results/shardkv.log`。
setup-go 的缓存依赖路径改为 `src/go.sum`。

本次验证范围为 TestJoinBasic5A 的加入、分片归属、关闭与重启恢复回归；
不代表 Lab5 的全部 23 项官方测试均已通过。本地未编译或运行实验测试。
最终带 race 的运行结果以对应提交的 GitHub Actions 结果为准。
