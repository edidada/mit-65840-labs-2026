# MIT 6.5840 Lab5 全套验证修复记录

本记录接续 shardkv-restart-fix.md。此前三次 TestJoinBasic5A 成功仅证明该单项；完整23项的 run43037644145 已失败，不能沿用单项结果宣称整个Lab5完成。

## 已确认问题

- RSM在非leader上反复等待十次才返回，每次客户端切换服务器都会付出约660ms代价；迁移中每个分片的Freeze/Install/Delete都重新找leader，放大为原始进度、加入和不可靠网络用例超时。
- 每次Get/Put都查询配置服务，稳定分片也依赖额外RPC。
- InstallShard重试携带非空数据时直接覆盖目标状态，可能回滚安装后发生的客户端写入；DeleteShard删除全部状态，无法抵挡迟到的旧Install。
- 控制器NEXT槽的CAS重试会用新版本覆盖竞争者配置；清理时重新读取版本但不核对该版本的内容，可能误清新的迁移。
- 源分片已被其他恢复控制器删除时，仍将空状态安装到目标。Put丢失回复后遇到分片迁移，也可能丢弃原写入的不确定性。

## 修复

RSM非leader立即返回，退出时清理自己的waiter；Submit保留Lab4A要求的等待契约，单独提供SubmitWithTimeout供分片RPC使用800ms响应期限。超时不撤销已进入Raft的命令，Put使用版本检查，迁移使用配置号实现安全重试。

客户端缓存配置，仅ErrWrongGroup后重新查询；Put的重试如遇迁移，保留ErrMaybe语义。分片相同配置号的重复安装不覆盖已应用数据；删除后保留配置号墓碑，并随快照恢复。更新配置号的合法迁移仍能重新取得分片。

后续静态检查发现缓存还必须处理分片组已退出、无法回复ErrWrongGroup的情况。分片组Clerk每轮最多尝试全部服务器一次，无明确回复时返回内部ErrWrongLeader；顶层Clerk重新查询配置并重试，跨轮Put仍保留不确定性。迁移RPC同样按轮返回，控制器在重试间核对NEXT，已完成或被替代的迁移不再追踪已退出的组。TestRetiredGroupCacheRefresh显式先缓存新组地址、完成leave并退出该组，再验证同一Clerk读取原键成功；原始23项继续保留。

NEXT只在读取到空槽时用该版本尝试占用；失败后帮助已有迁移。清理时同时核对内容并CAS该次读取的版本。源Freeze返回ErrWrongGroup时不安装空数据；成功安装后才删除源。

## 验证与证据边界

原始shardkv_test.go和shardcfg测试未修改，独立shardkv1.yml仍运行 `go test -race -count=1 -v -timeout 75m ./...`，覆盖全部23项及子包。补充TestMigrationReplayFences验证写入后重复安装、删除墓碑、快照恢复和后续合法迁移。

工作流新增独立RSM回归job，先构建rsm1d，再执行原始8项RSM测试。修复过程中b5cd88e的首次回归因漏构建rsm1d而失败，aeef6d1已补构建；run43214711215的RSM job成功。它不能代替最终HEAD的完整Lab5结果。另通过kvraft1.yml对同分支执行原始Lab4回归。

最终SHA、完整运行链接与逐项结果统一保存到langnote/todo.md及课程台账；本文描述实现和验收命令，不用进行中状态冒充通过。本地只做源码、gofmt与git diff --check，未编译或运行实验。
