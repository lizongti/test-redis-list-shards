# TestRedisListShards

## 设计目标

- 使用本地docker启动一个测试用的Redis进程，测试Redis集群的分片列表获取功能。
- Redis进程里面有多个List。将这些List分成很多Shard，每个Shard包含多个List。每个Shard使用独立的Goroutine对List进行Pop，并进行打印。当List动态增减时，Shard数量也会动态调整。
- 编写一个客户端，会随机的创建新的List并随机向里面Push数据。
- 提供一个接口用于获取当前所有Shard的信息，包括每个Shard包含的List数量和每个List的名称。
- 编写Dockerfile和docker-compose.yml文件，用于快速启动Redis测试环境。

## 编写要求

- 使用Golang代码进行编写。
- 参照现有代码的风格进行编写，包括命名规范、错误处理和日志记录等。
- 每次生成新代码以后，都要检查并删除多余的旧代码。
- 代码中需要包含详细的注释，解释每个函数和重要代码段的作用。
- 生成一个 Readme.md 来讲解如何使用该项目，并画图展示程序架构。

## 执行约定

- Windows 平台下统一使用 WSL（wsl.exe -e bash -lc）；macOS 平台下统一使用 zsh；linux 平台下统一使用 bash。

> 说明：本项目为本地 Redis + Go 程序验证用途，不涉及创建任何 AWS 资源。

## 实现补充：多实例无中心再平衡

当前仓库实现支持同时启动多个 server 实例（见 `docker-compose.yml` 的 `server1/server2/server3`），并在无中心调度的情况下实现自动再平衡：

- 所有实例把自己的 `INSTANCE_ID` 作为 member 心跳写入 Redis ZSET（默认 key：`test-redis-list-shards:members`）。
- 所有实例读取同一份 `members` 列表，并用 Rendezvous Hash 为每个 shard 计算唯一 `owner`。
- 每个实例只启动 `owner==自己` 的 shard worker（goroutine），不拥有的 shard 会停止 worker。
- 建议在多实例模式使用固定 `SHARD_COUNT`（例如 32/64），保证 shard ID 集合稳定（`[0..SHARD_COUNT)`），避免动态 shardCount 带来的短暂不一致。

可观测性：

- `GET /shards` 会返回每个 shard 的 `owner` 以及该 shard 是否为本实例本地拥有（`local`）。

## 测试用例

- 编写一个测试用例，动态增加和减少Redis中的List，验证Shard的动态调整功能是否正常工作。
- 编写一个启动脚本deploy.sh，用于自动化部署测试环境，包括启动Redis容器和运行测试客户端。
- 编写一个测试脚本test.sh，用于自动化执行测试用例，并输出测试结果。
