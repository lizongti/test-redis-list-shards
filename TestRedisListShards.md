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
- 允许根据需求创建所需的AWS资源

## 测试用例

- 编写一个测试用例，动态增加和减少Redis中的List，验证Shard的动态调整功能是否正常工作。
- 编写一个启动脚本deploy.sh，用于自动化部署测试环境，包括启动Redis容器和运行测试客户端。
- 编写一个测试脚本test.sh，用于自动化执行测试用例，并输出测试结果。
