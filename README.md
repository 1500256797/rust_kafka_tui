# kafka-tui

基于 [Ratatui](https://docs.rs/ratatui/latest/ratatui/) 的终端 Kafka 浏览与生产工具。

> 由 Claude Opus 4.8 提供技术支持

## 功能

- 多 Cluster 连接与切换（SASL/SSL 配置透传）
- Topic 列表、搜索、刷新
- 多 Partition 合并分页（按 timestamp 排序）
- 单 Partition 模式（精确 offset 排查）
- 消息详情：Key / Value / Headers / Timestamp
- 消息格式化：auto / json / raw / avro（Schema Registry）
- 单条 Replay + 手动 Produce
- 只读模式（`allow-produce: false`）
- 剪贴板复制（OSC 52 + 系统剪贴板 fallback）
- 配置热重载（`Ctrl+R`）

## 系统依赖

macOS:

```bash
brew install cmake openssl
```

Linux:

```bash
apt install cmake libssl-dev libsasl2-dev
```

## 构建与运行

```bash
# 快速启动：指定配置并直接连接 local 集群
cargo run -- --config config.local.yaml --cluster local

# 仅指定配置（启动后手动选择 Cluster）
cargo run -- --config config.example.yaml

# Release 构建
cargo build --release

# 带 debug 日志
RUST_LOG=kafka_tui=debug cargo run -- --config config.local.yaml --cluster local
```

## 配置

默认配置文件路径：`~/.config/kafka-tui/config.yaml`

```bash
mkdir -p ~/.config/kafka-tui
cp config.example.yaml ~/.config/kafka-tui/config.yaml
# 编辑配置后
kafka-tui
```

配置结构参考 AKHQ，示例见 [config.example.yaml](./config.example.yaml)。

## CLI 参数

```
kafka-tui [OPTIONS]

  -c, --config <FILE>    配置文件路径
      --cluster <NAME>   启动时直接连接指定 Cluster
  -v, --verbose          启用 debug 日志
```

## 快捷键

| 场景 | 按键 | 说明 |
|------|------|------|
| 全局 | `q` | 退出 |
| 全局 | `?` | 帮助 |
| 全局 | `Ctrl+R` | 重载配置 |
| Cluster | `j/k` | 选择 |
| Cluster | `Enter` | 连接 |
| Topic 列表 | `/` | 搜索 |
| Topic 列表 | `Enter` | 进入 Topic |
| 消息浏览 | `n/p` | 翻页 |
| 消息浏览 | `m` | 切换 Merged/Single 模式 |
| 消息浏览 | `y` | 复制 Value |
| 消息浏览 | `R` | Replay |
| 消息浏览 | `P` | Produce |

完整快捷键见应用内帮助页（`?`）。

## 测试

```bash
cargo test
```

## 文档

- [产品设计](./docs/product-design.md)
- [技术实施方案](./docs/technical-implementation.md)
