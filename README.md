# MaiBot Sowing Discord

把源群中的合并转发梗图、段子和聊天记录，经审核后搬运到其他群的 MaiBot 插件。

本插件参考 [`anka-afk/astrbot_sowing_discord`](https://github.com/anka-afk/astrbot_sowing_discord) 的搬运思路重写，使用关键词预筛和 LLM 上下文审核，并适配 MaiBot 与 NapCat adapter。

> 非原插件官方 MaiBot 端口。审核、消息解析、持久化和转发链路均为独立实现。

## 工作流程

```text
源群合并转发
  │
  ├─ 识别 adapter 展开的 forward marker
  ├─ 提取文字、已有图片描述及安全媒体元数据
  ├─ 生成稳定内容指纹
  └─ 写入 SQLite，前台立即返回
       │
       └─ 后台审核 worker
            ├─ 拒绝关键词：立即拒绝
            ├─ 放行关键词：立即放行
            └─ LLM 一次返回审核、概要、笑点和标签
                 │
                 └─ 为每个目标群创建独立投递
                      ├─ 精确指纹去重
                      ├─ 目标群近期消息相似度去重
                      ├─ NapCat 原样直转
                      ├─ 当前目标失败时 MaiBot send_forward 兜底
                      └─ 成功后写入 Bot ActionRecord
```

## 核心特性

- **持久化任务**：使用标准库 SQLite 保存审核任务、逐目标投递、冷却和内容历史。
- **逐目标状态**：一个目标失败不会影响其他目标，失败目标可单独重试。
- **内容去重**：同一目标群已经见过相同或高度相似内容时跳过。
- **结构化审核**：一次 LLM 请求返回决策、概要、笑点、上下文依赖和标签。
- **复用已有识图**：只读取 MaiBot 已有的 picid 图片描述，不触发新的 VLM 调用。
- **Bot 上下文同步**：搬运成功后写入内部 ActionRecord，让 Bot 知道自己搬了什么以及来源。
- **有界并发**：审核和不同目标群投递可并发，同一目标群保持串行。
- **重启恢复**：ON_START handler 会恢复 SQLite 中尚未完成的任务。

## 安装

放入 MaiBot 插件目录：

```text
modules/MaiBot/plugins/maibot_sowing_discord/
├── _manifest.json
├── plugin.py
└── config.toml                 # 首次启动自动生成
```

运行时会创建：

```text
sowing_state.sqlite3
sowing_state.sqlite3-wal
sowing_state.sqlite3-shm
```

这些文件已加入 .gitignore。旧版 runtime_state.json 不自动迁移，升级后会记录警告并从新的 SQLite 队列开始。

最低 MaiBot 版本：0.8.0。

## 基础配置

```toml
[plugin]
enabled = true

[source]
group_ids = ["123456789"]

[target]
group_ids = ["987654321"]
```

建议明确填写 target.group_ids。NapCat 可以直接按群号发送；即使目标群还没有 MaiBot stream，也会先尝试 NapCat。没有 stream 时无法使用 MaiBot fallback，也无法写入该群的 Bot 上下文记录。

## 等待和冷却

```toml
[cache]
wait_seconds = 600
process_in_background = true
poll_interval_seconds = 30
max_age_seconds = 3600
keep_raw_message = false

[cooldown]
day_seconds = 600
night_seconds = 3600
day_start = "09:00"
night_start = "01:00"
min_seconds = 0
```

wait_seconds 是上下文沉淀窗口，任务入队时会保存绝对到期时间。每个目标群独立计算冷却。

## 异步处理与重试

```toml
[processing]
batch_size = 20
moderation_concurrency = 2
delivery_concurrency = 4
max_retries = 3
retry_delay_seconds = 60
```

- moderation_concurrency：同时进行的审核任务数。
- delivery_concurrency：同时进行的目标投递数。
- 同一目标群始终由锁保护，避免并发重复发送。
- 循环本身不缩短模型响应时间；主要收益来自有界并发、逐目标隔离和避免重复 LLM/VLM 调用。

## 去重

```toml
[dedup]
check_recent_messages = true
history_hours = 72
history_limit = 20
record_retention_days = 30
similarity_threshold = 0.92
```

发送前执行两层判断：

1. 查询插件 SQLite 中该目标群是否已有同一内容指纹。
2. 查询目标 MaiBot stream 的真实近期消息，使用规范化文本相似度比较。

去重不调用 LLM。指纹会规范化 Unicode、空白、adapter marker 和 URL，并保留安全媒体标识。

## 媒体与 VLM

```toml
[media]
describe_picid = true
```

插件只调用 message_api.translate_pid_to_description 查询 MaiBot 已保存的描述：

- 有描述：提供给审核 LLM。
- 无描述：使用图片占位符。
- 不调用 ImageManager、process_image 或任何新的 VLM 请求。
- image/emoji 的 base64 不写入 SQLite 和 prompt。
- video/file/voice/audio 只保留名称、ID、大小、时长、MIME 等短元数据；不主动分析媒体内容。

纯视频或缺乏可理解文字、且没有现成描述的内容，应由审核策略保守拒绝。

## 审核

```toml
[evaluation]
strategy = "keyword_llm" # keyword_llm / always / keyword_any / keyword_all
keywords = []
case_sensitive = false

[llm_review]
enabled = true
message_types = ["forward"]
reject_keywords = ["nsfw", "黄图"]
pass_keywords = []
history_hours = 12
history_limit = 12
llm_group = "utils"
llm_list = []
max_tokens = 400
temperature = 0.2
```

优先级：

1. reject_keywords
2. pass_keywords
3. LLM

拒绝词优先于放行词，避免安全内容被放行关键词覆盖。

LLM 应只输出 JSON：

```json
{
  "decision": "allow",
  "reason": "错误前提导致连续反转",
  "summary": "群友认真排查故障，最后发现数据库被主动删除。",
  "joke_points": ["所有人认真排障", "结尾揭示主动删库"],
  "context_dependency": "none",
  "content_tags": ["技术梗", "反转"],
  "risk_tags": []
}
```

context_dependency 可选值：none、low、high。强依赖原群聊天上文才能理解的内容应拒绝。

## Bot 上下文同步

```toml
[bot_context]
enabled = true
```

每个目标群实际发送成功后，插件调用 database_api.store_action_info 写入内部动作记录，内容包括：

- 来源群号
- 源消息 ID
- 内容概要
- 最多三个可能笑点
- 内容标签和风险标签
- 内容指纹

记录不会额外向群里发送文本，但会通过 action_build_into_prompt 进入支持 ActionRecord 的 MaiBot 历史构建路径。这样群友回应刚搬运的内容时，Bot 能知道回应所指的话题。

如果 NapCat 向一个尚无 MaiBot stream 的群发送成功，消息仍会成功送达，但无法为该群写 ActionRecord。

## 转发链路

对每个目标独立执行：

1. 若启用 NapCat HTTP，调用 forward_group_single_msg 原样转发。
2. 当前目标失败且存在 MaiBot stream 时，使用 send_forward(message_id) 兜底。
3. 两路失败时只重试该目标。
4. 任一目标成功不会删除其他目标的待重试状态。

```toml
[napcat_http]
enabled = true
host = "127.0.0.1"
port = 3000
access_token = ""
timeout_seconds = 10
use_direct_forward = true
```

## 快速验证

临时配置：

```toml
[cache]
wait_seconds = 5
poll_interval_seconds = 2

[evaluation]
strategy = "always"

[cooldown]
day_seconds = 1
night_seconds = 1
min_seconds = 0
```

源群发送合并转发后，检查日志及 sowing_state.sqlite3 中的 jobs、deliveries、content_history。

## 注意事项

- 当前 MaiMessages 不直接携带 message_id，插件仍需从 chat stream 最近消息回查。极高并发下存在框架抽象层面的错位风险。
- NapCat adapter 当前对合并转发内部文字和图片支持较完整；视频、文件或语音可能只剩有限元数据。
- 目标群近期消息只能比较 MaiBot 已落库的 processed_plain_text/display_message，不能恢复全部原始协议节点。
- SQLite 状态库不应提交到 Git。

## 依赖

- MaiBot ≥ 0.8.0
- Python 标准库
- 可选：NapCat HTTP 服务
- 无额外第三方 Python 依赖

## 致谢

- [`anka-afk/astrbot_sowing_discord`](https://github.com/anka-afk/astrbot_sowing_discord)
- [MaiBot](https://github.com/MaiM-with-u/MaiBot)
- [NapCat](https://www.napcat.wiki)

## License

MIT
