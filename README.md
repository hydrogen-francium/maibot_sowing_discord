# MaiBot Sowing Discord

把群里的"史"（合并转发梗/段子）自动搬运到其他群的 MaiBot 插件。

基于 [`anka-afk/astrbot_sowing_discord`](https://github.com/anka-afk/astrbot_sowing_discord) 的搬运思路重写，审核机制从"贴表情评分"改为"关键词预筛 + LLM 上下文审核"，适配 MaiBot 消息抽象和 NapCat adapter 的合并转发展开行为。

> 非原插件官方 MaiBot 端口。审核机制、转发链路、消息解析均为独立实现。

---

## 工作原理

```
源群合并转发消息
  │
  ▼
handler 收消息 → 识别内容类型（通过 plain_text 文本标记检测合并转发）
  │
  ▼
消息类型白名单检查 → 入队 runtime_state.json
  │
  ▼
后台 worker 轮询（默认 30s）
  │
  ├─ 等待窗口未到期 → 跳过
  ├─ 冷却中 → 跳过
  └─ 到期 → 评估
       │
       ├─ pass_keywords 命中 → 直接放行
       ├─ reject_keywords 命中 → 直接拒绝
       └─ LLM 审核（带最近聊天记录上下文）→ JSON 决策
            │
            ├─ allow → NapCat 直转，失败回退 MaiBot send_forward
            └─ reject → 移出队列
```

---

## 核心特性

- **白名单源群/目标群**：只监听指定源群，转发到指定目标群
- **延迟评估**：消息先缓存等待沉淀（默认 10 分钟），避免秒转
- **动态冷却**：白天/夜间分别配置冷却时间，防止连续刷屏
- **三级审核**：关键词黑名单 → 关键词白名单 → LLM 上下文决策
- **双路转发**：NapCat HTTP 直转（保真度最高）→ MaiBot send_forward 兜底
- **上下文依赖检测**：LLM 会判断合并转发是否脱离聊天上下文仍能独立成梗，只搬完整的梗不搬语境碎片
- **NapCat adapter 适配**：合并转发被 adapter 展开为 seglist + 文本标记，插件通过 `========== 转发消息开始 ==========` 标记反推识别

---

## 安装

放到 MaiBot 插件目录：

```
modules/MaiBot/plugins/maibot_sowing_discord/
├── _manifest.json
├── plugin.py
├── config.toml      # 首次启动自动生成
└── runtime_state.json  # 运行时自动创建
```

最低 MaiBot 版本：`0.8.0`

---

## 配置

首次启动自动生成 `config.toml`，核心配置项：

### 基础

```toml
[plugin]
enabled = true

[source]
group_ids = ["123456789"]  # 源群

[target]
group_ids = ["987654321"]  # 目标群
```

### 缓存与冷却

```toml
[cache]
wait_seconds = 600          # 入队后等待多久再评估
process_in_background = true
poll_interval_seconds = 30
max_age_seconds = 3600      # 超时清理

[cooldown]
day_seconds = 600           # 白天冷却
night_seconds = 3600        # 夜间冷却
day_start = "09:00"
night_start = "01:00"
min_seconds = 0             # 全局冷却下限
```

### 过滤与评估

```toml
[filter]
allowed_message_types = ["forward"]  # 只入队合并转发
block_source_messages = false        # 是否拦截源群消息不让后续 handler 处理

[evaluation]
strategy = "keyword_llm"  # keyword_llm / always / keyword_any / keyword_all
```

### NapCat HTTP

```toml
[napcat_http]
enabled = true
host = "127.0.0.1"
port = 3000
access_token = ""
use_direct_forward = true  # 优先用 forward_group_single_msg 原样转发
```

### LLM 审核

```toml
[llm_review]
enabled = true
message_types = ["forward"]
reject_keywords = ["nsfw", "黄图"]  # 命中直接拒绝，不调 LLM
pass_keywords = []                   # 命中直接放行
history_hours = 20                   # 回看聊天记录时间范围
history_limit = 20                   # 最多带入条数
llm_group = "utils"                  # MaiBot 模型分组
llm_list = []                        # 手动指定模型（优先于 llm_group）
max_tokens = 400
temperature = 0.2
```

`prompt_template` 是单行字符串，支持以下占位符：

| 占位符 | 含义 |
|---|---|
| `{group_id}` | 源群号 |
| `{user_id}` / `{user_nickname}` | 发送者 |
| `{message_id}` | QQ message_id |
| `{plain_text}` / `{raw_message}` | 消息文本 |
| `{content_types}` | 如 `forward` |
| `{contains_forward}` | `true` / `false` |
| `{reply_segments}` | JSON 段列表 |
| `{history}` | 格式化的最近聊天记录 |

JSON 字面量 `{"decision":"allow"}` 不会被当占位符——使用安全替换器只识别 `{name}` 形式。

### 默认审核策略（P0/P1/P2）

- **P0 一票否决**：政治、违法、NSFW → reject
- **P1 输入边界**：只审合并转发本体；回复/引用评论 reject；纯图片 reject
- **P2 放行标准**：
  - allow：玩梗、抽象对话、段子、明确节目效果
  - allow：内容自成一体，脱离上文仍能看懂
  - reject：依赖上文语境才能理解的聊天片段
  - reject：纯通知/求助/讨论/吵架/客套
  - reject：无法判断时一律 reject

---

## 与原插件的差异

| 维度 | 原插件 (AstrBot) | 本插件 (MaiBot) |
|---|---|---|
| 审核机制 | 贴表情评分（GoodEmoji/BadEmoji） | 关键词预筛 + LLM 上下文审核 |
| 依赖社区互动 | 是 | 否 |
| 合并转发识别 | 直接读 segment type | 通过 adapter 文本标记反推 |
| 转发实现 | NapCat 直转 | NapCat 直转 + MaiBot send_forward 兜底 |
| 上下文依赖检测 | 无 | LLM 判断是否脱离语境仍成立 |

---

## 快速调试

临时配置快速验证：

```toml
[cache]
wait_seconds = 30

[evaluation]
strategy = "always"

[napcat_http]
use_direct_forward = false
```

源群发合并转发，30 秒后应看到日志：

```
[maibot_sowing_discord] cached message <id> from group <群号> types=['forward']
[maibot_sowing_discord] forwarded message <id> to N target streams
```

验证通过后改回正式配置。

---

## 常见问题

| 现象 | 原因 | 处理 |
|---|---|---|
| 完全没日志 | handler 未注册或 `enabled=false` | 启动日志搜 `sowing_forward_handler` |
| `source_info missing required fields` | message_id 取不到 | 检查 NapCat adapter 状态 |
| `content_types=['text'] not allowed` | 消息只识别为 text | 正常，只搬 forward |
| 缓存了但不转发 | 卡在评估或冷却 | 查 worker 日志和 cooldown 配置 |
| LLM 恒 reject | prompt/模型问题 | 临时切 `strategy="always"` 隔离 |

---

## 依赖

- MaiBot ≥ 0.8.0
- Python 标准库（无第三方依赖）
- 可选：NapCat HTTP 服务（用于直接转发）

---

## 致谢

- [`anka-afk/astrbot_sowing_discord`](https://github.com/anka-afk/astrbot_sowing_discord) — 搬运思路与缓存设计
- [MaiBot](https://github.com/MaiM-with-u/MaiBot) — 插件系统
- [NapCat](https://www.napcat.wiki) — QQ 协议实现

## License

MIT
