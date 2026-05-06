# MaiBot Sowing Discord

把"史"从一个 QQ 群搬到另一个 QQ 群的 MaiBot 插件——基于 [`anka-afk/astrbot_sowing_discord`](https://github.com/anka-afk/astrbot_sowing_discord) 重写为 MaiBot 端口,并把审核逻辑从"贴表情评分"换成"关键词预筛 + LLM 上下文审核",顺便针对 MaiBot 的消息抽象做了完整适配。

适合用来在熟人群之间流转梗图、合并转发段子,带白名单源群、目标群、动态冷却、消息类型过滤、自定义审核 prompt。

> 本插件不是 [@anka-afk](https://github.com/anka-afk) 的官方 MaiBot 端口,只是借用了原插件的搬运思路与缓存等待窗口设计。审核机制重新设计,实现也是基于 MaiBot Plugin System 重写的。

---

## 功能概览

- **白名单源群 → 白名单目标群**:只搬 `source.group_ids` 内发出的消息;命中后转发到 `target.group_ids`(默认排除源群本身)。
- **延迟评估窗口**:消息先进缓存,等 `cache.wait_seconds` 沉淀后再评估,避免发出去 1 秒就转发的尴尬。
- **动态冷却**:白天/夜间分别配冷却,避免连续刷屏。深夜冷却可设置得更长。
- **后台轮询 worker**:候选消息处理走独立协程,前台收消息只负责入队。
- **三档评估策略**(`evaluation.strategy`):
  - `keyword_llm`(默认):关键词黑/白名单先筛 → 命中关键词直接通过/拒绝;否则交给 LLM 结合最近聊天记录上下文审核,只输出 JSON 决策。
  - `keyword_any` / `keyword_all`:纯关键词命中即通过,不调 LLM。
  - `always`:不审核,全部放行(调试用)。
- **双路转发**:
  - 优先调 NapCat HTTP `forward_group_single_msg`,原消息原样转(保留合并转发结构、表情、回复等元信息)。
  - 失败/未启用时只回退到可重建的 MaiBot `send_forward()` 节点；不再使用 message_id 引用兜底。
- **引用合并消息过滤**:普通消息如果只是回复/引用了一条合并转发,会被当作普通评论拒绝入队；合并转发内部嵌套合并转发仍然是正常内容。
- **消息类型白名单**:默认只入队 `forward`,图片和纯文本不进缓存,从源头节省 LLM 调用并避开不稳定图片链路。
- **图片描述展开**:默认关闭。当前审核只面向合并转发文本上下文，不再把图片描述作为放行依据。
- **`block_source_messages`**:开启后源群命中的消息会被本插件拦截(`intercept_message=True`),不会再触达 LLM 主流程,避免麦麦自己回复源群里的史。

---

## 与原插件 (`astrbot_sowing_discord`) 的差异

| 维度 | 原插件 (AstrBot) | 本插件 (MaiBot) |
|---|---|---|
| 宿主框架 | AstrBot | MaiBot |
| 主要审核机制 | `GoodEmojiRule`:扫源消息上的好评/差评表情数,差评 < 好评则放行 | `keyword_llm`:关键词预筛 + LLM 审核 |
| 依赖社区互动 | 是(没人贴表情就没法判定) | 否 |
| 转发实现 | NapCat `forward_group_single_msg` | 同上,失败后回退 MaiBot `send_forward()` |
| 缓存与冷却 | LocalCache + `banshi_cooldown_*` | RuntimeState (JSON) + `cooldown.*` |
| 消息类型过滤 | `allowed_message_types` | 同名,默认收紧为 `["forward"]` |
| 配置版本化 | 无 | 走 MaiBot config_version 迁移 |

> MaiBot 没有"贴表情评分"对应的社区行为,原插件依赖的好评/差评表情数据拿不到,所以重新设计了 LLM 审核作为替代。如果你的部署里有别的方法采集消息热度,也可以仿着 `MessageEvaluator` 添加新策略。

---

## 安装

把整个目录放到 MaiBot 的插件目录(典型路径 `modules/MaiBot/plugins/maibot_sowing_discord/`),保证目录里至少有:

```
maibot_sowing_discord/
├── _manifest.json
├── plugin.py
├── config.toml          # 启动时若不存在会按 schema 自动生成
└── runtime_state.json   # 运行时自动创建,持久化候选池
```

**最低 MaiBot 主机版本**:`0.8.0`(写在 `_manifest.json`)。

启动 MaiBot,首次会按 [config_schema](plugin.py) 自动生成 `config.toml`。修改后重启即可加载。

---

## 配置说明

### `[plugin]`
| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 是否启用插件。配置文件里改 `true` 才生效。 |
| `config_version` | `1.4.0` | 配置版本号,改 schema 时同步抬升即可触发 MaiBot 配置迁移。 |

### `[source]` / `[target]`
| 字段 | 默认 | 说明 |
|---|---|---|
| `source.group_ids` | `[]` | 源群号列表。**只有这些群发出的消息才会进入候选池。** |
| `target.group_ids` | `[]` | 目标群号列表。留空时会回退为当前平台所有群聊流(慎用)。 |

> 推荐显式列出 `target.group_ids`。NapCat 直接转发(`use_direct_forward=true`)硬性要求显式群号。

### `[cache]`
| 字段 | 默认 | 说明 |
|---|---|---|
| `wait_seconds` | `600` | 候选消息进入缓存后的等待时间,到期才开始评估。 |
| `process_in_background` | `true` | 是否走后台 worker。关掉则在收消息的同一个协程里直接评估转发(可能阻塞主流程)。 |
| `poll_interval_seconds` | `30` | 后台 worker 轮询间隔。值越小越及时,但 IO 也越频繁。 |
| `keep_raw_message` | `false` | 是否把原始 raw_message 写入缓存文件。关掉省空间;只有在 `plain_text` 为空且消息是合并转发时才会保留必要原文。 |
| `max_age_seconds` | `3600` | 候选最大保留时间。超时未处理会被清理。 |

### `[cooldown]`
| 字段 | 默认 | 说明 |
|---|---|---|
| `day_seconds` | `600` | 白天冷却,单位秒。成功转发后至少等待这么久才允许下次。 |
| `night_seconds` | `3600` | 夜间冷却。 |
| `day_start` | `09:00` | 白天起始时间,24 小时制。 |
| `night_start` | `01:00` | 夜间起始时间。 |
| `min_seconds` | `0` | 全局冷却下限,单位秒。实际冷却 = `max(动态冷却, min_seconds)`。设 0 时不生效;设大值用于一刀切兜底,避免白天冷却被设得太短。 |

判定区间:`now ≥ day_start` 或 `now < night_start` 视为白天,其余视为夜间。即默认 `09:00–次日 01:00` 白天,`01:00–09:00` 夜间。

### `[filter]`
| 字段 | 默认 | 说明 |
|---|---|---|
| `allowed_message_types` | `["forward"]` | 允许入队的消息类型。默认只支持合并转发；图片消息不再支持。**消息中包含未授权的核心媒体类型(`image` / `voice` / `video` / `forward`)时整条消息会被拒绝。** |
| `block_source_messages` | `false` | 开启后,命中源群的消息会在本插件处终止后续 EventHandler(防止麦麦在源群里被史触发回复)。需要 `intercept_message=True` 才生效——已默认开启。 |

### `[evaluation]`
| 字段 | 默认 | 说明 |
|---|---|---|
| `strategy` | `keyword_llm` | 评估策略:`keyword_llm` / `always` / `keyword_any` / `keyword_all`。 |
| `keywords` | `[]` | 仅 `keyword_any` / `keyword_all` 使用。 |
| `case_sensitive` | `false` | 关键词匹配是否区分大小写。 |

### `[napcat_http]`
| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 是否启用 NapCat HTTP。`use_direct_forward` 依赖它。 |
| `host` | `127.0.0.1` | |
| `port` | `3000` | NapCat HTTP 端口。 |
| `access_token` | `""` | NapCat 启用鉴权时填写。 |
| `timeout_seconds` | `10` | 请求超时。超时不会立即清缓存,下轮还会重试。 |
| `use_direct_forward` | `true` | 优先用 `forward_group_single_msg` 直接转发原消息(保真度最高)。 |

### `[llm_review]`
| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 是否启用 LLM 审核。 |
| `message_types` | `["forward"]` | 允许进入 LLM 审核的消息类型,其它类型会被跳过。 |
| `only_forward_messages` | `false` | 旧兼容开关,仅 `message_types` 留空时才生效。 |
| `expand_picid_descriptions` | `false` | 是否把 `[picid:xxx]` 展开成 MaiBot 数据库里现成的图片描述。图片消息不再支持，默认关闭。 |
| `prompt_template` | (见下) | LLM 审核提示词模板。占位符仅识别 `{name}` 形式,JSON 字面量花括号会被原样保留。 |
| `reject_keywords` | `[]` | 命中即直接拒绝,优先级高于 LLM。 |
| `pass_keywords` | `[]` | 命中即直接放行,优先级高于 LLM 和 reject_keywords。 |
| `history_hours` | `12` | 审核时回看最近多少小时聊天记录。 |
| `history_limit` | `12` | 最多带入多少条聊天记录。 |
| `history_filter_mai` | `false` | 构建聊天记录时是否过滤 bot 自己的消息。 |
| `history_truncate` | `true` | 是否截断超长消息。 |
| `history_show_actions` | `false` | 是否展示动作记录。 |
| `llm_group` | `"utils"` | 用 MaiBot 全局模型分组(走 `llm_api.get_available_models`)。 |
| `llm_list` | `[]` | 手动指定模型名,非空时优先级高于 `llm_group`。 |
| `max_tokens` | `400` | 仅 `llm_list` 非空时生效。 |
| `temperature` | `0.2` | 同上。 |
| `slow_threshold` | `30` | 同上。 |
| `selection_strategy` | `"balance"` | 同上。`balance` / `random`。 |

#### 默认 prompt 策略

默认模板按优先级把审核分成 3 档:

- **P0 一票否决**:政治议题(无论立场/玩梗/二创)、明显违法违规、明显 NSFW 或色情引流——立即 reject。
- **P1 输入边界**:只审核当前消息本体是合并转发的消息；回复/引用某条合并转发后的普通评论 reject；单独图片搬运不支持。
- **P2 合并转发放行标准**:玩梗、抽象对话、低质段子、明确节目效果可 allow；纯通知/求助/认真讨论/吵架/无上下文普通聊天 reject；无法判断时 reject。

要换风格直接改 `prompt_template`(toml 里是单行字符串)。注意保留所有 `{name}` 占位符。

#### 可用占位符

| 占位符 | 含义 |
|---|---|
| `{group_id}` | 源群号 |
| `{user_id}` / `{user_nickname}` | 发送者 ID / 群昵称 |
| `{message_id}` | 真实 QQ message_id |
| `{plain_text}` / `{raw_message}` | 文本内容 / 原 CQ 码 |
| `{current_message}` | `plain_text` 优先,否则 `raw_message` |
| `{content_types}` | 例如 `forward` 或 `forward, text` |
| `{contains_forward}` | `true` / `false` |
| `{reply_segments}` | JSON 序列化后的段列表 |
| `{history}` | 最近聊天记录(已格式化) |

字面量 JSON 例如 `{"decision":"allow","reason":"..."}` 不会被当成占位符——本插件用的是只识别 `{name}` 形式的安全替换器。

---

## 工作流程

```
源群消息
  │
  ▼
ON_MESSAGE handler (intercept_message=True)
  │
  ├─ block_source_messages=true && 命中源群 → 标记不继续后续 handler
  │
  ▼
_resolve_source_info  ←  从 chat_stream.context.get_last_message 取真实 message_id
  │
  ▼
_extract_content_types  ←  遍历 message.message_segments 按 Seg.type 识别
  │
  ▼
普通评论引用合并转发 → 按 text 拒绝入队
  │
  ▼
_is_allowed_message  →  filter.allowed_message_types 检查
  │
  ▼
RuntimeState.add_pending(...)  →  写入 runtime_state.json
                                   │
后台 worker (poll_interval_seconds)│
  ▼                                ▼
get_matured(wait_seconds) → 冷却检查 → MessageEvaluator.evaluate
                                   │
                                   ├─ pass_keywords 命中 → allow
                                   ├─ reject_keywords 命中 → reject
                                   └─ LLM 审核 → JSON {decision, reason}
                                   │
            allow                  ▼                  reject
              │                                          │
              ▼                                          ▼
    NapCat forward_group_single_msg                remove_pending
              │
              └→ 失败回退 MaiBot send_forward
                    └→ 仅发送可重建节点,不使用 message_id 引用
              │
              ▼
        set_last_forward_at  →  动态冷却开始
```

---

## 调试与排查

### 启动后第一次跑

把 `cache.wait_seconds=30, evaluation.strategy="always", napcat_http.use_direct_forward=false` 临时调小,5 分钟内能看到效果。

源群发合并转发应当看到:
```
[maibot_sowing_discord] cached message <真实 QQ id> from group <群号> types=['forward']
[maibot_sowing_discord] background worker started, poll_interval=30s
...
[maibot_sowing_discord] forwarded message <id> to N target streams
```

调通后改回真实配置(`wait_seconds=600`、`strategy="keyword_llm"`、`use_direct_forward=true`)。

### 常见问题

| 现象 | 大概率原因 | 处理 |
|---|---|---|
| 源群发消息后**完全没日志** | handler 没注册或 `plugin.enabled=false` | 启动日志搜 `sowing_forward_handler` 是否成功注册 |
| `skip caching: source_info missing required fields` | chat_stream context 里没拿到 message_id | 检查 NapCat adapter 是否正常,贴日志反馈 |
| `skip caching message ...: content_types=['text'] not allowed` | 消息只识别为 text,被 `allowed_message_types` 拦掉 | 正常行为。要审文本就把 `text` 加进 allowed |
| 缓存了但一直不转发 | 卡在评估或冷却 | 看 background worker 日志、`evaluation.strategy`、`cooldown.*` |
| `failed to forward message ... to stream ...` | NapCat 直转失败且 `send_forward` 也失败 | 关 `use_direct_forward` 看 send_forward 是否单独可用 |
| LLM 恒 reject 或恒 allow | prompt/模型问题 | 暂时切 `strategy="always"` 隔离;再排查 LLM 端 |
| 报 `KeyError: '"decision"'` 之类 | prompt 里有非 `{name}` 形式的花括号被当成占位符 | 已修;若 prompt 自定义出问题,检查是否在 `{}` 里塞了非合法字段名 |

调高日志详细程度可以在 MaiBot 主配置开 DEBUG。

---

## 依赖

- MaiBot 主机 ≥ `0.8.0`
- 仅 Python 标准库(`json` / `urllib` / `asyncio` / `re` / `dataclasses`),不引入第三方包
- 可选:NapCat (启用 HTTP 服务) 用于 `use_direct_forward=true`

---

## 来源与致谢

- 原插件:[`anka-afk/astrbot_sowing_discord`](https://github.com/anka-afk/astrbot_sowing_discord) —— 借鉴了搬运思路、缓存等待窗口、消息类型白名单设计
- MaiBot Plugin System 文档:[MaiBot/MaiBot 仓库](https://github.com/MaiM-with-u/MaiBot) `docs-src/plugins/`
- NapCat:[https://www.napcat.wiki](https://www.napcat.wiki)

---

## License

MIT(与原插件保持一致)
