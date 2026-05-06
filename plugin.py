import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime
from urllib import error as urllib_error
from urllib import request as urllib_request
from typing import Any, Dict, List, Optional, Tuple, Type

from src.chat.message_receive.chat_stream import get_chat_manager
from src.config.api_ada_configs import TaskConfig
from src.plugin_system import (
    BaseEventHandler,
    BasePlugin,
    ComponentInfo,
    ConfigField,
    EventType,
    MaiMessages,
    ReplyContentType,
    chat_api,
    get_logger,
    llm_api,
    message_api,
    plugin_manage_api,
    register_plugin,
)


PLUGIN_NAME = "maibot_sowing_discord"
logger = get_logger(PLUGIN_NAME)


@dataclass
class PendingMessage:
    message_id: str
    stream_id: str
    group_id: str
    platform: str
    user_id: str
    user_nickname: str
    plain_text: str
    raw_message: str
    content_types: List[str]
    reply_segments: List[Tuple[str, str]]
    cached_at: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "stream_id": self.stream_id,
            "group_id": self.group_id,
            "platform": self.platform,
            "user_id": self.user_id,
            "user_nickname": self.user_nickname,
            "plain_text": self.plain_text,
            "raw_message": self.raw_message,
            "content_types": self.content_types,
            "reply_segments": self.reply_segments,
            "cached_at": self.cached_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PendingMessage":
        return cls(
            message_id=str(data.get("message_id", "")),
            stream_id=str(data.get("stream_id", "")),
            group_id=str(data.get("group_id", "")),
            platform=str(data.get("platform", "qq")),
            user_id=str(data.get("user_id", "")),
            user_nickname=str(data.get("user_nickname", "")),
            plain_text=str(data.get("plain_text", "")),
            raw_message=str(data.get("raw_message", "")),
            content_types=[str(item) for item in data.get("content_types", [])],
            reply_segments=[
                (str(item[0]), str(item[1]))
                for item in data.get("reply_segments", [])
                if isinstance(item, (list, tuple)) and len(item) == 2
            ],
            cached_at=float(data.get("cached_at", 0.0)),
        )


@dataclass
class EvaluationResult:
    should_forward: bool
    is_final: bool
    reason: str = ""


class RuntimeState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._state_path = os.path.join(self._get_plugin_dir(), "runtime_state.json")
        self._ensure_file()

    def _get_plugin_dir(self) -> str:
        try:
            plugin_dir = plugin_manage_api.get_plugin_path(PLUGIN_NAME)
            if plugin_dir:
                return plugin_dir
        except Exception:
            pass
        return os.path.dirname(os.path.abspath(__file__))

    def _ensure_file(self) -> None:
        os.makedirs(os.path.dirname(self._state_path), exist_ok=True)
        if not os.path.exists(self._state_path):
            with open(self._state_path, "w", encoding="utf-8") as file:
                json.dump({"pending": [], "last_forward_at": 0.0}, file, ensure_ascii=False)

    def _read_state_unlocked(self) -> Dict[str, Any]:
        try:
            with open(self._state_path, "r", encoding="utf-8") as file:
                data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {"pending": [], "last_forward_at": 0.0}
        data.setdefault("pending", [])
        data.setdefault("last_forward_at", 0.0)
        return data

    def _write_state_unlocked(self, state: Dict[str, Any]) -> None:
        with open(self._state_path, "w", encoding="utf-8") as file:
            json.dump(state, file, ensure_ascii=False, indent=2)

    async def cleanup(self, max_age_seconds: int) -> int:
        async with self._lock:
            state = self._read_state_unlocked()
            now = time.time()
            original_count = len(state["pending"])
            state["pending"] = [
                item
                for item in state["pending"]
                if now - float(item.get("cached_at", 0.0)) <= max_age_seconds
            ]
            removed_count = original_count - len(state["pending"])
            if removed_count > 0:
                self._write_state_unlocked(state)
            return removed_count

    async def add_pending(self, message: PendingMessage) -> None:
        async with self._lock:
            state = self._read_state_unlocked()
            pending: List[Dict[str, Any]] = state["pending"]
            pending = [item for item in pending if str(item.get("message_id", "")) != message.message_id]
            pending.append(message.to_dict())
            pending.sort(key=lambda item: float(item.get("cached_at", 0.0)))
            state["pending"] = pending
            self._write_state_unlocked(state)

    async def remove_pending(self, message_id: str) -> None:
        async with self._lock:
            state = self._read_state_unlocked()
            state["pending"] = [
                item for item in state["pending"] if str(item.get("message_id", "")) != str(message_id)
            ]
            self._write_state_unlocked(state)

    async def get_matured(self, wait_seconds: int) -> List[PendingMessage]:
        async with self._lock:
            state = self._read_state_unlocked()
            now = time.time()
            matured: List[PendingMessage] = []
            for item in state["pending"]:
                if now - float(item.get("cached_at", 0.0)) >= wait_seconds:
                    matured.append(PendingMessage.from_dict(item))
            return matured

    async def get_last_forward_at(self) -> float:
        async with self._lock:
            state = self._read_state_unlocked()
            return float(state.get("last_forward_at", 0.0))

    async def set_last_forward_at(self, timestamp: float) -> None:
        async with self._lock:
            state = self._read_state_unlocked()
            state["last_forward_at"] = timestamp
            self._write_state_unlocked(state)


class MessageEvaluator:
    PIC_ID_PATTERN = re.compile(r"\[picid:([^\]]+)\]")

    @staticmethod
    async def evaluate(
        strategy: str,
        message: PendingMessage,
        keywords: List[str],
        case_sensitive: bool,
        plugin: BaseEventHandler,
    ) -> EvaluationResult:
        if strategy == "always":
            return EvaluationResult(True, True, "strategy=always")

        if strategy == "keyword_llm":
            return await MessageEvaluator._evaluate_keyword_llm(message, case_sensitive, plugin)

        if not keywords:
            return EvaluationResult(False, True, "keyword list is empty")

        haystack = message.plain_text or message.raw_message
        if not case_sensitive:
            haystack = haystack.lower()
            keywords = [keyword.lower() for keyword in keywords]

        if strategy == "keyword_any":
            return EvaluationResult(any(keyword in haystack for keyword in keywords), True, "strategy=keyword_any")
        if strategy == "keyword_all":
            return EvaluationResult(all(keyword in haystack for keyword in keywords), True, "strategy=keyword_all")
        return EvaluationResult(False, True, f"unknown strategy: {strategy}")

    @staticmethod
    async def _evaluate_keyword_llm(
        message: PendingMessage,
        case_sensitive: bool,
        plugin: BaseEventHandler,
    ) -> EvaluationResult:
        llm_enabled = bool(plugin.get_config("llm_review.enabled", True))
        if not llm_enabled:
            return EvaluationResult(False, True, "keyword_llm requires llm_review.enabled=true")

        review_types = set(MessageEvaluator._resolve_review_message_types(plugin))
        if review_types and not review_types.intersection(set(message.content_types)):
            return EvaluationResult(
                False,
                True,
                f"llm review skipped because message types {message.content_types} are outside {sorted(review_types)}",
            )

        reject_keywords = MessageEvaluator._normalize_keywords(plugin.get_config("llm_review.reject_keywords", []))
        pass_keywords = MessageEvaluator._normalize_keywords(plugin.get_config("llm_review.pass_keywords", []))
        text_to_match = MessageEvaluator._build_review_text(message, plugin)
        normalized_text = text_to_match if case_sensitive else text_to_match.lower()

        if pass_keywords:
            matched_pass = [
                keyword for keyword in pass_keywords
                if (keyword if case_sensitive else keyword.lower()) in normalized_text
            ]
            if matched_pass:
                return EvaluationResult(True, True, f"matched pass keywords: {', '.join(matched_pass[:5])}")

        matched_reject = [
            keyword for keyword in reject_keywords
            if (keyword if case_sensitive else keyword.lower()) in normalized_text
        ]
        if matched_reject:
            return EvaluationResult(False, True, f"matched reject keywords: {', '.join(matched_reject[:5])}")

        try:
            history = MessageEvaluator._build_recent_history(message, plugin)
        except Exception as exc:
            return EvaluationResult(False, False, f"failed to build chat history: {exc}")

        try:
            model_config = MessageEvaluator._build_llm_model_config(plugin)
        except RuntimeError as exc:
            return EvaluationResult(False, True, str(exc))

        prompt_template = str(
            plugin.get_config(
                "llm_review.prompt_template",
                "",
            )
            or ""
        ).strip()
        if not prompt_template:
            return EvaluationResult(False, True, "llm_review.prompt_template is empty")

        contains_image = "image" in message.content_types
        contains_forward = "forward" in message.content_types
        plain_text = MessageEvaluator._expand_picid_descriptions(message.plain_text, plugin)
        raw_message = MessageEvaluator._expand_picid_descriptions(message.raw_message, plugin)
        current_message = plain_text or raw_message or ""
        prompt_vars = {
            "group_id": message.group_id,
            "user_id": message.user_id,
            "user_nickname": message.user_nickname or message.user_id,
            "message_id": message.message_id,
            "plain_text": plain_text,
            "raw_message": raw_message,
            "current_message": current_message,
            "content_types": ", ".join(message.content_types),
            "contains_image": str(contains_image).lower(),
            "contains_forward": str(contains_forward).lower(),
            "reply_segments": json.dumps(message.reply_segments, ensure_ascii=False),
            "history": history,
        }
        prompt = MessageEvaluator._safe_format(prompt_template, prompt_vars)

        try:
            success, response, _, model_name = await llm_api.generate_with_model(
                prompt,
                model_config=model_config,
                request_type="plugin.sowing_moderation",
            )
        except Exception as exc:
            return EvaluationResult(False, False, f"llm review request failed: {exc}")

        if not success:
            return EvaluationResult(False, False, f"llm review failed: {response}")

        parsed = MessageEvaluator._parse_llm_review_response(response)
        if not parsed:
            return EvaluationResult(False, False, "llm review returned non-json decision")

        decision = str(parsed.get("decision", "")).strip().lower()
        reason = str(parsed.get("reason", "")).strip()
        if decision == "allow":
            return EvaluationResult(True, True, f"keyword_llm allow via {model_name}: {reason}")
        if decision == "reject":
            return EvaluationResult(False, True, f"keyword_llm reject via {model_name}: {reason}")
        return EvaluationResult(False, False, f"llm review returned invalid decision: {decision}")

    @staticmethod
    def _normalize_keywords(value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    @staticmethod
    def _resolve_review_message_types(plugin: BaseEventHandler) -> List[str]:
        configured = MessageEvaluator._normalize_keywords(plugin.get_config("llm_review.message_types", []))
        if configured:
            return [item.lower() for item in configured]
        if bool(plugin.get_config("llm_review.only_forward_messages", False)):
            return ["forward"]
        return ["forward"]

    @staticmethod
    def _build_review_text(message: PendingMessage, plugin: BaseEventHandler) -> str:
        plain_text = MessageEvaluator._expand_picid_descriptions(message.plain_text, plugin)
        raw_message = MessageEvaluator._expand_picid_descriptions(message.raw_message, plugin)
        return plain_text or raw_message or ""

    @staticmethod
    def _expand_picid_descriptions(content: str, plugin: BaseEventHandler) -> str:
        if not content:
            return ""
        if not bool(plugin.get_config("llm_review.expand_picid_descriptions", False)):
            return content
        if not hasattr(message_api, "translate_pid_to_description"):
            return content

        def replace_pic(match: re.Match[str]) -> str:
            pic_id = str(match.group(1) or "").strip()
            if not pic_id:
                return "[图片]"
            try:
                description = message_api.translate_pid_to_description(pic_id)
            except Exception:
                return "[图片]"
            description = str(description or "").strip()
            return description or "[图片]"

        return MessageEvaluator.PIC_ID_PATTERN.sub(replace_pic, content)

    @staticmethod
    def _build_llm_model_config(plugin: BaseEventHandler) -> TaskConfig:
        llm_list = MessageEvaluator._normalize_keywords(plugin.get_config("llm_review.llm_list", []))
        if llm_list:
            model_config = TaskConfig()
            model_config.model_list = llm_list
            model_config.max_tokens = int(plugin.get_config("llm_review.max_tokens", 800))
            model_config.temperature = float(plugin.get_config("llm_review.temperature", 0.2))
            model_config.slow_threshold = float(plugin.get_config("llm_review.slow_threshold", 30))
            model_config.selection_strategy = str(plugin.get_config("llm_review.selection_strategy", "balance"))
            return model_config

        llm_group = str(plugin.get_config("llm_review.llm_group", "utils"))
        models = llm_api.get_available_models()
        model_config = models.get(llm_group)
        if not model_config:
            raise RuntimeError(f"no available llm config for group {llm_group}")
        return model_config

    @staticmethod
    def _build_recent_history(message: PendingMessage, plugin: BaseEventHandler) -> str:
        history_hours = float(plugin.get_config("llm_review.history_hours", 12))
        history_limit = int(plugin.get_config("llm_review.history_limit", 30))
        history_filter_mai = bool(plugin.get_config("llm_review.history_filter_mai", False))
        history_truncate = bool(plugin.get_config("llm_review.history_truncate", True))
        history_show_actions = bool(plugin.get_config("llm_review.history_show_actions", False))

        messages = message_api.get_recent_messages(
            chat_id=message.stream_id,
            hours=history_hours,
            limit=history_limit,
            limit_mode="latest",
            filter_mai=history_filter_mai,
        )
        if not messages:
            return "(no recent history)"
        history = message_api.build_readable_messages_to_str(
            messages,
            replace_bot_name=True,
            timestamp_mode="relative",
            truncate=history_truncate,
            show_actions=history_show_actions,
        )
        return history or "(no recent history)"

    @staticmethod
    def _safe_format(template: str, variables: Dict[str, Any]) -> str:
        """只替换 ``{name}`` 形式的占位符,字面量花括号(如 JSON 示例)原样保留。

        避免直接用 ``str.format`` 时把 prompt 末尾的 ``{"decision":"..."}``
        当成占位符触发 KeyError。
        """
        pattern = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

        def replace(match: re.Match[str]) -> str:
            key = match.group(1)
            if key in variables:
                return str(variables[key])
            return match.group(0)

        return pattern.sub(replace, template)

    @staticmethod
    def _parse_llm_review_response(response: str) -> Optional[Dict[str, Any]]:
        response = (response or "").strip()
        if not response:
            return None
        try:
            parsed = json.loads(response)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{[\s\S]*\}", response)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None


class NapCatHttpClient:
    def __init__(self, plugin: BaseEventHandler):
        self.plugin = plugin

    def is_enabled(self) -> bool:
        return bool(self.plugin.get_config("napcat_http.enabled", False))

    def _build_url(self, action: str) -> str:
        host = str(self.plugin.get_config("napcat_http.host", "127.0.0.1"))
        port = int(self.plugin.get_config("napcat_http.port", 3000))
        action = action.lstrip("/")
        return f"http://{host}:{port}/{action}"

    def _build_headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = str(self.plugin.get_config("napcat_http.access_token", "") or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def call_action(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_enabled():
            raise RuntimeError("napcat_http is disabled")

        url = self._build_url(action)
        headers = self._build_headers()
        timeout = float(self.plugin.get_config("napcat_http.timeout_seconds", 10))
        payload = json.dumps(params).encode("utf-8")

        def _send() -> Dict[str, Any]:
            req = urllib_request.Request(url=url, data=payload, headers=headers, method="POST")
            with urllib_request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                if not raw:
                    return {}
                return json.loads(raw)

        try:
            return await asyncio.to_thread(_send)
        except urllib_error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"napcat http {action} failed: {exc.code} {body}") from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(f"napcat http {action} connection failed: {exc.reason}") from exc

    async def forward_group_single_msg(self, group_id: str, message_id: str) -> bool:
        response = await self.call_action(
            "forward_group_single_msg",
            {
                "group_id": str(group_id),
                "message_id": str(message_id),
            },
        )
        return str(response.get("status", "")) == "ok" and int(response.get("retcode", -1)) == 0


class SowingForwardHandler(BaseEventHandler):
    event_type = EventType.ON_MESSAGE
    handler_name = "sowing_forward_handler"
    handler_description = "Cache source-group messages and forward them after evaluation."
    intercept_message = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = RuntimeState()
        self.process_lock = asyncio.Lock()
        self.napcat = NapCatHttpClient(self)
        self.background_task: Optional[asyncio.Task] = None

    async def execute(self, message: MaiMessages | None) -> Tuple[bool, bool, Optional[str], None, None]:
        if not message:
            return True, True, None, None, None

        if not self.get_config("plugin.enabled", False):
            return True, True, None, None, None

        source_info = self._resolve_source_info(message)
        if not source_info["is_group"]:
            return True, True, None, None, None

        source_group_ids = self._normalize_list(self.get_config("source.group_ids", []))
        is_source_group = source_info["group_id"] in source_group_ids
        should_continue = not (is_source_group and self.get_config("filter.block_source_messages", False))

        cleaned = await self.state.cleanup(int(self.get_config("cache.max_age_seconds", 3600)))
        if cleaned > 0:
            logger.info(f"[{PLUGIN_NAME}] cleaned {cleaned} expired pending messages")

        self._ensure_background_worker()

        if is_source_group:
            pending = self._build_pending_message(message, source_info)
            if pending is None:
                logger.warning(
                    f"[{PLUGIN_NAME}] skip caching: source_info missing required fields "
                    f"(stream_id={source_info.get('stream_id')!r}, "
                    f"group_id={source_info.get('group_id')!r}, "
                    f"message_id={source_info.get('message_id')!r})"
                )
            elif not self._is_allowed_message(pending):
                logger.debug(
                    f"[{PLUGIN_NAME}] skip caching message {pending.message_id}: "
                    f"content_types={pending.content_types} not allowed"
                )
            else:
                await self.state.add_pending(pending)
                logger.info(
                    f"[{PLUGIN_NAME}] cached message {pending.message_id} from group {pending.group_id} "
                    f"types={pending.content_types}"
                )

        if not bool(self.get_config("cache.process_in_background", True)):
            await self._process_pending()
        return True, should_continue, None, None, None

    def _normalize_list(self, value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        return [str(item) for item in value if str(item).strip()]

    def _parse_time(self, value: str, fallback: dtime) -> dtime:
        try:
            hour, minute = value.split(":", 1)
            parsed = dtime(int(hour), int(minute))
            return parsed
        except Exception:
            return fallback

    def _get_dynamic_cooldown(self) -> int:
        day_seconds = int(self.get_config("cooldown.day_seconds", 600))
        night_seconds = int(self.get_config("cooldown.night_seconds", 3600))
        min_seconds = int(self.get_config("cooldown.min_seconds", 0))
        day_start = self._parse_time(str(self.get_config("cooldown.day_start", "09:00")), dtime(9, 0))
        night_start = self._parse_time(str(self.get_config("cooldown.night_start", "01:00")), dtime(1, 0))
        now = datetime.now().time()
        dynamic = day_seconds if (now >= day_start or now < night_start) else night_seconds
        return max(dynamic, max(min_seconds, 0))

    def _resolve_source_info(self, message: MaiMessages) -> Dict[str, Any]:
        stream_id = str(getattr(message, "stream_id", "") or "")
        is_group = bool(getattr(message, "is_group_message", False))
        base_info: Dict[str, Any] = getattr(message, "message_base_info", {}) or {}

        platform = str(base_info.get("platform", "") or "qq")
        group_id = str(base_info.get("group_id", "") or "")
        group_name = str(base_info.get("group_name", "") or "")
        user_id = str(base_info.get("user_id", "") or "")
        user_nickname = str(
            base_info.get("user_cardname") or base_info.get("user_nickname") or user_id or ""
        )

        # MaiMessages 不直接携带 message_id; 通过 chat_stream 的最近一条消息回查
        message_id = ""
        chat_stream = None
        if stream_id:
            try:
                chat_stream = get_chat_manager().get_stream(stream_id)
            except Exception as exc:
                logger.debug(f"[{PLUGIN_NAME}] get_stream({stream_id}) failed: {exc}")
                chat_stream = None

        if chat_stream is not None:
            if not platform:
                platform = str(getattr(chat_stream, "platform", "") or "qq")
            if not group_id and getattr(chat_stream, "group_info", None):
                group_id = str(getattr(chat_stream.group_info, "group_id", "") or "")
                group_name = str(getattr(chat_stream.group_info, "group_name", "") or group_name)
                is_group = bool(group_id)
            if not user_id and getattr(chat_stream, "user_info", None):
                user_id = str(getattr(chat_stream.user_info, "user_id", "") or "")
                user_nickname = user_nickname or str(
                    getattr(chat_stream.user_info, "user_cardname", "")
                    or getattr(chat_stream.user_info, "user_nickname", "")
                    or user_id
                )

            try:
                context = getattr(chat_stream, "context", None)
                last_msg = context.get_last_message() if context is not None else None
            except Exception as exc:
                logger.debug(f"[{PLUGIN_NAME}] get_last_message failed: {exc}")
                last_msg = None
            if last_msg is not None:
                msg_info = getattr(last_msg, "message_info", None)
                if msg_info is not None:
                    message_id = str(getattr(msg_info, "message_id", "") or "")

        if not group_id and stream_id:
            for stream in chat_api.get_group_streams(platform):
                stream_info = chat_api.get_stream_info(stream)
                if str(stream_info.get("stream_id", "")) == stream_id:
                    group_id = str(stream_info.get("group_id", "") or "")
                    group_name = str(stream_info.get("group_name", "") or group_name)
                    is_group = bool(group_id)
                    break

        return {
            "stream_id": stream_id,
            "platform": platform or "qq",
            "group_id": group_id,
            "group_name": group_name,
            "user_id": user_id,
            "user_nickname": user_nickname or user_id or "unknown",
            "message_id": message_id,
            "is_group": is_group,
        }

    def _extract_content_types(self, message: MaiMessages) -> List[str]:
        collected: List[str] = []

        def visit(seg: Any) -> None:
            if seg is None:
                return
            seg_type = str(getattr(seg, "type", "") or "").lower()
            seg_data = getattr(seg, "data", None)

            if seg_type == "seglist":
                if isinstance(seg_data, list):
                    for child in seg_data:
                        visit(child)
                return
            if seg_type == "forward":
                collected.append("forward")
                return
            if seg_type in {"image", "emoji", "voice", "video", "audio", "file"}:
                collected.append(seg_type)
                return
            if seg_type == "text":
                if isinstance(seg_data, str) and seg_data.strip():
                    collected.append("text")
                return
            if seg_type:
                collected.append(seg_type)

        segments = getattr(message, "message_segments", None) or []
        for seg in segments:
            visit(seg)

        if not collected:
            plain_text = str(getattr(message, "plain_text", "") or "").strip()
            raw_message = str(getattr(message, "raw_message", "") or "").strip()
            if plain_text or raw_message:
                collected.append("text")

        return sorted(set(collected))

    def _extract_reply_segments(self, message: MaiMessages) -> List[Tuple[str, str]]:
        segments: List[Tuple[str, str]] = []

        def walk(seg: Any) -> None:
            if seg is None:
                return
            seg_type = str(getattr(seg, "type", "") or "").lower()
            seg_data = getattr(seg, "data", None)

            if seg_type == "seglist" and isinstance(seg_data, list):
                for child in seg_data:
                    walk(child)
                return

            if seg_type in {"text", "image", "emoji"} and isinstance(seg_data, str):
                if seg_type == "text" and not seg_data.strip():
                    return
                segments.append((seg_type, seg_data))

        for seg in getattr(message, "message_segments", None) or []:
            walk(seg)

        if not segments:
            plain_text = str(getattr(message, "plain_text", "") or "")
            if plain_text:
                segments.append(("text", plain_text))

        return segments

    def _is_comment_on_forward_message(self, plain_text: str) -> bool:
        text = plain_text.strip()
        return (
            text.startswith("[回复<")
            and "转发消息开始" in text
            and "转发消息结束" in text
            and "]，说：" in text
        )

    def _build_pending_message(self, message: MaiMessages, source_info: Dict[str, Any]) -> Optional[PendingMessage]:
        stream_id = str(source_info["stream_id"])
        group_id = str(source_info["group_id"])
        message_id = str(source_info["message_id"])
        if not stream_id or not group_id or not message_id:
            return None

        user_nickname = str(source_info["user_nickname"]) or str(source_info["user_id"]) or "unknown"
        content_types = self._extract_content_types(message)
        plain_text = str(getattr(message, "plain_text", "") or "")
        if self._is_comment_on_forward_message(plain_text):
            logger.debug(f"[{PLUGIN_NAME}] skip quoted forward comment {message_id}")
            content_types = [item for item in content_types if item != "forward"] or ["text"]

        raw_message = str(getattr(message, "raw_message", "") or "")
        if not bool(self.get_config("cache.keep_raw_message", False)):
            raw_message = raw_message if (not plain_text and "forward" in content_types) else ""

        return PendingMessage(
            message_id=message_id,
            stream_id=stream_id,
            group_id=group_id,
            platform=str(source_info["platform"]),
            user_id=str(source_info["user_id"]),
            user_nickname=user_nickname,
            plain_text=plain_text,
            raw_message=raw_message,
            content_types=content_types,
            reply_segments=self._extract_reply_segments(message),
            cached_at=time.time(),
        )

    def _is_allowed_message(self, pending: PendingMessage) -> bool:
        allowed = set(self._normalize_list(self.get_config("filter.allowed_message_types", ["forward"])))
        if not allowed:
            return False

        content_types = set(pending.content_types)
        protected_types = {"image", "voice", "video", "forward"}
        if any(item in content_types and item not in allowed for item in protected_types):
            return False
        return bool(content_types.intersection(allowed))

    def _resolve_target_stream_ids(self, platform: str) -> List[str]:
        configured_group_ids = self._normalize_list(self.get_config("target.group_ids", []))
        if not configured_group_ids:
            stream_ids: List[str] = []
            for stream in chat_api.get_group_streams(platform):
                stream_info = chat_api.get_stream_info(stream)
                stream_id = str(stream_info.get("stream_id", ""))
                if stream_id:
                    stream_ids.append(stream_id)
            return stream_ids

        stream_ids = []
        for group_id in configured_group_ids:
            stream = chat_api.get_stream_by_group_id(group_id, platform)
            if not stream:
                logger.warning(f"[{PLUGIN_NAME}] target group {group_id} not found on platform {platform}")
                continue
            stream_info = chat_api.get_stream_info(stream)
            stream_id = str(stream_info.get("stream_id", ""))
            if stream_id:
                stream_ids.append(stream_id)
        return stream_ids

    def _resolve_target_group_ids(self) -> List[str]:
        return self._normalize_list(self.get_config("target.group_ids", []))

    def _exclude_source_stream(self, stream_ids: List[str], platform: str, source_group_id: str) -> List[str]:
        if not source_group_id:
            return stream_ids
        filtered: List[str] = []
        for stream_id in stream_ids:
            matched_group_id = ""
            for stream in chat_api.get_group_streams(platform):
                stream_info = chat_api.get_stream_info(stream)
                if str(stream_info.get("stream_id", "")) != str(stream_id):
                    continue
                matched_group_id = str(stream_info.get("group_id", ""))
                break
            if matched_group_id == str(source_group_id):
                continue
            filtered.append(stream_id)
        return filtered

    def _to_reply_content(self, segment_type: str) -> Optional[ReplyContentType]:
        mapping = {
            "text": ReplyContentType.TEXT,
            "image": ReplyContentType.IMAGE,
            "emoji": ReplyContentType.EMOJI,
        }
        return mapping.get(segment_type)

    def _ensure_background_worker(self) -> None:
        if not bool(self.get_config("cache.process_in_background", True)):
            return
        if self.background_task and not self.background_task.done():
            return
        self.background_task = asyncio.create_task(self._background_process_loop())

    async def _background_process_loop(self) -> None:
        poll_seconds = max(1, int(self.get_config("cache.poll_interval_seconds", 30)))
        logger.info(f"[{PLUGIN_NAME}] background worker started, poll_interval={poll_seconds}s")
        try:
            while True:
                try:
                    cleaned = await self.state.cleanup(int(self.get_config("cache.max_age_seconds", 3600)))
                    if cleaned > 0:
                        logger.info(f"[{PLUGIN_NAME}] cleaned {cleaned} expired pending messages")
                    await self._process_pending()
                except Exception as exc:
                    logger.error(f"[{PLUGIN_NAME}] background worker iteration failed: {exc}")
                await asyncio.sleep(poll_seconds)
        except asyncio.CancelledError:
            logger.info(f"[{PLUGIN_NAME}] background worker stopped")
            raise

    async def _process_pending(self) -> None:
        if self.process_lock.locked():
            return

        async with self.process_lock:
            matured = await self.state.get_matured(int(self.get_config("cache.wait_seconds", 600)))
            if not matured:
                return

            last_forward_at = await self.state.get_last_forward_at()
            cooldown_seconds = self._get_dynamic_cooldown()
            if time.time() - last_forward_at < cooldown_seconds:
                return

            strategy = str(self.get_config("evaluation.strategy", "always"))
            keywords = self._normalize_list(self.get_config("evaluation.keywords", []))
            case_sensitive = bool(self.get_config("evaluation.case_sensitive", False))

            for pending in matured:
                evaluation = await MessageEvaluator.evaluate(
                    strategy,
                    pending,
                    keywords,
                    case_sensitive,
                    self,
                )
                if not evaluation.is_final:
                    logger.warning(
                        f"[{PLUGIN_NAME}] deferred message {pending.message_id} because evaluation is incomplete: {evaluation.reason}"
                    )
                    return
                if not evaluation.should_forward:
                    await self.state.remove_pending(pending.message_id)
                    logger.info(f"[{PLUGIN_NAME}] skipped message {pending.message_id}: {evaluation.reason}")
                    continue

                reply_payload: List[Tuple[ReplyContentType, str]] = []
                for segment_type, content in pending.reply_segments:
                    reply_type = self._to_reply_content(segment_type)
                    if reply_type and content:
                        reply_payload.append((reply_type, content))

                target_stream_ids = self._exclude_source_stream(
                    self._resolve_target_stream_ids(pending.platform),
                    pending.platform,
                    pending.group_id,
                )
                if not target_stream_ids:
                    await self.state.remove_pending(pending.message_id)
                    logger.warning(
                        f"[{PLUGIN_NAME}] no eligible target streams resolved for message {pending.message_id} after excluding source group"
                    )
                    continue

                direct_forward_enabled = bool(self.get_config("napcat_http.use_direct_forward", False))
                target_group_ids = [
                    group_id for group_id in self._resolve_target_group_ids() if str(group_id) != str(pending.group_id)
                ]
                if direct_forward_enabled and not target_group_ids:
                    logger.warning(
                        f"[{PLUGIN_NAME}] napcat direct forward requires explicit target.group_ids for message {pending.message_id}"
                    )

                direct_success_count = 0
                if direct_forward_enabled and target_group_ids:
                    for target_group_id in target_group_ids:
                        try:
                            success = await self.napcat.forward_group_single_msg(target_group_id, pending.message_id)
                        except Exception as exc:
                            logger.error(
                                f"[{PLUGIN_NAME}] napcat direct forward failed for message {pending.message_id} to group {target_group_id}: {exc}"
                            )
                            success = False
                        if success:
                            direct_success_count += 1

                if direct_success_count > 0:
                    await self.state.remove_pending(pending.message_id)
                    await self.state.set_last_forward_at(time.time())
                    logger.info(
                        f"[{PLUGIN_NAME}] direct-forwarded message {pending.message_id} to {direct_success_count} target groups through NapCat HTTP"
                    )
                    return

                # MaiBot 兜底只发送可重建的节点。不要用 message_id 引用原消息，否则合并消息会变成
                # “引用合并消息的合并转发”，并且在 message_id 回查偏移时会转发到引用消息本身。
                sender_id = pending.user_id or "0"
                sender_name = pending.user_nickname or sender_id
                manual_payload: List[Any] = (
                    [(sender_id, sender_name, reply_payload)] if reply_payload else []
                )

                async def _try_send(target_stream_id: str, payload: List[Any]) -> bool:
                    try:
                        return await self.send_forward(
                            stream_id=target_stream_id,
                            messages_list=payload,
                        )
                    except Exception as exc:
                        logger.error(
                            f"[{PLUGIN_NAME}] send_forward raised for message {pending.message_id} to stream {target_stream_id}: {exc}"
                        )
                        return False

                success_count = 0
                if manual_payload:
                    for target_stream_id in target_stream_ids:
                        success = await _try_send(target_stream_id, manual_payload)
                        if success:
                            success_count += 1
                        else:
                            logger.error(
                                f"[{PLUGIN_NAME}] failed to forward message {pending.message_id} to stream {target_stream_id}"
                            )
                else:
                    logger.error(
                        f"[{PLUGIN_NAME}] message {pending.message_id} has no manual fallback payload; "
                        f"keep pending for next NapCat direct-forward attempt"
                    )

                if success_count > 0:
                    await self.state.remove_pending(pending.message_id)
                    await self.state.set_last_forward_at(time.time())
                    logger.info(
                        f"[{PLUGIN_NAME}] forwarded message {pending.message_id} to {success_count} target streams"
                    )
                    return
                # 全部转发失败,保留候选,等下一轮再试
                continue


@register_plugin
class MaiBotSowingDiscordPlugin(BasePlugin):
    plugin_name = PLUGIN_NAME
    enable_plugin = False
    dependencies: List[str] = []
    python_dependencies: List[str] = []
    config_file_name = "config.toml"

    config_section_descriptions = {
        "plugin": "插件基础配置",
        "source": "源群配置：只有这些群的消息会进入候选池",
        "target": "目标群配置：命中的消息会被转发到这些群",
        "cache": "缓存与等待窗口：消息会先缓存，等待贴表情统计稳定后再评估",
        "cooldown": "动态冷却配置：成功转发后进入冷却，避免连续播史",
        "filter": "消息过滤配置：控制哪些消息类型允许进入候选池",
        "evaluation": "评估策略配置：决定消息是否应该被转发",
        "napcat_http": "NapCat HTTP 配置：用于直接转发原消息(forward_group_single_msg)",
        "llm_review": "LLM 审核配置：关键词预筛 + 聊天记录上下文审核",
    }

    config_schema = {
        "plugin": {
            "enabled": ConfigField(
                type=bool,
                default=False,
                description="是否启用插件。未启用时插件不会处理任何消息。",
                example="true",
            ),
            "config_version": ConfigField(
                type=str,
                default="1.5.0",
                description="配置文件版本。修改 config_schema 后应同步提升版本，便于 MaiBot 执行配置迁移。",
                example="1.5.0",
            ),
        },
        "source": {
            "group_ids": ConfigField(
                type=list,
                default=[],
                description="源群群号列表。只有这些群发出的消息会进入候选池。留空等于不监听任何源群。",
                item_type="string",
                example='["123456789", "987654321"]',
            ),
        },
        "target": {
            "group_ids": ConfigField(
                type=list,
                default=[],
                description="目标群群号列表。设置后优先用于 NapCat 直接转发。留空时会回退为当前平台已知的全部群聊流。",
                item_type="string",
                example='["1122334455", "5566778899"]',
            ),
        },
        "cache": {
            "wait_seconds": ConfigField(
                type=int,
                default=600,
                description="候选消息进入缓存后，需要等待多少秒才开始评估。表情统计需要时间沉淀，这个值过小会导致误判。",
                min=1,
                example="600",
            ),
            "process_in_background": ConfigField(
                type=bool,
                default=True,
                description="是否使用后台独立协程轮询处理缓存消息。开启后，前台收消息只负责入队，转发和审核异步执行，主流程更轻，但处理会有轮询延迟。",
                example="true",
            ),
            "poll_interval_seconds": ConfigField(
                type=int,
                default=30,
                description="后台轮询间隔，单位秒。值越大越省资源，但候选消息从到期到真正处理的延迟也越大。",
                min=1,
                example="30",
            ),
            "keep_raw_message": ConfigField(
                type=bool,
                default=False,
                description="是否把原始 `raw_message` 一并写入运行时缓存。关闭后更省内存和状态文件空间；只有在 `plain_text` 为空且消息是合并转发时才会保留必要原文。",
                example="false",
            ),
            "max_age_seconds": ConfigField(
                type=int,
                default=3600,
                description="候选消息在缓存中的最大保留时间。超过这个时间还没处理就会被清理。",
                min=60,
                example="3600",
            ),
        },
        "cooldown": {
            "day_seconds": ConfigField(
                type=int,
                default=600,
                description="白天冷却时间，单位秒。当前时间位于白天区间时，成功转发后至少等待这么久才允许下次转发。",
                min=1,
                example="600",
            ),
            "night_seconds": ConfigField(
                type=int,
                default=3600,
                description="夜间冷却时间，单位秒。夜间一般冷却更长，用来降低深夜刷屏概率。",
                min=1,
                example="3600",
            ),
            "day_start": ConfigField(
                type=str,
                default="09:00",
                description="白天开始时间，24 小时制，格式 HH:MM。当前逻辑中：时间 >= day_start 或 < night_start 视为白天。",
                example="09:00",
            ),
            "night_start": ConfigField(
                type=str,
                default="01:00",
                description="夜间开始时间，24 小时制，格式 HH:MM。与 day_start 共同定义冷热却区间切换。",
                example="01:00",
            ),
            "min_seconds": ConfigField(
                type=int,
                default=0,
                description="全局冷却下限,单位秒。无论白天/夜间,实际冷却 = max(动态冷却, 这个值)。设为 0 时不生效。用于在 day/night 之外再压一层全局上限。",
                min=0,
                example="1800",
            ),
        },
        "filter": {
            "allowed_message_types": ConfigField(
                type=list,
                default=["forward"],
                description='允许进入候选池的消息类型。默认仅接收 `forward`；图片消息不再支持。若消息包含未授权的核心媒体类型，会被整条拦截。',
                item_type="string",
                example='["forward"]',
            ),
            "block_source_messages": ConfigField(
                type=bool,
                default=False,
                description="是否阻断源群消息的后续处理。开启后，命中源群的消息会在当前插件处停止继续流转给后续处理器。",
                example="false",
            ),
        },
        "evaluation": {
            "strategy": ConfigField(
                type=str,
                default="keyword_llm",
                description="候选消息评估策略。`keyword_llm` 为关键词预筛 + LLM 结合聊天记录审核;`always` 全部放行;`keyword_any/all` 走纯关键词。",
                choices=["keyword_llm", "always", "keyword_any", "keyword_all"],
                example="keyword_llm",
            ),
            "keywords": ConfigField(
                type=list,
                default=[],
                description="关键词评估策略使用的关键词列表。仅当 strategy 为 `keyword_any` 或 `keyword_all` 时生效。",
                item_type="string",
                example='["奶龙", "麦麦"]',
            ),
            "case_sensitive": ConfigField(
                type=bool,
                default=False,
                description="关键词匹配是否区分大小写。仅对关键词策略生效。",
                example="false",
            ),
        },
        "napcat_http": {
            "enabled": ConfigField(
                type=bool,
                default=False,
                description="是否启用 NapCat HTTP。`use_direct_forward` 依赖它,用于通过 NapCat 原样合并转发消息。",
                example="true",
            ),
            "host": ConfigField(
                type=str,
                default="127.0.0.1",
                description="NapCat HTTP 服务地址。若 MaiBot 与 NapCat 在同一台机器，通常填 `127.0.0.1`。",
                example="127.0.0.1",
            ),
            "port": ConfigField(
                type=int,
                default=3000,
                description="NapCat HTTP 端口。你本地 WebUI 调试页默认就是往 3000 发请求。",
                min=1,
                example="3000",
            ),
            "access_token": ConfigField(
                type=str,
                default="",
                description="NapCat HTTP 鉴权 token。若 NapCat 未启用鉴权可留空；若启用了，这里必须与 NapCat 配置一致。",
                example="your_napcat_token",
            ),
            "timeout_seconds": ConfigField(
                type=int,
                default=10,
                description="NapCat HTTP 请求超时，单位秒。接口超时不会直接删缓存消息，下轮还会继续尝试。",
                min=1,
                example="10",
            ),
            "use_direct_forward": ConfigField(
                type=bool,
                default=True,
                description="是否优先调用 NapCat `forward_group_single_msg` 直接转发原消息。要求 `target.group_ids` 明确填写，否则会回退到 MaiBot `send_forward()`。",
                example="true",
            ),
        },
        "llm_review": {
            "enabled": ConfigField(
                type=bool,
                default=True,
                description="是否启用 LLM 审核能力。仅当 evaluation.strategy=keyword_llm 时生效。",
                example="true",
            ),
            "message_types": ConfigField(
                type=list,
                default=["forward"],
                description="允许进入 LLM 审核的消息类型列表。默认只审合并转发；图片消息不再支持。",
                item_type="string",
                example='["forward"]',
            ),
            "only_forward_messages": ConfigField(
                type=bool,
                default=False,
                description="旧兼容开关。仅当 `llm_review.message_types` 留空时才生效；设为 true 时等价于只审 `forward`。",
                example="false",
            ),
            "expand_picid_descriptions": ConfigField(
                type=bool,
                default=False,
                description="是否把消息里的 `[picid:xxx]` 替换为 MaiBot 数据库中已存在的图片描述。图片消息不再支持，默认关闭。",
                example="false",
            ),
            "prompt_template": ConfigField(
                type=str,
                default=(
                    "# Role\n"
                    "你是一个严格的群合并转发搬运审核器,只判断当前合并转发是否值得搬到目标群作为节目效果消息。\n\n"
                    "# Context\n"
                    "当前消息来自群 {group_id},发送者 {user_nickname}({user_id}),消息ID={message_id}。\n"
                    "消息类型:{content_types}\n"
                    "是否包含合并转发:{contains_forward}\n\n"
                    "# Current Message\n"
                    "plain_text:\n{plain_text}\n\n"
                    "raw_message:\n{raw_message}\n\n"
                    "reply_segments:\n{reply_segments}\n\n"
                    "# Recent Chat History\n"
                    "{history}\n\n"
                    "# Core Policy (按优先级)\n\n"
                    "## P0 一票否决,任何场景立即 reject\n"
                    "1. 政治内容:涉及国家领导人、政府政策、敏感历史事件、政治立场表态、地缘政治、社会运动、民族宗教冲突、领土争议等任何形式的政治议题,无论立场、无论玩梗、无论二创,一律 reject,不解释。\n"
                    "2. 明显违法违规:真实违法犯罪记录、人肉开盒、儿童色情、暴恐血腥、自残教唆、毒品交易。\n"
                    "3. 明显 NSFW 或色情引流:露点、性器官、性行为、成人视频、约炮广告、色情网站等直接 reject。\n\n"
                    "## P1 输入边界\n"
                    "- 只审核当前消息本体是合并转发的消息。若 contains_forward=false,或内容明显只是回复/引用某条合并转发后的普通评论,一律 reject。\n"
                    "- 不支持单独图片搬运。不要根据图片占位符、图片数量或无法确认的图片内容作 allow 判断。\n"
                    "- 如果合并转发主要由图片占位符构成,且没有足够文字上下文证明节目效果,一律 reject。\n\n"
                    "## P2 合并转发放行标准\n"
                    "- allow:玩梗、抽象对话、低质段子、轻度冒犯但无实质风险、群友互动产生的明确节目效果。\n"
                    "- allow:最近聊天记录能佐证该合并转发有上下文笑点。\n"
                    "- reject:纯通知、纯求助、纯认真讨论、纯吵架骂战、纯客套、无上下文的普通聊天记录。\n"
                    "- reject:无法判断笑点或节目效果时一律 reject。\n\n"
                    "# Output\n"
                    "只输出一个 JSON 对象,不要输出任何额外文字:\n"
                    "{\"decision\":\"allow|reject\",\"reason\":\"不超过40字\"}"
                ),
                description="LLM 审核提示词模板。必须只要求模型输出 JSON,字段至少包含 decision 和 reason。占位符仅识别 {name} 形式,JSON 字面量花括号会被原样保留。",
                example='{"decision":"allow","reason":"合并转发有节目效果"}',
            ),
            "reject_keywords": ConfigField(
                type=list,
                default=[],
                description="命中即直接拒绝转发的关键词列表。优先级高于 LLM。",
                item_type="string",
                example='["广告", "引流", "nsfw"]',
            ),
            "pass_keywords": ConfigField(
                type=list,
                default=[],
                description="命中即直接放行的关键词列表。优先级高于 LLM。",
                item_type="string",
                example='["爆典", "神回复"]',
            ),
            "history_hours": ConfigField(
                type=float,
                default=12.0,
                description="审核时向消息库回看最近多少小时的聊天记录。",
                example="12",
            ),
            "history_limit": ConfigField(
                type=int,
                default=12,
                description="审核时最多带入多少条最近聊天记录。默认值已下调，减少内存和上下文占用。",
                min=1,
                example="12",
            ),
            "history_filter_mai": ConfigField(
                type=bool,
                default=False,
                description="构建聊天记录时是否过滤 bot 自己的消息。",
                example="false",
            ),
            "history_truncate": ConfigField(
                type=bool,
                default=True,
                description="构建聊天记录时是否截断超长消息。",
                example="true",
            ),
            "history_show_actions": ConfigField(
                type=bool,
                default=False,
                description="构建聊天记录时是否展示动作记录。",
                example="false",
            ),
            "llm_group": ConfigField(
                type=str,
                default="utils",
                description="LLM 模型分组。仅当 llm_list 为空时生效。",
                example="utils",
            ),
            "llm_list": ConfigField(
                type=list,
                default=[],
                description="手动指定模型名称列表。非空时优先于 llm_group。",
                item_type="string",
                example='["gemini-2.5-pro", "glm-4.7"]',
            ),
            "max_tokens": ConfigField(
                type=int,
                default=400,
                description="手动指定模型时的最大输出 token 数。默认值已下调，控制审核开销。",
                min=1,
                example="400",
            ),
            "temperature": ConfigField(
                type=float,
                default=0.2,
                description="手动指定模型时的温度。",
                example="0.2",
            ),
            "slow_threshold": ConfigField(
                type=float,
                default=30,
                description="手动指定模型时的慢请求阈值，单位秒。",
                example="30",
            ),
            "selection_strategy": ConfigField(
                type=str,
                default="balance",
                choices=["balance", "random"],
                description="手动指定模型列表时的模型选择策略。",
                example="balance",
            ),
        },
    }

    def get_plugin_components(self) -> List[Tuple[ComponentInfo, Type]]:
        return [
            (SowingForwardHandler.get_handler_info(), SowingForwardHandler),
        ]
