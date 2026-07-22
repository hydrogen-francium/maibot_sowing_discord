import asyncio
import hashlib
import difflib
import json
import os
import re
import sqlite3
import time
import unicodedata
from contextlib import contextmanager
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
    chat_api,
    database_api,
    get_logger,
    llm_api,
    message_api,
    plugin_manage_api,
    register_plugin,
)


PLUGIN_NAME = "maibot_sowing_discord"
logger = get_logger(PLUGIN_NAME)


def normalize_content_text(value: Any) -> str:
    """生成跨 adapter 格式稳定的去重文本。"""
    value = unicodedata.normalize("NFKC", str(value or "")).replace("\r\n", "\n")
    value = re.sub(r"={3,}\s*转发消息(?:开始|结束)\s*={3,}", "", value)
    value = re.sub(r"https?://\S+", "[url]", value)
    return re.sub(r"\s+", " ", value).strip().lower()


def build_content_fingerprint(text: str, segments: List[Tuple[str, str]]) -> str:
    """Hash durable snapshot data only; never include timestamps, URLs, or base64."""
    media = []
    for kind, value in segments:
        if kind in {"image", "emoji", "video", "file", "voice", "audio"}:
            value = str(value or "")
            # identifiers/descriptions are useful; transport payloads are not.
            if len(value) > 300 or value.startswith(("data:", "http://", "https://")):
                value = f"[{kind}]"
            media.append((kind, normalize_content_text(value)))
    payload = json.dumps({"text": normalize_content_text(text), "media": media},
                         ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def content_similarity(left: str, right: str) -> float:
    left, right = normalize_content_text(left), normalize_content_text(right)
    if not left or not right:
        return 0.0
    seq = difflib.SequenceMatcher(None, left, right).ratio()
    words_a, words_b = set(left.split()), set(right.split())
    jaccard = len(words_a & words_b) / len(words_a | words_b) if words_a or words_b else 0.0
    return max(seq, jaccard)


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
    analysis: Optional[Dict[str, Any]] = None


class RuntimeState:
    """SQLite-backed durable queue.  Legacy runtime_state.json is deliberately not imported."""

    def __init__(self, state_path: Optional[str] = None) -> None:
        self._lock = asyncio.Lock()
        try:
            base = plugin_manage_api.get_plugin_path(PLUGIN_NAME) or os.path.dirname(os.path.abspath(__file__))
        except Exception:
            base = os.path.dirname(os.path.abspath(__file__))
        self._state_path = state_path or os.path.join(base, "sowing_state.sqlite3")
        self._init_db()
        legacy = os.path.join(base, "runtime_state.json")
        if state_path is None and os.path.exists(legacy):
            logger.warning(f"[{PLUGIN_NAME}] legacy runtime_state.json was not migrated; SQLite starts empty")

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self._state_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connect() as db:
            db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY, message_id TEXT NOT NULL, stream_id TEXT NOT NULL,
                source_group_id TEXT NOT NULL, platform TEXT NOT NULL, user_id TEXT, nickname TEXT,
                payload_json TEXT NOT NULL, fingerprint TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                analysis_json TEXT, created_at REAL NOT NULL, due_at REAL NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0, next_retry_at REAL NOT NULL DEFAULT 0, last_error TEXT,
                UNIQUE(message_id, stream_id));
            CREATE TABLE IF NOT EXISTS deliveries (
                id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL, target_stream_id TEXT NOT NULL DEFAULT '',
                target_group_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', retry_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at REAL NOT NULL DEFAULT 0, last_error TEXT, completed_at REAL, sending_started_at REAL,
                UNIQUE(job_id, target_group_id), FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE);
            CREATE TABLE IF NOT EXISTS content_history (
                target_stream_id TEXT NOT NULL, fingerprint TEXT NOT NULL, normalized_text TEXT NOT NULL,
                sent_at REAL NOT NULL, source_group_id TEXT, summary TEXT,
                PRIMARY KEY(target_stream_id, fingerprint));
            CREATE TABLE IF NOT EXISTS cooldowns (target_stream_id TEXT PRIMARY KEY, last_success_at REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS jobs_due ON jobs(status, due_at, next_retry_at);
            CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries(status, next_retry_at);
            """)
            delivery_columns = {
                str(row[1]) for row in db.execute("PRAGMA table_info(deliveries)").fetchall()
            }
            if "sending_started_at" not in delivery_columns:
                db.execute("ALTER TABLE deliveries ADD COLUMN sending_started_at REAL")

    async def _run(self, operation, *args):
        async with self._lock:
            return await asyncio.to_thread(operation, *args)

    async def cleanup(self, max_age_seconds: int, history_retention_days: int = 30) -> int:
        def op():
            now = time.time()
            with self._connect() as db:
                result = db.execute("UPDATE jobs SET status='expired' WHERE status IN ('pending','retry') AND created_at<?", (now - max_age_seconds,))
                db.execute(
                    "UPDATE deliveries SET status='retry',next_retry_at=?,last_error='recovered expired sending lease' "
                    "WHERE status='sending' AND sending_started_at<?",
                    (now, now - 300),
                )
                db.execute("DELETE FROM content_history WHERE sent_at<?", (now - max(1, history_retention_days) * 86400,))
                db.execute("DELETE FROM jobs WHERE status IN ('completed','rejected','failed','expired') AND created_at<?", (now - max(1, history_retention_days) * 86400,))
                return result.rowcount
        return await self._run(op)

    async def add_pending(self, message: PendingMessage, wait_seconds: int) -> None:
        def op():
            now = time.time()
            due_at = message.cached_at + max(0, wait_seconds)
            payload = message.to_dict()
            fingerprint = build_content_fingerprint(message.plain_text or message.raw_message, message.reply_segments)
            with self._connect() as db:
                db.execute("""INSERT INTO jobs(message_id,stream_id,source_group_id,platform,user_id,nickname,payload_json,fingerprint,created_at,due_at,next_retry_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(message_id,stream_id) DO NOTHING""",
                    (message.message_id,message.stream_id,message.group_id,message.platform,message.user_id,message.user_nickname,
                     json.dumps(payload,ensure_ascii=False),fingerprint,now,due_at,now))
        await self._run(op)

    async def get_matured(self, limit: int = 20) -> List[PendingMessage]:
        def op():
            now=time.time()
            with self._connect() as db:
                rows=db.execute("SELECT payload_json FROM jobs WHERE status IN ('pending','retry') AND due_at<=? AND next_retry_at<=? ORDER BY created_at LIMIT ?",(now,now,max(1,limit))).fetchall()
                return [PendingMessage.from_dict(json.loads(row[0])) for row in rows]
        return await self._run(op)

    async def finalize_evaluation(
        self,
        message: PendingMessage,
        result: EvaluationResult,
        targets: List[Tuple[str, str]],
    ) -> None:
        def op():
            analysis = result.analysis or {
                "decision": "allow" if result.should_forward else "reject",
                "reason": result.reason,
                "summary": "",
                "joke_points": [],
                "context_dependency": "unknown",
                "content_tags": [],
                "risk_tags": [],
            }
            status = "approved" if result.should_forward else "rejected"
            with self._connect() as db:
                row = db.execute(
                    "SELECT id FROM jobs WHERE message_id=? AND stream_id=?",
                    (message.message_id, message.stream_id),
                ).fetchone()
                if not row:
                    return
                if result.should_forward and not targets:
                    raise RuntimeError("approved message has no eligible targets")
                if result.should_forward:
                    db.executemany(
                        "INSERT INTO deliveries(job_id,target_stream_id,target_group_id) VALUES(?,?,?) "
                        "ON CONFLICT(job_id,target_group_id) DO UPDATE SET target_stream_id=excluded.target_stream_id",
                        [(row[0], stream_id, group_id) for stream_id, group_id in targets],
                    )
                db.execute(
                    "UPDATE jobs SET status=?,analysis_json=?,last_error=NULL WHERE id=?",
                    (status, json.dumps(analysis, ensure_ascii=False), row[0]),
                )
        await self._run(op)

    async def retry_job(self, message: PendingMessage, reason: str, delay: int, maximum: int) -> None:
        def op():
            with self._connect() as db:
                db.execute("UPDATE jobs SET retry_count=retry_count+1,status=CASE WHEN retry_count+1>=? THEN 'failed' ELSE 'retry' END,next_retry_at=?,last_error=? WHERE message_id=? AND stream_id=?",(maximum,time.time()+delay,reason[:500],message.message_id,message.stream_id))
        await self._run(op)

    async def due_deliveries(self, limit: int = 20) -> List[Dict[str,Any]]:
        def op():
            with self._connect() as db:
                rows=db.execute("""SELECT d.*,j.message_id,j.source_group_id,j.platform,j.user_id,j.nickname,j.payload_json,j.fingerprint,j.analysis_json
                    FROM deliveries d JOIN jobs j ON d.job_id=j.id WHERE d.status IN ('pending','retry') AND d.next_retry_at<=? AND j.status='approved' ORDER BY d.id LIMIT ?""",(time.time(),max(1,limit))).fetchall()
                return [dict(row) for row in rows]
        return await self._run(op)

    async def claim_delivery(self, delivery_id: int) -> bool:
        def op():
            with self._connect() as db:
                result = db.execute(
                    "UPDATE deliveries SET status='sending',sending_started_at=? "
                    "WHERE id=? AND status IN ('pending','retry') AND next_retry_at<=?",
                    (time.time(), delivery_id, time.time()),
                )
                return result.rowcount == 1
        return await self._run(op)

    async def duplicate(self,target: str,fingerprint: str) -> bool:
        def op():
            with self._connect() as db: return bool(db.execute("SELECT 1 FROM content_history WHERE target_stream_id=? AND fingerprint=?",(target,fingerprint)).fetchone())
        return await self._run(op)

    async def history(self,target: str,limit: int) -> List[str]:
        def op():
            with self._connect() as db: return [r[0] for r in db.execute("SELECT normalized_text FROM content_history WHERE target_stream_id=? ORDER BY sent_at DESC LIMIT ?",(target,limit)).fetchall()]
        return await self._run(op)

    async def cooling_remaining(self, target: str, seconds: int) -> float:
        def op():
            with self._connect() as db:
                row = db.execute(
                    "SELECT last_success_at FROM cooldowns WHERE target_stream_id=?",
                    (target,),
                ).fetchone()
                if not row:
                    return 0.0
                return max(0.0, seconds - (time.time() - float(row[0])))
        return await self._run(op)

    async def defer_delivery(self, delivery_id: int, delay: float) -> None:
        def op():
            with self._connect() as db:
                db.execute(
                    "UPDATE deliveries SET status='retry',next_retry_at=?,sending_started_at=NULL "
                    "WHERE id=? AND status='sending'",
                    (time.time() + max(1.0, delay), delivery_id),
                )
        await self._run(op)

    async def complete_delivery(self, delivery_id: int, state: str, payload: PendingMessage, fingerprint: str, summary: str="") -> None:
        def op():
            now=time.time()
            with self._connect() as db:
                db.execute("UPDATE deliveries SET status=?,completed_at=?,last_error=NULL,sending_started_at=NULL WHERE id=?",(state,now,delivery_id))
                if state == "sent":
                    target_row=db.execute("SELECT target_stream_id,target_group_id FROM deliveries WHERE id=?",(delivery_id,)).fetchone()
                    target=target_row[0] or f"group:{target_row[1]}"
                    db.execute("INSERT OR IGNORE INTO content_history VALUES(?,?,?,?,?,?)",(target,fingerprint,normalize_content_text(payload.plain_text),now,payload.group_id,summary))
                    db.execute("INSERT INTO cooldowns VALUES(?,?) ON CONFLICT(target_stream_id) DO UPDATE SET last_success_at=excluded.last_success_at",(target,now))
                db.execute("UPDATE jobs SET status='completed' WHERE id=(SELECT job_id FROM deliveries WHERE id=?) AND NOT EXISTS(SELECT 1 FROM deliveries WHERE job_id=(SELECT job_id FROM deliveries WHERE id=?) AND status IN ('pending','retry'))",(delivery_id,delivery_id))
        await self._run(op)

    async def retry_delivery(self,delivery_id: int,reason: str,delay: int,maximum: int) -> None:
        def op():
            with self._connect() as db:
                db.execute("UPDATE deliveries SET retry_count=retry_count+1,status=CASE WHEN retry_count+1>=? THEN 'failed' ELSE 'retry' END,next_retry_at=?,last_error=?,sending_started_at=NULL WHERE id=?",(maximum,time.time()+delay,reason[:500],delivery_id))
                db.execute("UPDATE jobs SET status='completed' WHERE id=(SELECT job_id FROM deliveries WHERE id=?) AND NOT EXISTS(SELECT 1 FROM deliveries WHERE job_id=(SELECT job_id FROM deliveries WHERE id=?) AND status IN ('pending','retry'))",(delivery_id,delivery_id))
        await self._run(op)


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
            return EvaluationResult(
                True,
                True,
                "strategy=always",
                MessageEvaluator._minimal_analysis("allow", "strategy=always"),
            )

        if strategy == "keyword_llm":
            return await MessageEvaluator._evaluate_keyword_llm(message, case_sensitive, plugin)

        if not keywords:
            return EvaluationResult(False, True, "keyword list is empty")

        haystack = message.plain_text or message.raw_message
        if not case_sensitive:
            haystack = haystack.lower()
            keywords = [keyword.lower() for keyword in keywords]

        if strategy == "keyword_any":
            allowed = any(keyword in haystack for keyword in keywords)
            reason = "strategy=keyword_any"
            return EvaluationResult(
                allowed,
                True,
                reason,
                MessageEvaluator._minimal_analysis("allow" if allowed else "reject", reason),
            )
        if strategy == "keyword_all":
            allowed = all(keyword in haystack for keyword in keywords)
            reason = "strategy=keyword_all"
            return EvaluationResult(
                allowed,
                True,
                reason,
                MessageEvaluator._minimal_analysis("allow" if allowed else "reject", reason),
            )
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

        # Safety denylist is authoritative even if a pass keyword also matches.
        matched_reject = [
            keyword for keyword in reject_keywords
            if (keyword if case_sensitive else keyword.lower()) in normalized_text
        ]
        if matched_reject:
            reason = f"matched reject keywords: {', '.join(matched_reject[:5])}"
            return EvaluationResult(False, True, reason, MessageEvaluator._minimal_analysis("reject", reason))

        if pass_keywords:
            matched_pass = [
                keyword for keyword in pass_keywords
                if (keyword if case_sensitive else keyword.lower()) in normalized_text
            ]
            if matched_pass:
                reason = f"matched pass keywords: {', '.join(matched_pass[:5])}"
                return EvaluationResult(True, True, reason, MessageEvaluator._minimal_analysis("allow", reason))

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
            return EvaluationResult(True, True, f"keyword_llm allow via {model_name}: {reason}", parsed)
        if decision == "reject":
            return EvaluationResult(False, True, f"keyword_llm reject via {model_name}: {reason}", parsed)
        return EvaluationResult(False, False, f"llm review returned invalid decision: {decision}")

    @staticmethod
    def _minimal_analysis(decision: str, reason: str) -> Dict[str, Any]:
        return {
            "decision": decision,
            "reason": reason,
            "summary": "",
            "joke_points": [],
            "context_dependency": "unknown",
            "content_tags": [],
            "risk_tags": [],
        }

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
        if not bool(plugin.get_config("media.describe_picid", True)):
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
            if not isinstance(parsed, dict):
                return None
        except json.JSONDecodeError:
            decoder = json.JSONDecoder()
            parsed = None
            for match in re.finditer(r"\{", response):
                try:
                    candidate, _ = decoder.raw_decode(response[match.start():])
                except json.JSONDecodeError:
                    continue
                if isinstance(candidate, dict):
                    parsed = candidate
                    break
            if parsed is None:
                return None
        # Keep the complete analysis contract even when a model omits optional
        # fields, so keyword and LLM decisions have one downstream shape.
        parsed["decision"] = str(parsed.get("decision", "")).strip().lower()
        parsed["reason"] = str(parsed.get("reason", "")).strip()
        parsed.setdefault("summary", "")
        parsed.setdefault("joke_points", [])
        parsed.setdefault("context_dependency", "unknown")
        parsed.setdefault("content_tags", [])
        parsed.setdefault("risk_tags", [])
        for key in ("joke_points", "content_tags", "risk_tags"):
            if not isinstance(parsed[key], list):
                parsed[key] = [str(parsed[key])] if parsed[key] else []
        dependency = parsed.get("context_dependency", "unknown")
        if isinstance(dependency, bool):
            dependency = "high" if dependency else "none"
        dependency = str(dependency).strip().lower()
        parsed["context_dependency"] = dependency if dependency in {"none", "low", "high", "unknown"} else "unknown"
        return parsed


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
    _shared_background_task: Optional[asyncio.Task] = None
    _shared_worker_owner: Optional["SowingForwardHandler"] = None
    _shared_worker_guard: Optional[asyncio.Lock] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = RuntimeState()
        self.process_lock = asyncio.Lock()
        self.napcat = NapCatHttpClient(self)
        self.background_task: Optional[asyncio.Task] = None
        self.llm_semaphore = asyncio.Semaphore(2)
        self.delivery_semaphore = asyncio.Semaphore(4)
        self.target_locks: Dict[str, asyncio.Lock] = {}

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

        cleaned = await self.state.cleanup(
            int(self.get_config("cache.max_age_seconds", 3600)),
            int(self.get_config("dedup.record_retention_days", 30)),
        )
        if cleaned > 0:
            logger.info(f"[{PLUGIN_NAME}] cleaned {cleaned} expired pending messages")

        await self._ensure_background_worker()

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
                await self.state.add_pending(
                    pending,
                    int(self.get_config("cache.wait_seconds", 600)),
                )
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
        """识别消息的内容类型。

        关键约束:在 MaiBot-Napcat-Adapter 下,合并转发会被预渲染成
        ``Seg(type="seglist", data=[<text "===转发消息开始==">, ..., <text "===转发消息结束==">])``,
        ``type="forward"`` 段被消化掉,无法直接判定。所以用 plain_text 里
        ``========== 转发消息开始 ==========`` 标记 + 不是``[回复<...]``评论模式来识别。

        引用合并消息的普通评论(plain_text 形如 ``[回复<...转发消息开始...转发消息结束...]，说：xxx``)
        不应该被认作 forward,会进入搬运队列。
        """
        collected: List[str] = []

        def visit(seg: Any) -> None:
            if seg is None:
                return
            seg_type = str(getattr(seg, "type", "") or "").lower()
            seg_data = getattr(seg, "data", None)

            if seg_type == "forward":
                # 协议层就是 forward 段;少见但留着以防 adapter 改变行为
                collected.append("forward")
                return
            if seg_type == "seglist":
                # 内嵌 seglist 不直接当 forward,递归看具体段
                if isinstance(seg_data, list):
                    for child in seg_data:
                        visit(child)
                return
            if seg_type == "reply":
                # 引用段,不算正文类型,直接跳过
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
            plain_text_init = str(getattr(message, "plain_text", "") or "").strip()
            raw_message_init = str(getattr(message, "raw_message", "") or "").strip()
            if plain_text_init or raw_message_init:
                collected.append("text")

        # MaiBot-Napcat-Adapter 把合并转发展开成 seglist + 文本头尾,看 marker 反推。
        # 命中时整条消息就是一个合并转发包,内部 text/image 是包内内容而非消息本身的
        # 多媒体类型——必须 collapse 成 ["forward"],否则 _is_allowed_message 的
        # protected_types 检查会因内部 image 把整个合并转发拒掉。
        plain_text_for_check = str(getattr(message, "plain_text", "") or "")
        raw_text_for_check = str(getattr(message, "raw_message", "") or "")
        is_quote_comment = self._is_comment_on_forward_message(plain_text_for_check)
        napcat_forward_marker = "转发消息开始"
        # NapCat normally exposes a seglist plus marker.  Do not require a
        # top-level ``forward`` segment: adapters consume it before plugins run.
        if (napcat_forward_marker in plain_text_for_check or napcat_forward_marker in raw_text_for_check) and not is_quote_comment:
            return ["forward"]

        # 二重护栏:adapter 哪天保留了真 forward 段,但 plain_text 又是评论模式 -> 降级
        if "forward" in collected and is_quote_comment:
            collected = [t for t in collected if t != "forward"]
            if not collected:
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

            if seg_type == "text" and isinstance(seg_data, str):
                if not seg_data.strip():
                    return
                segments.append(("text", seg_data))
                return

            if seg_type in {"image", "emoji"}:
                pic_match = MessageEvaluator.PIC_ID_PATTERN.search(str(seg_data or ""))
                if pic_match and bool(self.get_config("media.describe_picid", True)):
                    content = MessageEvaluator._expand_picid_descriptions(pic_match.group(0), self)
                    segments.append((seg_type, content or f"[{seg_type}]"))
                else:
                    segments.append((seg_type, f"[{seg_type}]"))
                return
            if seg_type in {"video", "file", "voice", "audio"}:
                metadata: Dict[str, Any] = {}
                if isinstance(seg_data, dict):
                    for key in ("name", "file", "file_id", "size", "duration", "mime", "type"):
                        value = seg_data.get(key)
                        if value not in (None, ""):
                            metadata[key] = str(value)[:160]
                segments.append((seg_type, json.dumps(metadata, ensure_ascii=False, sort_keys=True) if metadata else f"[{seg_type}]"))
                return

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

    def _group_id_for_stream(self, stream_id: str, platform: str) -> str:
        for stream in chat_api.get_group_streams(platform):
            stream_info = chat_api.get_stream_info(stream)
            if str(stream_info.get("stream_id", "")) == str(stream_id):
                return str(stream_info.get("group_id", "") or "")
        return ""

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

    async def _ensure_background_worker(self) -> None:
        if not bool(self.get_config("cache.process_in_background", True)):
            return
        if SowingForwardHandler._shared_worker_guard is None:
            SowingForwardHandler._shared_worker_guard = asyncio.Lock()
        async with SowingForwardHandler._shared_worker_guard:
            task = SowingForwardHandler._shared_background_task
            if task and not task.done():
                self.background_task = task
                return
            SowingForwardHandler._shared_worker_owner = self
            task = asyncio.create_task(self._background_process_loop())
            SowingForwardHandler._shared_background_task = task
            self.background_task = task

    async def _background_process_loop(self) -> None:
        poll_seconds = max(1, int(self.get_config("cache.poll_interval_seconds", 30)))
        logger.info(f"[{PLUGIN_NAME}] background worker started, poll_interval={poll_seconds}s")
        try:
            while True:
                try:
                    cleaned = await self.state.cleanup(
                        int(self.get_config("cache.max_age_seconds", 3600)),
                        int(self.get_config("dedup.record_retention_days", 30)),
                    )
                    if cleaned > 0:
                        logger.info(f"[{PLUGIN_NAME}] cleaned {cleaned} expired pending messages")
                    await self._process_pending()
                except Exception as exc:
                    logger.error(f"[{PLUGIN_NAME}] background worker iteration failed: {exc}")
                await asyncio.sleep(poll_seconds)
        except asyncio.CancelledError:
            logger.info(f"[{PLUGIN_NAME}] background worker stopped")
            raise
        finally:
            current = asyncio.current_task()
            if SowingForwardHandler._shared_background_task is current:
                SowingForwardHandler._shared_background_task = None
                SowingForwardHandler._shared_worker_owner = None

    async def _process_pending(self) -> None:
        if self.process_lock.locked():
            return
        async with self.process_lock:
            batch_size = int(self.get_config("processing.batch_size", 20))
            self.llm_semaphore = asyncio.Semaphore(
                max(1, int(self.get_config("processing.moderation_concurrency", 2)))
            )
            self.delivery_semaphore = asyncio.Semaphore(
                max(1, int(self.get_config("processing.delivery_concurrency", 4)))
            )
            matured = await self.state.get_matured(batch_size)
            strategy = str(self.get_config("evaluation.strategy", "keyword_llm"))
            keywords = self._normalize_list(self.get_config("evaluation.keywords", []))
            case_sensitive = bool(self.get_config("evaluation.case_sensitive", False))
            maximum = int(self.get_config("processing.max_retries", 3))
            delay = int(self.get_config("processing.retry_delay_seconds", 60))
            async def evaluate_one(pending: PendingMessage) -> Tuple[PendingMessage, EvaluationResult]:
                async with self.llm_semaphore:
                    result = await MessageEvaluator.evaluate(
                        strategy,
                        pending,
                        keywords,
                        case_sensitive,
                        self,
                    )
                return pending, result

            evaluation_results = await asyncio.gather(
                *(evaluate_one(pending) for pending in matured[:batch_size])
            )
            for pending, evaluation in evaluation_results:
                if not evaluation.is_final:
                    await self.state.retry_job(pending, evaluation.reason, delay, maximum)
                    continue
                if not evaluation.should_forward:
                    await self.state.finalize_evaluation(pending, evaluation, [])
                    logger.info(f"[{PLUGIN_NAME}] rejected {pending.message_id}: {evaluation.reason}")
                    continue
                target_pairs: List[Tuple[str, str]] = []
                configured_targets = self._resolve_target_group_ids()
                if configured_targets:
                    for group_id in configured_targets:
                        if str(group_id) == pending.group_id:
                            continue
                        stream_id = ""
                        stream = chat_api.get_stream_by_group_id(group_id, pending.platform)
                        if stream:
                            stream_id = str(chat_api.get_stream_info(stream).get("stream_id", "") or "")
                        target_pairs.append((stream_id, str(group_id)))
                else:
                    for stream_id in self._exclude_source_stream(
                        self._resolve_target_stream_ids(pending.platform),
                        pending.platform,
                        pending.group_id,
                    ):
                        group_id = self._group_id_for_stream(stream_id, pending.platform)
                        if group_id:
                            target_pairs.append((stream_id, group_id))
                if not target_pairs:
                    await self.state.retry_job(pending, "no target groups resolved", delay, maximum)
                    continue
                await self.state.finalize_evaluation(pending, evaluation, target_pairs)

            deliveries = await self.state.due_deliveries(batch_size)
            await asyncio.gather(*(self._deliver_one(delivery) for delivery in deliveries))

    async def _target_recent_texts(self, target_stream: str) -> List[str]:
        if not target_stream or not bool(self.get_config("dedup.check_recent_messages", True)):
            return []
        try:
            def load_recent_messages():
                return message_api.get_recent_messages(
                    chat_id=target_stream,
                    hours=float(self.get_config("dedup.history_hours", 72)),
                    limit=int(self.get_config("dedup.history_limit", 20)),
                    limit_mode="latest",
                    filter_mai=False,
                )

            messages = await asyncio.to_thread(load_recent_messages)
        except Exception as exc:
            logger.warning(f"[{PLUGIN_NAME}] failed to query recent target messages: {exc}")
            return []
        return [
            str(getattr(item, "display_message", "") or getattr(item, "processed_plain_text", "") or "")
            for item in messages or []
        ]

    async def _deliver_one(self, delivery: Dict[str, Any]) -> None:
        async with self.delivery_semaphore:
            if not await self.state.claim_delivery(int(delivery["id"])):
                return
            target_key = str(delivery["target_group_id"] or delivery["target_stream_id"])
            target_lock = self.target_locks.setdefault(target_key, asyncio.Lock())
            async with target_lock:
                await self._deliver_one_locked(delivery)

    async def _deliver_one_locked(self, delivery: Dict[str, Any]) -> None:
        pending = PendingMessage.from_dict(json.loads(delivery["payload_json"]))
        target_stream = str(delivery["target_stream_id"])
        fingerprint = str(delivery["fingerprint"])
        dedup_target = target_stream or f"group:{delivery['target_group_id']}"
        if await self.state.duplicate(dedup_target, fingerprint):
            await self.state.complete_delivery(delivery["id"], "skipped_duplicate", pending, fingerprint)
            return
        threshold = float(self.get_config("dedup.similarity_threshold", 0.92))
        history = await self.state.history(
            dedup_target,
            int(self.get_config("dedup.history_limit", 20)),
        )
        history.extend(await self._target_recent_texts(target_stream))
        if any(content_similarity(pending.plain_text, item) >= threshold for item in history):
            await self.state.complete_delivery(delivery["id"], "skipped_duplicate", pending, fingerprint)
            return
        cooldown_target = target_stream or f"group:{delivery['target_group_id']}"
        cooldown_remaining = await self.state.cooling_remaining(
            cooldown_target,
            self._get_dynamic_cooldown(),
        )
        if cooldown_remaining > 0:
            await self.state.defer_delivery(delivery["id"], cooldown_remaining)
            return
        success = False
        if bool(self.get_config("napcat_http.use_direct_forward", True)) and self.napcat.is_enabled():
            try:
                success = await self.napcat.forward_group_single_msg(delivery["target_group_id"], pending.message_id)
            except Exception as exc:
                logger.warning(f"[{PLUGIN_NAME}] NapCat target {delivery['target_group_id']} failed: {exc}")
        if not success and target_stream:
            try:
                success = await self.send_forward(stream_id=target_stream, messages_list=[pending.message_id])
            except Exception as exc:
                logger.warning(f"[{PLUGIN_NAME}] MaiBot fallback target {target_stream} failed: {exc}")
        if not success:
            reason = "both send paths failed" if target_stream else "NapCat failed and target stream is unavailable"
            await self.state.retry_delivery(
                delivery["id"],
                reason,
                int(self.get_config("processing.retry_delay_seconds", 60)),
                int(self.get_config("processing.max_retries", 3)),
            )
            return
        analysis = json.loads(delivery["analysis_json"] or "{}")
        summary = str(analysis.get("summary") or analysis.get("reason") or "")
        await self.state.complete_delivery(
            delivery["id"],
            "sent",
            pending,
            fingerprint,
            summary,
        )
        if not target_stream:
            logger.info(
                f"[{PLUGIN_NAME}] sent {pending.message_id} to group {delivery['target_group_id']}, "
                "but no MaiBot stream exists for context recording"
            )
            return
        if not bool(self.get_config("bot_context.enabled", True)):
            return
        try:
            target_chat = get_chat_manager().get_stream(target_stream)
            if target_chat is None:
                raise RuntimeError(f"target stream {target_stream} is unavailable")
            joke_points = [str(item) for item in analysis.get("joke_points", []) if str(item).strip()][:3]
            content_tags = [str(item) for item in analysis.get("content_tags", []) if str(item).strip()][:5]
            prompt_lines = [
                "[搬运消息记录]",
                f"你刚刚向当前群搬运了一条来自群 {pending.group_id} 的合并转发。",
                f"内容概要：{summary or '未生成概要'}",
            ]
            if joke_points:
                prompt_lines.append("可能的笑点：" + "；".join(joke_points))
            if content_tags:
                prompt_lines.append("内容标签：" + "、".join(content_tags))
            saved = await database_api.store_action_info(
                chat_stream=target_chat,
                action_build_into_prompt=True,
                action_prompt_display="\n".join(prompt_lines),
                action_name="sowing_forward",
                action_reasoning="记录插件搬运内容，供后续对话理解",
                action_data={"source_group_id": pending.group_id, "source_message_id": pending.message_id, "summary": summary,
                             "joke_points": analysis.get("joke_points", []),
                             "content_tags": analysis.get("content_tags", []),
                             "risk_tags": analysis.get("risk_tags", []), "fingerprint": fingerprint},
            )
            if not saved:
                raise RuntimeError("database_api.store_action_info returned no record")
        except Exception as exc:
            logger.warning(
                f"[{PLUGIN_NAME}] target {delivery['target_group_id']} was sent, "
                f"but its ActionRecord could not be stored: {exc}"
            )


class SowingStartupHandler(SowingForwardHandler):
    """启动时恢复 SQLite 中尚未完成的任务。"""
    event_type = EventType.ON_START
    handler_name = "sowing_startup_handler"
    handler_description = "Restore persistent sowing worker."
    intercept_message = False

    async def execute(self, message: Any) -> Tuple[bool, bool, Optional[str], None, None]:
        if self.get_config("plugin.enabled", False):
            await self._ensure_background_worker()
            logger.info(f"[{PLUGIN_NAME}] restored SQLite worker on startup")
        return True, True, None, None, None


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
        "cache": "缓存与等待窗口：收消息只解析并持久化 SQLite，worker 异步处理",
        "processing": "有界后台处理和失败重试",
        "dedup": "逐目标精确指纹与近期消息相似度去重",
        "media": "复用 MaiBot 已有媒体描述，不触发额外 VLM",
        "bot_context": "转发成功后写入 Bot 内部认知记录",
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
                default="2.0.0",
                description="SQLite jobs/deliveries 架构版本；旧 runtime_state.json 不自动迁移。",
                example="2.0.0",
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
                description="候选消息进入缓存后，需要等待多少秒才开始评估。该窗口用于让聊天上下文沉淀，避免收到后立即搬运。",
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
        "processing": {
            "batch_size": ConfigField(type=int, default=20, min=1, description="每轮最多处理的任务/投递数，避免无限 gather。", example="20"),
            "moderation_concurrency": ConfigField(type=int, default=2, min=1, description="同时进行的审核任务上限。", example="2"),
            "delivery_concurrency": ConfigField(type=int, default=4, min=1, description="同时进行的目标投递任务上限；同一目标群仍保持串行。", example="4"),
            "max_retries": ConfigField(type=int, default=3, min=1, description="LLM 或发送暂时失败的最大重试次数。", example="3"),
            "retry_delay_seconds": ConfigField(type=int, default=60, min=1, description="失败后的下一次重试延迟。", example="60"),
        },
        "dedup": {
            "check_recent_messages": ConfigField(type=bool, default=True, description="发送前比对目标群真实近期消息，避免群友已发过的内容。", example="true"),
            "history_hours": ConfigField(type=float, default=72.0, description="查询目标群近期消息的时间范围。", example="72"),
            "history_limit": ConfigField(type=int, default=20, min=1, description="每目标群用于相似度比对的近期消息条数。", example="20"),
            "record_retention_days": ConfigField(type=int, default=30, min=1, description="插件投递指纹的保留天数。", example="30"),
            "similarity_threshold": ConfigField(type=float, default=0.92, description="规范化文本相似度阈值，达到后该目标标为 skipped_duplicate。", example="0.92"),
        },
        "media": {
            "describe_picid": ConfigField(type=bool, default=True, description="仅复用已有 picid 描述；绝不调用新的 VLM，也不保存 base64/大 URL。", example="true"),
        },
        "bot_context": {
            "enabled": ConfigField(type=bool, default=True, description="成功转发后写入 ActionRecord，让 Bot 知道自己搬运的内容、来源与笑点。", example="true"),
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
                default=True,
                description="兼容旧配置名；只查询 MaiBot 已有图片描述，不会触发新的 VLM 调用。",
                example="true",
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
                    "- allow:合并转发内容自成一体，脱离 Recent Chat History 仍能理解节目效果。\n"
                    "- reject:内容强依赖原群上文，脱离上下文无法理解。\n"
                    "- reject:纯通知、纯求助、纯认真讨论、纯吵架骂战、纯客套、无上下文的普通聊天记录。\n"
                    "- reject:无法判断笑点或节目效果时一律 reject。\n\n"
                    "# Output\n"
                    "只输出一个 JSON 对象,不要输出任何额外文字:\n"
                    '{"decision":"allow|reject","reason":"不超过40字","summary":"不超过200字的内容概要","joke_points":["最多3条可能笑点"],"context_dependency":"none|low|high","content_tags":["内容标签"],"risk_tags":["风险标签"]}'
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
            (SowingStartupHandler.get_handler_info(), SowingStartupHandler),
        ]
