from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from fastink.common.config import get_config
from fastink.inkdb.inkredis import redis_connect
from fastink.service.assistant.hermes_client import HermesClient
from fastink.service.assistant.repository import AssistantRedisRepository
from fastink.service.assistant.tools.job_control import cancel_user_job
from fastink.service.assistant.tools.job_query import (
    collect_user_jobs,
    extract_job_id,
    find_job_card,
    is_cancel_request,
    should_query_jobs,
)


def _now_string() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class AssistantService:
    def __init__(self) -> None:
        self.repository = AssistantRedisRepository(redis_connect())
        self.hermes_client = HermesClient()
        self.config = get_config("assistant", fallback={})

    def is_enabled(self) -> bool:
        return bool(self.config.get("enabled", False))

    async def create_session(self, username: str, title: str | None = None) -> dict[str, str]:
        now = _now_string()
        session_id = f"sess_{uuid4().hex}"
        resolved_title = (title or "").strip() or f"{username} assistant session"
        return await self.repository.create_session(username, session_id, resolved_title, now)

    async def list_sessions(self, username: str) -> list[dict[str, str]]:
        return await self.repository.list_sessions(username)

    async def list_messages(self, username: str, session_id: str) -> list[dict[str, Any]]:
        await self._get_owned_session(username, session_id)
        return await self.repository.list_messages(session_id)

    async def get_turn(self, username: str, session_id: str, turn_id: str) -> dict[str, Any]:
        await self._get_owned_session(username, session_id)
        turn = await self.repository.get_turn(turn_id)
        if not turn or str(turn.get("session_id")) != session_id:
            raise ValueError(f"Assistant turn not found: {turn_id}")
        return turn

    async def create_message(self, username: str, session_id: str, message: str) -> dict[str, Any]:
        await self._get_owned_session(username, session_id)

        now = _now_string()
        turn_id = f"turn_{uuid4().hex}"
        turn = {
            "turn_id": turn_id,
            "session_id": session_id,
            "username": username,
            "user_message": message,
            "assistant_message": "",
            "status": "processing",
            "job_cards": [],
            "tool_results": [],
            "pending_action": None,
            "action_result": None,
            "created_at": now,
            "updated_at": now,
        }

        await self.repository.append_message(session_id, "user", message, now)
        await self.repository.save_turn(turn)

        recent_messages = await self.repository.list_messages(
            session_id,
            limit=int(self.config.get("max_context_turns", 12)) * 2,
        )

        turn_result = await self._build_turn_result(
            username=username,
            message=message,
            recent_messages=recent_messages,
        )
        turn.update(turn_result)
        turn["updated_at"] = _now_string()

        await self.repository.save_turn(turn)
        await self.repository.append_message(
            session_id,
            "assistant",
            str(turn["assistant_message"]),
            turn["updated_at"],
            metadata={
                "turn_id": turn_id,
                "status": turn["status"],
                "pending_action": turn["pending_action"] or {},
            },
        )
        await self.repository.touch_session(username, session_id, turn["updated_at"])
        return {"turn_id": turn_id, "status": turn["status"]}

    async def confirm_action(
        self,
        username: str,
        session_id: str,
        turn_id: str,
        action_id: str,
        confirmed: bool,
    ) -> dict[str, Any]:
        turn = await self.get_turn(username, session_id, turn_id)
        pending_action = turn.get("pending_action") or {}
        if not pending_action:
            raise ValueError("No pending action found for this turn")
        if str(pending_action.get("action_id")) != action_id:
            raise ValueError(f"Pending action does not match: {action_id}")

        now = _now_string()
        if not confirmed:
            turn["status"] = "action_cancelled"
            turn["pending_action"] = None
            turn["action_result"] = {
                "confirmed": False,
                "message": "已取消本次操作请求，未执行任何作业控制。",
            }
            turn["assistant_message"] = str(turn["action_result"]["message"])
        else:
            action_type = str(pending_action.get("action_type", ""))
            if action_type != "cancel_job":
                raise ValueError(f"Unsupported assistant action: {action_type}")

            payload = pending_action.get("payload", {})
            action_result = await cancel_user_job(
                username,
                cluster_id=str(payload.get("cluster_id", "")),
                job_id=str(payload.get("job_id", "") or "") or None,
                submit_uuid=str(payload.get("submit_uuid", "") or "") or None,
            )
            turn["status"] = "action_completed"
            turn["pending_action"] = None
            turn["action_result"] = action_result
            turn["assistant_message"] = (
                f"已执行取消作业请求：{payload.get('cluster_id')} "
                f"{payload.get('job_id') or payload.get('submit_uuid')}。"
            )

        turn["updated_at"] = now
        await self.repository.save_turn(turn)
        await self.repository.append_message(
            session_id,
            "assistant",
            str(turn["assistant_message"]),
            now,
            metadata={
                "turn_id": turn_id,
                "status": turn["status"],
                "action_result": turn["action_result"] or {},
            },
        )
        await self.repository.touch_session(username, session_id, now)
        return turn

    async def _build_turn_result(
        self,
        *,
        username: str,
        message: str,
        recent_messages: list[dict[str, Any]],
    ) -> dict[str, Any]:
        should_fetch_jobs = should_query_jobs(message) or is_cancel_request(message)
        is_cancel = is_cancel_request(message)
        allow_job_control = bool(self.config.get("allow_job_control", False))
        job_cards: list[dict[str, Any]] = []
        tool_results: list[dict[str, Any]] = []
        pending_action: dict[str, Any] | None = None

        if should_fetch_jobs:
            job_cards, job_errors = await collect_user_jobs(username)
            tool_results.append(
                {
                    "tool": "query_jobs",
                    "status": "ok" if not job_errors else "partial",
                    "job_count": len(job_cards),
                    "errors": job_errors,
                }
            )

        if is_cancel and allow_job_control:
            pending_action = self._build_pending_cancel_action(message, job_cards)
            if pending_action is None:
                tool_results.append(
                    {
                        "tool": "cancel_job",
                        "status": "skipped",
                        "reason": "No explicit target job id found in request",
                    }
                )

        status = "awaiting_confirmation" if pending_action else "completed"
        assistant_message = ""
        if is_cancel and not allow_job_control:
            assistant_message = "当前部署未开启作业控制能力，所以这次只能查询状态，不能直接执行取消作业。"
        elif is_cancel and pending_action is None:
            assistant_message = "要发起取消作业，请在请求里明确写出 job id，例如：帮我取消 slurm 的 123456 作业。"
        else:
            reply = await self.hermes_client.generate_reply(
                username=username,
                message=message,
                recent_messages=recent_messages,
                job_cards=job_cards[:10],
                tool_results=tool_results,
                pending_action=pending_action,
            )
            assistant_message = str(reply.get("answer", "")).strip()

        if not assistant_message and pending_action is None and not job_cards:
            assistant_message = "当前没有可展示的作业结果。"

        return {
            "assistant_message": assistant_message,
            "status": status,
            "job_cards": job_cards[:10],
            "tool_results": tool_results,
            "pending_action": pending_action,
            "action_result": None,
        }

    def _build_pending_cancel_action(
        self,
        message: str,
        job_cards: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        job_id = extract_job_id(message)
        target_job = find_job_card(job_cards, job_id)
        if not target_job:
            return None

        return {
            "action_id": f"act_{uuid4().hex}",
            "action_type": "cancel_job",
            "title": f"取消作业 {target_job.get('job_id')}",
            "description": (
                f"该操作会尝试取消 {target_job.get('cluster_id')} 上的 "
                f"{target_job.get('job_type')} 作业。"
            ),
            "risk_level": "high",
            "confirm_required": bool(self.config.get("require_action_confirmation", True)),
            "payload": {
                "cluster_id": target_job.get("cluster_id"),
                "job_id": target_job.get("job_id"),
                "submit_uuid": target_job.get("submit_uuid"),
                "job_type": target_job.get("job_type"),
            },
        }

    async def _get_owned_session(self, username: str, session_id: str) -> dict[str, Any]:
        session = await self.repository.get_session(session_id)
        if not session:
            raise ValueError(f"Assistant session not found: {session_id}")
        if str(session.get("username")) != username:
            raise ValueError(f"Assistant session does not belong to user: {session_id}")
        return session
