from __future__ import annotations

import json
from typing import Any

import httpx

from fastink.common.config import get_config
from fastink.common.logger import logger


class HermesClient:
    def __init__(self) -> None:
        self.config = get_config("assistant", fallback={})

    def is_enabled(self) -> bool:
        return bool(self.config.get("enabled", False))

    async def generate_reply(
        self,
        *,
        username: str,
        message: str,
        recent_messages: list[dict[str, Any]],
        job_cards: list[dict[str, Any]],
        tool_results: list[dict[str, Any]],
        pending_action: dict[str, Any] | None,
    ) -> dict[str, str]:
        hermes_url = str(self.config.get("hermes_url", "") or "").strip()
        payload = {
            "model": str(self.config.get("hermes_model_name", "hermes-agent")),
            "messages": self._build_messages(
                username=username,
                recent_messages=recent_messages,
                job_cards=job_cards,
                tool_results=tool_results,
                pending_action=pending_action,
            ),
            "stream": False,
        }

        if hermes_url:
            try:
                timeout = float(self.config.get("request_timeout", 15))
                headers = {"Content-Type": "application/json"}
                hermes_api_key = str(self.config.get("hermes_api_key", "") or "").strip()
                if hermes_api_key:
                    headers["Authorization"] = f"Bearer {hermes_api_key}"
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(hermes_url, json=payload, headers=headers)
                    response.raise_for_status()
                data = response.json()
                content = self._extract_chat_completion_content(data)
                if content:
                    return {"answer": content}
            except Exception as exc:
                logger.warning("assistant.hermes request failed, fallback to local reply: %s", exc)

        return {"answer": self._build_local_reply(message, job_cards, pending_action, tool_results)}

    @staticmethod
    def _build_messages(
        *,
        username: str,
        recent_messages: list[dict[str, Any]],
        job_cards: list[dict[str, Any]],
        tool_results: list[dict[str, Any]],
        pending_action: dict[str, Any] | None,
    ) -> list[dict[str, str]]:
        system_prompt = (
            "You are the FastINK assistant for backend users. "
            "Use the provided FastINK context as the source of truth. "
            "Do not invent job states. "
            "Do not execute or suggest side effects beyond the explicitly provided pending action. "
            "Summarize clearly for developers using concise Chinese by default."
        )
        context_lines = [
            f"username: {username}",
            "job_cards:",
            json.dumps(job_cards[:10], ensure_ascii=False),
            "tool_results:",
            json.dumps(tool_results, ensure_ascii=False),
            "pending_action:",
            json.dumps(pending_action or {}, ensure_ascii=False),
        ]

        messages: list[dict[str, str]] = [
            {"role": "system", "content": system_prompt},
            {"role": "system", "content": "\n".join(context_lines)},
        ]

        for message in recent_messages[-12:]:
            role = str(message.get("role", "user"))
            if role not in {"system", "user", "assistant"}:
                continue
            content = str(message.get("content", "")).strip()
            if not content:
                continue
            messages.append({"role": role, "content": content})

        return messages

    @staticmethod
    def _extract_chat_completion_content(response_json: dict[str, Any]) -> str:
        choices = response_json.get("choices")
        if not isinstance(choices, list) or not choices:
            return ""

        message = choices[0].get("message", {})
        if not isinstance(message, dict):
            return ""

        content = message.get("content", "")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") in {"text", "output_text"}:
                    text = str(item.get("text", "")).strip()
                    if text:
                        parts.append(text)
            return "\n".join(parts).strip()
        return ""

    @staticmethod
    def _build_local_reply(
        message: str,
        job_cards: list[dict[str, Any]],
        pending_action: dict[str, Any] | None,
        tool_results: list[dict[str, Any]],
    ) -> str:
        if pending_action:
            payload = pending_action.get("payload", {})
            return (
                f"已识别到作业控制请求。目标作业为 `{payload.get('job_id', '')}` "
                f"（{payload.get('cluster_id', '')}）。请先确认后再执行。"
            )

        if job_cards:
            running_jobs = [job for job in job_cards if job.get("job_status") == "RUNNING"]
            queueing_jobs = [job for job in job_cards if job.get("job_status") == "QUEUEING"]
            lines = [
                f"当前共整理出 {len(job_cards)} 个作业，运行中 {len(running_jobs)} 个，排队中 {len(queueing_jobs)} 个。"
            ]
            for job in job_cards[:5]:
                lines.append(
                    f"- {job.get('cluster_id')} #{job.get('job_id') or job.get('submit_uuid')}: "
                    f"{job.get('job_type')} / {job.get('job_status')}"
                )
            if len(job_cards) > 5:
                lines.append("其余作业已放在下方结构化卡片中。")
            return "\n".join(lines)

        if tool_results:
            return "本次请求已执行工具查询，但没有拿到可展示的作业结果。"

        return (
            "当前 demo 助手主要支持查询作业状态和发起取消作业确认。"
            "你可以直接问“我现在有哪些作业在跑”或“帮我取消 slurm 的 123456 作业”。"
        )
