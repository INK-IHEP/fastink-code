import json
import time
from typing import Any


class AssistantRedisRepository:
    def __init__(self, redis_client):
        self.redis = redis_client

    @staticmethod
    def _session_key(session_id: str) -> str:
        return f"assistant:session:{session_id}"

    @staticmethod
    def _session_index_key(username: str) -> str:
        return f"assistant:user_sessions:{username}"

    @staticmethod
    def _turn_key(turn_id: str) -> str:
        return f"assistant:turn:{turn_id}"

    @staticmethod
    def _messages_key(session_id: str) -> str:
        return f"assistant:session_messages:{session_id}"

    async def create_session(self, username: str, session_id: str, title: str, now: str) -> dict[str, str]:
        session = {
            "session_id": session_id,
            "username": username,
            "title": title,
            "created_at": now,
            "updated_at": now,
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(self._session_key(session_id), mapping=session)
            pipe.zadd(self._session_index_key(username), {session_id: time.time()})
            await pipe.execute()
        return session

    async def get_session(self, session_id: str) -> dict[str, str]:
        return await self.redis.hgetall(self._session_key(session_id))

    async def list_sessions(self, username: str) -> list[dict[str, str]]:
        session_ids = await self.redis.zrevrange(self._session_index_key(username), 0, -1)
        if not session_ids:
            return []

        async with self.redis.pipeline(transaction=False) as pipe:
            for session_id in session_ids:
                pipe.hgetall(self._session_key(session_id))
            sessions = await pipe.execute()

        return [session for session in sessions if session]

    async def touch_session(self, username: str, session_id: str, now: str) -> None:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(self._session_key(session_id), mapping={"updated_at": now})
            pipe.zadd(self._session_index_key(username), {session_id: time.time()})
            await pipe.execute()

    async def append_message(
        self,
        session_id: str,
        role: str,
        content: str,
        now: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        message = {
            "message_id": f"msg_{int(time.time() * 1000)}",
            "role": role,
            "content": content,
            "created_at": now,
            "metadata": metadata or {},
        }
        await self.redis.rpush(
            self._messages_key(session_id),
            json.dumps(message, ensure_ascii=False),
        )
        return message

    async def list_messages(self, session_id: str, limit: int = 50) -> list[dict[str, Any]]:
        start = -max(1, limit)
        raw_messages = await self.redis.lrange(self._messages_key(session_id), start, -1)
        messages: list[dict[str, Any]] = []
        for raw_message in raw_messages:
            try:
                messages.append(json.loads(raw_message))
            except json.JSONDecodeError:
                continue
        return messages

    async def save_turn(self, turn: dict[str, Any]) -> None:
        await self.redis.set(
            self._turn_key(str(turn["turn_id"])),
            json.dumps(turn, ensure_ascii=False),
        )

    async def get_turn(self, turn_id: str) -> dict[str, Any] | None:
        raw_turn = await self.redis.get(self._turn_key(turn_id))
        if not raw_turn:
            return None
        return json.loads(raw_turn)
