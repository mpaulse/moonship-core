#  Copyright (c) 2023, Marlon Paulse
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#
#  1. Redistributions of source code must retain the above copyright notice, this
#     list of conditions and the following disclaimer.
#
#  2. Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
#  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
#  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
#  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import aiohttp.web
import aiohttp_session
import asyncio
import json
import logging
import os
import redis.asyncio as aioredis

from moonship.core import *
from moonship.core.ipc import *
from typing import Awaitable, Callable, Optional, Union

__all__ = [
    "RedisMessageBus",
    "RedisSessionStore",
    "RedisSharedCache"
]

redis: Optional[aioredis.Redis] = None
redis_ref_count = 0

logger = logging.getLogger(__name__)


async def init_redis(config: Config) -> aioredis.Redis:
    global redis, redis_ref_count
    if redis is None:
        url = config.get("moonship.redis.url")
        if isinstance(url, str):
            if len(url) > 1 and url.startswith("$"):
                try:
                    url = os.environ[url[1:]]
                except KeyError:
                    raise StartUpException(f"No {url[1:]} environment variable set")
        else:
            raise StartUpException("Redis URL not configured")
        options = {
            "decode_responses": True
        }
        if url.startswith("rediss://"):
            ssl_verify_cert = config.get("moonship.redis.ssl_verify_cert", default=True)
            options["ssl_cert_reqs"] = "required" if ssl_verify_cert else None
            options["ssl_check_hostname"] = ssl_verify_cert
        redis = await aioredis.from_url(url, **options)
    redis_ref_count += 1
    return redis


async def close_redis() -> None:
    global redis, redis_ref_count
    redis_ref_count -= 1
    if redis_ref_count == 0:
        await redis.close()
        await redis.connection_pool.disconnect()


# Hack to fix the "Redis.close was never awaited" error at shutdown
aioredis.Redis.__del__ = lambda *args: None


class RedisSharedCacheBulkOp(SharedCacheBulkOp):

    def __init__(self, pipeline: aioredis.client.Pipeline):
        self.pipeline = pipeline

    async def list_push_head(self, storage_key: str, element: str) -> "SharedCacheBulkOp":
        self.pipeline.lpush(storage_key, element)
        return self

    async def list_push_tail(self, storage_key: str, element: str) -> "SharedCacheBulkOp":
        self.pipeline.rpush(storage_key, element)
        return self

    async def list_pop_head(self, storage_key: str) -> "SharedCacheBulkOp":
        self.pipeline.lpop(storage_key)
        return self

    async def list_pop_tail(self, storage_key: str) -> "SharedCacheBulkOp":
        self.pipeline.rpop(storage_key)
        return self

    async def list_remove(self, storage_key: str, element: str) -> "SharedCacheBulkOp":
        self.pipeline.lrem(storage_key, 0, element)
        return self

    def set_add(self, storage_key: str, element: str) -> SharedCacheBulkOp:
        self.pipeline.sadd(storage_key, element)
        return self

    def set_remove(self, storage_key: str, element: str) -> SharedCacheBulkOp:
        self.pipeline.srem(storage_key, element)
        return self

    def map_put(self, storage_key: str, entries: dict[str, str]) -> SharedCacheBulkOp:
        self.pipeline.hset(storage_key, mapping=entries)
        return self

    def delete(self, storage_key: str) -> SharedCacheBulkOp:
        self.pipeline.delete(storage_key)
        return self

    def expire(self, storage_key: str, time_msec: int) -> SharedCacheBulkOp:
        self.pipeline.pexpire(storage_key, time_msec)
        return self

    async def execute(self) -> None:
        await self.pipeline.execute()


class RedisSharedCache(SharedCache):

    def __init__(self, config: Config) -> None:
        super().__init__(config)

    async def open(self) -> None:
        await init_redis(self.config)

    async def close(self) -> None:
        await close_redis()

    async def list_push_head(self, storage_key: str, element: str) -> None:
        await redis.lpush(storage_key, element)

    async def list_push_tail(self, storage_key: str, element: str) -> None:
        await redis.rpush(storage_key, element)

    async def list_pop_head(self, storage_key: str) -> str:
        return await redis.lpop(storage_key)

    async def list_pop_tail(self, storage_key: str) -> str:
        return await redis.rpop(storage_key)

    async def list_remove(self, storage_key: str, element: str) -> None:
        await redis.lrem(storage_key, 0, element)

    async def list_get_head(self, storage_key: str) -> str:
        return await redis.lindex(storage_key, 0)

    async def list_get_tail(self, storage_key: str) -> str:
        return await redis.lindex(storage_key, -1)

    async def list_get_elements(self, storage_key: str) -> list[str]:
        return await redis.lrange(storage_key, 0, -1)

    async def set_add(self, storage_key: str, element: str) -> None:
        await redis.sadd(storage_key, element)

    async def set_remove(self, storage_key: str, element: str) -> None:
        await redis.srem(storage_key, element)

    async def set_get_elements(self, storage_key: str) -> set[str]:
        return await redis.smembers(storage_key)

    async def map_put(self, storage_key: str, entries: dict[str, str], append=True) -> None:
        if not append:
            await self.start_bulk() \
                .delete(storage_key) \
                .map_put(storage_key, entries) \
                .execute()
        else:
            await redis.hset(storage_key, mapping=entries)

    async def map_get(self, storage_key: str, key: str) -> str:
        return await redis.hget(storage_key, key)

    async def map_get_entries(self, storage_key: str) -> dict[str, str]:
        return await redis.hgetall(storage_key)

    async def delete(self, storage_key: str) -> None:
        await redis.delete(storage_key)

    async def expire(self, storage_key: str, time_msec: int) -> None:
        await redis.pexpire(storage_key, time_msec)

    def start_bulk(self, transaction=True) -> SharedCacheBulkOp:
        return RedisSharedCacheBulkOp(redis.pipeline(transaction))


class RedisMessageBus(MessageBus):

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._channel_handlers: dict[str, list[Callable[[dict, str], Awaitable[None]]]] = {}
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._listen_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        await init_redis(self.config)
        self._pubsub = redis.pubsub(ignore_subscribe_messages=True)
        await self._pubsub.connect()
        self._listen_task = asyncio.create_task(self._listen())

    async def close(self) -> None:
        for channel in list(self._channel_handlers.keys()):
            await self.unsubscribe(channel)
        await self._pubsub.close()
        if self._listen_task is not None:
            self._listen_task.cancel()
        await close_redis()

    async def subscribe(self, channel: str, handler: Callable[[dict, str], Awaitable[None]]) -> None:
        handlers = self._channel_handlers.get(channel)
        if handlers is None:
            await self._pubsub.subscribe(channel)
            handlers = []
            self._channel_handlers[channel] = handlers
        if handler not in handlers:
            handlers.append(handler)

    async def unsubscribe(self, channel: str, handler: Callable[[dict, str], Awaitable[None]] = None) -> None:
        if handler is None:
            del self._channel_handlers[channel]
        else:
            handlers = self._channel_handlers.get(channel)
            if handlers is not None:
                handlers.remove(handler)
                if len(handlers) == 0:
                    del self._channel_handlers[channel]
        if channel not in self._channel_handlers:
            await self._pubsub.unsubscribe(channel)

    async def publish(self, msg: dict, channel: str) -> None:
        msg_str = json.dumps(msg, separators=(",", ":"))
        await redis.publish(channel, msg_str)
        logger.debug(f"Message published: [{channel}] {msg_str}")

    async def _listen(self) -> None:
        try:
            while True:
                msg = await self._pubsub.get_message(timeout=None)
                if msg is None:
                    continue
                logger.debug(f"Message received: [{msg['channel']}] {msg['data']}")
                handlers = self._channel_handlers.get(msg["channel"])
                if handlers is not None:
                    try:
                        msg_data = json.loads(msg["data"])
                        for handler in handlers:
                            await handler(msg_data, msg["channel"])
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.exception(
                            f"Error handling message received on channel {msg['channel']}",
                            exc_info=e)
        except asyncio.CancelledError:
            pass


class RedisSessionStore(aiohttp_session.AbstractStorage):

    def __init__(self, config: Config, idle_session_expiry_msec: int = None):
        super().__init__(
            cookie_name="__Host-session_token",
            domain=None,
            max_age=None,
            httponly=True,
            path="/",
            secure=True,
            samesite="Strict")
        self.idle_session_expiry_msec = idle_session_expiry_msec
        self.shared_cache = RedisSharedCache(config)

    async def open(self) -> None:
        await self.shared_cache.open()

    async def close(self) -> None:
        await self.shared_cache.close()

    def load_cookie(self, request: aiohttp.web.Request) -> str:
        auth_header = request.headers.get("Authorization")
        if auth_header is not None:
            s = auth_header.split()
            if len(s) == 2 and s[0] == "Bearer":
                return s[1]
        return super().load_cookie(request)

    async def load_session(self, request: aiohttp.web.Request):
        session_id = self.load_cookie(request)
        if session_id is None:
            return aiohttp_session.Session(None, data=None, new=True, max_age=None)
        else:
            storage_key = self._storage_key(session_id)
            session_save_data = await self.shared_cache.map_get_entries(storage_key)
            if session_save_data is None or len(session_save_data) == 0:
                return aiohttp_session.Session(None, data=None, new=True, max_age=None)
            if self.idle_session_expiry_msec is not None:
                await self.shared_cache.expire(storage_key, self.idle_session_expiry_msec)
            session_data = {
                "created": int(session_save_data["created"]),
                "session": session_save_data
            }
            del session_save_data["created"]
            return aiohttp_session.Session(session_id, data=session_data, new=False, max_age=None)

    async def save_session(
            self,
            request: aiohttp.web.Request,
            response: aiohttp.web.Response,
            session: aiohttp_session.Session):
        storage_key = self._storage_key(session)
        if session.empty:
            self.save_cookie(response, None)
            await self.shared_cache.delete(storage_key)
        else:
            self.save_cookie(response, session.identity, max_age=None)
            session_data = self._get_session_data(session)
            session_save_data = session_data["session"]
            session_save_data["created"] = session_data["created"]
            b = self.shared_cache.start_bulk() \
                .delete(storage_key) \
                .map_put(storage_key, session_save_data)
            if self.idle_session_expiry_msec is not None:
                b.expire(storage_key, self.idle_session_expiry_msec)
            await b.execute()

    def _storage_key(self, session: Union[str, aiohttp_session.Session]) -> str:
        session_id = session if isinstance(session, str) else session.identity
        return f"moonship:session:{session_id}"
