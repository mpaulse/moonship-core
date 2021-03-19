#  Copyright (c) 2021, Marlon Paulse
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
import aioredis
import asyncio
import json
import os
import ssl

from moonship.core import *
from moonship.core.ipc import *
from typing import Callable, Coroutine, Optional, Union

__all__ = [
    "RedisMessageBus",
    "RedisSessionStore",
    "RedisSharedCache"
]

STORAGE_KEY_PREFIX = "moonship."

redis: Optional[aioredis.Redis] = None
redis_ref_count = 0


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
        ssl_context = None
        ssl_enabled = config.get("moonship.redis.ssl_enabled", default=True)
        if isinstance(ssl_enabled, bool) and ssl_enabled:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            verify_cert = config.get("moonship.redis.ssl_verify_cert", default=True)
            if isinstance(verify_cert, bool) and verify_cert:
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                ssl_context.check_hostname = True
        redis = await aioredis.create_redis_pool(url, ssl=ssl_context)
    redis_ref_count += 1
    return redis


def close_redis() -> None:
    global redis, redis_ref_count
    redis_ref_count -= 1
    if redis_ref_count == 0:
        redis.close()


class RedisSharedCacheBulkOp(SharedCacheBulkOp):

    def __init__(self, pipeline: aioredis.commands.Pipeline):
        self.pipeline = pipeline

    def set_add(self, storage_key: str, element: str) -> None:
        self.pipeline.sadd(f"{STORAGE_KEY_PREFIX}{storage_key}", element)

    def set_remove(self, storage_key: str, element: str) -> None:
        self.pipeline.srem(f"{STORAGE_KEY_PREFIX}{storage_key}", element)

    def map_set(self, storage_key: str, mappings: dict[str, str]) -> None:
        self.pipeline.hmset_dict(f"{STORAGE_KEY_PREFIX}{storage_key}", mappings)

    def delete(self, storage_key: str) -> None:
        self.pipeline.delete(f"{STORAGE_KEY_PREFIX}{storage_key}")

    def expire(self, storage_key: str, time_msec: int) -> None:
        self.pipeline.pexpire(f"{STORAGE_KEY_PREFIX}{storage_key}", time_msec)

    async def execute(self) -> None:
        await self.pipeline.execute()


class RedisSharedCache(SharedCache):

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        init_redis(config)

    async def close(self) -> None:
        close_redis()

    async def set_add(self, storage_key: str, element: str) -> None:
        await redis.sadd(f"{STORAGE_KEY_PREFIX}{storage_key}", element)

    async def set_remove(self, storage_key: str, element: str) -> None:
        await redis.srem(f"{STORAGE_KEY_PREFIX}{storage_key}", element)

    async def set_entries(self, storage_key: str) -> set[str]:
        return await redis.smembers(f"{STORAGE_KEY_PREFIX}{storage_key}")

    async def map_set(self, storage_key: str, mappings: dict[str, str], append=True) -> None:
        storage_key = f"{STORAGE_KEY_PREFIX}{storage_key}"
        if not append:
            b = self.start_bulk()
            b.delete(storage_key)
            b.map_set(storage_key, mappings)
            await b.execute()
        else:
            await redis.hmset_dict(storage_key, mappings)

    async def map_get(self, storage_key: str, key: str) -> str:
        return redis.hget(f"{STORAGE_KEY_PREFIX}{storage_key}", key)

    async def map_entries(self, storage_key: str) -> dict[str, str]:
        return redis.hgetall(f"{STORAGE_KEY_PREFIX}{storage_key}")

    async def delete(self, storage_key: str) -> None:
        await redis.delete(f"{STORAGE_KEY_PREFIX}{storage_key}")

    async def expire(self, storage_key: str, time_msec: int) -> None:
        await redis.pexpire(f"{STORAGE_KEY_PREFIX}{storage_key}", time_msec)

    def start_bulk(self, transaction=True) -> SharedCacheBulkOp:
        return RedisSharedCacheBulkOp(redis.multi_exec() if transaction else redis.pipeline())


class RedisMessageBus(MessageBus):

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        init_redis(config)

    async def close(self) -> None:
        close_redis()

    async def subscribe(self, channel_name: str, msg_handler: Callable[[any, str], Coroutine[None]]) -> None:
        channel = (await redis.subscribe(channel_name))[0]
        asyncio.create_task(self._read_channel(channel, msg_handler))

    async def _read_channel(
            self,
            channel: aioredis.Channel,
            msg_handler: Callable[[any, str], Coroutine[None]]) -> None:
        async for msg in channel.iter(encoding="utf-8", decoder=json.loads):
            await msg_handler(msg, channel.name)

    async def unsubscribe(self, channel_name: str) -> None:
        await redis.unsubscribe(channel_name)

    async def publish(self, msg: any, channel_name: str) -> None:
        await redis.publish_json(channel_name, msg)


class RedisSessionStore(aiohttp_session.AbstractStorage):

    def __init__(self, config: Config):
        super().__init__(
            cookie_name="__Host-session_token",
            domain=None,
            max_age=None,
            httponly=True,
            path="/",
            secure=True)
        self.cookie_params["samesite"] = "Strict"
        self.shared_cache = RedisSharedCache(config)

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
            return aiohttp_session.Session(None, data=None, new=True, max_age=self.max_age)
        else:
            storage_key = self._storage_key(session_id)
            session_data = await self.shared_cache.map_entries(storage_key)
            if session_data is None or len(session_data) == 0:
                return aiohttp_session.Session(None, data=None, new=True, max_age=self.max_age)
            if self.max_age is not None:
                await self.shared_cache.expire(storage_key, self.max_age * 1000)
            return aiohttp_session.Session(session_id, data=session_data, new=False, max_age=self.max_age)

    async def save_session(
            self,
            request: aiohttp.web.Request,
            response: aiohttp.web.Response,
            session: aiohttp_session.Session):
        if session.empty:
            self.save_cookie(response, None)
            await self.shared_cache.delete(f"session.{session.identity}")
        else:
            self.save_cookie(response, session.identity, max_age=session.max_age)
            session_data = self._get_session_data(session)
            storage_key = self._storage_key(session)
            b = self.shared_cache.start_bulk()
            b.delete(storage_key)
            b.map_set(storage_key, session_data)
            if session.max_age is not None:
                b.expire(storage_key, session.max_age * 1000)
            await b.execute()

    def _storage_key(self, session: Union[str, aiohttp_session.Session]) -> str:
        session_id = session if isinstance(session, str) else session.identity
        return f"session.{session_id}"
