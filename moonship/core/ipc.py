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

import abc

from moonship.core import *
from typing import Callable, Coroutine

__all__ = [
    "MessageBus",
    "SharedCache",
    "SharedCacheBulkOp"
]


class SharedCacheBulkOp(abc.ABC):

    @abc.abstractmethod
    def set_add(self, storage_key: str, element: str) -> None:
        pass

    @abc.abstractmethod
    def set_remove(self, storage_key: str, element: str) -> None:
        pass

    @abc.abstractmethod
    def map_set(self, storage_key: str, mappings: dict[str, str]) -> None:
        pass

    @abc.abstractmethod
    def delete(self, storage_key: str) -> None:
        pass

    @abc.abstractmethod
    def expire(self, storage_key: str, time_msec: int) -> None:
        pass

    @abc.abstractmethod
    async def execute(self) -> None:
        pass


class SharedCache(abc.ABC):

    def __init__(self, config: Config) -> None:
        pass

    async def close(self) -> None:
        pass

    @abc.abstractmethod
    async def set_add(self, storage_key: str, element: str) -> None:
        pass

    @abc.abstractmethod
    async def set_remove(self, storage_key: str, element: str) -> None:
        pass

    @abc.abstractmethod
    async def set_entries(self, storage_key: str) -> set[str]:
        pass

    @abc.abstractmethod
    async def map_set(self, storage_key: str, mappings: dict[str, str], append=True) -> None:
        pass

    @abc.abstractmethod
    async def map_get(self, storage_key: str, key: str) -> str:
        pass

    @abc.abstractmethod
    async def map_entries(self, storage_key: str) -> dict[str, str]:
        pass

    @abc.abstractmethod
    async def delete(self, storage_key: str) -> None:
        pass

    @abc.abstractmethod
    async def expire(self, storage_key: str, time_msec: int) -> None:
        pass

    @abc.abstractmethod
    def start_bulk(self, transaction=True) -> SharedCacheBulkOp:
        pass


class MessageBus(abc.ABC):

    def __init__(self, config: Config) -> None:
        pass

    async def close(self) -> None:
        pass

    @abc.abstractmethod
    async def subscribe(self, channel_name: str, msg_handler: Callable[[any, str], Coroutine[None]]) -> None:
        pass

    @abc.abstractmethod
    async def unsubscribe(self, channel_name: str) -> None:
        pass

    @abc.abstractmethod
    async def publish(self, msg: any, channel_name: str) -> None:
        pass
