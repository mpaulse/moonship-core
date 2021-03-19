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

import logging

from moonship.core import *
from moonship.core.ipc import SharedCache
from typing import Union

__all__ = [
    "TradingAlgo"
]


class Strategy:

    def __init__(
            self,
            name: str,
            engine_name: str,
            algo_class: type,
            markets: dict[str, Market],
            shared_cache: SharedCache) -> None:
        self._name = name
        self._engine_name = engine_name
        self._markets = markets
        self._shared_cache = shared_cache
        self._logger = logging.getLogger(f"moonship.strategy.{name}")
        self._running = False
        self._auto_start = False
        self._algo = algo_class(self)

    @property
    def name(self) -> str:
        return self._name

    @property
    def markets(self) -> dict[str, Market]:
        return self._markets

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def running(self) -> bool:
        return self._running

    @property
    def auto_start(self) -> bool:
        return self._auto_start

    def init_config(self, app_config: Config) -> None:
        auto_start = app_config.get(f"moonship.strategies.{self.name}.auto_start")
        if isinstance(auto_start, bool):
            self._auto_start = auto_start
        self._algo.init_config(config)

    async def start(self) -> None:
        if not self._running:
            for market in self._markets.values():
                market.subscribe(self._algo)
            self._running = True
            await self._algo.on_started()
            await self.record_state({"running": "true"})

    async def stop(self) -> None:
        if self._running:
            for market in self.markets.values():
                market.unsubscribe(self._algo)
            self._running = False
            await self._algo.on_stopped()
            await self.record_state({"running": "false"})

    async def record_state(self, state: dict[str, str]) -> None:
        if self._shared_cache is not None:
            await self._shared_cache.map_set(f"{self._engine_name}.{self.name}", state)


class TradingAlgo(MarketSubscriber):

    def __init__(self, strategy: Strategy):
        self.strategy = strategy

    @property
    def strategy_name(self) -> str:
        return self.strategy.name

    @property
    def market(self) -> Market:
        return next(iter(self.strategy.markets.values()))

    @property
    def markets(self) -> dict[str, Market]:
        return self.strategy.markets

    @property
    def logger(self) -> logging.Logger:
        return self.strategy.logger

    def init_config(self, app_config: Config) -> None:
        pass

    async def on_started(self) -> None:
        pass

    async def on_stopped(self) -> None:
        pass

    async def on_order_book_update(
            self,
            event: Union[OrderBookInitEvent, OrderBookItemAddedEvent, OrderBookItemRemovedEvent]) -> None:
        pass

    async def on_order_book_init(self, event: OrderBookInitEvent) -> None:
        await self.on_order_book_update(event)

    async def on_order_book_item_added(self, event: OrderBookItemAddedEvent) -> None:
        await self.on_order_book_update(event)

    async def on_order_book_item_removed(self, event: OrderBookItemRemovedEvent) -> None:
        await self.on_order_book_update(event)
