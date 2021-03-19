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
import bcrypt
import logging
import os
import ssl
import uuid

from aiohttp.web import Request, Response, StreamResponse, WebSocketResponse
from moonship.core import *
from moonship.core.redis import *
from typing import Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


class APIService:

    def __init__(self, config: Config) -> None:
        self.config = config
        self.port = self._get_port(config)
        self.ssl_context = self._get_ssl_context(config)
        self.user = config.get("moonship.api.user")
        if not isinstance(self.user, str):
            raise StartUpException("No API user configured")
        password = config.get("moonship.api.password")
        if not isinstance(password, str):
            raise StartUpException("No API password configured")
        self.password = password.encode("utf-8")
        self.web_app_runner: Optional[aiohttp.web.AppRunner] = None
        self.session_store: Optional[RedisSessionStore] = None

    async def post_login(self, req: Request) -> StreamResponse:
        req_body = await req.json()
        user = req_body.get("user")
        if not isinstance(user, str):
            return self._bad_request("Missing or bad user field")
        password = req_body.get("password")
        if not isinstance(password, str):
            return self._bad_request("Missing or bad password field")
        if user == self.user and bcrypt.checkpw(password.encode("utf-8"), self.password):
            session = await aiohttp_session.new_session(req)
            session.set_new_identity(uuid.uuid4().hex)
            session["user"] = self.user
            return self._ok({
                "session_token": session.identity
            })
        return self._unauthorized("Invalid user or password")

    async def get_logout(self, req: Request) -> StreamResponse:
        session = await aiohttp_session.get_session(req)
        session.invalidate()
        return self._ok()

    async def get_strategies(self, req: Request) -> StreamResponse:
        pass

    async def get_strategy(self, req: Request) -> StreamResponse:
        pass

    async def put_strategy(self, req: Request) -> StreamResponse:
        pass

    async def get_strategy_config(self, req: Request) -> StreamResponse:
        pass

    async def put_strategy_config(self, req: Request) -> StreamResponse:
        pass

    async def get_trades(self, req: Request) -> StreamResponse:
        pass

    async def get_event_stream(self, req: Request) -> WebSocketResponse:
        pass

    @aiohttp.web.middleware
    async def verify_session(
            self,
            req: Request,
            handler: Callable[[Request], Awaitable[StreamResponse]]) -> StreamResponse:
        if req.path != "/login":
            session = await aiohttp_session.get_session(req)
            if session.new:
                return self._unauthorized("Access denied")
        return await handler(req)

    def _ok(self, json_rsp_body: dict = None) -> Response:
        if json_rsp_body is not None:
            return aiohttp.web.json_response(json_rsp_body, status=200)
        return Response(status=200)

    def _bad_request(self, error_title: str) -> Response:
        return self._error_response("error/validation", error_title, 400)

    def _unauthorized(self, error_title: str) -> Response:
        return self._error_response("error/access", error_title, 401)

    def _error_response(self, type: str, title: str, status: int) -> Response:
        return aiohttp.web.json_response(
            {
                "type": type,
                "title": title,
                "status": status
            },
            status=status)

    def _get_port(self, config: Config) -> int:
        port = config.get("moonship.api.port", 8080)
        if isinstance(port, str) and len(port) > 1 and port.startswith("$"):
            try:
                port = int(os.environ[port[1:]])
            except KeyError:
                raise StartUpException(f"No {port[1:]} environment variable set")
        if not isinstance(port, int) or port < 0 or port > 65535:
            raise StartUpException(f"Invalid API port configuration: {port}")
        return port

    def _get_ssl_context(self, config: Config) -> Optional[ssl.SSLContext]:
        ssl_context = None
        ssl_cert = config.get("moonship.api.ssl_cert")
        ssl_key = config.get("moonship.api.ssl_key")
        if isinstance(ssl_cert, str) and isinstance(ssl_key, str):
            ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2)
            ssl_context.load_cert_chain(ssl_cert, ssl_key)
        return ssl_context

    async def start(self) -> None:
        web_app = aiohttp.web.Application()
        web_app.add_routes([
            aiohttp.web.post("/login", self.post_login),
            aiohttp.web.get("/logout", self.get_logout),
            aiohttp.web.get("/strategies", self.get_strategies),
            aiohttp.web.get("/strategies/{engine}/{strategy}", self.get_strategy),
            aiohttp.web.put("/strategies/{engine}/{strategy}", self.put_strategy),
            aiohttp.web.get("/strategies/{engine}/{strategy}/config", self.get_strategy_config),
            aiohttp.web.put("/strategies/{engine}/{strategy}/config", self.put_strategy_config),
            aiohttp.web.get("/trades", self.get_trades),
            aiohttp.web.get("/stream", self.get_event_stream)
        ])
        web_app.on_response_prepare.append(self.on_prepare_response)
        self.session_store = RedisSessionStore(self.config)
        aiohttp_session.setup(web_app, self.session_store)
        web_app.middlewares.append(self.verify_session)
        self.web_app_runner = aiohttp.web.AppRunner(web_app, logger=logger, access_log=logger)
        await self.web_app_runner.setup()
        site = aiohttp.web.TCPSite(self.web_app_runner, port=self.port, ssl_context=self.ssl_context)
        await site.start()
        logger.info(f"Listening on port {self.port}")

    async def stop(self) -> None:
        if self.session_store is not None:
            await self.session_store.close()
        if self.web_app_runner is not None:
            await self.web_app_runner.cleanup()

    async def on_prepare_response(self, req: Request, rsp: StreamResponse) -> None:
        rsp.headers["Server"] = "Moonship"
