import asyncio
import contextvars
import json
import logging
import time
import warnings
from typing import Any, Iterable, List, Mapping, Optional

import aiohttp
import elastic_transport
import elasticsearch
from aiohttp import BaseConnector, RequestInfo
from aiohttp.client_proto import ResponseHandler
from aiohttp.helpers import BaseTimerContext
from elastic_transport import (
    ApiResponse,
    BinaryApiResponse,
    HeadApiResponse,
    ListApiResponse,
    ObjectApiResponse,
    TextApiResponse,
)
from elastic_transport.client_utils import DEFAULT, percent_encode
from elasticsearch.compat import warn_stacklevel
from elasticsearch.exceptions import (
    HTTP_EXCEPTIONS,
    ApiError,
    ElasticsearchWarning,
    UnsupportedProductError,
)
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from esrally.client_utils import (
    _COMPAT_MIMETYPE_RE,
    _COMPAT_MIMETYPE_SUB,
    _COMPAT_MIMETYPE_TEMPLATE,
    _WARNING_RE,
    _mimetype_header_to_compat,
    _quote_query,
)
from esrally.utils import io, versions


class RequestContextManager:
    """
    Ensures that request context span the defined scope and allow nesting of request contexts with proper propagation.
    This means that we can span a top-level request context, open sub-request contexts that can be used to measure
    individual timings and still measure the proper total time on the top-level request context.
    """

    def __init__(self, request_context_holder):
        self.ctx_holder = request_context_holder
        self.ctx = None
        self.token = None

    async def __aenter__(self):
        self.ctx, self.token = self.ctx_holder.init_request_context()
        return self

    @property
    def request_start(self):
        return self.ctx["request_start"]

    @property
    def request_end(self):
        return self.ctx["request_end"]

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # propagate earliest request start and most recent request end to parent
        request_start = self.request_start
        request_end = self.request_end
        self.ctx_holder.restore_context(self.token)
        # don't attempt to restore these values on the top-level context as they don't exist
        if self.token.old_value != contextvars.Token.MISSING:
            self.ctx_holder.update_request_start(request_start)
            self.ctx_holder.update_request_end(request_end)
        self.token = None
        return False


class RequestContextHolder:
    """
    Holds request context variables. This class is only meant to be used together with RequestContextManager.
    """

    request_context = contextvars.ContextVar("rally_request_context")

    def new_request_context(self):
        return RequestContextManager(self)

    @classmethod
    def init_request_context(cls):
        ctx = {}
        token = cls.request_context.set(ctx)
        return ctx, token

    @classmethod
    def restore_context(cls, token):
        cls.request_context.reset(token)

    @classmethod
    def update_request_start(cls, new_request_start):
        meta = cls.request_context.get()
        # this can happen if multiple requests are sent on the wire for one logical request (e.g. scrolls)
        if "request_start" not in meta:
            meta["request_start"] = new_request_start

    @classmethod
    def update_request_end(cls, new_request_end):
        meta = cls.request_context.get()
        meta["request_end"] = new_request_end

    @classmethod
    def on_request_start(cls):
        cls.update_request_start(time.perf_counter())

    @classmethod
    def on_request_end(cls):
        cls.update_request_end(time.perf_counter())

    @classmethod
    def return_raw_response(cls):
        ctx = cls.request_context.get()
        ctx["raw_response"] = True


class StaticTransport:
    def __init__(self):
        self.closed = False

    def is_closing(self):
        return False

    def close(self):
        self.closed = True


class StaticConnector(BaseConnector):
    async def _create_connection(self, req: "ClientRequest", traces: List["Trace"], timeout: "ClientTimeout") -> ResponseHandler:
        handler = ResponseHandler(self._loop)
        handler.transport = StaticTransport()
        handler.protocol = ""
        return handler


class StaticRequest(aiohttp.ClientRequest):
    RESPONSES = None

    async def send(self, conn: "Connection") -> "ClientResponse":
        self.response = self.response_class(
            self.method,
            self.original_url,
            writer=self._writer,
            continue100=self._continue,
            timer=self._timer,
            request_info=self.request_info,
            traces=self._traces,
            loop=self.loop,
            session=self._session,
        )
        path = self.original_url.path
        self.response.static_body = StaticRequest.RESPONSES.response(path)
        return self.response


class StaticResponse(aiohttp.ClientResponse):
    def __init__(
        self,
        method: str,
        url: URL,
        *,
        writer: "asyncio.Task[None]",
        continue100: Optional["asyncio.Future[bool]"],
        timer: BaseTimerContext,
        request_info: RequestInfo,
        traces: List["Trace"],
        loop: asyncio.AbstractEventLoop,
        session: "ClientSession",
    ) -> None:
        super().__init__(
            method,
            url,
            writer=writer,
            continue100=continue100,
            timer=timer,
            request_info=request_info,
            traces=traces,
            loop=loop,
            session=session,
        )
        self.static_body = None

    async def start(self, connection: "Connection") -> "ClientResponse":
        self._closed = False
        self._protocol = connection.protocol
        self._connection = connection
        self._headers = CIMultiDictProxy(CIMultiDict())
        self.status = 200
        return self

    async def text(self, encoding=None, errors="strict"):
        return self.static_body


class RawClientResponse(aiohttp.ClientResponse):
    """
    Returns the body as bytes object (instead of a str) to avoid decoding overhead.
    """

    async def text(self, encoding=None, errors="strict"):
        """Read response payload and decode."""
        if self._body is None:
            await self.read()

        return self._body


class ResponseMatcher:
    def __init__(self, responses):
        self.logger = logging.getLogger(__name__)
        self.responses = []

        for response in responses:
            path = response["path"]
            if path == "*":
                matcher = ResponseMatcher.always()
            elif path.startswith("*"):
                matcher = ResponseMatcher.endswith(path[1:])
            elif path.endswith("*"):
                matcher = ResponseMatcher.startswith(path[:-1])
            else:
                matcher = ResponseMatcher.equals(path)

            body = response["body"]
            body_encoding = response.get("body-encoding", "json")
            if body_encoding == "raw":
                body = json.dumps(body).encode("utf-8")
            elif body_encoding == "json":
                body = json.dumps(body)
            else:
                raise ValueError(f"Unknown body encoding [{body_encoding}] for path [{path}]")

            self.responses.append((path, matcher, body))

    @staticmethod
    def always():
        # pylint: disable=unused-variable
        def f(p):
            return True

        return f

    @staticmethod
    def startswith(path_pattern):
        def f(p):
            return p.startswith(path_pattern)

        return f

    @staticmethod
    def endswith(path_pattern):
        def f(p):
            return p.endswith(path_pattern)

        return f

    @staticmethod
    def equals(path_pattern):
        def f(p):
            return p == path_pattern

        return f

    def response(self, path):
        for path_pattern, matcher, body in self.responses:
            if matcher(path):
                self.logger.debug("Path pattern [%s] matches path [%s].", path_pattern, path)
                return body


class RallyAiohttpHttpNode(elastic_transport.AiohttpHttpNode):
    def __init__(self, config):
        super().__init__(config)

        self._loop = asyncio.get_running_loop()
        self._limit = None

        client_options = config._extras.get("_rally_client_options")
        if client_options:
            self._trace_configs = client_options.get("trace_config")
            self._enable_cleanup_closed = client_options.get("enable_cleanup_closed")

        static_responses = client_options.get("static_responses")
        self.use_static_responses = static_responses is not None

        if self.use_static_responses:
            # read static responses once and reuse them
            if not StaticRequest.RESPONSES:
                with open(io.normalize_path(static_responses)) as f:
                    StaticRequest.RESPONSES = ResponseMatcher(json.load(f))

            self._request_class = StaticRequest
            self._response_class = StaticResponse
        else:
            self._request_class = aiohttp.ClientRequest
            self._response_class = RawClientResponse

    def _create_aiohttp_session(self):
        if self.use_static_responses:
            connector = StaticConnector(limit=self._limit, enable_cleanup_closed=self._enable_cleanup_closed)
        else:
            connector = aiohttp.TCPConnector(
                limit=self._limit,
                use_dns_cache=True,
                # May need to be just ssl=self._ssl_context
                ssl_context=self._ssl_context,
                enable_cleanup_closed=self._enable_cleanup_closed,
            )

        self.session = aiohttp.ClientSession(
            headers=self.headers,
            auto_decompress=True,
            loop=self._loop,
            cookie_jar=aiohttp.DummyCookieJar(),
            request_class=self._request_class,
            response_class=self._response_class,
            connector=connector,
            trace_configs=self._trace_configs,
        )


class RallyAsyncElasticsearch(elasticsearch.AsyncElasticsearch, RequestContextHolder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # skip verification at this point; we've already verified this earlier with the synchronous client.
        # The async client is used in the hot code path and we use customized overrides (such as that we don't
        # parse response bodies in some cases for performance reasons, e.g. when using the bulk API).
        self._verified_elasticsearch = True

    async def perform_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[Any] = None,
    ) -> ApiResponse[Any]:

        print(f"ASYNC VERSION: {self.distribution_version}")

        # We need to ensure that we provide content-type and accept headers
        if body is not None:
            if headers is None:
                headers = {"content-type": "application/json", "accept": "application/json"}
            else:
                if headers.get("content-type") is None:
                    headers["content-type"] = "application/json"
                if headers.get("accept") is None:
                    headers["accept"] = "application/json"

        if headers:
            request_headers = self._headers.copy()
            request_headers.update(headers)
        else:
            request_headers = self._headers

        if self.distribution_version is not None and self.distribution_version >= versions.Version.from_string("8.0.0"):
            _mimetype_header_to_compat("Accept", request_headers)
            _mimetype_header_to_compat("Content-Type", request_headers)

        if params:
            target = f"{path}?{_quote_query(params)}"
        else:
            target = path

        meta, resp_body = await self.transport.perform_request(
            method,
            target,
            headers=request_headers,
            body=body,
            request_timeout=self._request_timeout,
            max_retries=self._max_retries,
            retry_on_status=self._retry_on_status,
            retry_on_timeout=self._retry_on_timeout,
            client_meta=self._client_meta,
        )

        # HEAD with a 404 is returned as a normal response
        # since this is used as an 'exists' functionality.
        if not (method == "HEAD" and meta.status == 404) and (
            not 200 <= meta.status < 299
            and (self._ignore_status is DEFAULT or self._ignore_status is None or meta.status not in self._ignore_status)
        ):
            message = str(resp_body)

            # If the response is an error response try parsing
            # the raw Elasticsearch error before raising.
            if isinstance(resp_body, dict):
                try:
                    error = resp_body.get("error", message)
                    if isinstance(error, dict) and "type" in error:
                        error = error["type"]
                    message = error
                except (ValueError, KeyError, TypeError):
                    pass

            raise HTTP_EXCEPTIONS.get(meta.status, ApiError)(message=message, meta=meta, body=resp_body)

        # 'Warning' headers should be reraised as 'ElasticsearchWarning'
        if "warning" in meta.headers:
            warning_header = (meta.headers.get("warning") or "").strip()
            warning_messages: Iterable[str] = _WARNING_RE.findall(warning_header) or (warning_header,)
            stacklevel = warn_stacklevel()
            for warning_message in warning_messages:
                warnings.warn(
                    warning_message,
                    category=ElasticsearchWarning,
                    stacklevel=stacklevel,
                )

        if method == "HEAD":
            response = HeadApiResponse(meta=meta)
        elif isinstance(resp_body, dict):
            response = ObjectApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        elif isinstance(resp_body, list):
            response = ListApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        elif isinstance(resp_body, str):
            response = TextApiResponse(  # type: ignore[assignment]
                body=resp_body,
                meta=meta,
            )
        elif isinstance(resp_body, bytes):
            response = BinaryApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        else:
            response = ApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]

        return response
