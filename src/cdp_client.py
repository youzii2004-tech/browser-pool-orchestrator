"""
Low-Level Asynchronous Chrome DevTools Protocol (CDP) WebSocket Client.

Provides direct JSON-RPC 2.0 protocol dispatching across Chrome DevTools domains
(Page, DOM, Input, Runtime, Network, Target) with correlation ID tracking,
event streaming, and timeout supervision.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Union
import urllib.request
import websockets

logger = logging.getLogger("browser_pool.cdp")


class CDPError(Exception):
    """Base exception for all Chrome DevTools Protocol errors."""
    pass


class CDPTimeoutError(CDPError):
    """Raised when a CDP command exceeds timeout threshold."""
    pass


class CDPResponseError(CDPError):
    """Raised when the browser returns an error object in JSON-RPC response."""

    def __init__(self, code: int, message: str, data: Optional[Any] = None) -> None:
        super().__init__(f"CDP Error ({code}): {message}")
        self.code = code
        self.message = message
        self.data = data


def _http_json(url: str, method: str = "GET", timeout: float = 5.0) -> Any:
    req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (localhost only)
        return json.loads(resp.read().decode("utf-8"))


async def resolve_page_ws_url(port: int, host: str = "127.0.0.1", timeout: float = 5.0) -> str:
    """Return the WebSocket URL of a page target on the given debugging port (opening one if needed)."""
    base = f"http://{host}:{port}"
    targets: List[Dict[str, Any]] = await asyncio.to_thread(_http_json, f"{base}/json/list", "GET", timeout)
    for t in targets:
        if t.get("type") == "page" and t.get("webSocketDebuggerUrl"):
            return t["webSocketDebuggerUrl"]
    created = await asyncio.to_thread(_http_json, f"{base}/json/new?about:blank", "PUT", timeout)
    return created["webSocketDebuggerUrl"]


class CDPClient:
    """
    Asynchronous WebSocket client communicating directly with Chromium DevTools.

    ``mock_mode=True`` returns canned responses with no browser behind them. It exists
    only so protocol plumbing can be unit-tested offline; use ``CDPClient.for_port``
    against a running browser for anything real.
    """

    @classmethod
    async def for_port(cls, port: int, timeout: float = 15.0, host: str = "127.0.0.1") -> "CDPClient":
        """Connect to a page target of the Chromium listening on ``port``."""
        client = cls(await resolve_page_ws_url(port, host), timeout=timeout)
        await client.connect()
        return client

    def __init__(
        self,
        endpoint_url: str,
        timeout: float = 15.0,
        mock_mode: bool = False,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.default_timeout = timeout
        self.mock_mode = mock_mode

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._message_id = 0
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._event_listeners: Dict[str, list[Callable[[Dict[str, Any]], None]]] = {}
        self._listener_task: Optional[asyncio.Task] = None
        self._event_waiters: Dict[str, List[asyncio.Future]] = {}
        self._connected = False
        self._latencies: list[float] = []

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def average_latency_ms(self) -> float:
        if not self._latencies:
            return 0.0
        return sum(self._latencies) / len(self._latencies)

    async def connect(self) -> None:
        """Establishes WebSocket connection to Chromium remote debugging target."""
        if self.mock_mode:
            self._connected = True
            logger.info("CDP Client connected in mock mode to %s", self.endpoint_url)
            return

        try:
            self._ws = await websockets.connect(
                self.endpoint_url,
                max_size=20 * 1024 * 1024,  # 20MB payload limit
                ping_interval=10,
                ping_timeout=10,
            )
            self._connected = True
            self._listener_task = asyncio.create_task(self._listen_loop())
            logger.info("Connected to CDP endpoint: %s", self.endpoint_url)
        except Exception as e:
            self._connected = False
            logger.error("Failed to connect to CDP endpoint %s: %s", self.endpoint_url, e)
            raise

    async def disconnect(self) -> None:
        """Closes the WebSocket connection and terminates background listeners."""
        self._connected = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None

        if self._ws:
            await self._ws.close()
            self._ws = None

        # Cancel any pending futures
        for fut in self._pending_requests.values():
            if not fut.done():
                fut.cancel()
        self._pending_requests.clear()
        logger.info("CDP client disconnected from %s", self.endpoint_url)

    async def _listen_loop(self) -> None:
        """Background coroutine processing incoming JSON-RPC responses and events."""
        if not self._ws:
            return

        try:
            async for raw_message in self._ws:
                self._handle_raw_message(raw_message)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning("CDP listener encountered exception: %s", e)
        finally:
            self._connected = False
            # The socket is gone: fail in-flight commands now instead of letting them time out.
            for fut in self._pending_requests.values():
                if not fut.done():
                    fut.set_exception(CDPError("CDP connection closed before response arrived"))
            self._pending_requests.clear()

    def _handle_raw_message(self, raw_message: Union[str, bytes]) -> None:
        """Parses and dispatches incoming WebSocket frames."""
        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.error("Received malformed JSON from CDP: %s", raw_message)
            return

        # 1. Check if response matches pending command ID
        msg_id = data.get("id")
        if msg_id is not None and msg_id in self._pending_requests:
            future = self._pending_requests.pop(msg_id)
            if not future.done():
                if "error" in data:
                    err = data["error"]
                    future.set_exception(
                        CDPResponseError(
                            code=err.get("code", -1),
                            message=err.get("message", "Unknown error"),
                            data=err.get("data"),
                        )
                    )
                else:
                    future.set_result(data.get("result", {}))
            return

        # 2. Check if message is a CDP event notification
        method = data.get("method")
        if method:
            for waiter in self._event_waiters.pop(method, []):
                if not waiter.done():
                    waiter.set_result(data.get("params", {}))
        if method and method in self._event_listeners:
            params = data.get("params", {})
            for callback in self._event_listeners[method]:
                try:
                    callback(params)
                except Exception as cb_err:
                    logger.error("Error in CDP event listener for %s: %s", method, cb_err)

    async def send(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Sends a JSON-RPC 2.0 command over WebSocket and awaits its response.
        """
        self._message_id += 1
        req_id = self._message_id
        timeout_val = timeout if timeout is not None else self.default_timeout

        payload = {
            "id": req_id,
            "method": method,
            "params": params or {},
        }

        start_time = time.time()

        if self.mock_mode:
            # Synthetic response generation for testing & offline orchestration
            await asyncio.sleep(0.005)  # simulate 5ms latency
            elapsed_ms = (time.time() - start_time) * 1000
            self._latencies.append(elapsed_ms)
            return self._generate_mock_response(method, params or {})

        if not self._connected or not self._ws:
            raise CDPError("Cannot send command: WebSocket client is not connected")

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending_requests[req_id] = future

        try:
            await self._ws.send(json.dumps(payload))
            result = await asyncio.wait_for(future, timeout=timeout_val)
            elapsed_ms = (time.time() - start_time) * 1000
            self._latencies.append(elapsed_ms)
            return result
        except asyncio.TimeoutError:
            self._pending_requests.pop(req_id, None)
            raise CDPTimeoutError(
                f"CDP command '{method}' timed out after {timeout_val:.1f}s (id={req_id})"
            )
        except Exception:
            self._pending_requests.pop(req_id, None)
            raise

    async def wait_for_event(self, method: str, timeout: Optional[float] = None) -> Dict[str, Any]:
        """Await the next occurrence of a CDP event (register *before* triggering it)."""
        fut = self.expect_event(method)
        try:
            return await asyncio.wait_for(fut, timeout if timeout is not None else self.default_timeout)
        except asyncio.TimeoutError:
            raise CDPTimeoutError(f"Timed out waiting for CDP event '{method}'")

    def expect_event(self, method: str) -> "asyncio.Future[Dict[str, Any]]":
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._event_waiters.setdefault(method, []).append(fut)
        return fut

    def add_event_listener(self, event_name: str, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Registers a callback for specific CDP event stream notifications."""
        if event_name not in self._event_listeners:
            self._event_listeners[event_name] = []
        self._event_listeners[event_name].append(callback)

    def _generate_mock_response(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Synthetic mock responses for tests and dry-run telemetry simulations."""
        if method == "Page.navigate":
            return {"frameId": "F001_MOCK", "loaderId": "L001_MOCK"}
        elif method == "DOM.querySelector":
            return {"nodeId": 42 if params.get("selector") != "#missing" else 0}
        elif method == "Input.dispatchMouseEvent":
            return {}
        elif method == "Runtime.evaluate":
            return {"result": {"type": "string", "value": "evaluation_success"}}
        elif method == "Network.setExtraHTTPHeaders":
            return {}
        elif method == "Target.getTargets":
            return {
                "targetInfos": [
                    {
                        "targetId": "T01_MOCK",
                        "type": "page",
                        "title": "Orchestrator Fleet Tab",
                        "url": "about:blank",
                        "attached": True,
                    }
                ]
            }
        return {"status": "ok", "mock": True}

    # ---------------- Domain Specific Methods ----------------

    async def page_navigate(self, url: str, referrer: Optional[str] = None) -> Dict[str, Any]:
        """Navigates current target to specified URL."""
        params: Dict[str, Any] = {"url": url}
        if referrer:
            params["referrer"] = referrer
        return await self.send("Page.navigate", params)

    async def navigate(self, url: str, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Navigate and wait for the load event. Returns ``{"url", "status", "frameId"}`` where
        ``status`` is the HTTP status of the main document (None for non-HTTP URLs).
        Raises CDPResponseError if the navigation itself fails (DNS, refused, ...).
        """
        timeout_val = timeout if timeout is not None else self.default_timeout
        if self.mock_mode:
            res = await self.page_navigate(url)
            return {"url": url, "status": 200, "frameId": res.get("frameId")}

        await self.send("Page.enable")
        await self.send("Network.enable")
        # responseReceived can arrive before Page.navigate returns the frameId, so collect
        # every document response by frame and pick the main frame's afterwards.
        doc_status: Dict[str, int] = {}

        def on_response(params: Dict[str, Any]) -> None:
            if params.get("type") == "Document" and params.get("frameId"):
                doc_status[params["frameId"]] = int(params["response"].get("status", 0))

        self.add_event_listener("Network.responseReceived", on_response)
        loaded = self.expect_event("Page.loadEventFired")
        frame_id: Optional[str] = None
        try:
            res = await self.send("Page.navigate", {"url": url}, timeout=timeout_val)
            frame_id = res.get("frameId")
            if res.get("errorText"):
                raise CDPResponseError(-1, f"Navigation to {url} failed: {res['errorText']}")
            await asyncio.wait_for(loaded, timeout=timeout_val)
        except asyncio.TimeoutError:
            raise CDPTimeoutError(f"navigation timeout after {timeout_val:.1f}s loading {url}")
        finally:
            listeners = self._event_listeners.get("Network.responseReceived", [])
            if on_response in listeners:
                listeners.remove(on_response)
        code = doc_status.get(frame_id) if frame_id else None
        return {"url": url, "status": code or None, "frameId": frame_id}

    async def dom_query_selector(self, selector: str, node_id: Optional[int] = None) -> int:
        """Finds the matching DOM node ID (0 if none). Resolves the document root when node_id is omitted."""
        if node_id is None:
            if self.mock_mode:
                node_id = 1
            else:
                doc = await self.send("DOM.getDocument", {"depth": 0})
                node_id = doc["root"]["nodeId"]
        res = await self.send("DOM.querySelector", {"nodeId": node_id, "selector": selector})
        return res.get("nodeId", 0)

    async def input_dispatch_mouse_event(
        self,
        event_type: str,
        x: int,
        y: int,
        button: str = "left",
        click_count: int = 1,
    ) -> Dict[str, Any]:
        """Dispatches mouse input events (mousePressed, mouseReleased, mouseMoved)."""
        return await self.send(
            "Input.dispatchMouseEvent",
            {
                "type": event_type,
                "x": x,
                "y": y,
                "button": button,
                "clickCount": click_count,
            },
        )

    async def runtime_evaluate(
        self,
        expression: str,
        await_promise: bool = True,
        return_by_value: bool = True,
    ) -> Any:
        """Evaluates arbitrary JavaScript in page context."""
        res = await self.send(
            "Runtime.evaluate",
            {
                "expression": expression,
                "awaitPromise": await_promise,
                "returnByValue": return_by_value,
            },
        )
        if "exceptionDetails" in res:
            details = res["exceptionDetails"]
            text = details.get("exception", {}).get("description") or details.get("text", "JS exception")
            raise CDPResponseError(-2, f"JavaScript exception: {text}")
        return res.get("result", {}).get("value")

    async def network_set_extra_http_headers(self, headers: Dict[str, str]) -> Dict[str, Any]:
        """Injects custom HTTP headers across all subsequent network requests."""
        return await self.send("Network.setExtraHTTPHeaders", {"headers": headers})

    async def target_get_targets(self) -> list[Dict[str, Any]]:
        """Retrieves list of all available browser targets/tabs."""
        res = await self.send("Target.getTargets")
        return res.get("targetInfos", [])
