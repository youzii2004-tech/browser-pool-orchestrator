"""
Tests for low-level asynchronous CDP client and JSON-RPC dispatching.
"""

import asyncio
import pytest

from src.cdp_client import (
    CDPClient,
    CDPResponseError,
    CDPTimeoutError,
)


@pytest.mark.asyncio
async def test_cdp_client_connect_and_disconnect() -> None:
    client = CDPClient(endpoint_url="ws://127.0.0.1:9222/devtools/page/mock", mock_mode=True)
    await client.connect()
    assert client.is_connected is True

    await client.disconnect()
    assert client.is_connected is False


@pytest.mark.asyncio
async def test_cdp_client_send_mock_navigation() -> None:
    client = CDPClient(endpoint_url="ws://127.0.0.1:9222/devtools/page/mock", mock_mode=True)
    await client.connect()

    res = await client.page_navigate("https://example.internal/dashboard")
    assert "frameId" in res
    assert res["frameId"] == "F001_MOCK"
    assert client.average_latency_ms > 0

    await client.disconnect()


@pytest.mark.asyncio
async def test_cdp_client_dom_and_runtime() -> None:
    client = CDPClient(endpoint_url="ws://127.0.0.1:9222/devtools/page/mock", mock_mode=True)
    await client.connect()

    node_id = await client.dom_query_selector(".main-container")
    assert node_id == 42

    eval_val = await client.runtime_evaluate("window.innerWidth")
    assert eval_val == "evaluation_success"

    header_res = await client.network_set_extra_http_headers({"X-Client": "Orchestrator"})
    assert isinstance(header_res, dict)

    targets = await client.target_get_targets()
    assert len(targets) == 1
    assert targets[0]["targetId"] == "T01_MOCK"

    await client.disconnect()


def test_cdp_client_raw_message_parsing() -> None:
    client = CDPClient(endpoint_url="ws://127.0.0.1:9222/devtools/page/mock")
    # Simulate pending future
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    fut = loop.create_future()
    client._pending_requests[1] = fut

    # Raw success frame
    raw_success = '{"id": 1, "result": {"navigated": true}}'
    client._handle_raw_message(raw_success)
    assert fut.done() is True
    assert fut.result() == {"navigated": True}

    # Test error frame
    fut_err = loop.create_future()
    client._pending_requests[2] = fut_err
    raw_error = '{"id": 2, "error": {"code": -32601, "message": "Method not found"}}'
    client._handle_raw_message(raw_error)
    assert fut_err.done() is True
    with pytest.raises(CDPResponseError) as exc_info:
        fut_err.result()
    assert exc_info.value.code == -32601
    assert "Method not found" in exc_info.value.message

    loop.close()


def test_cdp_event_listener_dispatch() -> None:
    client = CDPClient(endpoint_url="ws://127.0.0.1:9222/devtools/page/mock")
    received_events = []

    def _on_nav(data: dict) -> None:
        received_events.append(data)

    client.add_event_listener("Page.loadEventFired", _on_nav)
    raw_event = '{"method": "Page.loadEventFired", "params": {"timestamp": 12345.67}}'
    client._handle_raw_message(raw_event)

    assert len(received_events) == 1
    assert received_events[0]["timestamp"] == 12345.67
