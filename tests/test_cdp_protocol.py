"""CDP client tested against a real local WebSocket server (no mock_mode)."""

import asyncio
import json

import pytest
import websockets

from src.cdp_client import CDPClient, CDPError, CDPResponseError, CDPTimeoutError


async def _serve(handler):
    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"ws://127.0.0.1:{port}"


@pytest.mark.asyncio
async def test_correlates_out_of_order_responses_and_errors() -> None:
    async def handler(ws):
        reqs = [json.loads(await ws.recv()), json.loads(await ws.recv())]
        for r in reversed(reqs):  # answer in reverse order
            if r["method"] == "Bad.method":
                await ws.send(json.dumps({"id": r["id"], "error": {"code": -32601, "message": "nope"}}))
            else:
                await ws.send(json.dumps({"id": r["id"], "result": {"echo": r["method"]}}))
        await ws.wait_closed()

    server, url = await _serve(handler)
    client = CDPClient(url)
    await client.connect()
    try:
        ok, bad = await asyncio.gather(
            client.send("Good.method"), client.send("Bad.method"), return_exceptions=True
        )
        assert ok == {"echo": "Good.method"}
        assert isinstance(bad, CDPResponseError) and bad.code == -32601
    finally:
        await client.disconnect()
        server.close()


@pytest.mark.asyncio
async def test_timeout_and_connection_loss_fail_pending_commands_fast() -> None:
    async def handler(ws):
        await ws.recv()
        await ws.close()  # drop the connection without answering

    server, url = await _serve(handler)
    client = CDPClient(url, timeout=30.0)
    await client.connect()
    try:
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        with pytest.raises(CDPError):
            await client.send("Never.answered")
        assert loop.time() - t0 < 5  # failed on close, not after the 30s timeout
    finally:
        await client.disconnect()
        server.close()

    async def silent(ws):
        await ws.wait_closed()

    server, url = await _serve(silent)
    client = CDPClient(url)
    await client.connect()
    try:
        with pytest.raises(CDPTimeoutError):
            await client.send("Silent.method", timeout=0.2)
    finally:
        await client.disconnect()
        server.close()


@pytest.mark.asyncio
async def test_events_reach_waiters_and_listeners() -> None:
    async def handler(ws):
        req = json.loads(await ws.recv())
        await ws.send(json.dumps({"method": "Page.loadEventFired", "params": {"timestamp": 1.5}}))
        await ws.send(json.dumps({"id": req["id"], "result": {}}))
        await ws.wait_closed()

    server, url = await _serve(handler)
    client = CDPClient(url)
    await client.connect()
    try:
        seen = []
        client.add_event_listener("Page.loadEventFired", seen.append)
        fut = client.expect_event("Page.loadEventFired")
        await client.send("Page.enable")
        assert (await asyncio.wait_for(fut, 2)) == {"timestamp": 1.5}
        assert seen == [{"timestamp": 1.5}]
    finally:
        await client.disconnect()
        server.close()
