"""Paged transport, response budget, memory pressure and async job contracts."""
import asyncio
import base64
import json

import httpx
import pytest

import server
from fastmcp import Client
from result_cache import MIB, ResultCache, ResultNotFoundError, ResourceLimitError


def fresh_cache(monkeypatch):
    cache = ResultCache(rss_reader=lambda: 32 * MIB)
    monkeypatch.setattr(server, "RESULTS", cache)
    return cache


def test_paged_large_response_roundtrip(monkeypatch):
    cache = fresh_cache(monkeypatch)
    source = b"a" * (2 * MIB + 17)
    result = asyncio.run(server._execute_request(
        method="GET", url="https://example.test",
        transport=httpx.MockTransport(lambda _: httpx.Response(
            200, content=source, headers={"content-type": "text/plain"
        })),
    ))
    assert result["paged"] and result["size_bytes"] == len(source)
    assert "body" not in result
    with cache.borrow(result["result_ref"]) as entry:
        a = server._page_payload(result["result_ref"], entry.data,
            entry.body_format, 0, 2 * MIB, entry.status_code, entry.size_bytes, None)
        b = server._page_payload(result["result_ref"], entry.data,
            entry.body_format, a["next_offset"], 2 * MIB,
            entry.status_code, entry.size_bytes, None)
    assert a["returned_bytes"] == 2 * MIB
    assert a["has_more"] and not a["size_limit_adjusted"]
    assert a["data"].encode() + b["data"].encode() == source
    assert not b["has_more"] and b["next_offset"] is None


@pytest.mark.parametrize("kind", ["binary", "escaped"])
def test_final_mcp_page_budget_auto_shrinks(monkeypatch, kind):
    cache = fresh_cache(monkeypatch)
    raw = (b"\\\"\n" if kind == "escaped" else b"\xff") * (5 * MIB)
    fmt = "text" if kind == "escaped" else "base64"
    ref = cache.put(raw, fmt, 200, len(raw), None)
    with cache.borrow(ref) as item:
        first = server._page_payload(ref, item.data, item.body_format,
            0, 4 * MIB, item.status_code, item.size_bytes, None)
        second = server._page_payload(ref, item.data, item.body_format,
            first["next_offset"], 4 * MIB,
            item.status_code, item.size_bytes, None)
    assert first["size_limit_adjusted"] and first["returned_bytes"] < 4 * MIB
    assert first["has_more"] and first["next_offset"] == first["returned_bytes"]
    assert 0 < first["mcp_response_bytes"] < 4 * MIB
    assert server._framed_size(first) == first["mcp_response_bytes"]
    assert server._framed_size(second) < 4 * MIB
    if fmt == "base64":
        assert base64.b64decode(first["data"]) == raw[:first["returned_bytes"]]
    else:
        assert first["data"].encode() == raw[:first["returned_bytes"]]


def test_utf8_boundary_and_not_found(monkeypatch):
    cache = fresh_cache(monkeypatch)
    ref = cache.put("猫" .encode("utf-8") * 10, "text", 200, 30, None)
    with cache.borrow(ref) as item:
        page = server._page_payload(ref, item.data, item.body_format,
            0, 8, item.status_code, item.size_bytes, None)
        assert page["data"] == "猫猫"
        assert page["returned_bytes"] == 6
        assert page["next_offset"] == 6
        with pytest.raises(server.FreedoError):
            server._page_payload(ref, item.data, item.body_format,
                1, 8, item.status_code, item.size_bytes, None)
    cache.release(ref)
    with pytest.raises(ResultNotFoundError):
        with cache.borrow(ref):
            pass


def test_cache_evicts_only_on_rss_pressure_and_pins_reads():
    cache = None
    incoming = []
    def rss():
        return (10 * MIB + sum(len(x.data) for x in cache._entries.values())
                + sum(map(len, incoming)))
    cache = ResultCache(rss_reader=rss, rss_limit=13 * MIB)
    a = cache.put(b"a" * MIB, "text", 200, MIB, None)
    b = cache.put(b"b" * MIB, "text", 200, MIB, None)
    assert set(cache._entries) == {a, b}
    with cache.borrow(a):
        new_data = b"c" * (2 * MIB)
        incoming.append(new_data)
        c = cache.put(new_data, "text", 200, 2 * MIB, None)
        incoming.clear()
        assert a in cache._entries and b not in cache._entries
        assert c in cache._entries
    # The reserve checks actual memory admission, not arbitrary entry counts.
    with pytest.raises(ResourceLimitError):
        cache.reserve(20 * MIB)


async def _poll_until_done(client, task_id):
    for _ in range(50):
        r = await client.call_tool("get_request", {"task_id": task_id})
        result = json.loads(r.content[0].text)
        if result["status"] == "completed":
            return result
        await asyncio.sleep(0.01)
    raise AssertionError("job did not complete")


@pytest.mark.asyncio
async def test_auto_detaches_same_request_without_retry(monkeypatch):
    fresh_cache(monkeypatch)
    monkeypatch.setattr(server, "AUTO_FOREGROUND_SECONDS", 0.01)
    gate = asyncio.Event()
    count, observed_timeout = 0, []

    async def fake_execute(*args):
        nonlocal count
        count += 1
        observed_timeout.append(args[9])
        await gate.wait()
        return {"status_code": 202, "body": {"done": True}, "body_format": "json",
                "size_bytes": 13}
    monkeypatch.setattr(server, "_execute_request", fake_execute)
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        first = await client.call_tool("http_request", {
            "method": "POST", "url": "https://example.test", "execution": "auto",
        })
        payload = json.loads(first.content[0].text)
        assert payload["status"] == "working" and payload["task_id"]
        assert payload["origin_instance_id"] == server.INSTANCE_ID
        assert payload["docket_backend"] == server.DOCKET_BACKEND
        pending = await client.call_tool("get_request", {"task_id": payload["task_id"]})
        assert json.loads(pending.content[0].text)["status"] == "working"
        gate.set()
        done = await _poll_until_done(client, payload["task_id"])
        assert done["result"]["body"] == {"done": True}
    assert count == 1 and observed_timeout == [600]


@pytest.mark.asyncio
async def test_direct_is_inline_with_60_second_timeout(monkeypatch):
    called = []
    async def fake_execute(*args):
        called.append(args[9])
        return {"status_code": 200, "body": "ok", "body_format": "text",
                "size_bytes": 2}
    monkeypatch.setattr(server, "_execute_request", fake_execute)
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        result = await client.call_tool("http_request", {
            "method": "GET", "url": "https://example.test", "execution": "direct",
        })
        assert json.loads(result.content[0].text)["body"] == "ok"
        result = await client.call_tool("http_request", {
            "method": "GET", "url": "https://example.test", "execution": "background",
        })
        task_id = json.loads(result.content[0].text)["task_id"]
        done = await _poll_until_done(client, task_id)
        assert done["result"]["status_code"] == 200
    assert called == [60, 600]


def test_echoed_credential_is_redacted_before_paging(monkeypatch):
    cache = fresh_cache(monkeypatch)
    secret = "test-secret-value"
    monkeypatch.setenv("APIKEY_SAMPLE", json.dumps({"key": secret}))
    # The secret straddles the 2 MiB default page boundary.
    response = b"a" * (2 * MIB - 7) + secret.encode() + b"z" * 1024
    result = asyncio.run(server._execute_request(
        method="GET", url="https://example.test",
        headers={"Authorization": "Bearer {{env:APIKEY_SAMPLE}}"},
        transport=httpx.MockTransport(lambda _: httpx.Response(
            200, content=response, headers={"Content-Type": "text/plain"})),
    ))
    assert result["paged"] and result["size_bytes"] == len(response)
    with cache.borrow(result["result_ref"]) as entry:
        assert secret.encode() not in entry.data
        assert b"[REDACTED]" in entry.data
        page = server._page_payload(result["result_ref"], entry.data,
            entry.body_format, 0, 2 * MIB, entry.status_code, entry.size_bytes, None)
        assert page["mcp_response_bytes"] < 4 * MIB
        assert secret not in page["data"]


@pytest.mark.asyncio
async def test_mcp_read_result_contract_and_missing_reference(monkeypatch):
    cache = fresh_cache(monkeypatch)
    data = b"\\\"\n" * (2 * MIB)
    ref = cache.put(data, "text", 200, len(data), None)
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        response = await client.call_tool("read_result", {
            "result_ref": ref, "limit_bytes": 4 * MIB,
        })
        assert not response.is_error
        assert response.structured_content is None
        assert len(response.content) == 1
        result = json.loads(response.content[0].text)
        assert result["size_limit_adjusted"]
        assert result["mcp_response_bytes"] < 4 * MIB
        assert server._framed_size(result) == result["mcp_response_bytes"]
        assert result["returned_bytes"] > 0
        missing = await client.call_tool("read_result", {
            "result_ref": "does-not-exist",
        })
        assert json.loads(missing.content[0].text)["error"] == "RESULT_NOT_FOUND"


def test_512_mib_total_cap_and_compatibility():
    from result_cache import MAX_STORED_BYTES
    assert MAX_STORED_BYTES == 512 * MIB
    assert server._limit(MAX_STORED_BYTES, "max_total_response_bytes", MAX_STORED_BYTES) == 512 * MIB
    with pytest.raises(server.FreedoError):
        server._limit(MAX_STORED_BYTES + 1, "max_total_response_bytes", MAX_STORED_BYTES)


def test_streaming_redaction_handles_cross_block_boundaries(monkeypatch):
    cache = fresh_cache(monkeypatch)
    secret = "fake-access-secret-token"
    monkeypatch.setenv("APIKEY_FAKE", json.dumps({"key": secret}))
    block = b"x" * (MIB - 8) + secret.encode() + b"y" * (3 * MIB)
    result = asyncio.run(server._execute_request(
        method="GET", url="https://example.test",
        headers={"Authorization": "Bearer {{env:APIKEY_FAKE}}"},
        transport=httpx.MockTransport(lambda _: httpx.Response(
            200, content=block, headers={"Content-Type": "text/plain"})),
    ))
    assert result["paged"]
    with cache.borrow(result["result_ref"]) as entry:
        assert secret.encode() not in entry.data
        assert b"[REDACTED]" in entry.data
        assert entry.data.startswith(b"x" * (MIB - 8))
        assert entry.data.endswith(b"y" * (3 * MIB))


def test_large_binary_is_paged_without_text_decoding(monkeypatch):
    cache = fresh_cache(monkeypatch)
    data = b"\xff" * (3 * MIB)
    result = asyncio.run(server._execute_request(
        method="GET", url="https://example.test",
        transport=httpx.MockTransport(lambda _: httpx.Response(
            200, content=data, headers={"Content-Type": "application/octet-stream"})),
    ))
    assert result["body_format"] == "base64" and result["paged"]
    with cache.borrow(result["result_ref"]) as entry:
        assert entry.data == data


@pytest.mark.asyncio
async def test_missing_background_task_has_bounded_recovery_guidance():
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        missing = await client.call_tool("get_request", {
            "task_id": "unknown-task-id",
        })
    result = json.loads(missing.content[0].text)
    assert result["error"] == "TASK_NOT_FOUND"
    assert result["task_id"] == "unknown-task-id"
    assert result["status"] == "unknown" and result["outcome_unknown"] is True
    assert result["serving_instance_id"] == server.INSTANCE_ID
    assert result["docket_backend"] == server.DOCKET_BACKEND
    if server.DOCKET_BACKEND == "memory":
        assert "not shared between Horizon instances" in result["message"]
    assert result["retry_after_seconds"] == 5
    assert result["max_additional_polls"] == 2
    assert result["do_not_resubmit_automatically"] is True
    assert "STOP polling" in result["next_action"]
    assert "UNKNOWN" in result["next_action"]
    assert "may still have executed" in result["message"]
