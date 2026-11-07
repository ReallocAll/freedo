"""MCP-level tests for optional Base64-wrapped HTTP requests."""

import asyncio
import base64
import json

import httpx
import pytest
from fastmcp import Client

import server


def wrap(request):
    return base64.b64encode(json.dumps(request, ensure_ascii=False).encode("utf-8")).decode("ascii")


def value(response):
    assert not response.is_error
    assert response.structured_content is None
    return json.loads(response.content[0].text)


@pytest.mark.asyncio
async def test_base64_request_roundtrip_reuses_http_executor(monkeypatch):
    monkeypatch.setenv("APIKEY_SAMPLE", '{"key":"example-secret"}')
    sent = []

    def handle(request):
        sent.append(request)
        assert request.method == "POST"
        assert str(request.url) == "https://example.test/v1?q=%E4%B8%AD%E6%96%87"
        assert request.headers["authorization"] == "Bearer example-secret"
        assert json.loads(request.content) == {"text": "中文"}
        return httpx.Response(201, json={"message": "ok"})

    actual_execute = server._execute_request

    async def mocked_execute(*args):
        return await actual_execute(*args, transport=httpx.MockTransport(handle))

    monkeypatch.setattr(server, "_execute_request", mocked_execute)
    request = {
        "method": "POST",
        "url": "https://example.test/v1",
        "params": {"q": "中文"},
        "headers": {"Authorization": "Bearer {{env:APIKEY_SAMPLE}}"},
        "json_body": {"text": "中文"},
        "execution": "direct",
    }
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        response = value(await client.call_tool("http_request", {
            "base64": True, "payload": wrap(request),
        }))
    assert response["status_code"] == 201
    assert response["body"] == {"message": "ok"}
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_base64_request_supports_existing_body_base64(monkeypatch):
    sent = []

    def handle(request):
        sent.append(request.content)
        return httpx.Response(200, text="ok")

    actual_execute = server._execute_request

    async def mocked_execute(*args):
        return await actual_execute(*args, transport=httpx.MockTransport(handle))

    monkeypatch.setattr(server, "_execute_request", mocked_execute)
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        response = value(await client.call_tool("http_request", {
            "base64": True,
            "payload": wrap({
                "method": "POST", "url": "https://example.test/upload",
                "body_base64": "AP8=", "execution": "direct",
            }),
        }))
    assert response["status_code"] == 200
    assert sent == [b"\x00\xff"]


@pytest.mark.asyncio
async def test_base64_rejects_missing_invalid_or_mixed_input_without_http(monkeypatch):
    calls = []

    async def fake_execute(*args):
        calls.append(args)
        return {"status_code": 200}

    monkeypatch.setattr(server, "_execute_request", fake_execute)
    cases = [
        {"base64": True},
        {"base64": True, "payload": "!!!"},
        {"base64": True, "payload": base64.b64encode(b"\xff").decode()},
        {"base64": True, "payload": base64.b64encode(b"not-json").decode()},
        {"base64": True, "payload": wrap(["not", "an", "object"])},
        {"base64": True, "payload": wrap({"url": "https://example.test"})},
        {"base64": True, "payload": wrap({
            "method": "GET", "url": "https://example.test", "base64": False,
        })},
        {"base64": True, "payload": wrap({
            "method": "GET", "url": "https://example.test",
        }), "method": "GET"},
        {"base64": True, "payload": wrap({
            "method": "GET", "url": "https://example.test",
        }), "headers": {"X-Plaintext": "leak"}},
        {"payload": wrap({"method": "GET", "url": "https://example.test"})},
        {"method": "GET"},
    ]
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        for arguments in cases:
            response = value(await client.call_tool("http_request", arguments))
            assert response["error"] == "invalid_request", arguments
            assert "payload" not in response.get("detail", "")
    assert calls == []


@pytest.mark.asyncio
async def test_base64_background_dispatch_preserves_execution(monkeypatch):
    calls = []

    async def fake_execute(*args):
        calls.append(args)
        return {"status_code": 200, "body": "done", "body_format": "text", "size_bytes": 4}

    monkeypatch.setattr(server, "_execute_request", fake_execute)
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        scheduled = value(await client.call_tool("http_request", {
            "base64": True,
            "payload": wrap({
                "method": "GET", "url": "https://example.test",
                "execution": "background",
            }),
        }))
        assert scheduled["status"] == "working"
        for _ in range(50):
            result = value(await client.call_tool("get_request", {
                "task_id": scheduled["task_id"],
            }))
            if result["status"] == "completed":
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("background request did not complete")
    assert result["result"]["body"] == "done"
    assert len(calls) == 1
    assert calls[0][0:2] == ("GET", "https://example.test")
    assert calls[0][9] == 600
