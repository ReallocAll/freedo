"""Native ChatGPT attachment ingress and streaming upload contracts."""
import asyncio
import hashlib
import json

import httpx
import pytest
from fastmcp import Client

import server


FILE = {
    "download_url": "https://chatgpt-files.test/attachment?sig=private",
    "file_id": "sediment://file_example",
    "file_name": "test.bin",
    "mime_type": "application/octet-stream",
}


@pytest.mark.asyncio
async def test_native_file_parameter_registered():
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        definitions = {t.name: t for t in await client.list_tools()}
        tool = definitions["upload_file"].model_dump(by_alias=True)
        assert (tool.get("_meta") or tool.get("meta"))["openai/fileParams"] == ["file"]
        props = tool["inputSchema"]["properties"]
        assert {"file", "method", "url"} <= set(tool["inputSchema"]["required"])
        file_schema = props["file"]
        if "$ref" in file_schema:
            file_schema = tool["inputSchema"]["$defs"][file_schema["$ref"].rsplit("/", 1)[1]]
        assert {"download_url", "file_id"} <= set(file_schema["required"])


@pytest.mark.asyncio
async def test_raw_streaming_roundtrip_redacts_credentials(monkeypatch):
    original = b"\x00\xff\x10payload" * 1000
    observed = []
    monkeypatch.setenv("APIKEY_SAMPLE", json.dumps({"key": "example-secret"}))

    async def handler(request):
        if request.url.host == "chatgpt-files.test":
            assert "authorization" not in request.headers
            assert request.headers["accept-encoding"] == "identity"
            return httpx.Response(200, stream=httpx.ByteStream(original),
                                  headers={"Content-Length": str(len(original))})
        observed.append(request)
        assert request.method == "PUT"
        assert request.url.params["auth"] == "example-secret"
        assert request.headers["authorization"] == "Bearer example-secret"
        assert request.headers["content-type"] == "application/octet-stream"
        assert int(request.headers["content-length"]) == len(original)
        assert await request.aread() == original
        return httpx.Response(201, json={"message": "example-secret uploaded"})

    result = await server._execute_upload(
        FILE, "PUT", "https://upload.test/object",
        headers={"Authorization": "Bearer {{env:APIKEY_SAMPLE}}"},
        params={"auth": "{{env:APIKEY_SAMPLE}}"},
        transport=httpx.MockTransport(handler),
    )
    assert len(observed) == 1
    assert result["status_code"] == 201
    assert result["body"] == {"message": "[REDACTED] uploaded"}
    assert result["upload"]["size_bytes"] == len(original)
    assert result["upload"]["sha256"] == hashlib.sha256(original).hexdigest()
    assert result["upload"]["filename"] == "test.bin"
    assert "example-secret" not in str(result)


@pytest.mark.asyncio
async def test_multipart_streaming_and_form_fields(monkeypatch):
    monkeypatch.setenv("APIKEY_SAMPLE", '{"key":"form-secret"}')
    calls = []

    async def handler(request):
        if request.url.host == "chatgpt-files.test":
            return httpx.Response(200, stream=httpx.ByteStream(b"\x00binary\xff"))
        calls.append(request)
        assert request.method == "POST"
        assert request.headers["content-type"].startswith("multipart/form-data; boundary=")
        body = await request.aread()
        assert b'name="document"; filename="report.dat"' in body
        assert b"application/custom" in body
        assert b"\x00binary\xff" in body
        assert b'name="description"' in body
        assert b"form-secret" in body
        assert int(request.headers["content-length"]) == len(body)
        return httpx.Response(200, content=b"ok")

    result = await server._execute_upload(
        FILE, "POST", "https://upload.test/import",
        mode="multipart", field_name="document",
        filename="report.dat", content_type="application/custom",
        form_fields={"description": "{{env:APIKEY_SAMPLE}}"},
        transport=httpx.MockTransport(handler),
    )
    assert len(calls) == 1
    assert result["status_code"] == 200
    assert result["upload"]["filename"] == "report.dat"
    assert result["upload"]["size_bytes"] == 8


@pytest.mark.asyncio
async def test_limits_and_authorization_errors_do_not_upload():
    assert server.MAX_UPLOAD_BYTES == 512_000_000
    paths = []

    async def oversized(request):
        paths.append(request.url.path)
        return httpx.Response(200, headers={"Content-Length": str(server.MAX_UPLOAD_BYTES + 1)})

    result = await server._execute_upload(
        FILE, "PUT", "https://upload.test/destination",
        transport=httpx.MockTransport(oversized),
    )
    assert result == {"error": "upload_failed", "detail": "file_too_large"}
    assert paths == ["/attachment"]

    async def expired(request):
        paths.append(request.url.path)
        return httpx.Response(403)

    result = await server._execute_upload(
        FILE, "POST", "https://upload.test/destination",
        transport=httpx.MockTransport(expired),
    )
    assert result["detail"] == "FILE_AUTHORIZATION_EXPIRED"
    assert paths == ["/attachment", "/attachment"]


@pytest.mark.asyncio
async def test_empty_file_is_a_valid_raw_upload():
    calls = []

    async def handler(request):
        if request.url.host == "chatgpt-files.test":
            return httpx.Response(
                200, stream=httpx.ByteStream(b""), headers={"Content-Length": "0"}
            )
        calls.append(request.url.path)
        assert request.headers["content-length"] == "0"
        assert await request.aread() == b""
        return httpx.Response(204)

    result = await server._execute_upload(
        FILE, "PUT", "https://upload.test/empty",
        transport=httpx.MockTransport(handler),
    )
    assert result["status_code"] == 204
    assert result["upload"]["size_bytes"] == 0
    assert result["upload"]["sha256"] == hashlib.sha256(b"").hexdigest()
    assert calls == ["/empty"]


@pytest.mark.asyncio
async def test_checksum_mismatch_prevents_upstream_request():
    paths = []

    async def handler(request):
        paths.append(request.url.path)
        return httpx.Response(200, stream=httpx.ByteStream(b"source"))

    result = await server._execute_upload(
        FILE, "PUT", "https://upload.test/destination",
        expected_sha256="0" * 64, transport=httpx.MockTransport(handler),
    )
    assert result == {"error": "CONTENT_MISMATCH"}
    assert paths == ["/attachment"]


@pytest.mark.asyncio
async def test_invalid_upload_options_before_network():
    bad = await server._execute_upload(
        FILE, "PUT", "https://upload.test/destination", mode="not-a-mode",
    )
    assert bad["error"] == "upload_failed"

    bad = await server._execute_upload(
        FILE, "PUT", "https://upload.test/destination",
        headers={"Content-Length": "0"},
    )
    assert "Content-Length" in bad["detail"]

    bad = await server._execute_upload(
        FILE, "POST", "https://upload.test/destination", mode="multipart",
        headers={"Content-Type": "multipart/form-data"},
    )
    assert "Content-Type" in bad["detail"]

    bad = await server._execute_upload(
        {**FILE, "download_url": "http://127.0.0.1/x"},
        "PUT", "https://upload.test/destination",
        transport=httpx.MockTransport(lambda _: httpx.Response(200)),
    )
    assert "HTTPS" in bad["detail"]


@pytest.mark.asyncio
async def test_background_tool_polls_exactly_once(monkeypatch):
    monkeypatch.setattr(server, "AUTO_FOREGROUND_SECONDS", 0.01)
    gate = asyncio.Event()
    calls = []
    async def fake_upload(*args):
        calls.append(args)
        await gate.wait()
        return {"status_code": 204, "upload": {"size_bytes": 5}}

    monkeypatch.setattr(server, "_execute_upload", fake_upload)
    async with server.mcp._lifespan_manager(), Client(server.mcp) as client:
        started = await client.call_tool("upload_file", {
            "file": FILE, "method": "PUT",
            "url": "https://upload.test/destination", "execution": "auto",
        })
        payload = json.loads(started.content[0].text)
        assert payload["status"] == "working"
        assert payload["poll_tool"] == "get_request"
        gate.set()
        result = None
        for _ in range(50):
            polled = await client.call_tool("get_request", {"task_id": payload["task_id"]})
            result = json.loads(polled.content[0].text)
            if result["status"] == "completed":
                break
            await asyncio.sleep(0.01)
        assert result["status"] == "completed"
        assert result["result"]["status_code"] == 204
        assert len(calls) == 1
        assert calls[0][0]["file_id"] == FILE["file_id"]
