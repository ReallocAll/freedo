"""Focused behavior tests for Freedo's public MCP contract."""
import asyncio
import base64
import json

import httpx

from importlib.metadata import version

from mcp_server import mcp
from server import DEFAULT_LIMIT, MAX_LIMIT, _execute_request, _format_result, list_env


def run(**kwargs):
    return asyncio.run(_execute_request(**kwargs))


def test_discovery_exposes_description_not_key(monkeypatch):
    monkeypatch.setenv("APIKEY_SAMPLE", json.dumps({"key": "s3cret", "desc": "Test API"}))
    monkeypatch.setenv("APIKEY_BROKEN", "not-json")
    result = list_env()
    sample = next(x for x in result["credentials"] if x["name"] == "APIKEY_SAMPLE")
    broken = next(x for x in result["credentials"] if x["name"] == "APIKEY_BROKEN")
    assert sample == {"name": "APIKEY_SAMPLE", "desc": "Test API"}
    assert broken["error"] and "not-json" not in str(result)
    assert "s3cret" not in str(result)


def test_http_request_and_nested_credential_injection(monkeypatch):
    monkeypatch.setenv("APIKEY_SAMPLE", '{"key":"my-secret-value","desc":"test"}')
    observed = []

    def handle(request):
        observed.append(request)
        data = json.loads(request.content)
        assert request.headers["authorization"] == "Bearer my-secret-value"
        assert request.url.params["query"] == "my-secret-value"
        assert data == {"nested": ["my-secret-value"]}
        return httpx.Response(201, json={"message": "my-secret-value is visible at source"})

    result = run(
        method="POST", url="https://example.test/v1",
        headers={"Authorization": "Bearer {{env:APIKEY_SAMPLE}}"},
        params={"query": "{{env:APIKEY_SAMPLE}}"},
        json_body={"nested": ["{{env:APIKEY_SAMPLE}}"]},
        transport=httpx.MockTransport(handle),
    )
    assert len(observed) == 1
    assert result["status_code"] == 201
    assert result["body_format"] == "json"
    assert "headers" not in result
    assert "my-secret-value" not in json.dumps(result)
    assert "[REDACTED]" in result["body"]["message"]


def test_basic_auth_and_form(monkeypatch):
    monkeypatch.setenv("APIKEY_LOGIN", '{"key":"password123"}')

    def handle(request):
        value = request.headers["authorization"].removeprefix("Basic ")
        assert base64.b64decode(value).decode() == "user:password123"
        assert request.content == b"name=hello+world"
        return httpx.Response(200, json={"authorization": request.headers["authorization"]})

    result = run(
        method="POST", url="https://example.test",
        basic_auth={"username": "user", "password": "{{env:APIKEY_LOGIN}}"},
        form_body={"name": "hello world"},
        transport=httpx.MockTransport(handle),
    )
    assert result["status_code"] == 200
    assert result["body_format"] == "json"
    assert "[REDACTED]" in result["body"]["authorization"]
    assert "cGFzc3dvcmQxMjM=" not in result["body"]["authorization"]


def test_body_limits_and_no_truncated_response():
    calls = []
    def handle(request):
        calls.append(1)
        return httpx.Response(200, content=b"x" * (DEFAULT_LIMIT + 1))

    transport = httpx.MockTransport(handle)
    rejected = run(method="POST", url="https://example.test",
                   text_body="x" * (DEFAULT_LIMIT + 1), transport=transport)
    assert rejected["error"] == "request_too_large"
    assert not calls

    rejected = run(method="GET", url="https://example.test", transport=transport)
    assert rejected["paged"] is True
    assert rejected["size_bytes"] == DEFAULT_LIMIT + 1
    assert "result_ref" in rejected

    accepted = run(method="GET", url="https://example.test", transport=transport,
                   max_response_bytes=MAX_LIMIT)
    assert accepted["size_bytes"] == DEFAULT_LIMIT + 1
    assert accepted["body"] == "x" * (DEFAULT_LIMIT + 1)
    assert accepted["body_format"] == "text"

    overflow = run(method="GET", url="https://example.test", transport=transport,
                   max_response_bytes=MAX_LIMIT + 1)
    assert overflow["error"] == "invalid_request"


def test_redirect_not_followed():
    calls = []
    def handle(request):
        calls.append(request.url.path)
        return httpx.Response(302, headers={"Location": "https://elsewhere.test/"})
    result = run(method="GET", url="https://example.test/start",
                 transport=httpx.MockTransport(handle))
    assert calls == ["/start"]
    assert result["status_code"] == 302


def test_unknown_credential_and_network_errors_do_not_leak(monkeypatch):
    invalid = run(method="GET", url="https://example.test",
                  headers={"X-Key": "{{env:APIKEY_NOT_CONFIGURED}}"})
    assert invalid["error"] == "invalid_request"
    monkeypatch.setenv("APIKEY_SECRET", '{"key":"sensitive-123456"}')
    def fail(request):
        raise httpx.ConnectError("Cannot reach https://sensitive-123456.example.test")
    result = run(method="GET", url="https://{{env:APIKEY_SECRET}}.example.test",
                 transport=httpx.MockTransport(fail))
    assert result == {"error": "http_error", "type": "ConnectError"}


def test_mcp_framing_expansion():
    huge = _format_result({"body": '"' * (MAX_LIMIT - 10)})
    assert json.loads(huge)["error"] == "mcp_result_too_large"


def test_mcp_tools_registered():
    from fastmcp import Client

    async def check():
        async with Client(mcp, mode="legacy") as client:
            assert client.server_info.name == "Freedo"
            assert client.server_info.version == version("freedo") == "0.3.0"
            names = {tool.name for tool in await client.list_tools()}
            assert names == {"list_env", "http_request", "get_request", "read_result"}
            response = await client.call_tool("list_env", {})
            assert not response.is_error
            result = await client.call_tool("http_request", {
                "method": "GET", "url": "https://example.test",
                "max_response_bytes": 0,
            })
            assert not result.is_error
            assert result.structured_content is None
            assert len(result.content) == 1
            assert json.loads(result.content[0].text)["error"] == "invalid_request"

    asyncio.run(check())


def test_github_rest_and_graphql_with_generic_credentials(monkeypatch):
    monkeypatch.setenv(
        "APIKEY_GITHUB",
        '{"key":"github-test-token","desc":"GitHub REST and GraphQL APIs"}',
    )
    def handle(request):
        assert request.headers["authorization"] == "Bearer github-test-token"
        if request.url.path == "/user":
            assert request.method == "GET"
            return httpx.Response(200, json={"login":"sample-user"})
        assert request.method == "POST"
        assert request.url.path == "/graphql"
        assert json.loads(request.content) == {"query": "query { viewer { login } }"}
        return httpx.Response(200, json={"data": {"viewer": {"login": "sample-user"}}})

    transport = httpx.MockTransport(handle)
    rest = run(
        method="GET", url="https://api.github.com/user",
        headers={"Authorization": "Bearer {{env:APIKEY_GITHUB}}"},
        transport=transport
    )
    graphql = run(
        method="POST", url="https://api.github.com/graphql",
        headers={"Authorization": "Bearer {{env:APIKEY_GITHUB}}"},
        json_body={"query": "query { viewer { login } }"},
        transport=transport
    )
    assert rest["status_code"] == 200
    assert rest["body"]["login"] == "sample-user"
    assert rest["body_format"] == "json"
    assert graphql["status_code"] == 200
    assert graphql["body"]["data"]["viewer"]["login"] == "sample-user"
    assert graphql["body_format"] == "json"


def test_base64_raw_body():
    def handle(request):
        assert request.content == b"\x00\x01\xff"
        return httpx.Response(200, content=b"ok")
    result = run(
        method="POST", url="https://example.test",
        body_base64="AAH/",
        transport=httpx.MockTransport(handle),
    )
    assert result["status_code"] == 200
    assert result["body"] == "ok"
    assert result["body_format"] == "text"

def test_json_response_types_and_malformed_json():
    responses = {
        "/object": httpx.Response(200, json={"ready": True}),
        "/array": httpx.Response(400, content=b'[1,{"ok":false}]',
                                 headers={"Content-Type": "application/problem+json"}),
        "/null": httpx.Response(200, content=b"null",
                                headers={"Content-Type": "application/json; charset=utf-8"}),
        "/number": httpx.Response(200, content=b"42",
                                  headers={"Content-Type": "application/json"}),
        "/bool": httpx.Response(200, content=b"false",
                                headers={"Content-Type": "application/json"}),
        "/string": httpx.Response(200, content=b'"hello"',
                                  headers={"Content-Type": "application/json"}),
        "/invalid": httpx.Response(200, content=b'{"incomplete":',
                                   headers={"Content-Type": "application/json"}),
        "/text": httpx.Response(200, content=b'{"not":"json"}',
                                headers={"Content-Type": "text/plain"}),
        "/binary": httpx.Response(200, content=b"\x00\xff",
                                  headers={"Content-Type": "application/octet-stream"}),
    }
    transport = httpx.MockTransport(lambda request: responses[request.url.path])
    expected = {
        "/object": ("json", {"ready": True}),
        "/array": ("json", [1, {"ok": False}]),
        "/null": ("json", None),
        "/number": ("json", 42),
        "/bool": ("json", False),
        "/string": ("json", "hello"),
        "/invalid": ("text", '{"incomplete":'),
        "/text": ("text", '{"not":"json"}'),
        "/binary": ("base64", "AP8="),
    }
    for path, (fmt, value) in expected.items():
        result = run(method="GET", url="https://example.test" + path, transport=transport)
        assert result["body_format"] == fmt, path
        assert result["body"] == value, path
        assert "headers" not in result, path
    assert run(method="GET", url="https://example.test/array",
               transport=transport)["status_code"] == 400


def test_json_redaction_and_optional_headers(monkeypatch):
    monkeypatch.setenv("APIKEY_SAMPLE", '{"key":"my-secret-value"}')

    def handle(request):
        assert request.headers["authorization"] == "Bearer my-secret-value"
        return httpx.Response(
            201,
            json={"nested": [{"my-secret-value-key": "prefix my-secret-value"}],
                  "plain": ["safe", {"token": "my-secret-value"}]},
            headers={"X-Secret-Echo": "my-secret-value", "Location": "/created"},
        )

    transport = httpx.MockTransport(handle)
    kwargs = {
        "method": "POST", "url": "https://example.test/resource",
        "headers": {"Authorization": "Bearer {{env:APIKEY_SAMPLE}}"},
        "transport": transport,
    }
    normal = run(**kwargs)
    assert "headers" not in normal
    assert normal["body_format"] == "json"
    assert normal["body"]["nested"] == [{"[REDACTED]-key": "prefix [REDACTED]"}]
    assert normal["body"]["plain"] == ["safe", {"token": "[REDACTED]"}]
    assert "my-secret-value" not in json.dumps(normal)

    detailed = run(**kwargs, include_response_headers=True)
    assert detailed["headers"]["x-secret-echo"] == "[REDACTED]"
    assert detailed["headers"]["location"] == "/created"
    assert detailed["body"] == normal["body"]
    assert "my-secret-value" not in json.dumps(detailed)
