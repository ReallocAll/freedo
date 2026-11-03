"""Freedo: a provider-agnostic HTTP API MCP for trusted agents."""

import base64
import json
from importlib.metadata import version
import os
import re
from typing import Any

import httpx
from fastmcp import FastMCP
from fastmcp.tools import ToolResult

mcp = FastMCP("Freedo", version=version("freedo"))

DEFAULT_LIMIT = 2 * 1024 * 1024
MAX_LIMIT = 4 * 1024 * 1024
# Leave room for JSON/MCP framing within Horizon's measured 6 MB response limit.
MAX_TOOL_ENVELOPE = 5_500_000
REFERENCE = re.compile(r"\{\{env:(APIKEY_[A-Za-z0-9_]+)\}\}")


class FreedoError(ValueError):
    """A user-actionable error that contains no credential values."""


def _credential(name: str) -> dict[str, str]:
    raw = os.environ.get(name)
    if not name.startswith("APIKEY_") or raw is None:
        raise FreedoError(f"Unknown credential: {name}")
    try:
        config = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise FreedoError(f"Invalid JSON for {name}") from exc
    if not isinstance(config, dict) or not isinstance(config.get("key"), str) or not config["key"]:
        raise FreedoError(f"{name} must contain a nonempty string 'key'")
    if "desc" in config and not isinstance(config["desc"], str):
        raise FreedoError(f"{name} 'desc' must be a string")
    return config


@mcp.tool
def list_env() -> dict[str, list[dict[str, str]]]:
    """List available APIKEY_* credentials and optional descriptions; never return keys."""
    entries = []
    for name in sorted(n for n in os.environ if n.startswith("APIKEY_")):
        try:
            config = _credential(name)
            entry = {"name": name}
            if "desc" in config:
                entry["desc"] = config["desc"]
        except FreedoError as exc:
            entry = {"name": name, "error": str(exc)}
        entries.append(entry)
    return {"credentials": entries}


def _inject(value: Any, used: set[str]) -> Any:
    """Replace credential references in strings, including nested JSON values."""
    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            key = _credential(match.group(1))["key"]
            used.add(key)
            return key
        return REFERENCE.sub(replace, value)
    if isinstance(value, list):
        return [_inject(item, used) for item in value]
    if isinstance(value, dict):
        return {key: _inject(item, used) for key, item in value.items()}
    return value


def _limit(value: int, name: str) -> int:
    if not isinstance(value, int) or not 0 < value <= MAX_LIMIT:
        raise FreedoError(f"{name} must be between 1 and {MAX_LIMIT} bytes")
    return value


def _redact(value: Any, used: set[str]) -> Any:
    """Redact credential echoes in strings and nested JSON keys and values."""
    if isinstance(value, str):
        for secret in sorted(used, key=len, reverse=True):
            value = value.replace(secret, "[REDACTED]")
        return value
    if isinstance(value, list):
        return [_redact(item, used) for item in value]
    if isinstance(value, dict):
        return {_redact(key, used): _redact(item, used) for key, item in value.items()}
    return value


def _format_result(payload: dict[str, Any]) -> str:
    """Return one MCP text item; check framing expansion against Horizon's limit."""
    result = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    framed_bytes = len(json.dumps({"text": result}, ensure_ascii=False).encode("utf-8"))
    if framed_bytes > MAX_TOOL_ENVELOPE:
        return json.dumps({
            "error": "mcp_result_too_large",
            "detail": "Response fits the HTTP limit but not the MCP transport envelope. Retry with a smaller max_response_bytes.",
        })
    return result


async def _execute_request(
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | list[Any] | None = None,
    text_body: str | None = None,
    form_body: dict[str, str] | None = None,
    body_base64: str | None = None,
    basic_auth: dict[str, str] | None = None,
    timeout: float = 60,
    max_request_bytes: int = DEFAULT_LIMIT,
    max_response_bytes: int = DEFAULT_LIMIT,
    include_response_headers: bool = False,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Any]:
    """The HTTP execution core; transport is injected only by local tests."""
    try:
        request_limit = _limit(max_request_bytes, "max_request_bytes")
        response_limit = _limit(max_response_bytes, "max_response_bytes")
        if timeout <= 0:
            raise FreedoError("timeout must be positive")
        if sum(value is not None for value in (json_body, text_body, form_body, body_base64)) > 1:
            raise FreedoError("Choose only one of json_body, text_body, form_body or body_base64")

        used: set[str] = set()
        url = _inject(url, used)
        headers = _inject(headers or {}, used)
        params = _inject(params, used) if params is not None else None
        json_body = _inject(json_body, used)
        text_body = _inject(text_body, used)
        form_body = _inject(form_body, used)
        if basic_auth is not None:
            auth = _inject(basic_auth, used)
            if set(auth) != {"username", "password"} or "authorization" in {k.lower() for k in headers}:
                raise FreedoError("basic_auth requires username/password and no Authorization header")
            raw = f"{auth['username']}:{auth['password']}".encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")
            used.add(headers["Authorization"])

        body_kwargs: dict[str, Any] = {}
        if json_body is not None:
            body_kwargs["json"] = json_body
        elif text_body is not None:
            body_kwargs["content"] = text_body.encode("utf-8")
        elif form_body is not None:
            body_kwargs["data"] = form_body
        elif body_base64 is not None:
            try:
                body_kwargs["content"] = base64.b64decode(body_base64, validate=True)
            except (ValueError, base64.binascii.Error) as exc:
                raise FreedoError("body_base64 must be valid Base64") from exc

        async with httpx.AsyncClient(
            timeout=timeout, follow_redirects=False, transport=transport
        ) as client:
            request = client.build_request(method, url, headers=headers, params=params, **body_kwargs)
            size = len(request.content)
            if size > request_limit:
                return {"error": "request_too_large", "limit_bytes": request_limit, "size_bytes": size}

            response = await client.send(request, stream=True)
            try:
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > response_limit:
                        return {
                            "error": "response_too_large",
                            "limit_bytes": response_limit,
                            "status_code": response.status_code,
                        }
                    chunks.append(chunk)

                body = b"".join(chunks)
                try:
                    decoded = body.decode(response.encoding or "utf-8")
                except (UnicodeDecodeError, LookupError):
                    result_body: Any = base64.b64encode(body).decode("ascii")
                    body_format = "base64"
                else:
                    media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                    if media_type == "application/json" or media_type.endswith("+json"):
                        try:
                            result_body = json.loads(decoded)
                        except json.JSONDecodeError:
                            result_body = decoded
                            body_format = "text"
                        else:
                            body_format = "json"
                    else:
                        result_body = decoded
                        body_format = "text"
                    result_body = _redact(result_body, used)

                result = {
                    "status_code": response.status_code,
                    "body": result_body,
                    "body_format": body_format,
                    "size_bytes": len(body),
                }
                if include_response_headers:
                    result["headers"] = _redact(dict(response.headers), used)
                return result
            finally:
                await response.aclose()
    except FreedoError as exc:
        return {"error": "invalid_request", "detail": str(exc)}
    except (httpx.HTTPError, ValueError, TypeError) as exc:
        # Exceptions can contain the expanded URL, including an API key in its path.
        return {"error": "http_error", "type": type(exc).__name__}


@mcp.tool(annotations={
    "readOnlyHint": False, "destructiveHint": True,
    "idempotentHint": False, "openWorldHint": True,
})
async def http_request(
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | list[Any] | None = None,
    text_body: str | None = None,
    form_body: dict[str, str] | None = None,
    body_base64: str | None = None,
    basic_auth: dict[str, str] | None = None,
    timeout: float = 60,
    max_request_bytes: int = DEFAULT_LIMIT,
    max_response_bytes: int = DEFAULT_LIMIT,
    include_response_headers: bool = False,
) -> ToolResult:
    """Make any HTTP request. Use {{env:APIKEY_NAME}} in URL, headers, query or body.

    Consult list_env to discover configured credentials. Construct service-specific
    URLs, HTTP methods and auth headers using the provider's official API docs.
    basic_auth accepts username/password strings with the same reference syntax.
    Bodies are mutually exclusive: json_body, text_body, form_body or body_base64.
    Response is a single JSON text item with status_code, body, body_format
    (json/text/base64), and size_bytes. JSON media types return native JSON
    values; malformed JSON falls back to text. Use include_response_headers
    to return headers for pagination, redirects, caching, or diagnostics.
    HTTP 4xx/5xx are returned normally; redirects are never followed.
    Body limits default to 2 MiB and can be raised to 4 MiB.
    """
    result = await _execute_request(
        method, url, headers, params, json_body, text_body, form_body,
        body_base64, basic_auth, timeout, max_request_bytes, max_response_bytes,
        include_response_headers
    )
    return ToolResult(content=_format_result(result))

