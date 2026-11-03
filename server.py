"""Freedo: a provider-agnostic HTTP API MCP for trusted agents."""

import asyncio
import base64
import json
import secrets
import tempfile
from importlib.metadata import version
import os
import re
from typing import Any

import httpx
from fastmcp import FastMCP
from fastmcp.tools import ToolResult

from result_cache import MAX_STORED_BYTES, ResultCache, ResultNotFoundError, ResourceLimitError

mcp = FastMCP("Freedo", version=version("freedo"))

DEFAULT_LIMIT = 2 * 1024 * 1024
MAX_LIMIT = 4 * 1024 * 1024
# Leave room for JSON/MCP framing within Horizon's measured 6 MB response limit.
MAX_TOOL_ENVELOPE = 5_500_000
MAX_PAGE_RESPONSE_BYTES = 4 * 1024 * 1024
DEFAULT_PAGE_BYTES = 2 * 1024 * 1024
AUTO_FOREGROUND_SECONDS = 15
RESULTS = ResultCache()
_JOBS: dict[str, asyncio.Task[dict[str, Any]]] = {}
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


def _limit(value: int, name: str, maximum: int = MAX_LIMIT) -> int:
    if type(value) is not int or not 0 < value <= maximum:
        raise FreedoError(f"{name} must be between 1 and {maximum} bytes")
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



def _framed_size(payload: dict[str, Any]) -> int:
    """Count the serialized JSON-RPC result, including the MCP text wrapper."""
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    envelope = {
        "jsonrpc": "2.0", "id": "0" * 64,
        "result": {"content": [{"type": "text", "text": text}], "isError": False},
    }
    return len(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _safe_body(body: bytes, encoding: str, media_type: str, used: set[str],
               parse_json: bool) -> tuple[str, bytes, Any | None]:
    """Redact before caching; small JSON may also be returned as a native value."""
    try:
        decoded = body.decode(encoding)
    except (UnicodeDecodeError, LookupError):
        result = body
        for secret in sorted(used, key=len, reverse=True):
            result = result.replace(secret.encode("utf-8"), b"[REDACTED]")
        return "base64", result, None

    json_type = media_type == "application/json" or media_type.endswith("+json")
    if json_type and parse_json:
        try:
            value = _redact(json.loads(decoded), used)
        except json.JSONDecodeError:
            pass
        else:
            safe = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            return "json", safe, value

    decoded = _redact(decoded, used)
    if json_type:
        # JSON string escaping may hide a secret from a literal replacement.
        for secret in sorted(used, key=len, reverse=True):
            decoded = decoded.replace(json.dumps(secret, ensure_ascii=True)[1:-1], "[REDACTED]")
    return ("json" if json_type and not parse_json else "text"), decoded.encode("utf-8"), None


def _paged_result(status_code: int, size_bytes: int, body_format: str,
                  data: bytes, headers: dict[str, str] | None) -> dict[str, Any]:
    reference = RESULTS.put(data, body_format, status_code, size_bytes, headers)
    result: dict[str, Any] = {
        "status_code": status_code, "size_bytes": size_bytes,
        "body_format": body_format, "paged": True,
        "result_ref": reference, "stored_bytes": len(data),
    }
    if headers is not None:
        result["headers"] = headers
    return result


def _page_payload(reference: str, data: bytes, body_format: str, offset: int,
                  limit: int) -> dict[str, Any]:
    remaining = len(data) - offset
    requested = min(limit, remaining)

    def candidate(count: int) -> tuple[dict[str, Any], int]:
        chunk = data[offset:offset + count]
        if body_format != "base64":
            # Never return partial UTF-8 sequences. next_offset remains exact.
            while chunk:
                try:
                    value = chunk.decode("utf-8")
                    break
                except UnicodeDecodeError as exc:
                    if exc.reason != "unexpected end of data":
                        raise FreedoError("offset must be a UTF-8 boundary")
                    chunk = chunk[:exc.start]
            else:
                value = ""
        else:
            value = base64.b64encode(chunk).decode("ascii")
        actual = len(chunk)
        payload = {
            "result_ref": reference, "offset": offset,
            "requested_bytes": limit, "returned_bytes": actual,
            "total_bytes": len(data), "data": value,
            "data_encoding": "base64" if body_format == "base64" else "utf-8",
            "size_limit_adjusted": actual < requested,
            "has_more": offset + actual < len(data),
            "next_offset": offset + actual if offset + actual < len(data) else None,
            "mcp_response_bytes": 0,
        }
        if payload["size_limit_adjusted"]:
            payload["size_limit_reason"] = "mcp_response_budget"
        for _ in range(3):
            size = _framed_size(payload)
            if payload["mcp_response_bytes"] == size:
                break
            payload["mcp_response_bytes"] = size
        return payload, _framed_size(payload)

    result, total = candidate(requested)
    # Keep a small allowance for transport-specific framing outside Freedo.
    budget = MAX_PAGE_RESPONSE_BYTES - 8192
    if total < budget:
        return result
    low, high = 1, requested
    best: dict[str, Any] | None = None
    while low <= high:
        mid = (low + high) // 2
        trial, total = candidate(mid)
        if total < budget and trial["returned_bytes"]:
            best = trial
            low = mid + 1
        else:
            high = mid - 1
    if best is None:
        raise FreedoError("page cannot fit the MCP response budget")
    return best


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
    max_total_response_bytes: int = MAX_STORED_BYTES,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Any]:
    """The HTTP execution core; transport is injected only by local tests."""
    try:
        request_limit = _limit(max_request_bytes, "max_request_bytes")
        response_limit = _limit(max_response_bytes, "max_response_bytes")
        total_limit = _limit(max_total_response_bytes, "max_total_response_bytes", MAX_STORED_BYTES)
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
                with tempfile.TemporaryFile() as output:
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > total_limit:
                            return {
                                "error": "response_too_large", "limit_bytes": total_limit,
                                "status_code": response.status_code,
                            }
                        output.write(chunk)

                    # Reserve before materializing the buffered response, not afterwards.
                    if size > response_limit:
                        RESULTS.reserve(3 * size + 16 * 1024 * 1024)
                    output.seek(0)
                    body = output.read()

                media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                body_format, safe_bytes, value = _safe_body(
                    body, response.encoding or "utf-8", media_type, used,
                    parse_json=size <= response_limit
                )
                safe_headers = (_redact(dict(response.headers), used)
                                if include_response_headers else None)

                if size <= response_limit:
                    if body_format == "base64":
                        value = base64.b64encode(safe_bytes).decode("ascii")
                    elif value is None and body_format != "json":
                        value = safe_bytes.decode("utf-8")
                    elif value is None and body_format == "json":
                        # JSON null is a valid native value; distinguish it from
                        # text fallback by parsing the already sanitized document.
                        try:
                            value = json.loads(safe_bytes)
                        except json.JSONDecodeError:
                            body_format = "text"
                            value = safe_bytes.decode("utf-8")
                    inline: dict[str, Any] = {
                        "status_code": response.status_code, "body": value,
                        "body_format": body_format, "size_bytes": size,
                    }
                    if safe_headers is not None:
                        inline["headers"] = safe_headers
                    if _framed_size(inline) < MAX_TOOL_ENVELOPE:
                        return inline
                return _paged_result(response.status_code, size, body_format,
                                     safe_bytes, safe_headers)
            finally:
                await response.aclose()
    except FreedoError as exc:
        return {"error": "invalid_request", "detail": str(exc)}
    except (MemoryError, ResourceLimitError):
        return {"error": "resource_limit", "detail": "insufficient memory"}
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
    timeout: float | None = None,
    max_request_bytes: int = DEFAULT_LIMIT,
    max_response_bytes: int = DEFAULT_LIMIT,
    include_response_headers: bool = False,
    max_total_response_bytes: int = MAX_STORED_BYTES,
    execution: str = "auto",
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
    Request bodies have a 2-4 MiB limit. The response inline limit is 2-4 MiB;
    larger bodies up to max_total_response_bytes (128 MiB default) are cached
    for read_result pagination. execution is auto (15s front window), direct,
    or background. Direct defaults to 60s HTTP timeout; auto/background to 600s.
    Poll background task IDs with get_request. Jobs and results are process-local.
    """
    if execution not in ("auto", "direct", "background"):
        return ToolResult(content=_format_result({
            "error": "invalid_request",
            "detail": "execution must be auto, direct or background"
        }))
    effective_timeout = (60 if execution == "direct" else 600) if timeout is None else timeout
    coro = _execute_request(
        method, url, headers, params, json_body, text_body, form_body,
        body_base64, basic_auth, effective_timeout, max_request_bytes,
        max_response_bytes, include_response_headers, max_total_response_bytes
    )
    if execution == "direct":
        return ToolResult(content=_format_result(await coro))

    job = asyncio.create_task(coro)
    if execution == "auto":
        try:
            result = await asyncio.wait_for(asyncio.shield(job), AUTO_FOREGROUND_SECONDS)
            return ToolResult(content=_format_result(result))
        except asyncio.TimeoutError:
            pass
    task_id = secrets.token_urlsafe(18)
    _JOBS[task_id] = job
    return ToolResult(content=_format_result({
        "task_id": task_id, "status": "working", "poll_tool": "get_request",
        "poll_interval_seconds": 5,
    }))


@mcp.tool()
def get_request(task_id: str) -> ToolResult:
    """Poll a running HTTP request by task_id; terminal results are consumed once."""
    job = _JOBS.get(task_id)
    if job is None:
        return ToolResult(content=_format_result({"error": "TASK_NOT_FOUND"}))
    if not job.done():
        return ToolResult(content=_format_result({"task_id": task_id, "status": "working"}))
    _JOBS.pop(task_id, None)
    if job.cancelled():
        return ToolResult(content=_format_result({"task_id": task_id, "status": "failed"}))
    try:
        result = job.result()
    except Exception:
        result = {"error": "http_error", "type": "BackgroundExecutionError"}
    return ToolResult(content=_format_result({
        "task_id": task_id, "status": "completed", "result": result,
    }))


@mcp.tool()
def read_result(result_ref: str, offset: int = 0,
                limit_bytes: int = DEFAULT_PAGE_BYTES) -> ToolResult:
    """Read cached HTTP response by byte offset. Final MCP result stays below 4 MiB."""
    try:
        _limit(limit_bytes, "limit_bytes")
        if type(offset) is not int or offset < 0:
            raise FreedoError("offset must be a nonnegative integer")
        with RESULTS.borrow(result_ref) as item:
            if offset > len(item.data):
                raise FreedoError("offset exceeds cached result size")
            page = _page_payload(result_ref, item.data, item.body_format, offset, limit_bytes)
            page["body_format"] = item.body_format
            page["status_code"] = item.status_code
            page["source_size_bytes"] = item.size_bytes
            if item.headers is not None:
                page["headers"] = item.headers
            # Budget includes final metadata as well as the page.
            if _framed_size(page) >= MAX_PAGE_RESPONSE_BYTES - 4096:
                raise FreedoError("page metadata exceeds response budget")
            page["mcp_response_bytes"] = _framed_size(page)
            return ToolResult(content=_format_result(page))
    except ResultNotFoundError:
        return ToolResult(content=_format_result({"error": "RESULT_NOT_FOUND"}))
    except FreedoError as exc:
        return ToolResult(content=_format_result({"error": "invalid_request", "detail": str(exc)}))
