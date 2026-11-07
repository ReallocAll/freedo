"""Freedo: a provider-agnostic HTTP API MCP for trusted agents."""

import base64
import codecs
import hashlib
import json
import logging
import tempfile
from contextlib import nullcontext
from datetime import timedelta
from importlib.metadata import version
import os
import re
from typing import Any, BinaryIO
from uuid import uuid4

import httpx
from fastmcp import FastMCP
from fastmcp.tools import ToolResult
from fastmcp_tasks import TasksExtension
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from result_cache import MAX_STORED_BYTES, ResultCache, ResultNotFoundError, ResourceLimitError

mcp = FastMCP("Freedo", version=version("freedo"))
mcp.add_extension(TasksExtension())

DEFAULT_LIMIT = 2 * 1024 * 1024
MAX_LIMIT = 4 * 1024 * 1024
# Leave room for JSON/MCP framing within Horizon's measured 6 MB response limit.
MAX_TOOL_ENVELOPE = 5_500_000
MAX_PAGE_RESPONSE_BYTES = 4 * 1024 * 1024
DEFAULT_PAGE_BYTES = 2 * 1024 * 1024
MAX_UPLOAD_BYTES = 512_000_000  # 512 MB, decimal; matches the ChatGPT file limit.
AUTO_FOREGROUND_SECONDS = 15
RESULTS = ResultCache()
REFERENCE = re.compile(r"\{\{env:(APIKEY_[A-Za-z0-9_]+)\}\}")
LOGGER = logging.getLogger("freedo.tasks")
INSTANCE_ID = uuid4().hex[:12]
# Report the backend scheme only; never expose a Redis URL or password.
DOCKET_BACKEND = os.environ.get("FASTMCP_DOCKET_URL", "memory://").split(":", 1)[0]


class FreedoError(ValueError):
    """A user-actionable error that contains no credential values."""


class OpenAIFile(BaseModel):
    """ChatGPT-authorized file reference provided by openai/fileParams."""

    model_config = ConfigDict(extra="forbid")
    download_url: str
    file_id: str = Field(min_length=1)
    mime_type: str | None = None
    file_name: str | None = None


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
                  limit: int, status_code: int, source_size_bytes: int,
                  headers: dict[str, str] | None) -> dict[str, Any]:
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
            "body_format": body_format,
            "status_code": status_code,
            "source_size_bytes": source_size_bytes,
        }
        if headers is not None:
            payload["headers"] = headers
        if payload["size_limit_adjusted"]:
            payload["size_limit_reason"] = ("mcp_response_budget" if count < requested
                                            else "utf8_boundary")
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


def _redact_stream(source, dest, used: set[str]) -> None:
    """Bounded-memory credential replacement across read boundaries."""
    values = set()
    for secret in used:
        values.add(secret.encode("utf-8"))
        values.add(json.dumps(secret, ensure_ascii=True)[1:-1].encode("ascii"))
    values.discard(b"")
    if not values:
        while chunk := source.read(1024 * 1024):
            dest.write(chunk)
        return
    patterns = sorted(values, key=len, reverse=True)
    matcher = re.compile(b"|".join(re.escape(pattern) for pattern in patterns))
    overlap = max(map(len, patterns)) - 1
    pending = b""
    while chunk := source.read(1024 * 1024):
        pending += chunk
        safe = max(0, len(pending) - overlap)
        consumed = 0
        for match in matcher.finditer(pending):
            if match.start() >= safe:
                break
            dest.write(pending[consumed:match.start()])
            dest.write(b"[REDACTED]")
            consumed = match.end()
        if consumed < safe:
            dest.write(pending[consumed:safe])
            pending = pending[safe:]
        else:
            pending = pending[consumed:]
    dest.write(matcher.sub(b"[REDACTED]", pending))


def _materialize_paged(source, encoding: str, media_type: str,
                       used: set[str]) -> tuple[str, bytes]:
    """Redact and validate large responses without full-size intermediate copies."""
    # Avoid a second full-size disk copy when no credential needs redaction.
    with (tempfile.TemporaryFile() if used else nullcontext(source)) as clean:
        source.seek(0)
        if used:
            _redact_stream(source, clean, used)
        clean.seek(0, 2)
        size = clean.tell()
        if size > MAX_STORED_BYTES:
            raise ResourceLimitError("redacted response exceeds maximum storage size")
        clean.seek(0)
        body_format = "base64"
        if encoding.lower().replace("_", "-") in ("utf-8", "utf8", "utf-8-sig"):
            try:
                decoder = codecs.getincrementaldecoder(encoding)("strict")
                while chunk := clean.read(1024 * 1024):
                    decoder.decode(chunk)
                decoder.decode(b"", final=True)
                body_format = ("json" if media_type == "application/json"
                               or media_type.endswith("+json") else "text")
            except (UnicodeError, LookupError):
                pass
        RESULTS.reserve(size + 16 * 1024 * 1024)
        clean.seek(0)
        return body_format, clean.read()


async def _send_http_request(
    client: httpx.AsyncClient, request: httpx.Request, response_limit: int,
    total_limit: int, include_response_headers: bool, used: set[str],
) -> dict[str, Any]:
    """Shared streaming response path for ordinary requests and file uploads."""
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

            media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            if size > response_limit:
                body_format, safe_bytes = _materialize_paged(
                    output, response.encoding or "utf-8", media_type, used
                )
                value = None
            else:
                output.seek(0)
                body_format, safe_bytes, value = _safe_body(
                    output.read(), response.encoding or "utf-8", media_type,
                    used, parse_json=True
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

            return await _send_http_request(
                client, request, response_limit, total_limit, include_response_headers, used
            )
    except FreedoError as exc:
        return {"error": "invalid_request", "detail": str(exc)}
    except (MemoryError, ResourceLimitError):
        return {"error": "resource_limit", "detail": "insufficient memory"}
    except (httpx.HTTPError, ValueError, TypeError) as exc:
        # Exceptions can contain the expanded URL, including an API key in its path.
        return {"error": "http_error", "type": type(exc).__name__}



async def _download_attachment(
    client: httpx.AsyncClient, file: OpenAIFile, output: BinaryIO,
) -> tuple[int, str]:
    """Stream the original bytes of a ChatGPT attachment into a temporary file."""
    if httpx.URL(file.download_url).scheme != "https":
        raise FreedoError("file.download_url must use HTTPS")
    digest = hashlib.sha256()
    size = 0
    async with client.stream(
        "GET", file.download_url, headers={"Accept-Encoding": "identity"}
    ) as response:
        if response.status_code in (401, 403, 404, 410):
            raise FreedoError("FILE_AUTHORIZATION_EXPIRED")
        if response.status_code != 200:
            raise FreedoError("FILE_SOURCE_UNAVAILABLE")
        declared = response.headers.get("content-length")
        if declared is not None:
            try:
                announced = int(declared)
            except ValueError as exc:
                raise FreedoError("Invalid attachment Content-Length") from exc
            if announced < 0 or announced > MAX_UPLOAD_BYTES:
                raise FreedoError("file_too_large")
        async for chunk in response.aiter_raw(chunk_size=65536):
            if size + len(chunk) > MAX_UPLOAD_BYTES:
                raise FreedoError("file_too_large")
            output.write(chunk)
            digest.update(chunk)
            size += len(chunk)
    if declared is not None and size != announced:
        raise FreedoError("file_incomplete")
    output.seek(0)
    return size, digest.hexdigest()


async def _raw_file_chunks(source: BinaryIO):
    """Stream bounded file chunks without constructing a bytes request body."""
    while chunk := source.read(65536):
        yield chunk


async def _execute_upload(
    file: dict[str, Any], method: str, url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    mode: str = "raw",
    field_name: str = "file",
    form_fields: dict[str, str] | None = None,
    filename: str | None = None,
    content_type: str | None = None,
    expected_sha256: str | None = None,
    basic_auth: dict[str, str] | None = None,
    timeout: float = 600,
    max_response_bytes: int = DEFAULT_LIMIT,
    include_response_headers: bool = False,
    max_total_response_bytes: int = MAX_STORED_BYTES,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Any]:
    """Download and forward an attachment within one Docket execution."""
    try:
        response_limit = _limit(max_response_bytes, "max_response_bytes")
        total_limit = _limit(max_total_response_bytes, "max_total_response_bytes", MAX_STORED_BYTES)
        if timeout <= 0:
            raise FreedoError("timeout must be positive")
        if mode not in ("raw", "multipart"):
            raise FreedoError("mode must be raw or multipart")
        if not field_name:
            raise FreedoError("field_name cannot be empty")
        if expected_sha256 is not None and not re.fullmatch(r"[a-f0-9]{64}", expected_sha256):
            raise FreedoError("expected_sha256 must be a lowercase SHA-256 hex digest")
        if filename is not None and not filename:
            raise FreedoError("filename cannot be empty")
        file_ref = OpenAIFile.model_validate(file)
        used: set[str] = set()
        url = _inject(url, used)
        headers = _inject(headers or {}, used)
        params = _inject(params, used) if params is not None else None
        form_fields = _inject(form_fields, used) if form_fields is not None else None
        if "content-length" in {k.lower() for k in headers}:
            raise FreedoError("Content-Length is calculated automatically for file uploads")
        if mode == "multipart" and "content-type" in {k.lower() for k in headers}:
            raise FreedoError("Multipart Content-Type is calculated automatically")
        if basic_auth is not None:
            auth = _inject(basic_auth, used)
            if set(auth) != {"username", "password"} or "authorization" in {k.lower() for k in headers}:
                raise FreedoError("basic_auth requires username/password and no Authorization header")
            raw_auth = f"{auth['username']}:{auth['password']}".encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(raw_auth).decode("ascii")
            used.add(headers["Authorization"])
        with tempfile.TemporaryFile(mode="w+b") as temporary:
            async with httpx.AsyncClient(
                timeout=timeout, follow_redirects=False, transport=transport,
            ) as client:
                size, digest = await _download_attachment(client, file_ref, temporary)
                if expected_sha256 is not None and digest != expected_sha256:
                    return {"error": "CONTENT_MISMATCH"}
                effective_name = filename or file_ref.file_name or "upload"
                media_type = content_type or file_ref.mime_type or "application/octet-stream"
                if mode == "raw":
                    headers.setdefault("Content-Type", media_type)
                    headers["Content-Length"] = str(size)
                    request = client.build_request(
                        method, url, headers=headers, params=params,
                        content=_raw_file_chunks(temporary),
                    )
                else:
                    request = client.build_request(
                        method, url, headers=headers, params=params,
                        data=form_fields, files={
                            field_name: (effective_name, temporary, media_type)
                        },
                    )
                result = await _send_http_request(
                    client, request, response_limit, total_limit,
                    include_response_headers, used,
                )
                result["upload"] = {
                    "filename": effective_name, "size_bytes": size, "sha256": digest,
                }
                return result
    except FreedoError as exc:
        return {"error": "upload_failed", "detail": str(exc)}
    except (MemoryError, ResourceLimitError):
        return {"error": "resource_limit", "detail": "insufficient memory"}
    except (httpx.HTTPError, ValueError, TypeError) as exc:
        # Network exception messages can contain signed URLs or credentials.
        return {"error": "http_error", "type": type(exc).__name__}


class HttpRequestPayload(BaseModel):
    """Original http_request parameters represented inside a Base64 JSON envelope."""

    model_config = ConfigDict(extra="forbid")
    method: str
    url: str
    headers: dict[str, str] | None = None
    params: dict[str, Any] | None = None
    json_body: dict[str, Any] | list[Any] | None = None
    text_body: str | None = None
    form_body: dict[str, str] | None = None
    body_base64: str | None = None
    basic_auth: dict[str, str] | None = None
    timeout: float | None = None
    max_request_bytes: int = DEFAULT_LIMIT
    max_response_bytes: int = DEFAULT_LIMIT
    include_response_headers: bool = False
    max_total_response_bytes: int = MAX_STORED_BYTES
    execution: str = "auto"


def _decode_http_payload(payload: str) -> HttpRequestPayload:
    """Parse a Base64-wrapped request without exposing its contents in errors."""
    try:
        raw = base64.b64decode(payload, validate=True)
        return HttpRequestPayload.model_validate(json.loads(raw.decode("utf-8")))
    except (ValueError, UnicodeError, base64.binascii.Error, ValidationError) as exc:
        raise FreedoError("payload must be Base64-encoded UTF-8 JSON with valid http_request arguments") from exc


@mcp.tool(task=True, annotations={
    "readOnlyHint": False, "destructiveHint": True,
    "idempotentHint": False, "openWorldHint": True,
})
async def http_request(
    method: str | None = None,
    url: str | None = None,
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
    base64: bool = False,
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
    larger bodies up to max_total_response_bytes (512 MiB default) are cached
    for read_result pagination. execution is auto (15s front window), direct,
    or background. Direct defaults to 60s HTTP timeout; auto/background to 600s.
    Poll background task IDs with get_request. Docket defaults to process-local
    memory; set FASTMCP_DOCKET_URL to shared Redis/Valkey for cross-instance tasks.
    The paged response cache remains process-local.
    Set base64=true and supply only body_base64 containing the complete
    original http_request arguments as Base64-encoded UTF-8 JSON (including
    method, url, and optional execution). In ordinary mode, body_base64 still
    means the upstream HTTP request body. No plaintext HTTP parameters may
    accompany an encoded envelope. Responses remain plaintext.
    Base64 is encoding, not encryption, and is easy for intermediaries to decode.
    """
    if base64:
        if (method is not None or url is not None
                or any(value is not None for value in (
                    headers, params, json_body, text_body, form_body,
                    basic_auth, timeout,
                ))
                or max_request_bytes != DEFAULT_LIMIT
                or max_response_bytes != DEFAULT_LIMIT
                or include_response_headers
                or max_total_response_bytes != MAX_STORED_BYTES
                or execution != "auto"):
            return ToolResult(content=_format_result({
                "error": "invalid_request",
                "detail": "base64=true requires body_base64 only; omit plaintext HTTP arguments",
            }))
        if body_base64 is None:
            return ToolResult(content=_format_result({
                "error": "invalid_request", "detail": "base64=true requires body_base64",
            }))
        try:
            decoded = _decode_http_payload(body_base64)
        except FreedoError as exc:
            return ToolResult(content=_format_result({
                "error": "invalid_request", "detail": str(exc),
            }))
        method, url = decoded.method, decoded.url
        headers, params, json_body = decoded.headers, decoded.params, decoded.json_body
        text_body, form_body, body_base64 = decoded.text_body, decoded.form_body, decoded.body_base64
        basic_auth, timeout = decoded.basic_auth, decoded.timeout
        max_request_bytes, max_response_bytes = decoded.max_request_bytes, decoded.max_response_bytes
        include_response_headers = decoded.include_response_headers
        max_total_response_bytes, execution = decoded.max_total_response_bytes, decoded.execution
    if not method or not url:
        return ToolResult(content=_format_result({
            "error": "invalid_request", "detail": "method and url are required",
        }))
    if execution not in ("auto", "direct", "background"):
        return ToolResult(content=_format_result({
            "error": "invalid_request",
            "detail": "execution must be auto, direct or background"
        }))
    effective_timeout = (60 if execution == "direct" else 600) if timeout is None else timeout
    arguments = (
        method, url, headers, params, json_body, text_body, form_body,
        body_base64, basic_auth, effective_timeout, max_request_bytes,
        max_response_bytes, include_response_headers, max_total_response_bytes,
    )
    if execution == "direct":
        return ToolResult(content=_format_result(await _execute_request(*arguments)))

    return await _enqueue_task(_run_background_http, arguments, execution)


async def _enqueue_task(runner, arguments: tuple[Any, ...], execution: str) -> ToolResult:
    """Enqueue one HTTP operation; expose a shared polling contract."""
    docket = mcp.docket
    if docket is None:
        return ToolResult(content=_format_result({"error": "background_unavailable"}))
    try:
        execution_ref = await docket.add(runner)(arguments)
    except Exception:
        return ToolResult(content=_format_result({"error": "background_schedule_failed"}))

    task_id = execution_ref.key
    LOGGER.info("task_scheduled task_id=%s instance=%s backend=%s",
                task_id, INSTANCE_ID, DOCKET_BACKEND)
    if execution == "auto":
        try:
            # Waiting does not cancel or resubmit the scheduled Docket execution.
            result = await execution_ref.get_result(
                timeout=timedelta(seconds=AUTO_FOREGROUND_SECONDS)
            )
            return ToolResult(content=_format_result(result))
        except TimeoutError:
            pass
        except Exception:
            return ToolResult(content=_format_result({
                "error": "background_execution_failed", "task_id": task_id,
                "origin_instance_id": INSTANCE_ID, "docket_backend": DOCKET_BACKEND,
                "outcome_unknown": True,
                "next_action": "Inspect the task with get_request; do not automatically replay the HTTP request.",
            }))
    payload = {
        "task_id": task_id, "status": "working", "poll_tool": "get_request",
        "poll_interval_seconds": 5,
        "origin_instance_id": INSTANCE_ID, "docket_backend": DOCKET_BACKEND,
    }
    if DOCKET_BACKEND == "memory":
        payload["warning"] = (
            "This task is stored in process-local memory. Horizon may route later polls "
            "to another process; use shared Redis/Valkey via FASTMCP_DOCKET_URL for reliable task lookup."
        )
    return ToolResult(content=_format_result(payload))


async def _run_background_http(arguments: tuple[Any, ...]) -> dict[str, Any]:
    """Docket worker entrypoint; credentials are resolved only during execution."""
    return await _execute_request(*arguments)



async def _run_background_upload(arguments: tuple[Any, ...]) -> dict[str, Any]:
    """Download and forward on the same worker; never share temporary paths."""
    return await _execute_upload(*arguments)


@mcp.tool(meta={"openai/fileParams": ["file"]}, task=True, annotations={
    "readOnlyHint": False, "destructiveHint": True,
    "idempotentHint": False, "openWorldHint": True,
})
async def upload_file(
    file: OpenAIFile,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    mode: str = "raw",
    field_name: str = "file",
    form_fields: dict[str, str] | None = None,
    filename: str | None = None,
    content_type: str | None = None,
    expected_sha256: str | None = None,
    basic_auth: dict[str, str] | None = None,
    timeout: float | None = None,
    max_response_bytes: int = DEFAULT_LIMIT,
    include_response_headers: bool = False,
    max_total_response_bytes: int = MAX_STORED_BYTES,
    execution: str = "auto",
) -> ToolResult:
    """Upload a ChatGPT attachment directly to any HTTP API (raw/multipart).

    The file reference is bound by ChatGPT; no file bytes pass through MCP.
    Downloads up to 512 MB into an automatically removed temporary file, then
    streams it upstream without caching across Horizon instances.
    Use {{env:APIKEY_NAME}} in the URL, headers, params, or form fields.
    As with http_request, poll long jobs via get_request(task_id).
    """
    if execution not in ("auto", "direct", "background"):
        return ToolResult(content=_format_result({
            "error": "invalid_request", "detail": "execution must be auto, direct or background",
        }))
    effective_timeout = 600 if timeout is None else timeout
    arguments = (
        file.model_dump(), method, url, headers, params, mode, field_name, form_fields,
        filename, content_type, expected_sha256, basic_auth, effective_timeout,
        max_response_bytes, include_response_headers, max_total_response_bytes,
    )
    if execution == "direct":
        return ToolResult(content=_format_result(await _execute_upload(*arguments)))
    return await _enqueue_task(_run_background_upload, arguments, execution)


@mcp.tool()
async def get_request(task_id: str) -> ToolResult:
    """Poll a Docket HTTP execution. TASK_NOT_FOUND: retry twice total, then STOP; outcome unknown, never replay automatically."""
    docket = mcp.docket
    if docket is None:
        return ToolResult(content=_format_result({
            "error": "background_unavailable", "task_id": task_id,
            "next_action": "Stop polling: the background task service is unavailable.",
        }))
    try:
        execution_ref = await docket.get_execution(task_id)
    except Exception as exc:
        LOGGER.warning("task_lookup_failed task_id=%s instance=%s error_type=%s",
                       task_id, INSTANCE_ID, type(exc).__name__)
        return ToolResult(content=_format_result({
            "error": "background_lookup_failed", "task_id": task_id,
            "error_type": type(exc).__name__, "status": "unknown",
            "serving_instance_id": INSTANCE_ID,
            "next_action": "Retry the same task ID briefly; if lookups still fail, stop and report an unknown outcome. Do not replay the original request automatically.",
        }))
    if execution_ref is None:
        LOGGER.warning("task_not_found task_id=%s instance=%s backend=%s",
                       task_id, INSTANCE_ID, DOCKET_BACKEND)
        message = (
            "This process has no record of the task. In-memory Docket state is not "
            "shared between Horizon instances and is lost on process shutdown."
            if DOCKET_BACKEND == "memory" else
            "The configured Docket backend has no record of this task; it may be "
            "invalid, expired, or removed."
        )
        return ToolResult(content=_format_result({
            "error": "TASK_NOT_FOUND", "task_id": task_id,
            "status": "unknown", "outcome_unknown": True,
            "serving_instance_id": INSTANCE_ID, "docket_backend": DOCKET_BACKEND,
            "message": message + " The upstream HTTP request may still have executed.",
            "retry_after_seconds": 5, "max_additional_polls": 2,
            "do_not_resubmit_automatically": True,
            "next_action": (
                "After the FIRST TASK_NOT_FOUND, retry get_request with this SAME task_id "
                "at most twice TOTAL, at least 5 seconds apart. Do not reset this budget "
                "on another TASK_NOT_FOUND. If still missing, STOP polling and report the original HTTP outcome "
                "as UNKNOWN. Never re-send a potentially non-idempotent request without "
                "checking upstream state or obtaining user approval."
            ),
        }))
    status = execution_ref.state.value
    if status in ("scheduled", "queued", "running"):
        return ToolResult(content=_format_result({
            "task_id": task_id, "status": "working", "docket_state": status,
            "serving_instance_id": INSTANCE_ID, "poll_interval_seconds": 5,
        }))
    if status != "completed":
        return ToolResult(content=_format_result({
            "task_id": task_id, "status": "failed", "docket_state": status,
            "serving_instance_id": INSTANCE_ID,
        }))
    try:
        result = await execution_ref.get_result()
    except Exception as exc:
        LOGGER.warning("task_result_failed task_id=%s instance=%s error_type=%s",
                       task_id, INSTANCE_ID, type(exc).__name__)
        return ToolResult(content=_format_result({
            "task_id": task_id, "status": "failed",
            "error": "background_result_unavailable", "error_type": type(exc).__name__,
            "serving_instance_id": INSTANCE_ID,
        }))
    return ToolResult(content=_format_result({
        "task_id": task_id, "status": "completed", "result": result,
        "serving_instance_id": INSTANCE_ID,
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
            page = _page_payload(
                result_ref, item.data, item.body_format, offset, limit_bytes,
                item.status_code, item.size_bytes, item.headers
            )
            return ToolResult(content=_format_result(page))
    except ResultNotFoundError:
        return ToolResult(content=_format_result({"error": "RESULT_NOT_FOUND"}))
    except FreedoError as exc:
        return ToolResult(content=_format_result({"error": "invalid_request", "detail": str(exc)}))
