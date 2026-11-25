# Freedo

Freedo is a provider-agnostic HTTP API gateway for trusted AI agents, exposed through MCP. The agent reads the provider's documentation and builds requests; Freedo substitutes stored credentials, executes HTTP, and returns results.

## Quick start

### Deploy and configure

Connect this repository to [Prefect Horizon](https://horizon.prefect.io), use `mcp_server.py:mcp` as the entrypoint, and enable managed authentication. Horizon installs dependencies from [pyproject.toml](pyproject.toml).

Add one environment variable per credential with an `APIKEY_` prefix. For example, set `APIKEY_GITHUB` to:

```json
{
  "key": "ghp_your_token",
  "desc": "GitHub REST API. Documentation: https://docs.github.com/en/rest"
}
```

`key` must be a nonempty string. `desc` is optional; use it for documentation links and granted scopes, keeping secrets in `key`. Connect the deployed MCP to your agent and call `list_env()` to discover credential names and descriptions.

Freedo lets callers choose network destinations and attach configured credentials. Connect only trusted agents; the agent is responsible for choosing the provider's endpoints, authentication scheme, and required permissions.

### Send a request

Use `{{env:APIKEY_GITHUB}}` in place of the secret. By default, `http_request` accepts the complete request as Base64-encoded UTF-8 JSON in `payload`. This Python snippet prints the tool arguments for an authenticated GitHub request:

```python
import base64
import json

request = {
    "method": "GET",
    "url": "https://api.github.com/user",
    "headers": {"Authorization": "Bearer {{env:APIKEY_GITHUB}}"},
}
payload = base64.b64encode(json.dumps(request).encode("utf-8")).decode("ascii")
print(json.dumps({"payload": payload}))
```

Pass the printed object to `http_request`. Keep all HTTP options, including `execution`, timeouts, and size limits, inside the encoded request. Do not mix `payload` with plaintext HTTP arguments. Invalid envelopes return `invalid_request` without sending HTTP.

Plaintext calls are also supported: explicitly set `base64=false`, supply the HTTP arguments directly, and omit `payload`. Base64 is encoding, not encryption, and adds about one third to the input size.

## Tools

| Tool | Purpose |
| --- | --- |
| `list_env()` | List credential names and descriptions without returning secret values. |
| `http_request(...)` | Send an HTTP request with optional credential substitution. |
| `upload_file(file, method, url, ...)` | Stream a ChatGPT attachment to an HTTP API. |
| `get_request(task_id)` | Poll a background request and retrieve its result. |
| `read_result(result_ref, offset=0, limit_bytes=2097152)` | Read a cached response by byte offset. |

## Requests and responses

Credential placeholders work in URLs, header values, query parameters, text/form/JSON body values, and Basic Auth fields. For Basic Auth, provide `basic_auth` with `username` and `password`; omit the `Authorization` header.

Choose one request body field:

- `text_body`: text, including serialized JSON with `Content-Type: application/json`.
- `form_body`: form fields.
- `body_base64`: the Base64-encoded binary HTTP body. This is separate from the request envelope; its decoded bytes do not undergo credential substitution.

`json_body` remains accepted for compatibility but is deprecated.

HTTP results are returned as one MCP text item containing JSON:

```json
{"status_code":200,"body":{"login":"example"},"body_format":"json","size_bytes":19}
```

`body_format` is `json` for parsed JSON media types, `text` for other decodable content or malformed JSON, and `base64` for binary content. Set `include_response_headers=true` when you need headers such as `Link`, `Location`, or `Retry-After`.

HTTP 4xx/5xx responses are returned as HTTP results; redirects are not followed. Validation and transport failures return an `error` field. Credential strings used in a request are redacted when echoed in text or JSON responses.

### Size limits

| Limit | Default | Maximum |
| --- | --- | --- |
| HTTP request body (`max_request_bytes`) | 2 MiB | 4 MiB |
| Inline response threshold (`max_response_bytes`) | 2 MiB | 4 MiB |
| Total response (`max_total_response_bytes`) | 512 MiB | 512 MiB |
| Requested page size (`read_result.limit_bytes`) | 2 MiB | 4 MiB |
| Attachment upload (`upload_file`) | — | 512 MB (512,000,000 bytes) |

The request body limit is measured after credential substitution and encoding. Responses above the inline threshold are cached for pagination; exceeding the total response limit returns `response_too_large`. Available cache memory can impose a lower limit and return `resource_limit`.

### Background requests

Both `http_request` and `upload_file` accept `execution`:

| Mode | Return behavior | Default `http_request` timeout |
| --- | --- | --- |
| `auto` (default) | Wait up to 15 seconds, then return a `task_id` if still running. | 600 seconds |
| `direct` | Wait for the result. | 60 seconds |
| `background` | Return a `task_id` immediately. | 600 seconds |

The request continues after a task ID is returned. Poll `get_request(task_id)` at the returned `poll_interval_seconds` (currently 5 seconds) while `status` is `working`; a completed response contains the HTTP result in `result`. Uploads default to a 600-second timeout in every mode. Set `timeout` to override these defaults. HTTPX applies it to network operations; total elapsed time can be longer.

Task state is process-local by default. For task lookup across Horizon instances, configure the same shared Redis/Valkey backend and Docket queue name on every instance. Set this environment variable separately from `APIKEY_*` credentials:

```text
FASTMCP_DOCKET_URL=rediss://USER:PASSWORD@REDIS_HOST:PORT/0
```

Replace the placeholders with your provider's connection details and keep the URL secret. Redis shares task state; it does not guarantee exactly-once execution after a worker crash or share the paged response cache.

If `get_request` returns `TASK_NOT_FOUND`, the upstream outcome is unknown. Retry the same task ID at most twice more, at least five seconds apart, then stop polling. Verify upstream state before resubmitting a request that may have side effects.

### Paged responses

When an HTTP result contains `paged=true`, pass its `result_ref` to `read_result`. Start at offset zero and use each page's `next_offset` until `has_more=false`. Offsets count bytes; pages may shrink to keep the serialized MCP response below 4 MiB.

Page content is in `data`, with `data_encoding` set to `utf-8` or `base64`. Decode and join the pages before parsing JSON: individual fragments need not be valid JSON.

The response cache is process-local and can lose entries through eviction, restarts, or requests routed to another instance, even with Redis configured. `RESULT_NOT_FOUND` means the cached body is unavailable, not that the original HTTP operation failed. Do not automatically replay a request with side effects to recover it.

## File uploads

`upload_file` accepts a ChatGPT-bound attachment reference in `file`, plus the target `method` and `url`. It takes ordinary tool arguments; the Base64 request envelope applies only to `http_request`.

- `mode="raw"` (default): send the file as the HTTP body. Set its media type with `content_type` or a `Content-Type` header; the header takes precedence.
- `mode="multipart"`: send a multipart form using `field_name` (default `file`) and optional `form_fields`. Freedo generates the multipart `Content-Type`; do not set that header yourself.

`filename` overrides the attachment name. Freedo calculates `Content-Length` in both modes. Optional `expected_sha256` verifies the file before contacting the target API. The result includes `upload.filename`, `upload.size_bytes`, and `upload.sha256` alongside the HTTP response.

Each call downloads the attachment again and streams it through a temporary file. The target API's size limits and upload protocol still apply.

## Local development

From a checkout, with Python 3.12 or newer:

```bash
python -m pip install -e ".[test]"
python -m pytest -q
python mcp_server.py
```

The local HTTP server listens on port 8000; set `PORT` to change it.
