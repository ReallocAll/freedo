# Freedo

**Freedo** is a provider-agnostic HTTP API MCP for trusted AI agents, deployable to Prefect Horizon. It does not include provider SDKs, hard-coded endpoints, or an API discovery engine. The agent reads API documentation and constructs requests; Freedo holds secrets and performs HTTP.

## Configure

Add one Horizon environment variable per credential, with a short name starting with `APIKEY_`:

**Name:** `APIKEY_GITHUB`

**Value:**

```json
{
  "key": "ghp_your_token",
  "desc": "GitHub REST API. Documentation: https://docs.github.com/en/rest"
}
```

`key` is required. `desc` is optional free text that may describe the URL, provider, scopes or documentation. No other fields are interpreted. Keep descriptions free of secrets.

## GitHub API migration (recommended first integration)

Configure Horizon variable **APIKEY_GITHUB** with a GitHub token and description:

```json
{"key":"ghp_your_token","desc":"GitHub REST https://api.github.com; docs https://docs.github.com/en/rest and https://docs.github.com/en/graphql; credential has the scopes I granted."}
```

The tool does not contain GitHub-specific code. Agents should obtain exact paths, request fields and permission requirements from the official API documentation.

REST authenticated identity:

```json
{"method":"GET","url":"https://api.github.com/user","headers":{"Authorization":"Bearer {{env:APIKEY_GITHUB}}","Accept":"application/vnd.github+json"}}
```

GraphQL:

```json
{"method":"POST","url":"https://api.github.com/graphql","headers":{"Authorization":"Bearer {{env:APIKEY_GITHUB}}"},"json_body":{"query":"query { viewer { login } }"}}
```

Trigger a GitHub Actions workflow (write operation, subject to token permissions):

```json
{"method":"POST","url":"https://api.github.com/repos/OWNER/REPO/actions/workflows/WORKFLOW_ID/dispatches","headers":{"Authorization":"Bearer {{env:APIKEY_GITHUB}}"},"json_body":{"ref":"main"}}
```

Unlike the retiring GitHub-specific MCP, Freedo uses a complete upstream URL and an explicit credential reference. Other API providers work exactly the same way. GitHub remains the first integration/test target, **not** an internal provider dependency.

## Tools

- `list_env()` lists configured `APIKEY_*` names and optional descriptions. Secret values are never returned.
- `http_request(method, url, ...)` sends arbitrary HTTP requests and supports `execution="auto"|"direct"|"background"`.
- `get_request(task_id)` polls background HTTP requests. Completed tasks return their result while retained by Docket.
- `read_result(result_ref, offset=0, limit_bytes=2097152)` reads cached large response bodies.

Use a credential reference anywhere in a string value (including nested JSON values):

```json
{
  "method": "GET",
  "url": "https://api.github.com/user",
  "headers": {"Authorization": "Bearer {{env:APIKEY_GITHUB}}"}
}
```

Supported request options: `headers`, `params`, one of `json_body` / `text_body` / `form_body` / `body_base64`, optional `basic_auth` (`username`, `password`), `timeout`, `max_request_bytes`, `max_response_bytes`, `max_total_response_bytes`, `execution`, `include_response_headers` (default `false`). For Basic Auth, put the credential reference in `basic_auth.password`; Freedo constructs the standard Basic header.

Request body size defaults to 2 MiB (maximum 4 MiB); it is measured after credential substitution and encoding. `max_response_bytes` now controls the **inline response threshold** (default 2 MiB; maximum 4 MiB), not the total upstream response cap. Upstream responses may total 512 MiB by default (`max_total_response_bytes`, configurable downward). Large responses are retained for `read_result` rather than returned partially or rejected because they exceed the inline threshold. A response exceeding the total limit still returns `response_too_large`. Binary pages use Base64; all page responses are bounded by the serialized MCP size.

### Response format (v0.2.0)

`http_request` emits one MCP text item containing a JSON object, without duplicate `structuredContent`. Response headers are **omitted by default**:

```json
{"status_code":200,"body":{"login":"example"},"body_format":"json","size_bytes":19}
```

- `body_format="json"`: valid `application/json` or `+json` media types are parsed into native JSON (object, array, string, number, boolean, or null).
- `body_format="text"`: decodable non-JSON content, and malformed JSON, is returned as text.
- `body_format="base64"`: undecodable binary content is Base64 encoded.
- Pass `include_response_headers=true` to include `headers` for `Link` pagination, `Location` redirects, `Retry-After` limits, or diagnostics.

**Breaking change (0.1.x → 0.2.0):** JSON `body` is now parsed, so read `result["body"]["field"]` instead of calling `json.loads(result["body"])`. `body_encoding` becomes `body_format`; `headers` are no longer present by default. No legacy response mode is included.

Large `http_request` outputs use a single MCP `content` text item, without duplicating the JSON under `structuredContent`. `list_env` retains its small structured result for discovery.

HTTP 4xx/5xx status codes are returned as normal HTTP results, not MCP failures. Redirects are not followed. Network errors are returned without exception text because exception text could contain credential-bearing URLs. Exact credentials used in a request are redacted from text and nested JSON string keys/values when echoed by a server.

**Trust boundary:** This is a private, powerful HTTP executor. An authorized agent can choose arbitrary network destinations and attach configured credentials. Enable Horizon's managed authentication and connect only trusted agents. Freedo does not constrain provider hosts, inspect API permissions, or add confirmation workflows.

## Long requests and large bodies (v0.3.0)

`execution="auto"` starts the HTTP request once, waits up to 15 seconds,
and returns a `task_id` if still running. The HTTP request continues
within the Horizon server process; it is **not** reissued. `direct`
always awaits the result, while `background` immediately returns a task ID.
Direct requests default to HTTPX `timeout=60`; automatic/background requests
default to `timeout=600`. An explicit timeout overrides either default.

The inline response threshold defaults to 2 MiB (maximum 4 MiB).
The upstream response can total up to 512 MiB by default, separately bounded
by `max_total_response_bytes`. Large bodies are streamed to temporary files, credential-redacted, then cached in process memory, and
`http_request` / `get_request` returns `paged=true` and `result_ref`.
`read_result` pages by raw **byte offsets** with a 2 MiB default and
4 MiB maximum requested byte count. Every complete serialized MCP page
is strictly below 4 MiB, with a framing allowance. Pages automatically shrink
when Base64 or JSON escaping expands the wire size. The response includes
`requested_bytes`, `returned_bytes`, `mcp_response_bytes`,
`size_limit_adjusted`, `has_more`, and `next_offset`. Use `next_offset`
without assuming a full requested page was returned. UTF-8 pages return text,
binary pages return Base64; JSON fragments are not necessarily valid JSON.

Result caching follows Flarith's process-local memory-pressure LRU:
512 MiB hard response ceiling, subject to the 896 MiB process RSS admission budget; oldest
inactive results are evicted only when memory reservation requires it.
Short read leases prevent eviction during an individual page query.
No TTL, external database, fixed entry count or cross-restart persistence.
A lost result returns `RESULT_NOT_FOUND` and must not trigger automatic
replay of POST/DELETE. By default, Docket task IDs are also process-local;
completed task results are consumed by the first successful `get_request`.

### Task visibility across Horizon instances

Horizon may start, stop, or route requests between different Freedo server
processes **without creating a new Deployment**. The default Docket backend,
`memory://`, cannot share tasks between those processes. A task may therefore
return `working`, then `TASK_NOT_FOUND`, and sometimes `working` again.
This behavior was observed during a long-running Ark conversation experiment.

For reliable task **lookup across processes**, configure a shared Redis/Valkey
backend using FastMCP's built-in setting; no custom Freedo queue is needed.
Add a Horizon Production environment variable (not an `APIKEY_*` credential):

```text
FASTMCP_DOCKET_URL=rediss://USER:PASSWORD@REDIS_HOST:PORT/0
```

Use an actual Redis/Valkey connection string supplied by your provider.
Keep it secret: it contains credentials. Every Freedo instance must use the
same URL (and Docket queue name). Redis makes task state shared, but does not
guarantee exactly-once execution after a worker crash: never automatically
replay non-idempotent HTTP requests. The **large-response cache is still
process-local**, so Redis does not yet solve cross-instance
`RESULT_NOT_FOUND` for paged bodies.

For background requests, Freedo now returns `origin_instance_id` and
`docket_backend`. `get_request` reports the serving instance, and
`TASK_NOT_FOUND` includes `status="unknown"`, its backend and process
diagnostics, `retry_after_seconds=5`, `max_additional_polls=2`,
`do_not_resubmit_automatically=true`, and a `next_action` message.
**This error does not mean the upstream HTTP request failed.** Try the same
task ID at most twice more with 5-second intervals. If it remains missing,
stop polling and report an unknown outcome; do not resubmit the HTTP POST
without independent verification or user approval.

Background HTTP is submitted to the FastMCP Tasks extension Docket worker from the start, and `get_request` polls Docket execution state independently of MCP Tasks client capability negotiation. Without a shared Redis/Valkey backend, Docket state is **process-local** and unsuitable for reliable long-running Horizon tasks, even when occasional long requests complete successfully. See the configuration above.

## Deploy to Horizon

1. Connect this repository at https://horizon.prefect.io.
2. Use entrypoint `mcp_server.py:mcp` and enable Horizon authentication.
3. Add `APIKEY_*` environment variables.
4. Connect the deployed MCP to your agent.

Horizon installs dependencies from `pyproject.toml` (the `requirements.txt` is a compatible `-e .` shim). Freedo reports its own version in MCP initialization metadata. Local development:

```bash
python -m pip install -r requirements.txt
python mcp_server.py
```

Local tests: `python -m pip install -e ".[test]" && python -m pytest -q`.

**Current scope:** generic HTTP APIs, background HTTP with built-in optional Docket Redis/Valkey state sharing, and process-local paged responses. No OAuth lifecycle management, HMAC/SigV4 signer, automatic HTTP replay or distributed result-cache storage.
