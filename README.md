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
- `http_request(method, url, ...)` sends arbitrary HTTP requests. The agent decides which provider, endpoint, parameters and authentication scheme to use.

Use a credential reference anywhere in a string value (including nested JSON values):

```json
{
  "method": "GET",
  "url": "https://api.github.com/user",
  "headers": {"Authorization": "Bearer {{env:APIKEY_GITHUB}}"}
}
```

Supported request options: `headers`, `params`, one of `json_body` / `text_body` / `form_body` / `body_base64`, optional `basic_auth` (`username`, `password`), `timeout`, `max_request_bytes`, `max_response_bytes`, `include_response_headers` (default `false`). For Basic Auth, put the credential reference in `basic_auth.password`; Freedo constructs the standard Basic header.

Each request and response body defaults to 2 MiB (2,097,152 bytes); both limits can be increased independently up to 4 MiB (4,194,304 bytes). Request size is calculated *after* credential substitution and encoding. If a response exceeds the selected limit, Freedo stops reading it and returns an error rather than a partial body. Binary response bodies are base64 encoded. Unusually escape-heavy output may hit the Horizon MCP envelope limit even if the raw HTTP body fits; the tool returns an explicit error.

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

**Current scope:** conventional HTTP APIs, including REST, GraphQL and non-streaming model inference. No OAuth lifecycle management, HMAC/SigV4 signer, file-upload protocol, persistence, retries or long-running agent runtime.
