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

Supported request options: `headers`, `params`, one of `json_body` / `text_body` / `form_body`, optional `basic_auth` (`username`, `password`), `timeout`, `max_request_bytes`, `max_response_bytes`. For Basic Auth, put the credential reference in `basic_auth.password`; Freedo constructs the standard Basic header.

Each request and response body defaults to 2 MiB (2,097,152 bytes); both limits can be increased independently up to 4 MiB (4,194,304 bytes). Request size is calculated *after* credential substitution and encoding. If a response exceeds the selected limit, Freedo stops reading it and returns an error rather than a partial body. Binary response bodies are base64 encoded. Unusually escape-heavy output may hit the Horizon MCP envelope limit even if the raw HTTP body fits; the tool returns an explicit error.

HTTP 4xx/5xx status codes are returned as normal HTTP results, not MCP failures. Redirects are not followed. Network errors are returned without exception text because exception text could contain credential-bearing URLs. Exact credential values used in a request are redacted from textual output when echoed by a server.

**Trust boundary:** This is a private, powerful HTTP executor. An authorized agent can choose arbitrary network destinations and attach configured credentials. Enable Horizon's managed authentication and connect only trusted agents. Freedo does not constrain provider hosts, inspect API permissions, or add confirmation workflows.

## Deploy to Horizon

1. Connect this repository at https://horizon.prefect.io.
2. Use entrypoint `server.py:mcp` and enable Horizon authentication.
3. Add `APIKEY_*` environment variables.
4. Connect the deployed MCP to your agent.

Horizon installs `requirements.txt` automatically. Local development:

```bash
python -m pip install -r requirements.txt
python server.py
```

Local tests: `python -m pip install pytest && python -m pytest -q`.

**Current scope:** conventional HTTP APIs, including REST, GraphQL and non-streaming model inference. No OAuth lifecycle management, HMAC/SigV4 signer, file-upload protocol, persistence, retries or long-running agent runtime.
