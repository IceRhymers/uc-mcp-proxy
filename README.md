# uc-mcp-proxy

MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth.

Lets any MCP client that speaks **stdio** (e.g. Claude Desktop, Claude Code) connect to any **Databricks MCP server** — Managed, External, or Apps — handling authentication automatically.

## Installation

```bash
# Run directly (no install needed)
uvx uc-mcp-proxy --url <MCP_SERVER_URL>

# Or install globally
uv tool install uc-mcp-proxy
```

Requires Python 3.10+.

## First-run authentication

uc-mcp-proxy expects a configured Databricks CLI profile. Set one up first:

    databricks configure --host https://<workspace>

Then pass it to the proxy:

    uvx uc-mcp-proxy --url <MCP_SERVER_URL> --profile <name>

If the profile uses OAuth U2M (`auth_type = databricks-cli`) and the cached
token is expired, uc-mcp-proxy runs `databricks auth login --profile <name>`
automatically the first time it launches, opening a browser tab. Subsequent
runs use the refreshed token.

**uc-mcp-proxy will only auto-login for OAuth (`databricks-cli`) profiles.**
For PAT, M2M, Azure, or other auth types, the proxy diagnoses the failure
and points you at the right remediation — it never runs `databricks auth
login` against a non-OAuth profile because that would overwrite your
existing credentials in `~/.databrickscfg`.

To skip the auto-login (CI / headless), pass `--no-auto-login` and ensure
`DATABRICKS_TOKEN` or another credential is set in the environment.

## Databricks MCP Server Types

| Server Type | URL Pattern |
|-------------|-------------|
| **Managed MCP** (UC Functions, Vector Search, Genie, SQL) | `https://<workspace>/api/2.0/mcp/functions/{catalog}/{schema}` |
| **External MCP** (GitHub, Google Drive, and others) | `https://<workspace>/api/2.0/mcp/external/{connection_name}` |
| **Apps** (custom MCP servers) | `https://<app-name>-<workspace-id>.<region>.databricksapps.com/<path>` |

An App is served from its own hostname on `databricksapps.com` — **not** from a
path under your workspace host. Databricks assigns the URL when the app is
created and it cannot be changed afterwards, so copy it from the **Apps** page
in the workspace UI (or `databricks apps list`) rather than constructing it by
hand — on the workspace we tested, the segment Databricks documents as
`<region>` is the cloud name (`aws`), not a region like `us-east-1`. The
`<path>` is whatever route the app serves MCP on; `/mcp` is the common
convention.

> **Apps reject raw personal access tokens.** The App front door requires an
> OAuth token. Either use `--auth-type databricks-cli` (browser-based OAuth
> U2M), or keep your PAT profile and pass `--client-id` so the proxy exchanges
> the PAT for an app-scoped token — see
> [Databricks Apps from a PAT profile](#databricks-apps-from-a-pat-profile).
> Managed and External MCP servers work with PAT and other auth types as-is.

## Usage

### Claude Desktop / Claude Code (`.mcp.json`)

Add to your MCP client configuration:

```json
{
  "mcpServers": {
    "unity-catalog": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "uc-mcp-proxy",
        "--url", "<MCP_SERVER_URL>"
      ]
    }
  }
}
```

### CLI

```bash
uc-mcp-proxy --url <MCP_SERVER_URL> [--profile <DATABRICKS_PROFILE>] [--auth-type <AUTH_TYPE>]
```

| Flag | Description |
|------|-------------|
| `--url` | **(required)** Remote MCP server URL |
| `--profile` | Databricks CLI profile name (uses default if omitted) |
| `--auth-type` | Databricks auth type, e.g. `databricks-cli` |
| `--meta KEY=VALUE` | Meta parameter injected into `tools/call` `_meta` (repeatable) |
| `--client-id` | App's `oauth2_app_client_id` — enables the RFC 8693 PAT exchange for Databricks Apps |
| `--scope SCOPE` | OAuth scope for `--client-id` (repeatable; a value may list several space-separated scopes). Optional |
| `--no-verify-ssl` | Disable SSL certificate verification (use with caution — see below) |

## Databricks Apps from a PAT profile

A Databricks App refuses a raw personal access token — its front door wants an
OAuth token minted *for that app*. Rather than forcing a browser login, the
proxy can trade your PAT for one (RFC 8693 token exchange) on your behalf.

Read the two values you need off the app:

```bash
databricks apps get <app-name> -o json
```

- `oauth2_app_client_id` → `--client-id`
- `effective_user_api_scopes` → `--scope` (optional; omit it if the list is empty)

Then point the proxy at the app:

```bash
uc-mcp-proxy \
  --url https://<app-name>-<workspace-id>.aws.databricksapps.com/mcp \
  --profile MY_PAT_PROFILE \
  --client-id 00000000-1111-2222-3333-444444444444 \
  --scope "sql dashboards.genie"
```

In `.mcp.json`:

```json
{
  "mcpServers": {
    "databricks-app": {
      "command": "uvx",
      "args": [
        "uc-mcp-proxy",
        "--url", "https://<app-name>-<workspace-id>.aws.databricksapps.com/mcp",
        "--profile", "MY_PAT_PROFILE",
        "--client-id", "00000000-1111-2222-3333-444444444444",
        "--scope", "sql dashboards.genie"
      ]
    }
  }
}
```

Worth knowing:

- **`--client-id` requires a PAT profile.** On any other auth type the proxy
  exits immediately rather than pretending to work. `DATABRICKS_TOKEN` counts
  as a PAT.
- **`--scope` is optional.** Omitted, the proxy sends no scope parameter at
  all, and says so in the failure message if the exchange is refused.
- **The exchanged token is short-lived** (about an hour) and is re-minted
  automatically, including once in-place if the app rejects it mid-session, so
  long sessions do not need restarting.
- **Your PAT is never forwarded to the app.** It goes only to the workspace
  token endpoint, as the subject of the exchange. For any `pat` profile the
  proxy also stops populating `X-Forwarded-Access-Token`, which the platform
  supplies itself — a raw PAT there would be handed to arbitrary app code, and
  it would survive a cross-origin redirect that strips `Authorization`.
- **Verified on one AWS workspace at one point in time.** The accepted request
  shape is undocumented, so treat it as empirical rather than contractual; a
  refusal prints the exact request that was sent so a platform change is
  diagnosable rather than mysterious.

## Meta Parameters (Managed MCP)

Databricks Managed MCP servers accept configuration — for example, selecting a SQL warehouse — via the MCP [`_meta`](https://modelcontextprotocol.io/specification/2025-11-25/basic#_meta) field in the JSON-RPC request body, **not** as HTTP headers. See the Databricks [meta-param docs](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp-meta-param) for the supported keys per server type.

Use `--meta KEY=VALUE` (repeatable) — the proxy merges these into `params._meta` on every outgoing `tools/call` request:

```bash
uvx uc-mcp-proxy \
  --url https://workspace.databricks.com/api/2.0/mcp/sql \
  --meta warehouse_id=abc123
```

Or in `.mcp.json`:

```json
{
  "mcpServers": {
    "sql-mcp": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "uc-mcp-proxy",
        "--url", "https://workspace.databricks.com/api/2.0/mcp/sql",
        "--meta", "warehouse_id=abc123"
      ]
    }
  }
}
```

If the MCP client already sets a `_meta` key that the proxy is also configured to inject, the proxy value wins and a warning is written to stderr.

## SSL Certificate Verification

Some Azure Databricks instances use self-signed or internally-signed certificates that are not trusted by the system's default CA bundle. This causes errors like:

```
SSL_CERTIFICATE_VERIFY_FAILED: certificate verify failed: unable to get local issuer certificate
```

Use `--no-verify-ssl` to disable certificate verification:

```bash
uvx uc-mcp-proxy \
  --url https://workspace.azuredatabricks.net/api/2.0/mcp/functions/main/default \
  --no-verify-ssl
```

Or in `.mcp.json`:

```json
{
  "mcpServers": {
    "unity-catalog": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "uc-mcp-proxy",
        "--url", "https://workspace.azuredatabricks.net/api/2.0/mcp/functions/main/default",
        "--no-verify-ssl"
      ]
    }
  }
}
```

> **Security warning:** `--no-verify-ssl` disables all certificate validation, which exposes connections to man-in-the-middle (MITM) attacks. Only use this flag in trusted network environments (e.g. a private corporate VPN) where you control the network path to the Databricks workspace.
>
> Combined with `--client-id` the exposure is wider than usual: the token
> exchange sends your PAT in a request **body** to the workspace token
> endpoint, not only as a header. Anyone able to intercept that connection
> reads a long-lived, full-privilege credential.

## How It Works

1. Starts an MCP **stdio** server (stdin/stdout)
2. Connects to the remote MCP server via **Streamable HTTP**
3. Injects a fresh Databricks OAuth token on every HTTP request
4. Bridges messages bidirectionally between the two transports

## Authentication

Authentication is handled by the [Databricks SDK](https://docs.databricks.com/dev-tools/sdk-python.html). The SDK auto-detects the method, or you can force one with `--auth-type`.

| Auth type | Managed / External MCP | Apps MCP |
|-----------|------------------------|----------|
| `databricks-cli` — token from `~/.databrickscfg` | ✅ | ✅ recommended |
| `pat` — personal access token | ✅ | ✅ with `--client-id` [^pat] |
| `oauth-m2m` — service principal | ✅ | ✅ [^m2m] |
| OAuth U2M — browser-based login | ✅ | ✅ |

[^pat]: An App rejects a PAT sent as-is, because it requires an OAuth token.
    That is a statement about the *token*, not about your profile: a PAT can be
    exchanged for an app-scoped OAuth token (RFC 8693), and passing
    `--client-id` makes uc-mcp-proxy do exactly that. Without `--client-id` a
    PAT profile still cannot reach an App. See
    [Databricks Apps from a PAT profile](#databricks-apps-from-a-pat-profile).

[^m2m]: Verified against a live App-hosted MCP server: `initialize`,
    `tools/list`, and `tools/call` all succeed through the proxy with a service
    principal's `oauth-m2m` credentials. The principal must be granted
    `CAN_USE` on the app first — without that grant the App answers **401**,
    not 403, so a missing permission is easy to misread as the auth type being
    unsupported.

## Troubleshooting

When the remote MCP server refuses a request, the proxy prints a diagnosis to
stderr naming the status, the URL, the profile, and the auth type in use.

| Message | Meaning | Fix |
|---------|---------|-----|
| `rejected your credentials (HTTP 401)` | The token was minted locally but the server rejected it — it may have expired, or this profile's identity is not recognized by the target. | Refresh the profile's credentials. For `databricks-cli`, run `databricks auth login --profile <name>`. Against a Databricks App this also appears when the identity simply lacks `CAN_USE` on the app — check the app's permissions before assuming the credential is bad. |
| `refused this request (HTTP 403)` | Authenticated successfully, but not authorized for this target. | Check your grants on the target. Pointing a `pat` profile at a Databricks App without `--client-id` produces this — Apps reject a raw PAT and need an OAuth token. |
| `refused the PAT token exchange (HTTP 400)` | The workspace would not exchange your PAT for an app token. The message prints the exact request that was sent. | Check `--client-id` is the app's `oauth2_app_client_id` and that `--scope` matches its `effective_user_api_scopes`. Note an `invalid audience` error can also mean the requested token type was rejected, so a correct `--client-id` is not proof the flag is at fault. |
| `the app rejected the exchanged token` (401 naming `--client-id`) | The exchange succeeded but the app refused the resulting token, twice — so it is configuration, not expiry. | Confirm this identity has `CAN USE` on the app, and that `--scope` covers what the app requires. |
| `no MCP endpoint at this URL (HTTP 404)` | The URL is wrong. Not an auth failure. | Check `--url`. |
| `the MCP session expired server-side (HTTP 404)` | The server no longer recognizes this session. | Restart the MCP client to establish a new session. |
| `the remote MCP server failed (HTTP 5xx)` | Server-side error, **not** an authentication problem. | Retry; check Databricks service status. |

A failure on the background server→client stream is reported but does not stop
the proxy: the SDK retries a bounded number of times and then stops, so
server-initiated messages may be lost while tool calls keep working.

## Development

```bash
uv sync                        # install dependencies
uv run pytest -m unit -v       # run unit tests
uv run pytest -m integration -v # run integration tests
uv build                       # build package
```

## License

MIT
