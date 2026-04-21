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

## Databricks MCP Server Types

| Server Type | URL Pattern |
|-------------|-------------|
| **Managed MCP** (UC Functions, Vector Search, Genie, SQL) | `https://<workspace>/api/2.0/mcp/functions/{catalog}/{schema}` |
| **External MCP** (GitHub, Google Drive, and others) | `https://<workspace>/api/2.0/mcp/external/{connection_name}` |
| **Apps** (custom MCP servers) | `https://<workspace>.databricks.com/apps/<app>/mcp` |

> **Apps require OAuth.** Use `--auth-type databricks-cli` when connecting to a Databricks App. Managed and External MCP servers also work with PAT and other auth types.

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
| `--header KEY=VALUE` | Extra HTTP header forwarded to upstream (repeatable) |

## Meta Parameters (Managed MCP)

Databricks Managed MCP servers accept transport-level HTTP headers for configuration — for example, selecting a SQL warehouse or scoping to a catalog/schema. Use `--header` to forward these:

```bash
uvx uc-mcp-proxy \
  --url https://workspace.databricks.com/api/2.0/mcp/functions/main/default \
  --header x-databricks-warehouse-id=abc123 \
  --header x-databricks-catalog=main \
  --header x-databricks-schema=default
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
        "--url", "https://workspace.databricks.com/api/2.0/mcp/functions/main/default",
        "--header", "x-databricks-warehouse-id=abc123"
      ]
    }
  }
}
```

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
| `pat` — personal access token | ✅ | ❌ not supported |
| `oauth-m2m` — service principal | ✅ | ✅ |
| OAuth U2M — browser-based login | ✅ | ✅ |

## Development

```bash
uv sync                        # install dependencies
uv run pytest -m unit -v       # run unit tests
uv run pytest -m integration -v # run integration tests
uv build                       # build package
```

## License

MIT
