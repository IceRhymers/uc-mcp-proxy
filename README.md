# uc-mcp-proxy

MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth.

Lets any MCP client that speaks **stdio** (e.g. Claude Desktop, Claude Code) connect to a remote **Streamable HTTP** MCP server hosted on Databricks Apps — handling OAuth authentication automatically.

## Installation

```bash
# Run directly (no install needed)
uvx uc-mcp-proxy --url https://<workspace>.databricks.com/apps/<app>/mcp

# Or install globally
uv tool install uc-mcp-proxy
```

Requires Python 3.10+.

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
        "--url", "https://<workspace>.databricks.com/apps/<app>/mcp"
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

## How It Works

1. Starts an MCP **stdio** server (stdin/stdout)
2. Connects to the remote MCP server via **Streamable HTTP**
3. Injects a fresh Databricks OAuth token on every HTTP request
4. Bridges messages bidirectionally between the two transports

## OAuth Authentication

Authentication is handled by the [Databricks SDK](https://docs.databricks.com/dev-tools/sdk-python.html), which supports multiple auth methods:

- **Databricks CLI** (`databricks-cli`) — uses tokens from `~/.databrickscfg`
- **OAuth U2M** — browser-based login flow
- **PAT** — personal access tokens
- **Azure / GCP / AWS** — cloud-native identity

The SDK auto-detects the method, or you can force one with `--auth-type`.

## Development

```bash
uv sync                        # install dependencies
uv run pytest -m unit -v       # run unit tests
uv run pytest -m integration -v # run integration tests
uv build                       # build package
```

## License

MIT
