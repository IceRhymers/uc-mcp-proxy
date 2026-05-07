"""Credential preflight for the Databricks MCP proxy.

This module is responsible for ensuring that the user has working Databricks
credentials *before* the MCP stdio transport is opened. Once
``stdio_server()`` is active, stdout is owned by the JSON-RPC framing and a
single stray byte will corrupt the session — so all preflight output is sent
to stderr, and any subprocesses we spawn inherit that contract.

Safety rail
-----------
``databricks auth login`` is the OAuth U2M flow. Running it overwrites the
target profile's entry in ``~/.databrickscfg`` with a fresh OAuth U2M block.
For PAT, M2M, Azure, federated, or env-var profiles, that would *clobber* the
user's working credentials. We therefore only auto-invoke ``databricks auth
login`` when the resolved ``auth_type`` is ``databricks-cli``; every other
auth type gets an auth-type-specific remediation message and a clean exit.
"""

from __future__ import annotations

import configparser
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.errors.base import DatabricksError

# Auth types we know how to recover from automatically. ``databricks-cli`` is
# the OAuth U2M flow — ``databricks auth login --profile <name>`` re-runs it
# safely. Every other auth type stores credentials we MUST NOT clobber by
# running login.
_RECOVERABLE_AUTH_TYPES: frozenset[str] = frozenset({"databricks-cli"})


# Type alias for the subprocess.run callable, exposed so tests can inject a
# mock without resorting to module-level monkeypatching.
SubprocessRunner = Callable[..., subprocess.CompletedProcess[Any]]


def _preflight_authenticate(
    profile: str | None,
    auth_type: str | None,
    *,
    runner: SubprocessRunner = subprocess.run,
) -> WorkspaceClient:
    """Construct a ``WorkspaceClient`` and verify it can mint a token.

    On a credential failure (``PermissionDenied`` or any other
    ``DatabricksError`` raised by ``config.authenticate()``), we *only* run
    ``databricks auth login`` if the resolved auth type is ``databricks-cli``
    (OAuth U2M). For any other auth type (PAT, M2M, azure-cli, env vars,
    etc.), running login would clobber the existing ``~/.databrickscfg``
    entry — so we diagnose and exit cleanly instead.

    Subprocess output is sent to stderr; stdout is reserved for MCP framing.
    """
    kwargs: dict[str, Any] = {}
    if profile:
        kwargs["profile"] = profile
    if auth_type:
        kwargs["auth_type"] = auth_type

    # The SDK authenticates eagerly in ``Config.__init__`` — a stale OAuth
    # refresh token raises straight out of ``WorkspaceClient(**kwargs)``
    # (wrapped as ``ValueError``), before we can call ``authenticate()``.
    # So construction and authenticate() must share the same except block.
    client: WorkspaceClient | None = None
    try:
        client = WorkspaceClient(**kwargs)
        client.config.authenticate()
        return client
    except (PermissionDenied, DatabricksError, ValueError):
        pass  # fall through to login

    if client is not None:
        resolved_auth_type = client.config.auth_type or "(auto-detect)"
        resolved_profile = client.config.profile or "DEFAULT"
    else:
        # WorkspaceClient() itself blew up — we can't read client.config.
        # Recover auth_type from the caller's args or, failing that,
        # ~/.databrickscfg so we still make the right recovery decision.
        # Profile name comes from --profile, then DATABRICKS_CONFIG_PROFILE,
        # then the SDK's "DEFAULT" fallback — matching the SDK's own
        # resolution order in Config.__init__.
        resolved_profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE") or "DEFAULT"
        resolved_auth_type = auth_type or _read_auth_type_from_cfg(resolved_profile) or "(auto-detect)"

    if resolved_auth_type not in _RECOVERABLE_AUTH_TYPES:
        # Non-OAuth auth — we can't safely re-run ``databricks auth login``
        # because it would overwrite the profile's existing credentials.
        # Diagnose and exit with a remediation specific to this auth type.
        raise SystemExit(_diagnose_non_oauth_auth(resolved_auth_type, resolved_profile))

    cmd = ["databricks", "auth", "login", "--profile", resolved_profile]
    print(
        f"uc-mcp-proxy: no valid credentials for profile {resolved_profile!r} "
        f"(auth_type={resolved_auth_type}), running "
        f"`databricks auth login --profile {resolved_profile}`...",
        file=sys.stderr,
    )
    try:
        result = runner(cmd, stdout=sys.stderr, stderr=sys.stderr)
    except FileNotFoundError as exc:
        raise SystemExit(
            "uc-mcp-proxy: `databricks` CLI not found on PATH. "
            "Install: https://docs.databricks.com/aws/en/dev-tools/cli/install"
        ) from exc
    if result.returncode != 0:
        raise SystemExit(
            f"uc-mcp-proxy: `databricks auth login --profile {resolved_profile}` failed (exit {result.returncode})"
        )

    # Retry — fresh client picks up the just-cached OAuth token. If
    # authenticate() still fails, let the original SDK error propagate so the
    # user sees the real cause (corrupt cache, browser flow aborted, etc.).
    client = WorkspaceClient(**kwargs)
    client.config.authenticate()
    return client


def _diagnose_non_oauth_auth(auth_type: str, profile: str) -> str:
    """Return a remediation message tailored to the user's auth type.

    We do NOT run ``databricks auth login`` in any of these branches because
    it would overwrite the profile's working credentials in
    ``~/.databrickscfg``.
    """
    if auth_type == "pat":
        return (
            f"uc-mcp-proxy: PAT for profile {profile!r} is missing or expired. "
            f"Generate a new token at <workspace>/settings/user/developer/access-tokens "
            f"and update the `token = ...` line in ~/.databrickscfg under [{profile}]. "
            f"(uc-mcp-proxy will not run `databricks auth login` for PAT profiles "
            f"because that command would overwrite your token entry with an OAuth "
            f"U2M entry.)"
        )
    if auth_type in {"oauth-m2m", "service-principal"}:
        return (
            f"uc-mcp-proxy: M2M / service-principal credentials for profile "
            f"{profile!r} are invalid or expired. Verify `client_id` and "
            f"`client_secret` in ~/.databrickscfg under [{profile}]."
        )
    if auth_type in {"azure-cli", "azure-client-secret", "azure-msi"}:
        return (
            f"uc-mcp-proxy: Azure auth for profile {profile!r} failed. "
            f"For azure-cli: run `az login`. For service-principal: verify "
            f"the credentials in ~/.databrickscfg under [{profile}]."
        )
    if auth_type in {"github-oidc", "google-credentials", "google-id"}:
        return (
            f"uc-mcp-proxy: federated auth ({auth_type}) for profile "
            f"{profile!r} failed. Verify your federated identity setup."
        )
    if auth_type in {"env", "(auto-detect)"}:
        return (
            "uc-mcp-proxy: no valid credentials found via env vars or "
            "auto-detection. Set `DATABRICKS_TOKEN` and `DATABRICKS_HOST`, or "
            "configure a profile with `databricks configure` and pass --profile."
        )
    # Unknown auth_type — be cautious. Tell the user what we saw and refuse
    # to act.
    return (
        f"uc-mcp-proxy: credentials for profile {profile!r} (auth_type="
        f"{auth_type}) are invalid. uc-mcp-proxy will not run "
        f"`databricks auth login` automatically for non-OAuth auth types "
        f"because that would overwrite your existing ~/.databrickscfg entry. "
        f"Refresh your credentials manually."
    )


def _read_auth_type_from_cfg(profile: str) -> str | None:
    """Read ``auth_type`` for ``profile`` from ``~/.databrickscfg``.

    Returns ``None`` if the file is absent, unreadable, or missing the
    profile/field. Used only as a fallback when ``WorkspaceClient()`` itself
    raises (eager auth in ``Config.__init__``) so we can't inspect
    ``client.config.auth_type``.
    """
    path_str = os.environ.get("DATABRICKS_CONFIG_FILE") or "~/.databrickscfg"
    path = Path(path_str).expanduser()
    if not path.is_file():
        return None
    parser = configparser.ConfigParser()
    try:
        parser.read(path)
    except configparser.Error:
        return None
    try:
        section = parser[profile]
    except KeyError:
        return None
    return section.get("auth_type")
