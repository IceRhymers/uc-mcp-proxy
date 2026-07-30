#!/usr/bin/env python3
"""Probe whether this workspace will exchange a PAT for an app-scoped token.

Standalone on purpose: stdlib only, no dependency on uc_mcp_proxy or the
Databricks SDK, so it can be dropped into a sandbox that has neither. The
`databricks` CLI is used only to discover an app, and only if --client-id is
not supplied.

Safety: this script never prints a credential. The PAT is described by shape
predicates (length, prefix, charset) and nothing else, and a successful
exchange reports the token's length rather than its value. Every exception is
caught and reduced to a type name, because a traceback from these frames would
carry the PAT in its locals.

Usage:
    python3 probe_token_exchange.py
    python3 probe_token_exchange.py --profile myprofile
    python3 probe_token_exchange.py --client-id <uuid> --scope "a b"
    python3 probe_token_exchange.py --url https://<app>-<id>.aws.databricksapps.com/mcp
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange"
REQUESTED_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"
PAT_SUBJECT_TYPE = "urn:databricks:params:oauth:token-type:personal-access-token"
OTHER_SUBJECT_TYPES = (
    "urn:ietf:params:oauth:token-type:access_token",
    "urn:ietf:params:oauth:token-type:jwt",
)
#: Probing every app in a large workspace is noise, and a silent cap would read
#: as "checked everything". Dropped apps are logged.
MAX_APPS = 3
TIMEOUT = 15


def resolve_credentials(profile: str | None) -> tuple[str, str, str]:
    """Return (host, token, source). Env wins; then ~/.databrickscfg."""
    env_host, env_token = os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN")
    if env_host and env_token and not profile:
        return env_host.rstrip("/"), env_token, "env (DATABRICKS_HOST/DATABRICKS_TOKEN)"

    path = os.path.expanduser("~/.databrickscfg")
    if os.path.exists(path):
        parser = configparser.ConfigParser()
        parser.read(path)
        section = profile or "DEFAULT"
        if parser.has_section(section) or section == "DEFAULT":
            values = dict(parser.items(section))
            host, token = values.get("host", "").rstrip("/"), values.get("token", "")
            if host and token:
                return host, token, f"~/.databrickscfg [{section}]"

    if env_host and env_token:
        return env_host.rstrip("/"), env_token, "env (DATABRICKS_HOST/DATABRICKS_TOKEN)"

    sys.exit(
        "Could not find a host + PAT. Set DATABRICKS_HOST and DATABRICKS_TOKEN, "
        "or pass --profile naming a profile in ~/.databrickscfg that has a `token`."
    )


def describe(token: str) -> None:
    print("--- credential shape (value never printed) ---")
    print(f"length:      {len(token)}")
    print(f"dapi prefix: {token.startswith('dapi')}")
    print(f"jwt shaped:  {token.count('.') == 2}")
    print(f"uuid shaped: {bool(re.fullmatch(r'[0-9a-fA-F-]{36}', token))}")
    charset = "hex+dash" if re.fullmatch(r"[0-9a-f-]+", token) else "mixed alnum"
    print(f"charset:     {charset}")


def cli_json(args: list[str], profile: str | None) -> object | None:
    """Run a databricks CLI command and parse its JSON, or return None."""
    command = ["databricks", *args, "-o", "json"]
    if profile:
        command += ["--profile", profile]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"  `databricks {' '.join(args)}` unavailable: {type(exc).__name__}")
        return None
    if result.returncode != 0:
        print(f"  `databricks {' '.join(args)}` failed rc={result.returncode}: {result.stderr[:300].strip()}")
        return None
    try:
        return json.loads(result.stdout or "null")
    except json.JSONDecodeError:
        print(f"  `databricks {' '.join(args)}` returned non-JSON")
        return None


def discover_apps(profile: str | None, url: str | None) -> list[tuple[str, str, tuple[str, ...]]]:
    """Return [(name, client_id, scopes)] for apps this identity can see."""
    print("\n--- app discovery (`databricks apps list`) ---")
    apps = cli_json(["apps", "list"], profile)
    if not isinstance(apps, list) or not apps:
        print("  no apps visible to this identity")
        return []
    print(f"  {len(apps)} app(s) visible — the identity can read app metadata")

    if url:
        host = urllib.parse.urlparse(url).netloc or url
        apps = [a for a in apps if (a.get("url") or "").split("//")[-1].rstrip("/") == host]
        print(f"  {len(apps)} match --url host {host}")

    selected, dropped = apps[:MAX_APPS], apps[MAX_APPS:]
    if dropped:
        print(f"  probing first {MAX_APPS}; skipping {len(dropped)}: {', '.join(a.get('name', '?') for a in dropped)}")

    out = []
    for app in selected:
        name, client_id = app.get("name", "?"), app.get("oauth2_app_client_id")
        if not client_id:
            print(f"  {name}: no oauth2_app_client_id")
            continue
        # `apps list` omits effective_user_api_scopes; only `apps get` has it.
        detail = cli_json(["apps", "get", name], profile)
        scopes = tuple((detail or {}).get("effective_user_api_scopes") or []) if isinstance(detail, dict) else ()
        state = (app.get("compute_status") or {}).get("state", "?")
        print(f"  {name}: client_id={client_id} compute={state} scopes={list(scopes) or '(none)'}")
        out.append((name, client_id, scopes))
    return out


def post_exchange(host: str, token: str, subject_type: str, audience: str, scopes: tuple[str, ...]) -> tuple[int, str]:
    """POST the exchange. Returns (status, message) -- never the token."""
    form = {
        "grant_type": GRANT_TYPE,
        "subject_token": token,
        "subject_token_type": subject_type,
        "requested_token_type": REQUESTED_TOKEN_TYPE,
        "audience": audience,
    }
    if scopes:
        form["scope"] = " ".join(scopes)

    request = urllib.request.Request(
        f"{host}/oidc/v1/token",
        data=urllib.parse.urlencode(form).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "identity",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            body = json.loads(response.read(65536) or b"{}")
            access = body.get("access_token") or ""
            return response.status, (
                f"OK — token len {len(access)}, expires_in {body.get('expires_in')}, "
                f"refresh_token present: {'refresh_token' in body}"
            )
    except urllib.error.HTTPError as exc:
        raw = exc.read(65536).decode("utf-8", "replace")
        try:
            parsed = json.loads(raw)
            message = parsed.get("error_description") or parsed.get("error") or raw[:300]
            request_id = parsed.get("request_id")
            if request_id:
                message = f"{message}  [request_id {request_id}]"
        except json.JSONDecodeError:
            message = raw[:300]
        return exc.code, message
    except Exception as exc:  # noqa: BLE001 — locals here hold the PAT
        return 0, f"transport error: {type(exc).__name__}"


def probe(host: str, token: str, name: str, client_id: str, scopes: tuple[str, ...]) -> None:
    print(f"\n--- exchange: {name} (audience {client_id}) ---")

    status, message = post_exchange(host, token, PAT_SUBJECT_TYPE, client_id, scopes)
    print(f"  pat + scopes {list(scopes) or '(none sent)'}\n    {status} {message}")

    if scopes:
        status, message = post_exchange(host, token, PAT_SUBJECT_TYPE, client_id, ())
        print(f"  pat, no scope param\n    {status} {message}")

    for subject_type in OTHER_SUBJECT_TYPES:
        status, message = post_exchange(host, token, subject_type, client_id, ())
        print(f"  subject_token_type={subject_type.rsplit(':', 1)[-1]}\n    {status} {message}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=None, help="profile in ~/.databrickscfg")
    parser.add_argument("--client-id", default=None, help="app oauth2_app_client_id; skips discovery")
    parser.add_argument("--scope", action="append", default=[], help="repeatable; a value may list several")
    parser.add_argument("--url", default=None, help="app MCP URL, to narrow discovery to one app")
    args = parser.parse_args()

    host, token, source = resolve_credentials(args.profile)
    print(f"host:   {host}")
    print(f"source: {source}\n")
    describe(token)

    scopes = tuple(s for value in args.scope for s in value.split())
    if args.client_id:
        probe(host, token, "explicit --client-id", args.client_id, scopes)
    else:
        apps = discover_apps(args.profile, args.url)
        if not apps:
            sys.exit("\nNo app to probe. Pass --client-id explicitly.")
        for name, client_id, app_scopes in apps:
            probe(host, token, name, client_id, scopes or app_scopes)

    print("\nDone. Paste this whole output back.")


if __name__ == "__main__":
    main()
