# The PAT → app token exchange

Reference for the RFC 8693 exchange in `token_exchange.py`: the exact request on
the wire, what the platform answers, and which workspaces it has actually been
observed to work on.

The request shape is **undocumented by Databricks**. Everything here is
empirical — recorded so that a platform change, or a workspace where the
exchange is unavailable, is diagnosable rather than mysterious.

## Why an exchange exists at all

A Databricks App does not accept a personal access token. Its front door wants
an OAuth token minted *for that app*. RFC 8693 is the way to get one without a
browser: the PAT is the `subject_token`, the app's client id is the `audience`,
and the workspace's OIDC endpoint returns a short-lived app-scoped token.

## The request

```
POST https://<workspace-host>/oidc/v1/token
Authorization: Bearer <pat>
Accept-Encoding: identity
Content-Type: application/x-www-form-urlencoded
```

| Form field | Value |
|---|---|
| `grant_type` | `urn:ietf:params:oauth:grant-type:token-exchange` |
| `subject_token` | the PAT |
| `subject_token_type` | `urn:databricks:params:oauth:token-type:personal-access-token` |
| `requested_token_type` | `urn:ietf:params:oauth:token-type:access_token` |
| `audience` | the app's `oauth2_app_client_id` |
| `scope` | space-joined `effective_user_api_scopes`, **omitted entirely** when empty |

Constraints that are load-bearing rather than incidental:

- **The host is the workspace host, never the app host.** The app has no token
  endpoint, and sending the PAT there is the exposure this feature removes.
- **HTTPS is required and redirects are not followed.** The PAT is in the
  request *body*, so cleartext would put a long-lived full-privilege credential
  on the wire, and a 307 would re-send that body to whatever host it names —
  with `Authorization` stripped cross-origin but the form intact.
- **`requested_token_type` has exactly one accepted value.** Every other value
  in the RFC's table is refused, and the refusal is reported as an *audience*
  error rather than a token-type one. A correct `--client-id` is therefore not
  proof the flag is at fault when you see "audience is unsupported".
- **`Authorization: Bearer <pat>` is redundant in practice.** The PAT is in the
  form too; sending the header changes no outcome we have observed (tested both
  ways against the sandbox workspace below). It is sent because the working
  request was captured with it.

## The response

`access_token` plus `expires_in`. There is **no `refresh_token`** — the
credential is re-derived from the PAT on expiry. `expires_in` defaults to 3600
when absent and is capped at 24h, because a token claiming a decade would pin a
stale credential in cache for the life of the process. A 60s margin is
subtracted so a token cannot go stale in flight, and the deadline is monotonic
so a clock adjustment cannot make a live token look expired.

The reply is read streamed and bounded to 64 KiB with `Accept-Encoding:
identity`, because the byte cap counts decoded bytes and this read happens on
the event-loop thread.

## Evidence

> **The exchange works for a `pat`-typed credential ONLY when that credential is
> the Lakebox-generated environment credential.** A classic, hand-minted
> `dapi…` PAT is refused — even though it reports the same `auth_type=pat`,
> authenticates fine for ordinary API calls, and can read app metadata. The
> `pat` label on the profile is not sufficient; the credential's *origin* is
> what the exchange validates.

The determining factor is the **credential type**, not the workspace. What the
endpoint validates is the subject token itself: a classic `dapi…` PAT is
rejected as a subject token; a Lakebox-issued environment credential is
accepted. Both report `auth_type=pat` to the CLI and both authenticate normally
for ordinary API calls — the difference surfaces only at the exchange.

How to tell them apart without printing the value: the Lakebox credential is 36
chars with **no** `dapi` prefix and a mixed-alphanumeric charset; a classic PAT
carries the `dapi` prefix. `scripts/probe_token_exchange.py` reports exactly
these shape predicates.

| Workspace | Credential | Result |
|---|---|---|
| One AWS workspace, one point in time | PAT | **Exchange succeeds**; app accepts the token |
| `dbc-a601fd91-796b.cloud.databricks.com`, 2026-07-29 | classic PAT, 36 chars, `dapi` prefix | **400** `invalid token for subject_token_type: …personal-access-token` |
| `dbc-31174ae0-1a02.cloud.databricks.com` (Lakebox), 2026-07-30 | Lakebox env credential, 36 chars, **no** `dapi` prefix, mixed alnum | **200 OK** — mints app-scoped token (len ~860, `expires_in` 3600, no refresh) |

### Why the credential type is the cause, not the workspace

The two failures and the success together isolate it. The `dapi` PAT is refused
with `invalid token for subject_token_type` — a complaint about the *token*, not
the request shape — while the Lakebox credential is accepted with the **same**
`subject_token_type`, `audience`, and `requested_token_type`. Same request,
different credential, opposite result. The workspaces differ too, but the error
semantics point at the token: one is rejected *as a subject token*, the other is
not.

Sweeping `subject_token_type` over the RFC's other values answers `audience is
unsupported` on both workspaces, so the PAT subject type is the only one that
reaches token validation at all — the audience being sent is accepted.

### `scope` is required for Apps, and non-empty

On the workspace where the exchange succeeds, omitting `scope` is refused:

```
400 "Request must specify at least one valid scope"
```

The app's `effective_user_api_scopes` satisfy it. **This means `--scope` is
effectively mandatory for a Databricks App**, even though the flag is nominally
optional and `exchange_pat` omits the field entirely when no scope is given (see
its comment: "an app with no `effective_user_api_scopes` has nothing to ask
for"). That branch produces the error above on any app that does declare scopes.
The `dapi`-PAT workspace never reaches this check, because the subject-token
rejection comes first — which is why the earlier local run did not surface it.

**Consequence for auto-discovery:** on a workspace where the exchange works, the
scope requirement makes `--scope` load-bearing, so discovering
`effective_user_api_scopes` from the app record is genuinely useful rather than
cosmetic. It does **not** rescue a classic `dapi` PAT — that is refused upstream
of anything discovery affects. Discovery helps where the credential is already
of an accepted type.

## Reading a failure

| Server says | Means | Do |
|---|---|---|
| `invalid token for subject_token_type: …personal-access-token` | This *credential* is not an accepted subject token — typically a classic `dapi…` PAT. Not a config problem. | Use a Lakebox env credential, `oauth-m2m`, or `databricks-cli` U2M. `--client-id`/`--scope` cannot help a `dapi` PAT. |
| `Request must specify at least one valid scope` | The credential was accepted but no `scope` was sent. | Pass `--scope` with the app's `effective_user_api_scopes`. It is required for Apps. |
| `audience is unsupported` | The `audience` or the `requested_token_type` was rejected. | Confirm `--client-id` is `oauth2_app_client_id`. Note a wrong `requested_token_type` reports as this too. |
| Exchange succeeds, app then answers **401** twice | Configuration, not expiry — the retry already re-minted once. | Confirm the identity has `CAN USE` on the app and `--scope` covers `effective_user_api_scopes`. |
| Exchange succeeds, app answers **403** | Authenticated, not authorized for that route. | Check grants on the target. |

## How the proxy decides to run it

The exchange engages without any flag: `_resolve_exchange` (in `__main__.py`)
inspects the credential, the target URL, and the flags, using `app_discovery.py`
for the two detections. The matrix:

| auth_type | `--url` | credential | flags | Outcome |
|---|---|---|---|---|
| non-`pat` | any | — | none | No exchange; credential sent as-is |
| non-`pat` | any | — | `--client-id`/`--pat-exchange` | Exit: exchange needs a PAT |
| `pat` | non-App | any | none | No exchange (managed/external MCP) |
| `pat` | App | Lakebox | none | **Auto**: discover client id + scopes, exchange |
| `pat` | App | classic `dapi…` | none | **Exit**: refused up front (would be rejected) |
| `pat` | any | any | `--client-id` | Exchange with that id; no discovery |
| `pat` | any | any | `--pat-exchange` | Force discovery + exchange; classic-PAT refusal bypassed with a warning |

Discovery matches `--url` against each app's registered `url` (via
`client.apps.list()`), reads `oauth2_app_client_id`, and fills scopes from
`effective_user_api_scopes` — falling back to `apps.get(name)` when the list
response omits them. Explicit `--scope` overrides discovered scopes. The
resolved client id and scopes are printed to stderr (they are metadata, not
secrets); the PAT is read only to check its shape and is never logged.

## Reproducing on another workspace

`scripts/probe_token_exchange.py` is standalone — stdlib only, no dependency on
this package — so it can be dropped into a sandbox that does not have the proxy
installed. It prints shape predicates and server error text, never a credential.

```bash
python3 scripts/probe_token_exchange.py                 # discovers an app itself
python3 scripts/probe_token_exchange.py --client-id <uuid>
```

Resolving the two flag values by hand, for a URL you already have — match on the
registered `url` rather than parsing the hostname, since the app name is
followed by the workspace id:

```bash
databricks apps list -o json    # match .url → .name, .oauth2_app_client_id
databricks apps get <name> -o json    # .effective_user_api_scopes
```

`apps list` does **not** carry `effective_user_api_scopes`; only `apps get`
does. Any auto-discovery needs both calls.
