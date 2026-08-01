# Changelog

## [0.6.0](https://github.com/IceRhymers/uc-mcp-proxy/compare/v0.5.1...v0.6.0) (2026-08-01)


### Features

* auto-discover the app exchange params, refuse a doomed classic PAT ([cbdbae6](https://github.com/IceRhymers/uc-mcp-proxy/commit/cbdbae6952c8721c69077374eeb94e481b20bcef))
* reach Databricks Apps from a PAT profile via RFC 8693 token exchange ([1f57152](https://github.com/IceRhymers/uc-mcp-proxy/commit/1f571526da3d613f5ec1b88dfd0445c62b4fbcc3))
* reach Databricks Apps from a PAT profile via RFC 8693 token exchange ([64d6eab](https://github.com/IceRhymers/uc-mcp-proxy/commit/64d6eab4a90d5861f5c5e5758ff0c827f773fc28)), closes [#24](https://github.com/IceRhymers/uc-mcp-proxy/issues/24)
* support MCP SDK 2.x alongside 1.x ([5299374](https://github.com/IceRhymers/uc-mcp-proxy/commit/52993740575dd88affbc14ea516c173db6007f8a))


### Bug Fixes

* bound supported SDK HTTP dependencies ([4b1adf9](https://github.com/IceRhymers/uc-mcp-proxy/commit/4b1adf9f39503b49f5fada191f67067f5686837f))
* bound wire bytes, clamp degenerate lifetimes, pin two load-bearing defaults ([c1b7f38](https://github.com/IceRhymers/uc-mcp-proxy/commit/c1b7f383a2b7b7474df47ef24cf0b93946dfad0c))
* close a leak my own redaction fix introduced, and two it did not cover ([ac6cc34](https://github.com/IceRhymers/uc-mcp-proxy/commit/ac6cc34bd4262f7a8f5c981357c713c0c06d897e))
* close two credential-leak paths found in review of the PAT exchange ([71ded8a](https://github.com/IceRhymers/uc-mcp-proxy/commit/71ded8a5d61e043d173064fe7d2a56e98140191d))
* eliminate a ReDoS and stop the read cap from severing secrets ([dc7eab1](https://github.com/IceRhymers/uc-mcp-proxy/commit/dc7eab15e254e24acf1a13f6c40981bcc9f79936))
* redact by span, and stop the pattern cap from exempting real tokens ([f05e303](https://github.com/IceRhymers/uc-mcp-proxy/commit/f05e3033c9503bd00c3bd13b87223d75829b82b5))
* stop an OverflowError escaping with the PAT in frame, and three more ([fb48df9](https://github.com/IceRhymers/uc-mcp-proxy/commit/fb48df979f6a627225e6ca248f0bbcc14c451265))

## [0.5.1](https://github.com/IceRhymers/uc-mcp-proxy/compare/v0.5.0...v0.5.1) (2026-07-30)


### Bug Fixes

* diagnose HTTP errors from the remote MCP server instead of crashing or hiding them ([c2fee34](https://github.com/IceRhymers/uc-mcp-proxy/commit/c2fee34e2ca8d026cbba1374367cff9a784a0982))
* diagnose HTTP errors from the remote MCP server instead of crashing or hiding them ([498e122](https://github.com/IceRhymers/uc-mcp-proxy/commit/498e122c01a77f5fb92f9ef68a5a36160882528b)), closes [#22](https://github.com/IceRhymers/uc-mcp-proxy/issues/22)


### Documentation

* correct the Apps URL pattern and the PAT/M2M support claims ([8e8cc42](https://github.com/IceRhymers/uc-mcp-proxy/commit/8e8cc427992e30f34dd9b084fa2b406fa479e537))
* correct the Apps URL pattern and the PAT/M2M support claims ([9666c40](https://github.com/IceRhymers/uc-mcp-proxy/commit/9666c40ceda0c4ddc807bbcd14157a4a7e6aa564)), closes [#23](https://github.com/IceRhymers/uc-mcp-proxy/issues/23)

## [0.5.0](https://github.com/IceRhymers/uc-mcp-proxy/compare/v0.4.0...v0.5.0) (2026-05-17)


### Features

* Support workspace-relative URL resolution ([de0ab4e](https://github.com/IceRhymers/uc-mcp-proxy/commit/de0ab4ece38437eb4084a1870c12d2498872deec))

## [0.4.0](https://github.com/IceRhymers/uc-mcp-proxy/compare/v0.3.0...v0.4.0) (2026-05-07)


### Features

* add ruff + mypy lint checks to CI ([#16](https://github.com/IceRhymers/uc-mcp-proxy/issues/16)) ([ccf3711](https://github.com/IceRhymers/uc-mcp-proxy/commit/ccf371134dcb610e3dfa5a6f9fd9865fe59b1445))
* auto-trigger `databricks auth login` for OAuth profiles when credentials are missing ([#19](https://github.com/IceRhymers/uc-mcp-proxy/issues/19)) ([6e633c2](https://github.com/IceRhymers/uc-mcp-proxy/commit/6e633c2c06b512cc5e171b7d483287c959ba70db))

## [0.3.0](https://github.com/IceRhymers/uc-mcp-proxy/compare/v0.2.0...v0.3.0) (2026-04-23)


### Features

* add --no-verify-ssl flag for self-signed certificate environments ([66f0d19](https://github.com/IceRhymers/uc-mcp-proxy/commit/66f0d19ad451d6c7503fae1e9bc55a684427a03c))
* add --no-verify-ssl flag for self-signed certificate support ([09f7513](https://github.com/IceRhymers/uc-mcp-proxy/commit/09f75132e53f33509383706ec183fe86a254ba0a)), closes [#12](https://github.com/IceRhymers/uc-mcp-proxy/issues/12)


### Bug Fixes

* update streamable_http_client import and enhance HTTP client configuration ([c31209f](https://github.com/IceRhymers/uc-mcp-proxy/commit/c31209f8275d869dae823efbb520aa8927a00204))


### Documentation

* add --no-verify-ssl section to README ([f1c6d7a](https://github.com/IceRhymers/uc-mcp-proxy/commit/f1c6d7aa6af31ca2bbeb0c5d3558217e0f7aee61))

## [0.2.0](https://github.com/IceRhymers/uc-mcp-proxy/compare/v0.1.1...v0.2.0) (2026-04-21)


### Features

* add Claude Code plugin with uc-mcp-proxy skill ([abac1c4](https://github.com/IceRhymers/uc-mcp-proxy/commit/abac1c45afe20640d8273cd7ad1cdbdfcc8fc433))
* add release-please automation ([e619fe7](https://github.com/IceRhymers/uc-mcp-proxy/commit/e619fe73ed6465d80ac9a2ea8162e816aa4f0f8b))
* add release-please automation ([d7376e2](https://github.com/IceRhymers/uc-mcp-proxy/commit/d7376e29e59ed16edf2170a319dde5f3faf88884))
* inject MCP _meta params into tools/call requests via --meta flag ([#11](https://github.com/IceRhymers/uc-mcp-proxy/issues/11)) ([0113e25](https://github.com/IceRhymers/uc-mcp-proxy/commit/0113e252f0ca3db70327ccd8dba107dcd4131e89))
