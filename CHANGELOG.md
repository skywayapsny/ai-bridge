# Changelog

## 2.2.0

- Added MIT licensing and an English quickstart that reproduces a function tool call without a model provider.
- Added a separate agent credential for the queue and agent endpoints. A deployment using only the client key can pass it to existing agents, or set a dedicated agent key.
- Made claims, renewals, completion and cleanup coordinate across processes. A disconnected session releases its own work, and a stale consumer cannot answer another lease.
- Added a simultaneous-request limit and cleanup for expired requests after a restart.
- Added loopback tests for the client and agent APIs, SSE, cancellation, upstream responses and the mock browser adapter.

Stop the bridge and file workers together when upgrading from 2.1, then
restart them with the same queue path. See the [operations guide](docs/operations.md)
for credentials and deployment details.
