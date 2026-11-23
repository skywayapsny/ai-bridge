# Connect a coding agent

Configure the application to use the bridge's `/v1` URL and give the
agent the bridge's root URL, the model tag and, when authentication is
enabled, the agent's Bearer key. For CLI use, set `AI_BRIDGE_URL` and
`AI_BRIDGE_AGENT_KEY` in the agent process. The client uses
`AI_BRIDGE_API_KEY` as a fallback when the agent key is absent.

Give the agent the project context it actually needs. A reproduction should
include the expected and observed result, where to find the calling code,
and any redacted log or trace that did not cross the LLM endpoint.

## HTTP flow

With the app's request waiting, claim the next `debug` request.

```bash
curl -sS http://127.0.0.1:8777/brain/claim \
  -H "Authorization: Bearer $AI_BRIDGE_AGENT_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"client_id":"coding-agent","model":"debug","wait":30}'
```

`204` means nothing matched before the wait ended. A successful claim
contains `request`, `lease_token` and `lease_expires_at` (Unix seconds).
Save that token and use the ID in `request.id`.

| Method and path | Purpose |
| --- | --- |
| `POST /brain/claim` | Reserve a job. Allow up to 120 s for long-polling. Optional `request_id` narrows the claim. |
| `POST /brain/renew/{id}` | Extend the lease. Send `{"lease_token":"..."}`. |
| `POST /brain/respond/{id}` | Complete the job. Send the token and a response body. |
| `POST /brain/release/{id}` | Return the job to the queue. Send the token. |
| `GET /brain/stream` | Receive claimed jobs over SSE. |
| `GET /brain/pending` | Inspect unclaimed jobs. |
| `GET /brain/stats` | Inspect claim and answer counts, plus timeout and lease-expiry counters. |

Set `content` for text, `error` for a failure (the app receives `502`),
or `tool_calls` to imitate a model asking the app to run a tool. Use a
function the app provided in its `tools` list.

```json
{
  "lease_token": "value-from-claim",
  "tool_calls": [
    {"id":"call_1","name":"lookup","arguments":"{\"name\":\"demo\"}"}
  ]
}
```

The bridge defaults to `finish_reason: "tool_calls"` when a tool call is
present, or `"stop"` for text. Report `input_tokens` and `output_tokens`
when actual counts are available. Missing counts default to zero.

The lease protects a single reply. A wrong token gets `403`, an expired
lease or already answered job can get `409`, and a job already removed
from the queue can get `404`. Discard stale work instead of trying to
force its response into a new claim. A timeout, disconnect or reassignment
may cause the app or agent to retry. Keep side effects idempotent in the
application and agent runner.

## SSE sessions

Subscribe to receive jobs and renew their leases while the connection is
active. The event types are `ready` and `claim`; heartbeat comments arrive
about every 15 seconds. Disconnect releases only the leases owned by that
session. A server crash leaves those leases to expire on disk.

```bash
curl -N 'http://127.0.0.1:8777/brain/stream?client_id=coding-agent&model=debug&max_inflight=1' \
  -H "Authorization: Bearer $AI_BRIDGE_AGENT_KEY"
```

Use `ai-bridge-agent stream --model debug --max-inflight 1 --out /path/to/private/claim.json`
to write incoming claims locally. The CLI uses numeric suffixes for later
claims. Respond with `ai-bridge-agent respond REQUEST_ID --claim-file FILE --content '...'`.
Keep the stream connected while you work and remove handled files because
they contain full request payloads and lease tokens. `watch` uses long
polling and needs explicit lease renewal for long work.
