# cluster-agent

An alert-triggered autonomous SRE for a GitOps-managed Kubernetes cluster.
Alertmanager POSTs alerts to `/alert`; the agent investigates with a local
OpenAI-compatible LLM (native tool calling required), guarded `kubectl`, and
optional Home Assistant / memory-service tools, then reports in a Discord forum post per alert. Designed to run fully offline: every dependency is in-cluster or
on-LAN; the Discord audit trail is fire-and-forget and never blocks.

## Safety model

- `MODE=propose` (default): read-only kubectl executes; every mutation is
  recorded as a proposal in the report instead of running.
- `MODE=auto`: only `delete pod` and `rollout restart` execute, and never
  against `PROTECTED` components (the agent itself and whatever you list —
  e.g. the LLM serving it thinks with). Grant matching RBAC only alongside
  this flag; neither works alone.
- Never, in any mode: secrets, exec, apply/edit/patch/create — config belongs
  to GitOps, so the agent describes changes for humans to merge.
- Context self-management: old tool outputs are truncated, and past
  `CTX_BUDGET_TOKENS` the agent compacts its own investigation into a dense
  state note and continues.

## Configuration (all via environment)

| Variable | Default | Purpose |
|---|---|---|
| `CLUSTER_NAME` | `the cluster` | Name used in the agent's own briefing |
| `LLM_URL` | `http://llm:8000/v1` | OpenAI-compatible endpoint with tool calling |
| `LLM_MODEL` | `default` | Model name to request |
| `LLM_API_KEY` | *(empty)* | Bearer key for the LLM endpoint |
| `MODE` | `propose` | `propose` or `auto` |
| `DISCORD_BOT_TOKEN` / `DISCORD_CHANNEL_ID` | *(empty)* | The bot and the FORUM channel it opens one post per alert in. Tags used if present (matched by name): `firing`, `investigating`, `fixed`, `awaiting-reply`, `operator-needed`, `resolved`. Bot only: Alertmanager posts the alerts elsewhere with its own webhook |
| `DISCORD_NOTIFY_USERS` | *(empty)* | Comma-separated Discord user ids to @mention when a report ends in a `DISCORD_NOTIFY_ON` status. Empty: never pings anyone |
| `DISCORD_NOTIFY_ON` | `awaiting-reply` | Comma-separated statuses that ping `DISCORD_NOTIFY_USERS` |
| `AGENT_WORKERS` | `2` | Alerts investigated in parallel (one run per incident at a time) |
| `ALERTMANAGER_URL` | *(empty)* | Alertmanager base URL (e.g. `http://alertmanager:9093`). When set, a queued firing is checked against `/api/v2/alerts` just before it is worked, and skipped — no post, no run — if its group is no longer active (resolved, silenced or inhibited while it waited). Unreachable or unset: the firing is worked as before |
| `HA_URL` / `HA_TOKEN` | *(empty)* | Home Assistant; empty disables HA tools |
| `MEMORY_URL` / `MEMORY_READ_TOKEN` | *(empty)* | agentmemory `/search`; empty disables |
| `PROTECTED` | `cluster-agent` | Comma list of self-preservation targets |
| `MAX_STEPS` | `12` | Tool-step cap per run |
| `CTX_BUDGET_TOKENS` | `48000` | Compaction threshold (also a prefill-latency budget) |
| `COOLDOWN_S` | `900` | Per-alert-group re-run cooldown |

## Run

Deploy with a ServiceAccount whose RBAC matches your MODE (read-only for
propose), point Alertmanager at `http://<svc>:8080/alert` with
`send_resolved: true`, and watch the audit channel.
