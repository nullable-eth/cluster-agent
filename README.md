# cluster-agent

An alert-triggered autonomous SRE for a GitOps-managed Kubernetes cluster.
Alertmanager POSTs alerts to `/alert`; the agent investigates with a local
OpenAI-compatible LLM (native tool calling required), guarded `kubectl`, and
optional Home Assistant / memory-service tools, then reports to a Discord
webhook. Designed to run fully offline: every dependency is in-cluster or
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
| `DISCORD_WEBHOOK` | *(empty)* | Audit channel webhook; optional |
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
