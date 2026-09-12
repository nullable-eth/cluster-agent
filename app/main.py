"""cluster-agent — alert-triggered autonomous reconciler.

Flow: Alertmanager POSTs /alert -> queue -> agent loop against the in-cluster
LLM (OpenAI-compatible tool calling) -> tools: kubectl (guarded), HA REST,
agentmemory search -> report to Discord webhook (audit only, never blocking).

Safety: MODE=propose (default) executes read-only kubectl and *reports*
mutations instead of running them. MODE=auto permits an allowlist (delete pod,
rollout restart) except against PROTECTED targets (itself, its own brain).
GitOps changes are never made directly: the agent recommends; humans merge.
"""
import asyncio, json, logging, os, shlex, subprocess, time

import httpx
from fastapi import FastAPI, Request

log = logging.getLogger("agent")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

E = os.environ.get
CLUSTER   = E("CLUSTER_NAME", "the cluster")
LLM_URL   = E("LLM_URL", "http://llm:8000/v1")   # any OpenAI-compatible server with tool calling
LLM_MODEL = E("LLM_MODEL", "default")
LLM_KEY   = E("LLM_API_KEY", "")
MODE      = E("MODE", "propose")            # propose | auto
DISCORD   = E("DISCORD_WEBHOOK", "")        # audit channel; optional
HA_URL    = E("HA_URL", "")                 # Home Assistant base URL; empty disables HA tools
HA_TOKEN  = E("HA_TOKEN", "")
MEM_URL   = E("MEMORY_URL", "")             # agentmemory base URL; empty disables memory tool
MEM_TOKEN = E("MEMORY_READ_TOKEN", "")
MAX_STEPS = int(E("MAX_STEPS", "12"))
CTX_BUDGET = int(E("CTX_BUDGET_TOKENS", "48000"))  # compact when prompt exceeds; also a prefill-latency budget
KEEP_FULL = int(E("KEEP_FULL_RESULTS", "4"))       # tool results older than this many messages get truncated to digests
COOLDOWN  = int(E("COOLDOWN_S", "900"))
PROTECTED = {p.strip() for p in E("PROTECTED", "cluster-agent").split(",") if p.strip()}

# ------------------------------------------------------------------ kubectl
READ_VERBS = {"get", "describe", "logs", "top", "events", "api-resources",
              "explain", "version", "cluster-info", "rollout"}  # rollout status only; guarded below
DENY = {"secret", "secrets", "exec", "attach", "cp", "port-forward", "proxy",
        "edit", "apply", "create", "patch", "replace", "label", "annotate",
        "cordon", "drain", "taint", "auth", "--token", "--kubeconfig"}
AUTO_OK = {("delete", "pod"), ("delete", "pods"),
           ("rollout", "restart")}


def kubectl_guard(args: list[str]) -> str | None:
    """Return a rejection reason, or None if the command may run."""
    if not args:
        return "empty command"
    low = [a.lower() for a in args]
    for tok in low:
        base = tok.split("=")[0]
        if base in DENY or tok in DENY:
            return f"'{tok}' is never permitted (read RBAC + GitOps: no direct writes)"
    for name in PROTECTED:
        if any(name in t for t in low[1:]):
            if low[0] not in READ_VERBS or low[0:2] == ["rollout", "restart"]:
                return f"target matches protected component '{name}' — self-preservation rule"
    verb = low[0]
    if verb in READ_VERBS and low[0:2] != ["rollout", "restart"]:
        return None
    pair = (verb, low[1] if len(low) > 1 else "")
    if pair in AUTO_OK or (verb, "restart") == ("rollout", "restart"):
        if MODE != "auto":
            return "propose mode: mutation recorded as a proposal, not executed"
        return None
    return f"verb '{verb}' is outside the action allowlist"


def run_kubectl(args: list[str]) -> str:
    why = kubectl_guard(args)
    if why:
        return f"REFUSED: {why}"
    try:
        r = subprocess.run(["kubectl", *args], capture_output=True, text=True, timeout=60)
        out = (r.stdout + ("\n" + r.stderr if r.stderr else "")).strip()
        return out[:8000] or f"(exit {r.returncode}, no output)"
    except subprocess.TimeoutExpired:
        return "REFUSED: command timed out after 60s"


# ------------------------------------------------------------------ HA / memory
async def ha_get_states(entity_id: str = "") -> str:
    if not HA_URL:
        return "Home Assistant is not configured (HA_URL unset)"
    url = f"{HA_URL}/api/states" + (f"/{entity_id}" if entity_id else "")
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(url, headers={"Authorization": f"Bearer {HA_TOKEN}"})
    return r.text[:8000]


async def ha_call_service(domain: str, service: str, entity_id: str = "", data: dict | None = None) -> str:
    if not HA_URL:
        return "Home Assistant is not configured (HA_URL unset)"
    if MODE != "auto":
        return f"PROPOSAL RECORDED (propose mode): ha {domain}.{service} on {entity_id or data}"
    body = dict(data or {})
    if entity_id:
        body["entity_id"] = entity_id
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(f"{HA_URL}/api/services/{domain}/{service}",
                         headers={"Authorization": f"Bearer {HA_TOKEN}"}, json=body)
    return f"HTTP {r.status_code}: {r.text[:2000]}"


async def search_memory(query: str, k: int = 6) -> str:
    if not MEM_URL:
        return "memory service is not configured (MEMORY_URL unset)"
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{MEM_URL}/search", params={"q": query, "k": k},
                            headers={"Authorization": f"Bearer {MEM_TOKEN}"})
        return r.text[:8000]
    except Exception as exc:  # memory is optional context, never fatal
        return f"memory unavailable: {exc}"


# ------------------------------------------------------------------ discord
async def discord_post(text: str) -> None:
    if not DISCORD:
        return
    for i in range(0, len(text), 1900):
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                await c.post(DISCORD, json={"content": text[i:i + 1900]})
        except Exception as exc:
            log.warning("discord post failed (continuing): %s", exc)
            break


TOOLS = [
    {"type": "function", "function": {"name": "run_kubectl",
        "description": "Run a guarded kubectl command against this cluster. Read verbs always allowed; mutations only per policy.",
        "parameters": {"type": "object", "properties": {"args": {"type": "array", "items": {"type": "string"},
            "description": "argv after 'kubectl', e.g. ['get','pods','-n','media']"}}, "required": ["args"]}}},
    {"type": "function", "function": {"name": "ha_get_states",
        "description": "Home Assistant: read entity state(s). Empty entity_id lists all.",
        "parameters": {"type": "object", "properties": {"entity_id": {"type": "string"}}, "required": []}}},
]
TOOLS += [
    {"type": "function", "function": {"name": "ha_call_service",
        "description": "Home Assistant: call a service (e.g. switch.turn_off). Executes only in auto mode; otherwise recorded as proposal.",
        "parameters": {"type": "object", "properties": {"domain": {"type": "string"}, "service": {"type": "string"},
            "entity_id": {"type": "string"}, "data": {"type": "object"}}, "required": ["domain", "service"]}}},
    {"type": "function", "function": {"name": "search_memory",
        "description": "Search the operator's long-term conversation memory for prior incidents, decisions, and known fixes.",
        "parameters": {"type": "object", "properties": {"query": {"type": "string"}, "k": {"type": "integer"}},
            "required": ["query"]}}},
    {"type": "function", "function": {"name": "finish",
        "description": "End the run with a report. Call this exactly once when diagnosis/action is complete.",
        "parameters": {"type": "object", "properties": {
            "summary": {"type": "string", "description": "what happened and root cause"},
            "actions_taken": {"type": "array", "items": {"type": "string"}},
            "proposals": {"type": "array", "items": {"type": "string"},
                "description": "mutations a human should run/approve, exact commands"}},
            "required": ["summary"]}}},
]

SYSTEM = f"""You are cluster-agent, the autonomous SRE for {CLUSTER} (Kubernetes, GitOps-managed).
You are triggered by an alert. Diagnose it with tools, fix what policy allows, report via finish().

Rules, in priority order:
1. SELF-PRESERVATION: never act on protected components ({', '.join(sorted(PROTECTED))}), their pods,
   deployments, or namespaces' LLM serving. If the fix requires touching them, put it in proposals.
2. Mode is '{MODE}'. In propose mode you diagnose fully but mutations become proposals.
   In auto mode only these run: delete pod, rollout restart — never on protected targets.
3. This cluster is GitOps (Flux). Never suggest kubectl apply/edit; config fixes are git changes —
   describe them precisely in proposals instead.
4. Prefer evidence over speculation: read pod status, events, logs BEFORE concluding. Check
   search_memory for prior occurrences of the same alert.
5. Be economical: you have {MAX_STEPS} tool steps. finish() with what you know rather than looping.
"""


# ------------------------------------------------------------------ agent loop
async def call_llm(messages: list[dict], tools: list | None = None) -> tuple[dict, int]:
    """Returns (assistant message, prompt_tokens used) so the loop can manage context."""
    async with httpx.AsyncClient(timeout=300) as c:
        r = await c.post(f"{LLM_URL}/chat/completions",
                         headers={"Authorization": f"Bearer {LLM_KEY}"},
                         json={"model": LLM_MODEL, "messages": messages,
                               "tools": tools if tools is not None else TOOLS,
                               "max_tokens": 2000})
    r.raise_for_status()
    body = r.json()
    used = int(body.get("usage", {}).get("prompt_tokens")
               or sum(len(json.dumps(m)) for m in messages) // 4)  # estimate fallback
    return body["choices"][0]["message"], used


def trim_old_results(messages: list[dict]) -> None:
    """Truncate tool outputs that are no longer recent — evidence already reasoned over."""
    tool_idx = [i for i, m in enumerate(messages) if m.get("role") == "tool"]
    for i in tool_idx[:-KEEP_FULL] if len(tool_idx) > KEEP_FULL else []:
        c = messages[i].get("content") or ""
        if len(c) > 600:
            messages[i]["content"] = c[:500] + f"\n…[truncated {len(c)-500} chars — already analyzed]"


async def compact(messages: list[dict]) -> list[dict]:
    """Claude-Code-style compaction: summarize the investigation into a dense state
    note and rebuild the conversation as [system, alert, state]. Loses verbatim
    transcript, keeps every fact that matters for continuing."""
    ask = messages + [{"role": "user", "content":
        "STOP investigating. Compact this investigation into a dense state summary for your own continuation: "
        "alert + exact resource names/namespaces, evidence gathered (key facts, exact error strings), "
        "hypotheses ruled in/out, actions taken, proposals so far, and immediate next step. Plain text."}]
    summary, _ = await call_llm(ask, tools=[])
    return [messages[0], messages[1],
            {"role": "assistant", "content": "[COMPACTED INVESTIGATION STATE]\n" + (summary.get("content") or "")[:6000]}]


async def dispatch(name: str, args: dict) -> str:
    if name == "run_kubectl":
        return await asyncio.to_thread(run_kubectl, args.get("args", []))
    if name == "ha_get_states":
        return await ha_get_states(args.get("entity_id", ""))
    if name == "ha_call_service":
        return await ha_call_service(args.get("domain", ""), args.get("service", ""),
                                     args.get("entity_id", ""), args.get("data"))
    if name == "search_memory":
        return await search_memory(args.get("query", ""), int(args.get("k", 6)))
    return f"unknown tool {name}"


def fmt_report(alert_name: str, args: dict, steps: int) -> str:
    lines = [f"**[cluster-agent] {alert_name}** (mode={MODE}, {steps} steps)",
             args.get("summary", "(no summary)")]
    if args.get("actions_taken"):
        lines.append("**Actions:** " + "; ".join(args["actions_taken"]))
    if args.get("proposals"):
        lines.append("**Proposals (needs human):**\n" + "\n".join(f"- {p}" for p in args["proposals"]))
    return "\n".join(lines)


async def handle_alert(payload: dict) -> None:
    name = payload.get("groupLabels", {}).get("alertname", "unknown-alert")
    log.info("agent run start: %s", name)
    messages = [{"role": "system", "content": SYSTEM},
                {"role": "user", "content": "Alertmanager payload:\n" + json.dumps(payload, indent=1)[:6000]}]
    used = 0
    for step in range(1, MAX_STEPS + 1):
        if used > CTX_BUDGET:   # checked between exchanges so tool_call pairs stay intact
            log.info("context %d > budget %d — compacting", used, CTX_BUDGET)
            try:
                messages = await compact(messages)
            except Exception as exc:
                log.warning("compaction failed (%s); keeping recent turns only", exc)
                messages = messages[:2] + messages[-4:]
        trim_old_results(messages)
        try:
            msg, used = await call_llm(messages)
        except Exception as exc:
            await discord_post(f"**[cluster-agent] {name}** LLM error at step {step}: {exc}")
            return
        messages.append(msg)
        calls = msg.get("tool_calls") or []
        if not calls:
            await discord_post(f"**[cluster-agent] {name}** (mode={MODE})\n{(msg.get('content') or '')[:1500]}")
            return
        for tc in calls:
            fn = tc["function"]["name"]
            try:
                fargs = json.loads(tc["function"].get("arguments") or "{}")
            except json.JSONDecodeError:
                fargs = {}
            if fn == "finish":
                await discord_post(fmt_report(name, fargs, step))
                log.info("agent run done: %s in %d steps", name, step)
                return
            result = await dispatch(fn, fargs)
            log.info("tool %s(%s) -> %.120s", fn, json.dumps(fargs)[:120], result)
            messages.append({"role": "tool", "tool_call_id": tc.get("id", ""), "content": result})
    await discord_post(f"**[cluster-agent] {name}** hit MAX_STEPS={MAX_STEPS} without finish(); see pod logs.")


# ------------------------------------------------------------------ service
app = FastAPI(title="cluster-agent")
QUEUE: asyncio.Queue = asyncio.Queue(maxsize=50)
_last_run: dict[str, float] = {}


async def worker() -> None:
    while True:
        payload = await QUEUE.get()
        try:
            await handle_alert(payload)
        except Exception:
            log.exception("agent run crashed")
        finally:
            QUEUE.task_done()


@app.on_event("startup")
async def _start() -> None:
    asyncio.create_task(worker())
    log.info("cluster-agent up: mode=%s llm=%s protected=%s", MODE, LLM_URL, sorted(PROTECTED))


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "mode": MODE, "queued": QUEUE.qsize()}


@app.post("/alert")
async def alert(req: Request) -> dict:
    payload = await req.json()
    status = payload.get("status", "firing")
    key = payload.get("groupKey") or json.dumps(payload.get("groupLabels", {}), sort_keys=True)
    now = time.time()
    if status == "resolved":
        _last_run.pop(key, None)          # allow the next firing to run immediately
        return {"queued": False, "reason": "resolved"}
    if now - _last_run.get(key, 0) < COOLDOWN:
        return {"queued": False, "reason": f"cooldown ({COOLDOWN}s) for {key}"}
    try:
        QUEUE.put_nowait(payload)
        _last_run[key] = now
    except asyncio.QueueFull:
        return {"queued": False, "reason": "queue full"}
    return {"queued": True}
