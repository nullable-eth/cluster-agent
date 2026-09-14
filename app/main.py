"""cluster-agent — alert-triggered reconciler that argues back.

Flow: Alertmanager posts the alert to #cluster-alerts AND webhooks this process.
The agent finds that exact message, opens a thread on it, and works there: its
report goes in the thread, its actions are announced in the thread as the
gateway runs them, the operator replies in the thread from a phone, the agent
answers, and Prometheus's own RESOLVED closes the thread out. One incident, one
readable trail, no context to reconstruct.

This process owns no tools and holds no cluster credentials. The tool loop,
kubectl, Home Assistant, memory and silences all live in the gateway behind
LLM_URL; what lives here is the conversation: take the webhook, find the
message, hold the thread, and make sure every alert ends with either a report
or an explanation of why there isn't one.
"""
import asyncio, json, logging, os, time

import httpx
from fastapi import FastAPI, Request

from . import discord, gateway

log = logging.getLogger("agent")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

E = os.environ.get
CLUSTER   = E("CLUSTER_NAME", "the cluster")
LLM_URL   = E("LLM_URL", "http://llm:8000/v1")   # the gateway, not a model server
LLM_MODEL = E("LLM_MODEL", "default")
LLM_KEY   = E("LLM_API_KEY", "")
MODE      = E("MODE", "propose")            # reported in the prompt; enforced in the gateway
MAX_TOKENS = int(E("LLM_MAX_TOKENS", "4000"))
COOLDOWN  = int(E("COOLDOWN_S", "900"))
POLL_S    = int(E("REPLY_POLL_S", "15"))
# Must exceed the gateway's REQUEST_MAX_SECONDS + ANSWER_TIMEOUT_S (240+150),
# or the caller abandons answers the gateway was about to give.
LLM_TIMEOUT_S = int(E("LLM_TIMEOUT_S", "420"))
# Waits between attempts when the brain is unreachable. Sized to outlast an
# llm-expert rollout (~12 min: pod restart plus a 29GB GGUF load before
# llama.cpp binds its port), not to be polite about a blip.
BRAIN_BACKOFF = [int(s) for s in E("BRAIN_BACKOFF_S", "30,60,120,240,300").split(",") if s.strip()]
# How much of a thread's conversation to carry forward. The gateway compacts,
# but sending a week of argument for every "any update?" is its own problem.
KEEP_TURNS = int(E("KEEP_TURNS", "12"))


SYSTEM = f"""You are cluster-agent, the autonomous SRE for {CLUSTER} (Kubernetes, GitOps-managed).
An alert fired. You are working in a Discord thread attached to that alert, with the operator
reading, so write like a colleague reporting in — short, specific, no ceremony.

1. FIRST, find out whether this is already known. Search the operator's memory for what THEY said
   about this alert — use sender="User" and phrase it the way they would have ("moved the ethernet
   cable", "waiting on a part"), not as the alertname, because your own past reports are in that
   archive too and an alertname search mostly finds those. Read around a hit before trusting it.
2. If the operator has said this state is known and should be ignored, do not investigate it again:
   silence_alert with their words and the hit you found them in, then finish() saying what you
   silenced, for how long, and on whose instruction.
3. Otherwise diagnose from evidence — pod status, events, logs — before concluding anything.
4. You may act to restore service, and every action you take is posted in this thread as it
   happens. Flux reverts direct writes within 30 minutes, so a write buys time NOW; anything meant
   to stick is a git change you describe instead.
5. Mode is '{MODE}'. Mutations outside what policy allows are recorded as proposals, not run.
6. End with finish(). Say plainly what you could not determine.

When the operator replies in the thread, they are talking to you: do what they ask, or say why not.
Their instruction outranks your diagnosis — if they say a state is expected, it is expected.
"""


# --------------------------------------------------------------- the brain
async def ask(messages: list[dict], thread_id: str | None) -> str:
    """One request to the gateway. It runs the tools and returns the report.

    Two failures, treated differently. 502/503 means the brain is NOT THERE —
    a model rollout, where llama.cpp will not bind its port until a 29GB GGUF is
    resident — so back off long enough to outlast one. A read timeout or a 504
    means it is there and still thinking, and a retry would start a second loop
    competing with the first for the same slots: fail once, say so.
    """
    body = {"model": LLM_MODEL, "max_tokens": MAX_TOKENS, "messages": messages}
    headers = {"Authorization": f"Bearer {LLM_KEY}"}
    if thread_id:
        # Tells the gateway where to announce each mutation as it makes it, so
        # actions appear in the incident thread rather than somewhere else.
        headers["X-Discord-Thread"] = thread_id
    last_exc: Exception | None = None
    for attempt in range(len(BRAIN_BACKOFF) + 1):
        try:
            async with httpx.AsyncClient(timeout=LLM_TIMEOUT_S) as c:
                r = await c.post(f"{LLM_URL}/chat/completions", headers=headers, json=body)
            r.raise_for_status()
            msg = r.json()["choices"][0]["message"]
            return (msg.get("content") or "").strip() or "(the model returned an empty answer)"
        except httpx.TimeoutException:
            log.warning("LLM timed out after %ds; not retrying", LLM_TIMEOUT_S)
            raise
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            if exc.response.status_code not in (502, 503):
                raise
            if attempt >= len(BRAIN_BACKOFF):
                break
            wait = BRAIN_BACKOFF[attempt]
            log.warning("LLM %s (attempt %d/%d) — waiting %ds", exc.response.status_code,
                        attempt + 1, len(BRAIN_BACKOFF) + 1, wait)
            await asyncio.sleep(wait)
        except httpx.TransportError as exc:
            last_exc = exc
            if attempt >= len(BRAIN_BACKOFF):
                break
            wait = BRAIN_BACKOFF[attempt]
            log.warning("LLM unreachable (attempt %d/%d): %s — waiting %ds",
                        attempt + 1, len(BRAIN_BACKOFF) + 1, exc, wait)
            await asyncio.sleep(wait)
    raise last_exc  # type: ignore[misc]


# --------------------------------------------------------------- incidents
class Incident:
    """One alert group: its thread, its conversation, where we read up to."""
    def __init__(self, key: str, name: str, thread: str | None):
        self.key, self.name, self.thread = key, name, thread
        self.messages: list[dict] = []
        self.last_seen: str | None = None
        self.ran_at: float = 0.0
        self.lock = asyncio.Lock()
        # Replies arrive twice by design — once down the socket, once from the
        # poller that exists in case the socket is lying. Idempotence is what
        # lets both run without coordinating.
        self.seen: set[str] = set()

    def trim(self) -> list[dict]:
        head, tail = self.messages[:2], self.messages[2:]
        return head + tail[-KEEP_TURNS:] if len(tail) > KEEP_TURNS else self.messages


INCIDENTS: dict[str, Incident] = {}
BY_THREAD: dict[str, Incident] = {}
SELF_ID: str | None = None


def presence_text() -> str:
    """What the member list says the bot is doing, so "up" is legible at a glance."""
    n = len(BY_THREAD)
    return "the cluster" if not n else f"{n} incident thread{'' if n == 1 else 's'}"


async def say(inc: Incident | None, text: str) -> None:
    """Into the incident thread if there is one, else the channel itself."""
    if not discord.enabled():
        log.info("discord disabled; would have said: %s", text[:200])
        return
    await discord.post((inc.thread if inc and inc.thread else discord.CHANNEL), text)


async def investigate(inc: Incident, first: bool) -> None:
    async with inc.lock:
        t0 = time.monotonic()
        try:
            report = await ask(inc.trim(), inc.thread)
        except Exception as exc:
            log.exception("run failed")
            # The alert is already in the channel, so the absence of a report
            # has to be explained where the alert is.
            await say(inc, f"**NOT investigated** — `{type(exc).__name__}: {exc}`")
            return
        inc.messages.append({"role": "assistant", "content": report})
        took = int(time.monotonic() - t0)
        head = "" if not first else ""
        await say(inc, f"{head}{report}\n\n-# {took}s · mode={MODE}")
        log.info("run done: %s in %ds", inc.name, took)


async def handle_alert(payload: dict) -> None:
    key = payload.get("groupKey") or json.dumps(payload.get("groupLabels", {}), sort_keys=True)
    labels = payload.get("groupLabels", {}) or {}
    name = labels.get("alertname", "unknown-alert")
    inc = INCIDENTS.get(key)

    if inc is None:
        thread = None
        if discord.enabled():
            # Wait for Alertmanager's own post before doing anything: the
            # channel learns about an alert before the agent acts on it, which
            # is the invariant, and the message is also the thread's anchor.
            mid = await discord.find_alert_message(name, labels, time.time())
            if mid:
                thread = await discord.open_thread(mid, f"{name} · {labels.get('namespace', '')}".strip(" ·"))
            else:
                log.warning("no Alertmanager message found for %s; reporting in-channel", name)
        inc = Incident(key, name, thread)
        inc.messages = [{"role": "system", "content": SYSTEM},
                        {"role": "user", "content":
                         "Alertmanager payload:\n" + json.dumps(payload, indent=1)[:6000]}]
        INCIDENTS[key] = inc
        if thread:
            BY_THREAD[thread] = inc
            await gateway.set_presence(presence_text())
        if not thread:
            await say(inc, f"**[cluster-agent] {name}** — could not find the alert message to "
                           f"thread on, so this is in-channel.")
    else:
        inc.messages.append({"role": "user", "content":
                             "The same alert group fired again:\n"
                             + json.dumps(payload.get("groupLabels", {}), indent=1)})

    inc.ran_at = time.time()
    await investigate(inc, first=True)


# --------------------------------------------------- the operator's replies
def ingest(inc: Incident, msgs: list[dict]) -> bool:
    """Take operator messages into the conversation. Idempotent, by message id."""
    fresh = [m for m in msgs if m["id"] not in inc.seen and (m.get("content") or "").strip()]
    if not fresh:
        return False
    for m in fresh:
        inc.seen.add(m["id"])
    if len(inc.seen) > 500:
        inc.seen = set(list(inc.seen)[-250:])
    # Snowflakes sort chronologically as integers, and `after=` needs the
    # highest one we have seen — the socket and the poller can hand them over in
    # either order. Anything unparseable is treated as newer rather than
    # crashing the reply path over an id format.
    last = fresh[-1]["id"]
    try:
        newer = inc.last_seen is None or int(last) > int(inc.last_seen)
    except (TypeError, ValueError):
        newer = True
    if newer:
        inc.last_seen = last
    text = "\n".join(f"{m['author']}: {m['content']}" for m in fresh)
    log.info("operator replied in %s (%s): %s", inc.name, inc.thread, text[:120])
    inc.messages.append({"role": "user", "content": text})
    return True


async def on_gateway_message(d: dict) -> None:
    """A message arrived on the socket. Only thread replies from people matter."""
    inc = BY_THREAD.get(str(d.get("channel_id") or ""))
    if not inc:
        return
    author = d.get("author") or {}
    if d.get("webhook_id") or author.get("bot") or author.get("id") == SELF_ID:
        return
    if ingest(inc, [{"id": d["id"], "author": author.get("username", "?"),
                     "content": (d.get("content") or "").strip()}]):
        asyncio.create_task(investigate(inc, first=False))


async def poll_replies() -> None:
    """The safety net under the socket.

    The socket delivers replies instantly and carries the presence that makes
    the bot show as online. This exists because a websocket can fail in ways
    that look exactly like silence, and silence is indistinguishable from "the
    operator had nothing to add". While the socket is up this is a slow
    background sweep; when it is down it is the whole reply path.
    """
    while True:
        await asyncio.sleep(POLL_S * 4 if gateway.connected() else POLL_S)
        if not discord.enabled() or not SELF_ID:
            continue
        for inc in list(BY_THREAD.values()):
            if inc.lock.locked():
                continue                     # busy investigating; read it next tick
            try:
                msgs = await discord.new_messages(inc.thread, inc.last_seen, SELF_ID)
            except Exception as exc:
                log.warning("poll failed on %s: %s", inc.thread, exc)
                continue
            if msgs and ingest(inc, msgs):
                asyncio.create_task(investigate(inc, first=False))


# ------------------------------------------------------------------ service
app = FastAPI(title="cluster-agent")
QUEUE: asyncio.Queue = asyncio.Queue(maxsize=50)


async def worker() -> None:
    while True:
        payload = await QUEUE.get()
        name = (payload.get("groupLabels") or {}).get("alertname", "unknown-alert")
        try:
            await handle_alert(payload)
        except Exception as exc:
            log.exception("alert handling failed")
            await say(None, f"**[cluster-agent] {name}** NOT investigated — "
                            f"`{type(exc).__name__}: {exc}`")
        finally:
            QUEUE.task_done()


@app.on_event("startup")
async def _start() -> None:
    global SELF_ID
    asyncio.create_task(worker())
    asyncio.create_task(poll_replies())
    if discord.enabled():
        SELF_ID = await discord.me()
        # Re-adopt the threads we were already in, so a restart does not strand
        # a conversation mid-incident. Their history is gone; the thread is not.
        for t in await discord.active_threads():
            inc = Incident(f"adopted:{t['id']}", t.get("name", "incident"), t["id"])
            inc.messages = [{"role": "system", "content": SYSTEM},
                            {"role": "user", "content":
                             f"You are resuming an incident thread titled {t.get('name')!r} after a "
                             f"restart. You do not have the earlier conversation; if the operator "
                             f"asks something that depends on it, say so and re-establish from the "
                             f"cluster."}]
            INCIDENTS[inc.key] = inc
            BY_THREAD[t["id"]] = inc
        log.info("adopted %d open thread(s)", len(BY_THREAD))
        # The socket: replies land instantly, and the bot shows as online in the
        # member list — which is the only way to tell "watching" from "dead",
        # since both look like an empty channel.
        asyncio.create_task(gateway.run(discord.TOKEN, on_gateway_message, presence_text))
    log.info("cluster-agent up: mode=%s llm=%s discord=%s poll=%ds timeout=%ds backoff=%s",
             MODE, LLM_URL, "on" if discord.enabled() else "off", POLL_S, LLM_TIMEOUT_S, BRAIN_BACKOFF)


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "mode": MODE, "queued": QUEUE.qsize(), "threads": len(BY_THREAD),
            "discord": gateway.status()}


@app.post("/alert")
async def alert(req: Request) -> dict:
    payload = await req.json()
    status = payload.get("status", "firing")
    key = payload.get("groupKey") or json.dumps(payload.get("groupLabels", {}), sort_keys=True)
    name = (payload.get("groupLabels") or {}).get("alertname", "unknown-alert")
    inc = INCIDENTS.get(key)
    now = time.time()

    if status == "resolved":
        # Prometheus closing the loop, in the same thread as everything else.
        if inc:
            await say(inc, "✅ **Resolved** — Prometheus says this alert has cleared.")
            inc.ran_at = 0.0
        return {"queued": False, "reason": "resolved"}

    if inc and now - inc.ran_at < COOLDOWN:
        left = int(COOLDOWN - (now - inc.ran_at))
        await say(inc, f"-# fired again; not re-investigating for another {left}s. "
                       f"Reply here to override.")
        return {"queued": False, "reason": f"cooldown ({COOLDOWN}s)"}

    try:
        QUEUE.put_nowait(payload)
    except asyncio.QueueFull:
        await say(inc, f"**[cluster-agent] {name}** NOT investigated: queue full "
                       f"({QUEUE.qsize()} waiting). Something is backing up.")
        return {"queued": False, "reason": "queue full"}
    return {"queued": True}
