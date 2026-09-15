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
import asyncio, hashlib, json, logging, os, time

import httpx
from fastapi import FastAPI, Request

from . import discord, gateway, prompt

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
# How long to let the model work. Generous on purpose: a run that dies at 5
# minutes saying it could not determine anything is worse than one that spends
# 20 and fixes it. This MUST stay above the gateway's own worst case
# (REQUEST_MAX_SECONDS + ANSWER_TIMEOUT_S), or the agent hangs up on work that
# was about to finish and reports a timeout for a run that succeeded.
LLM_TIMEOUT_S = int(E("LLM_TIMEOUT_S", "1800"))
# Waits between attempts when the brain is unreachable. Sized to outlast an
# llm-expert rollout (~12 min: pod restart plus a 29GB GGUF load before
# llama.cpp binds its port), not to be polite about a blip.
BRAIN_BACKOFF = [int(s) for s in E("BRAIN_BACKOFF_S", "30,60,120,240,300").split(",") if s.strip()]
# How much of a thread's conversation to carry forward. The gateway compacts,
# but sending a week of argument for every "any update?" is its own problem.
KEEP_TURNS = int(E("KEEP_TURNS", "12"))


# The playbook comes from the operator's own IaC (a mounted ConfigMap), so the
# prompt is a git change in the cluster repo instead of a rebuild here — and
# another cluster can bring its own without forking this image.
PLAYBOOK_PATH = E("PLAYBOOK_PATH", "/etc/cluster-agent/playbook.md")
SYSTEM = prompt.build(CLUSTER, MODE, prompt.load_playbook(PLAYBOOK_PATH))


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
        # Short id for THIS firing, carried in the thread title and the prompt
        # so one outage can be told from the next and quoted back later.
        self.sha: str = ""
        self.messages: list[dict] = []
        self.last_seen: str | None = None
        self.ran_at: float = 0.0
        # Set when Prometheus sends the RESOLVED. The incident leaves INCIDENTS
        # at that point so the next firing anchors its own thread, but stays
        # watched for replies until the watched set needs the room.
        self.resolved: bool = False
        # False for a thread adopted at startup: we hold the routing but not
        # the conversation, so the first reply pulls the history back out of
        # Discord instead of answering "I don't have the earlier thread".
        self.hydrated: bool = True
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

THREAD_SEP = " · "


def incident_key(payload: dict) -> str:
    """alertname + namespace, not Alertmanager's groupKey.

    The groupKey is opaque and lives only in this process's memory, so after a
    restart a RESOLVED for an incident we were working on matched nothing and
    fell out of its thread into the channel. This key is derivable from two
    places instead of one: the alert payload, and the thread title we wrote
    ("alertname · namespace"). That makes re-adoption complete — a restart
    rejoins the thread AND remembers which alert it belongs to.

    Alertmanager's grouping is the same grouping, just spelled in something
    both sides can reconstruct.

    Built from EVERY groupLabel, not a hardcoded two, so it stays correct if
    the routing ever groups by something else (adding `instance` for probes,
    say, which is what separates two different sites being down into two
    incidents instead of one thread that means both). Capped because it is also
    a Discord thread name, and those are limited to 100 characters.
    """
    gl = payload.get("groupLabels") or {}
    name = gl.get("alertname")
    if not name:
        return payload.get("groupKey") or json.dumps(gl, sort_keys=True)
    rest = [str(v) for k, v in sorted(gl.items()) if k != "alertname" and str(v).strip()]
    return THREAD_SEP.join([name] + rest)[:100].strip()


def incident_sha(payload: dict) -> str:
    """A short, stable id for ONE firing episode.

    Needed because thread titles stopped being unique the moment each firing
    got its own thread: two outages of the same alert are both
    "ProbeFailed · monitoring". The sha distinguishes them, in the title, in
    the prompt the model is given, and in what the operator quotes back.

    Seeded from the group plus the EARLIEST startsAt in the payload, so every
    re-fire inside one episode hashes the same while a genuinely new outage
    hashes differently.
    """
    starts = sorted((a.get("startsAt") or "") for a in (payload.get("alerts") or []))
    seed = f"{payload.get('groupKey') or incident_key(payload)}|{starts[0] if starts else ''}"
    return hashlib.sha256(seed.encode()).hexdigest()[:8]


def thread_title(key: str, sha: str) -> str:
    """Title IS the identity: the key to re-adopt by, plus the sha to tell
    one firing from the next. Discord caps thread names at 100 characters."""
    return f"{key[:100 - len(sha) - len(THREAD_SEP)]}{THREAD_SEP}{sha}"


def sha_from_title(title: str) -> str:
    tail = (title or "").split(THREAD_SEP)[-1].strip()
    return tail if len(tail) == 8 and all(c in "0123456789abcdef" for c in tail) else ""


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


EMPTY_ANSWER = "(the model returned an empty answer)"
# Discord's cap is 2000 per message; leave room for the fence and a little slack.
BLOCK = 1800
PROMPT_MAX = int(E("PROMPT_ECHO_MAX", "8000"))


def latest_prompt(inc: Incident) -> str:
    """The alert-specific prompt this run is about to act on.

    The newest user turn: the Alertmanager payload on a firing, or the
    operator's question on a reply. NOT the system prompt — that is the same
    several thousand characters on every single run, so echoing it would bury
    the thing that actually differs between one incident and the next.
    """
    for m in reversed(inc.messages):
        if m.get("role") == "user":
            return str(m.get("content") or "")
    return "(no prompt)"


async def say_block(inc: Incident, header: str, body: str, footer: str = "") -> None:
    """Post a long verbatim block, fencing each chunk separately.

    post() already splits at 1900 characters, but splitting a single fenced
    block mid-way leaves the opening ``` in one message and renders the rest as
    prose. Chunking first and fencing each piece keeps every part readable.
    """
    await say(inc, header)
    body = body[:PROMPT_MAX]
    for i in range(0, max(len(body), 1), BLOCK):
        await say(inc, "```\n" + (body[i:i + BLOCK] or " ") + "\n```")
    if footer:
        await say(inc, footer)


async def investigate(inc: Incident, first: bool) -> None:
    """Run one pass, and post SOMETHING to the thread whatever happens.

    Silence is the worst outcome here: an alert sits in the channel with a
    thread hanging off it and no way to tell "still thinking" from "the brain
    is down" from "crashed". So this brackets the run — an acknowledgement
    going in, a report or an explicit failure coming out.
    """
    async with inc.lock:
        t0 = time.monotonic()
        await say_block(
            inc,
            f"🧵 **ThreadID** `{inc.sha or '········'}` — prompt sent to the model:",
            latest_prompt(inc),
            f"Handed over, up to ~{LLM_TIMEOUT_S // 60} min of investigation allowed. "
            f"Actions appear here as they happen, and a report lands here either way.")
        try:
            report = await ask(inc.trim(), inc.thread)
        except Exception as exc:
            took = int(time.monotonic() - t0)
            log.exception("run failed")
            # The alert is already in the channel, so the absence of a report
            # has to be explained where the alert is.
            await say(inc, f"❌ **No report — the model never answered** (after {took}s).\n"
                           f"`{type(exc).__name__}: {exc}`\n"
                           f"Nothing was changed and the alert stands. Reply here to retry.")
            return
        took = int(time.monotonic() - t0)
        if not report.strip() or report.strip() == EMPTY_ANSWER:
            log.warning("empty report for %s after %ds", inc.name, took)
            await say(inc, f"❌ **No report — the model answered with nothing** (after {took}s). "
                           f"Nothing was changed. Reply here to make it try again.")
            return
        inc.messages.append({"role": "assistant", "content": report})
        await say(inc, f"{report}\n\n-# {took}s · mode={MODE}")
        log.info("run done: %s in %ds", inc.name, took)


async def handle_alert(payload: dict) -> None:
    key = incident_key(payload)
    sha = incident_sha(payload)
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
                # The title IS the key (see incident_key): a restart reads it
                # back and knows which alert this thread belongs to.
                thread = await discord.open_thread(mid, thread_title(key, sha))
            else:
                log.warning("no Alertmanager message found for %s; reporting in-channel", name)
        inc = Incident(key, name, thread)
        inc.sha = sha
        inc.messages = [{"role": "system", "content": SYSTEM},
                        {"role": "user", "content":
                         f"Incident {sha} ({key}).\n"
                         "Alertmanager payload:\n" + json.dumps(payload, indent=1)[:6000]}]
        INCIDENTS[key] = inc
        if thread:
            BY_THREAD[thread] = inc
            await gateway.set_presence(presence_text())
        if not thread:
            await say(inc, f"**[cluster-agent] {name}** — could not find the alert message to "
                           f"thread on, so this is in-channel.")
    else:
        # The FULL payload, exactly like a first firing. This used to send only
        # groupLabels, and Alertmanager groups by [alertname, namespace] here —
        # so a re-fire arrived carrying nothing but those two, with every
        # per-alert label stripped. A ProbeFailed re-fire therefore did not say
        # WHICH probe was failing, and the agent spent an entire budget
        # rediscovering a target that was sitting in the payload it never got.
        inc.messages.append({"role": "user", "content":
                             "The same alert group fired again. Current payload:\n"
                             + json.dumps(payload, indent=1)[:6000]})

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


async def rehydrate(thread_id: str) -> Incident | None:
    """Rebuild an incident from the thread itself.

    Discord IS the durable store. Nothing about an incident is written to a
    database, and the process that investigated it may have been replaced
    weeks ago — but the thread still holds the whole exchange, so a question
    asked two days later can be answered with the same context the original
    run had. Cheaper and far less to go wrong than persisting transcripts, and
    it cannot drift from what the operator is actually reading.

    Returns None for anything that is not one of our incident threads. Raises
    ThreadGone if it has been deleted, which is the caller's cue to forget it.
    """
    ch = await discord.channel(thread_id)                    # ThreadGone if deleted
    if not ch or ch.get("parent_id") != discord.CHANNEL:
        return None                                          # not ours; ignore
    title = ch.get("name") or ""
    key = title.rsplit(THREAD_SEP, 1)[0] if sha_from_title(title) else title
    inc = Incident(key, key.split(THREAD_SEP)[0] or "incident", thread_id)
    inc.sha = sha_from_title(title)
    inc.resolved = bool((ch.get("thread_metadata") or {}).get("archived"))

    msgs = await discord.history(thread_id)
    convo: list[dict] = []
    for m in msgs:
        role = "assistant" if m["mine"] else "user"
        who = "" if m["mine"] else f"{m['author']}: "
        convo.append({"role": role, "content": f"{who}{m['content']}"[:4000]})
        inc.seen.add(m["id"])
        inc.last_seen = m["id"]
    inc.messages = [{"role": "system", "content": SYSTEM},
                    {"role": "user", "content":
                     f"Incident {inc.sha or '(no id)'} ({key}). This thread is being picked up "
                     f"again later; what follows is the record of it so far."}] + convo
    BY_THREAD[thread_id] = inc
    if not inc.resolved:
        INCIDENTS.setdefault(key, inc)
    log.info("rehydrated %s (%s) from %d thread message(s)", key, inc.sha or "-", len(msgs))
    return inc


async def on_gateway_message(d: dict) -> None:
    """A message arrived on the socket. Only thread replies from people matter."""
    author = d.get("author") or {}
    if d.get("webhook_id") or author.get("bot") or author.get("id") == SELF_ID:
        return                                   # cheap checks BEFORE any API call
    cid = str(d.get("channel_id") or "")
    if not cid or cid == discord.CHANNEL:
        return
    inc, rebuilt = BY_THREAD.get(cid), None
    if inc is None or not inc.hydrated:
        # Either a thread nobody is watching any more — resolved and aged out,
        # or from before a restart — or one adopted for routing with no
        # conversation behind it. Read it back out of Discord rather than
        # ignoring the question or answering without the context.
        try:
            rebuilt = await rehydrate(cid)
        except discord.ThreadGone:
            if inc is not None:
                forget(inc)
            return
        inc = rebuilt or inc
        if inc is None:
            return
    if rebuilt is not None:
        # The message that triggered this is already part of the rebuilt
        # history, and rehydrate marked it seen — so ingest would suppress the
        # very question being asked. Only ingest if Discord had not caught up
        # yet, then answer either way.
        if d["id"] not in inc.seen:
            ingest(inc, [{"id": d["id"], "author": author.get("username", "?"),
                          "content": (d.get("content") or "").strip()}])
        asyncio.create_task(investigate(inc, first=False))
        return
    if ingest(inc, [{"id": d["id"], "author": author.get("username", "?"),
                     "content": (d.get("content") or "").strip()}]):
        asyncio.create_task(investigate(inc, first=False))


WATCH_MAX = int(E("WATCH_MAX_THREADS", "25"))


def retire_watched() -> None:
    """Keep watching resolved threads for replies, but not without limit.

    A resolved incident leaves INCIDENTS (so the next firing gets its own
    thread) yet stays in BY_THREAD, because an operator may well come back and
    ask about it afterwards. That set would otherwise grow for the life of the
    process, so the oldest resolved ones are dropped once it gets long. Live
    incidents are never dropped.
    """
    while len(BY_THREAD) > WATCH_MAX:
        for tid, inc in BY_THREAD.items():          # insertion-ordered: oldest first
            if inc.resolved:
                BY_THREAD.pop(tid, None)
                break
        else:
            return                                   # nothing resolved left to drop


def forget(inc: Incident) -> None:
    """Stop tracking an incident whose thread no longer exists.

    Both registries, or the incident comes straight back: poll_replies walks
    BY_THREAD, and a repeat firing of the same alert would re-adopt the dead
    thread out of INCIDENTS. If that alert fires again it gets a new message
    and a new thread, which is the right outcome — the old one is unreadable.
    """
    if inc.thread:
        BY_THREAD.pop(inc.thread, None)
    if INCIDENTS.get(inc.key) is inc:
        INCIDENTS.pop(inc.key, None)
    log.info("thread %s for %s is gone; stopped watching it", inc.thread, inc.name)


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
            except discord.ThreadGone:
                # Deleted thread, or the channel was cleared under us. Nothing
                # will ever be said here again, so stop watching it — otherwise
                # this id is polled every tick until the process dies.
                forget(inc)
                continue
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
        if SELF_ID:
            discord.remember_self(SELF_ID)   # lets history() tell our voice from theirs
        # Re-adopt the threads we were already in, so a restart does not strand
        # a conversation mid-incident. Their history is gone; the thread is not.
        for t in await discord.active_threads():
            # The thread title is the incident key plus this firing's sha, so
            # adoption restores routing as well as the conversation: a RESOLVED
            # arriving after a restart lands in its own thread instead of loose
            # in the channel. The sha is stripped back off to recover the key.
            title = (t.get("name") or "").strip()
            sha = sha_from_title(title)
            key = title.rsplit(THREAD_SEP, 1)[0] if sha else title
            inc = Incident(key or f"adopted:{t['id']}",
                           key.split(THREAD_SEP)[0] or "incident", t["id"])
            inc.sha = sha
            # Routing only. The conversation is left empty and hydrated=False:
            # the first reply reads the thread back out of Discord, so a
            # question after a restart is answered with the real history rather
            # than an apology for not having it.
            inc.hydrated = False
            inc.messages = [{"role": "system", "content": SYSTEM}]
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
    key = incident_key(payload)
    name = (payload.get("groupLabels") or {}).get("alertname", "unknown-alert")
    inc = INCIDENTS.get(key)
    now = time.time()

    if status == "resolved":
        # Prometheus closing the loop, in the same thread as everything else.
        # Falls back to the channel only when there is genuinely no thread —
        # the alert message was never found, or this resolve is for something
        # that fired before the agent existed. Saying it in the channel beats
        # saying nothing, which is what a dropped resolve looks like.
        await say(inc, f"✅ **Resolved** — Prometheus says {name} has cleared."
                  if not inc else "✅ **Resolved** — Prometheus says this alert has cleared.")
        if inc:
            inc.ran_at = 0.0
            inc.resolved = True
            # RETIRE the incident. It used to live in INCIDENTS forever, so
            # every later firing of the same alert was appended to the FIRST
            # thread — and each new Alertmanager message arrived in the channel
            # with no thread and no reply on it. From a phone that reads as "the
            # agent is dead", while it is in fact working two hours upstream in
            # a thread nobody is looking at. One firing, one message, one thread.
            INCIDENTS.pop(key, None)
            if inc.thread:
                await discord.archive_thread(inc.thread)
            retire_watched()
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
