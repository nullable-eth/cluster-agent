"""cluster-agent — alert-triggered reconciler that argues back.

Flow: Alertmanager posts the alert to #cluster-alerts (its own webhook, so the
alert is seen even if this process is dead) AND webhooks this process. The
agent opens a post for it in the #agent-actions forum and works there: the
alert, its report, its actions (announced by the gateway as they run), the
operator's replies from a phone, and Prometheus's own RESOLVED, one post per
incident. Tags on the post show where it stands; a resolved post is archived.

This process owns no tools and holds no cluster credentials. The tool loop,
kubectl, Home Assistant, memory and silences all live in the gateway behind
LLM_URL; what lives here is the conversation: take the webhook, open the post,
hold it, and make sure every alert ends with either a report or an
explanation of why there isn't one.
"""
import asyncio, hashlib, json, logging, os, re, time

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
# Alerts investigated at the same time. llm-expert serves 4 parallel slots,
# shared with chat clients and agentmemory's filing, so the agent takes 2.
# Each incident is still worked one run at a time (Incident.lock).
WORKERS = int(E("AGENT_WORKERS", "2"))
EMPTY_ANSWER = "(the model returned an empty answer)"
# Discord user ids to @mention when a report ends in one of NOTIFY_ON, so the
# operator's phone rings when the agent is waiting on them. Empty: no pings.
NOTIFY_USERS = [u.strip() for u in E("DISCORD_NOTIFY_USERS", "").split(",") if u.strip()]
NOTIFY_ON = {s.strip() for s in E("DISCORD_NOTIFY_ON", "awaiting-reply").split(",") if s.strip()}


# The playbook comes from the operator's own IaC (a mounted ConfigMap), so the
# prompt is a git change in the cluster repo instead of a rebuild here — and
# another cluster can bring its own without forking this image.
PLAYBOOK_PATH = E("PLAYBOOK_PATH", "/etc/cluster-agent/playbook.md")
SYSTEM = prompt.build(CLUSTER, MODE, prompt.load_playbook(PLAYBOOK_PATH))


# --------------------------------------------------------------- the brain
class GatewayError(Exception):
    """The gateway reported a failure inside a stream that had already started."""


# The gateway sends a keepalive every 15s while tools run, so a silence this
# long means the connection is dead, not that the model is thinking.
STREAM_IDLE_S = int(E("STREAM_IDLE_S", "180"))


async def _stream(body: dict, headers: dict, on_event) -> str:
    """One streamed request; returns all content.

    Every delta goes to on_event(kind, value) as it arrives, in order:
    ("reasoning", text), ("content", text) and ("tool", event).
    """
    content: list[str] = []
    timeout = httpx.Timeout(STREAM_IDLE_S, connect=15)
    async with httpx.AsyncClient(timeout=timeout) as c:
        async with c.stream("POST", f"{LLM_URL}/chat/completions",
                            headers=headers, json=body) as r:
            if r.status_code >= 400:
                await r.aread()
                r.raise_for_status()
            async for line in r.aiter_lines():
                if not line.startswith("data: "):
                    continue                              # keepalive comments
                data = line[6:].strip()
                if data == "[DONE]":
                    break
                try:
                    obj = json.loads(data)
                except ValueError:
                    continue
                delta = ((obj.get("choices") or [{}])[0].get("delta")) or {}
                if delta.get("content"):
                    content.append(delta["content"])
                if on_event is None:
                    continue
                for kind, val in (("reasoning", delta.get("reasoning_content")),
                                  ("tool", delta.get("tool_event")),
                                  ("content", delta.get("content"))):
                    if not val:
                        continue
                    try:
                        await on_event(kind, val)
                    except Exception:
                        log.exception("stream handler failed on %s; continuing", kind)
    text = "".join(content).strip()
    m = GATEWAY_FAIL.search(text)
    if m:
        raise GatewayError(f"agent loop {m.group(1)}: {m.group(2)}")
    return text


GATEWAY_FAIL = re.compile(r"\[gateway: agent loop (timed out|failed): (.*)\]\s*$", re.S)


async def ask(messages: list[dict], on_event=None) -> str:
    """One streamed request to the gateway, which runs the tools.

    Tool calls arrive as `tool_event`s while the loop works, and go to
    on_event (the incident post shows them live). The answer is the streamed
    content.

    Two failures, treated differently. 502/503, or no connection at all, means
    the brain is NOT THERE: a model rollout, where llama.cpp will not bind its
    port until a 29GB GGUF is resident, so back off long enough to outlast
    one. Anything after the stream has started means it is there and working,
    and a retry would start a second loop competing with the first for the
    same slots: fail once, say so.
    """
    body = {"model": LLM_MODEL, "max_tokens": MAX_TOKENS, "messages": messages, "stream": True}
    # Detached: the gateway cancels a run when its client hangs up, except
    # when asked not to. An incident run must finish (and its actions stay
    # consistent) even if this pod restarts mid-run.
    headers = {"Authorization": f"Bearer {LLM_KEY}", "X-Run-Detached": "1"}
    last_exc: Exception | None = None
    for attempt in range(len(BRAIN_BACKOFF) + 1):
        try:
            text = await asyncio.wait_for(_stream(body, headers, on_event), LLM_TIMEOUT_S)
            return text or EMPTY_ANSWER
        except (asyncio.TimeoutError, httpx.ReadTimeout):
            log.warning("LLM stream timed out; not retrying")
            raise
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            if exc.response.status_code not in (502, 503):
                raise
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            last_exc = exc
        if attempt >= len(BRAIN_BACKOFF):
            break
        wait = BRAIN_BACKOFF[attempt]
        log.warning("LLM unavailable (attempt %d/%d): %s — waiting %ds",
                    attempt + 1, len(BRAIN_BACKOFF) + 1, last_exc, wait)
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
        self.title: str = ""
        # Forum tags currently on the post (normalised names).
        self.tags: set[str] = set()
        self.messages: list[dict] = []
        self.last_seen: str | None = None
        self.ran_at: float = 0.0
        # Set when Prometheus sends the RESOLVED. The incident leaves INCIDENTS
        # at that point so the next firing anchors its own thread, but stays
        # watched for replies until the watched set needs the room.
        self.resolved: bool = False
        # True for anything built from a live payload or read back out of
        # Discord. Kept as a guard: an Incident carrying routing but no
        # conversation must pull its history before answering, rather than
        # replying "I don't have the earlier thread" while it sits in the post.
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
    return "the cluster" if not n else f"{n} incident post{'' if n == 1 else 's'}"


async def say(inc: Incident | None, text: str, mention: list[str] | None = None) -> None:
    """Into the incident's post; a new post if it has none or it was deleted."""
    if not discord.enabled():
        log.info("discord disabled; would have said: %s", text[:200])
        return
    if inc is None:
        # Not about one incident (the agent itself failing): its own post, so
        # it is seen and not lost in an unrelated incident.
        await discord.create_post("cluster-agent · notice", text, ["operator-needed"])
        return
    if inc.thread:
        try:
            await discord.post(inc.thread, text, mention)
            return
        except discord.ThreadGone:
            forget(inc)
    # No post yet, or the operator deleted it: open a fresh one rather than
    # talking into a post nobody can see.
    inc.thread = await discord.create_post(
        inc.title or inc.key, "-# the earlier post for this incident is gone; continuing here.\n" + text,
        inc.tags or ["firing"])
    if inc.thread:
        BY_THREAD[inc.thread] = inc
        if not inc.resolved:
            INCIDENTS[inc.key] = inc


async def tag(inc: Incident, *names: str) -> None:
    """Set the post's tags to exactly these."""
    inc.tags = set(names)
    if not (inc.thread and discord.enabled()):
        return
    try:
        await discord.set_tags(inc.thread, inc.tags)
    except discord.ThreadGone:
        forget(inc)


# The model ends every report with one of these (see prompt.CORE).
STATUS_RE = re.compile(r"^\s*\**\s*STATUS:\s*\**\s*(fixed|awaiting-reply|operator-needed|investigating)\b.*$",
                       re.IGNORECASE | re.MULTILINE)


def report_status(report: str) -> tuple[str, str]:
    """(status, report without the status line). No status means a human looks."""
    found = list(STATUS_RE.finditer(report))
    if not found:
        return "operator-needed", report
    m = found[-1]
    return m.group(1).lower(), (report[:m.start()] + report[m.end():]).strip()


def alert_summary(payload: dict) -> str:
    """The post's opening message: what fired, readable on a phone."""
    gl = payload.get("groupLabels") or {}
    alerts = payload.get("alerts") or []
    common = payload.get("commonLabels") or {}
    head = f"🔥 **{gl.get('alertname', 'alert')}**"
    if gl.get("namespace"):
        head += f" · `{gl['namespace']}`"
    if common.get("severity"):
        head += f" · {common['severity']}"
    lines = [head]
    ann = payload.get("commonAnnotations") or {}
    if ann.get("summary"):
        lines.append(ann["summary"])
    for al in alerts[:10]:
        lb = al.get("labels") or {}
        what = lb.get("pod") or lb.get("instance") or lb.get("deployment") or lb.get("node") or ""
        desc = (al.get("annotations") or {}).get("description") or ""
        lines.append(f"- {('`' + what + '` ') if what else ''}{desc[:300]}")
    if len(alerts) > 10:
        lines.append(f"- … and {len(alerts) - 10} more")
    return "\n".join(lines)


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


def _fmt_call(ev: dict) -> str:
    args = ev.get("args") or {}
    if ev.get("name") == "run_kubectl" and isinstance(args.get("args"), list):
        detail = "kubectl " + " ".join(str(a) for a in args["args"])
    else:
        detail = json.dumps(args, ensure_ascii=False)
    return detail if len(detail) <= 300 else detail[:300] + "…"


class Progress:
    """The run, as it happens: a timeline in the post, a full record on file.

    In the post, in order:
    - the model's narration between steps, as ordinary messages;
    - reads, a few lines per message: each burst of reads fills one message
      in place, and the next narration or action starts a new one, so the
      whole sequence stays visible without forty separate messages;
    - every action, its own message before and after.
    What is left after the last step is the report, returned by final().

    Everything, including the model's reasoning and each tool's output, is kept
    in the transcript, which is attached to the post when the run ends: too
    long for chat messages, too useful to throw away.
    """
    EDIT_EVERY_S = 2.0
    BLOCK_MAX = 1700
    TRANSCRIPT_MAX = 400_000

    def __init__(self, inc: "Incident"):
        self.inc, self.reads, self.actions = inc, 0, 0
        self.text: list[str] = []            # narration since the last tool call
        self.block: list[str] = []           # read lines in the current message
        self.block_msg: str | None = None
        self.shown = 0                       # block lines already in Discord
        self.last_edit = 0.0
        # (kind, text, wall-clock time of its first byte)
        self.trace: list[tuple[str, str, float]] = []
        self.started = time.time()
        self.first_action: float | None = None

    def _log(self, kind: str, text: str) -> None:
        if self.trace and self.trace[-1][0] == kind and kind in ("reasoning", "content"):
            k, t, ts = self.trace[-1]
            self.trace[-1] = (k, t + text, ts)
        else:
            self.trace.append((kind, text, time.time()))

    async def __call__(self, kind: str, val) -> None:
        if kind == "reasoning":
            self._log("reasoning", val)
            return
        if kind == "content":
            self._log("content", val)
            self.text.append(val)
            return
        ev = val
        if ev.get("phase") == "call":
            self._log("call", f"{'ACTION ' if ev.get('mutating') else ''}{ev.get('name')}: {_fmt_call(ev)}")
            await self._narration()
            if ev.get("mutating"):
                self.actions += 1
                self.first_action = self.first_action or time.time()
                await self._close_block()
                await say(self.inc, f"⚙️ **action** `{ev.get('name')}` `{_fmt_call(ev)}`")
            else:
                self.reads += 1
                await self._add_read(f"🔎 `{_fmt_call(ev)}`")
            return
        self._log("result", f"{ev.get('name')}:\n{ev.get('output') or ev.get('summary') or '(no output)'}")
        if ev.get("mutating"):
            await say(self.inc, f"↳ **result** `{ev.get('name')}`: {ev.get('summary') or '(no output)'}")

    async def _narration(self) -> None:
        said = "".join(self.text).strip()
        self.text = []
        if said:
            await self._close_block()
            await say(self.inc, said)

    async def _add_read(self, line: str) -> None:
        if sum(len(x) + 1 for x in self.block) + len(line) > self.BLOCK_MAX:
            await self._close_block()
        self.block.append(line)
        if time.monotonic() - self.last_edit >= self.EDIT_EVERY_S:
            await self._render()

    async def _render(self) -> None:
        if not self.block or self.shown == len(self.block):
            return
        self.last_edit = time.monotonic()
        text = "\n".join(self.block)
        if not (discord.enabled() and self.inc.thread):
            self.shown = len(self.block)
            return
        try:
            if self.block_msg:
                await discord.edit(self.inc.thread, self.block_msg, text)
            else:
                self.block_msg = await discord.post(self.inc.thread, text)
        except discord.ThreadGone:
            self.block_msg = None
        self.shown = len(self.block)

    async def _close_block(self) -> None:
        await self._render()
        self.block, self.block_msg, self.shown = [], None, 0

    async def final(self) -> str:
        """Close the timeline; what the model said after its last tool call."""
        await self._close_block()
        report = "".join(self.text).strip()
        self.text = []
        return report

    def transcript(self, title: str, prompt: str, system: str, report: str, status: str) -> bytes:
        """The run as a standalone record: what was asked, what happened
        when, what came of it, and the instructions the model was working to."""
        end = time.time()
        stamp = lambda t: time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(t))
        clock = lambda t: time.strftime("%H:%M:%S", time.localtime(t))
        def since(t: float) -> str:
            d = int(t - self.started)
            return f"+{d // 60}:{d % 60:02d}"
        def dur(t: float) -> str:
            d = int(t)
            return f"{d // 60}m {d % 60:02d}s"
        calls = [t for k, _, t in self.trace if k == "call"]
        first_said = next((t for k, _, t in self.trace if k == "content"), None)
        heads = {"reasoning": "Reasoning", "content": "Said", "call": "Tool call", "result": "Tool result"}

        out = [f"# {title}\n",
               "| | |", "|---|---|",
               *([f"| Alert firing since | {fired} |"] if (fired := min(re.findall(
                   r'"startsAt": "([0-9T:.\-]+Z)"', prompt), default="")) else []),
               f"| Started | {stamp(self.started)} |",
               f"| Finished | {stamp(end)} |",
               f"| Duration | {dur(end - self.started)} |",
               f"| Status | {status} |",
               f"| Tool calls | {self.reads} reads, {self.actions} actions |",
               f"| Model / mode | {LLM_MODEL} / {MODE} |", "",
               "## Prompt\n", "```", prompt.strip(), "```", "",
               "## Timeline\n"]
        for kind, text, ts in self.trace:
            body = text.strip()
            if not body:
                continue
            out.append(f"### {clock(ts)} ({since(ts)}) · {heads[kind]}")
            out.append(f"```\n{body}\n```\n" if kind in ("call", "result") else body + "\n")
        out += ["## Summary\n",
                "| Step | Time | Since start |", "|---|---|---|",
                f"| Started | {clock(self.started)} | +0:00 |"]
        if first_said:
            out.append(f"| First words | {clock(first_said)} | {since(first_said)} |")
        if calls:
            out.append(f"| First tool call | {clock(calls[0])} | {since(calls[0])} |")
        if self.first_action:
            out.append(f"| First action | {clock(self.first_action)} | {since(self.first_action)} |")
        if calls:
            out.append(f"| Last tool call | {clock(calls[-1])} | {since(calls[-1])} |")
        out += [f"| Finished | {clock(end)} | {since(end)} |", "",
                "## Report\n", report.strip() or "(none)", "",
                "## Appendix: system prompt\n", "```", system.strip(), "```", ""]
        data = "\n".join(out).encode()
        return data[:self.TRANSCRIPT_MAX]


async def investigate(inc: Incident, first: bool) -> None:
    """Run one pass, and post SOMETHING to the thread whatever happens.

    Silence is the worst outcome here: an alert sits in the channel with a
    thread hanging off it and no way to tell "still thinking" from "the brain
    is down" from "crashed". So this brackets the run — an acknowledgement
    going in, a report or an explicit failure coming out.
    """
    async with inc.lock:
        t0 = time.monotonic()
        await tag(inc, *(["resolved"] if inc.resolved else ["firing"]), "investigating")
        await say_block(
            inc,
            f"🧵 **ThreadID** `{inc.sha or '········'}` — prompt sent to the model:",
            latest_prompt(inc),
            f"Handed over, up to ~{LLM_TIMEOUT_S // 60} min of investigation allowed. "
            f"Actions appear here as they happen, and a report lands here either way.")
        progress = Progress(inc)
        prompt = latest_prompt(inc)
        first = next((m.get("content") or "" for m in inc.messages if m.get("role") == "user"), "")
        if first and first != prompt:
            # A later run (an operator reply): keep the record self-contained.
            prompt = f"{first}\n\n--- this run was started by ---\n{prompt}"
        try:
            full = await ask(inc.trim(), progress)
            # The report is what came after the last tool call; the narration
            # before it is already in the post. No tool calls: it is all report.
            report = (await progress.final()) or full
        except Exception as exc:
            took = int(time.monotonic() - t0)
            log.exception("run failed")
            # The alert is already in the channel, so the absence of a report
            # has to be explained where the alert is.
            await say(inc, f"❌ **No report — the model never answered** (after {took}s).\n"
                           f"`{type(exc).__name__}: {exc}`\n"
                           f"Nothing was changed and the alert stands. Reply here to retry.")
            await tag(inc, *(["resolved"] if inc.resolved else ["firing"]), "operator-needed")
            return
        took = int(time.monotonic() - t0)
        if not report.strip() or report.strip() == EMPTY_ANSWER:
            log.warning("empty report for %s after %ds", inc.name, took)
            await say(inc, f"❌ **No report — the model answered with nothing** (after {took}s). "
                           f"Nothing was changed. Reply here to make it try again.")
            await tag(inc, *(["resolved"] if inc.resolved else ["firing"]), "operator-needed")
            return
        inc.messages.append({"role": "assistant", "content": report})
        status, shown = report_status(report)
        ping = NOTIFY_USERS if status in NOTIFY_ON else []
        lead = " ".join(f"<@{u}>" for u in ping)
        await say(inc, (lead + "\n" if lead else "") + f"{shown}\n\n-# {took}s · {progress.reads} reads · "
                       f"{progress.actions} actions · mode={MODE} · status={status}", ping)
        if inc.thread and discord.enabled():
            n = sum(1 for m in inc.messages if m.get("role") == "assistant")
            name = f"{inc.sha or 'run'}-{n}.md"
            system = next((m.get("content") or "" for m in inc.messages if m.get("role") == "system"), "")
            record = progress.transcript(f"{inc.title or inc.key} · run {n}", prompt, system,
                                         shown, status)
            try:
                await discord.post_file(inc.thread, "-# full record of this run: prompt, timeline "
                                        "with times, every tool call and its output", name, record)
            except discord.ThreadGone:
                pass
        await tag(inc, *(["resolved"] if inc.resolved else ["firing"]), status)
        log.info("run done: %s in %ds", inc.name, took)


KEY_LOCKS: dict[str, asyncio.Lock] = {}


async def handle_alert(payload: dict) -> None:
    key = incident_key(payload)
    # Two workers may get the same alert group; only one may open its post.
    async with KEY_LOCKS.setdefault(key, asyncio.Lock()):
        inc = await _incident_for(payload, key)
    inc.ran_at = time.time()
    await investigate(inc, first=True)


async def _incident_for(payload: dict, key: str) -> Incident:
    sha = incident_sha(payload)
    labels = payload.get("groupLabels", {}) or {}
    name = labels.get("alertname", "unknown-alert")
    inc = INCIDENTS.get(key)

    if inc is not None and inc.thread and discord.enabled():
        # The operator may have deleted the post; a deleted post is a closed
        # conversation, so this firing starts a new one.
        try:
            await discord.channel(inc.thread)
        except discord.ThreadGone:
            forget(inc)
            inc = None

    if inc is None:
        inc = Incident(key, name, None)
        inc.sha = sha
        # The title IS the key (see incident_key): a restart reads it back
        # and knows which alert this post belongs to.
        inc.title = thread_title(key, sha)
        inc.tags = {"firing", "investigating"}
        inc.messages = [{"role": "system", "content": SYSTEM},
                        {"role": "user", "content":
                         f"Incident {sha} ({key}).\n"
                         "Alertmanager payload:\n" + json.dumps(payload, indent=1)[:6000]}]
        if discord.enabled():
            inc.thread = await discord.create_post(inc.title, alert_summary(payload), inc.tags)
            if not inc.thread:
                log.warning("could not open a post for %s", name)
        INCIDENTS[key] = inc
        if inc.thread:
            BY_THREAD[inc.thread] = inc
            await gateway.set_presence(presence_text())
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
    return inc


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
    inc.title = title
    inc.tags = discord.tag_names(ch.get("applied_tags"))
    inc.resolved = bool((ch.get("thread_metadata") or {}).get("archived")) or "resolved" in inc.tags

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
        # A post the agent is not holding: resolved and evicted from the
        # cache, or from before a restart, or simply one it has never seen.
        # This is the normal path now that Discord is the registry — read the
        # conversation back out of the post rather than ignoring the question
        # or answering without its context.
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
    """Bound the cache. Nothing here is authoritative, so dropping is cheap.

    BY_THREAD is a CACHE, not a registry: Discord holds the conversation, and
    rehydrate() rebuilds any post from it the moment someone writes in that
    post again. So the only cost of evicting is one extra read later. Oldest
    first, and never one that is mid-investigation — that object is live and a
    reply arriving after eviction would rebuild a second Incident for the same
    post while the first is still working.
    """
    while len(BY_THREAD) > WATCH_MAX:
        for tid, inc in list(BY_THREAD.items()):    # insertion-ordered: oldest first
            if not inc.lock.locked():
                BY_THREAD.pop(tid, None)
                break
        else:
            return                                  # every cached post is busy


async def on_gateway_delete(d: dict) -> None:
    """A post (or the whole forum) was deleted in Discord. Stop watching it.

    Deleting a post is the operator saying "drop this". Until now that was only
    noticed lazily, on the next 404, so the agent kept polling an id nobody
    could read.

    This does NOT stop an investigation that is already running. Cancelling a
    tool loop mid-flight can leave a half-applied mutation on the cluster, which
    is worse than a wasted run: the run finishes, and its report simply has
    nowhere to go (say() finds the post gone and drops it).
    """
    gone = str(d.get("id") or "")
    if not gone:
        return
    if gone == discord.CHANNEL:
        # The forum itself. Every post in it went with it.
        n = len(BY_THREAD)
        BY_THREAD.clear()
        INCIDENTS.clear()
        log.warning("forum channel %s deleted; stopped watching all %d post(s)", gone, n)
        return
    inc = BY_THREAD.get(gone)
    if inc is not None:
        forget(inc)
    else:
        # Not cached — evict any alert still routed at it, so the next firing
        # opens a fresh post instead of writing into a deleted one.
        for k, i in list(INCIDENTS.items()):
            if i.thread == gone:
                INCIDENTS.pop(k, None)
                log.info("post %s deleted; %s will open a new one if it fires again", gone, k)


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
    for _ in range(max(1, WORKERS)):
        asyncio.create_task(worker())
    asyncio.create_task(poll_replies())
    if discord.enabled():
        SELF_ID = await discord.me()
        if SELF_ID:
            discord.remember_self(SELF_ID)   # lets history() tell our voice from theirs
        await discord.load_tags()
        # No adoption sweep. The agent used to bulk-load every open post at
        # startup to keep routing alive across a restart; that made memory the
        # registry, grew with the channel, and meant a finished post had to be
        # ARCHIVED to stay out of the sweep — which in a forum means closing it,
        # burying it under "Older Posts" however recent its activity.
        #
        # Discord is the registry instead. The socket delivers every message in
        # the forum, and the first one for a post the agent is not holding
        # rebuilds it from the post itself (rehydrate). So a follow-up on a
        # week-old resolved incident works, nothing is held that nobody is
        # talking about, and there is no sweep to hide from.
        #
        # The one thing lost: after a restart, an alert that is STILL firing has
        # no in-memory route to its open post, so a re-fire opens a second post.
        # That is a duplicate, not a mismatch. Recovering the route would mean
        # matching posts by title, which is exactly how an investigation ends up
        # in the wrong place. Duplicates are safe; mismatches are not.
        # The socket: replies land instantly, and the bot shows as online in the
        # member list — which is the only way to tell "watching" from "dead",
        # since both look like an empty channel.
        asyncio.create_task(gateway.run(discord.TOKEN, on_gateway_message,
                                        presence_text, on_gateway_delete))
    log.info("cluster-agent up: mode=%s llm=%s discord=%s workers=%d poll=%ds timeout=%ds backoff=%s",
             MODE, LLM_URL, "on" if discord.enabled() else "off", WORKERS, POLL_S,
             LLM_TIMEOUT_S, BRAIN_BACKOFF)


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
        # Prometheus closing the loop, in the same post as everything else. A
        # resolve for something the agent never saw has no post, and needs
        # none: #cluster-alerts has the whole story.
        if not inc:
            log.info("resolved %s with no open post; nothing to close", name)
            return {"queued": False, "reason": "resolved"}
        await say(inc, "✅ **Resolved** — Prometheus says this alert has cleared.")
        inc.ran_at = 0.0
        inc.resolved = True
        # RETIRE the incident, so the next firing of the same alert opens its
        # own post instead of appending to this one.
        INCIDENTS.pop(key, None)
        if inc.thread:
            # Tag it and LEAVE IT OPEN. Archiving used to close the post here,
            # purely so the startup sweep would not re-adopt it — and that sweep
            # is gone. In a forum, archiving means closing: Discord buckets
            # closed posts under "Older Posts" and sorts by recency only WITHIN
            # each bucket, so a post resolved a minute ago sank below ones a day
            # old. The `resolved` tag says the same thing without hiding it, and
            # an open post is one you can still add to when the agent called it
            # resolved and you know better.
            await tag(inc, "resolved", *(["fixed"] if "fixed" in inc.tags else []))
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
