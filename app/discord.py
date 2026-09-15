"""The Discord side of the agent: one channel, one thread per alert.

A webhook could only shout. This is a bot, because the whole point is the
conversation: Alertmanager posts the alert, the agent opens a THREAD on that
exact message and works there, the operator replies in the thread from wherever
they are, and Prometheus's own RESOLVED lands in the same place. The thread is
the incident record — alert, diagnosis, actions, argument, outcome, in order.

Why the agent does not post the alert itself: then Discord would depend on the
agent being alive, and the agent being dead is exactly when the alert matters.
Alertmanager posts; the agent finds that message and threads on it. Finding it
also enforces the rule that the channel is told before the agent acts, which no
amount of Alertmanager config could guarantee (its integrations fan out in
parallel).
"""
import asyncio, logging, os, re, time

import httpx

log = logging.getLogger("agent.discord")

API = "https://discord.com/api/v10"
E = os.environ.get
TOKEN = E("DISCORD_BOT_TOKEN", "")
CHANNEL = E("DISCORD_CHANNEL_ID", "")
# Alertmanager and the agent race; the alert usually lands first but not always.
FIND_TIMEOUT_S = int(E("ALERT_FIND_TIMEOUT_S", "45"))
# 7 days. An incident nobody replied to in a week is over, one way or another.
AUTO_ARCHIVE_MIN = int(E("THREAD_AUTO_ARCHIVE_MIN", "10080"))
LIMIT = 1900          # Discord hard-caps a message at 2000 characters


def enabled() -> bool:
    return bool(TOKEN and CHANNEL)


async def call(method: str, path: str, json=None, retries: int = 3):
    """One Discord REST call, with the 429 handling everything else forgets."""
    headers = {"Authorization": f"Bot {TOKEN}",
               "User-Agent": "cluster-agent (+https://github.com/nullable-eth/cluster-agent, 1.0)"}
    for attempt in range(retries):
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.request(method, API + path, headers=headers, json=json)
        if r.status_code == 429:
            wait = float((r.json() or {}).get("retry_after", 1.0))
            log.warning("discord rate limited on %s; sleeping %.1fs", path, wait)
            await asyncio.sleep(wait + 0.25)
            continue
        if r.status_code >= 500 and attempt + 1 < retries:
            await asyncio.sleep(1 + attempt)
            continue
        body = r.json() if r.content and r.headers.get("content-type", "").startswith("application/json") else None
        return r.status_code, body
    return 599, None


async def post(channel_or_thread: str, text: str) -> str | None:
    """Post, chunked. Returns the id of the first message, or None."""
    first = None
    for i in range(0, len(text), LIMIT):
        s, body = await call("POST", f"/channels/{channel_or_thread}/messages",
                             {"content": text[i:i + LIMIT],
                              "allowed_mentions": {"parse": []}})
        if s == 404 or s == 403:
            # Most likely an archived thread: posting to one is refused until it
            # is reopened, and a week-old incident that comes back is exactly
            # when you want the old context, not a fresh thread.
            await call("PATCH", f"/channels/{channel_or_thread}", {"archived": False})
            s, body = await call("POST", f"/channels/{channel_or_thread}/messages",
                                 {"content": text[i:i + LIMIT],
                                  "allowed_mentions": {"parse": []}})
        if s >= 300:
            log.warning("discord post failed %s: %s", s, str(body)[:200])
            return first
        first = first or (body or {}).get("id")
    return first


def _mentions(text: str, alertname: str, labels: dict) -> bool:
    if alertname and alertname.lower() in text.lower():
        return True
    return False


async def find_alert_message(alertname: str, labels: dict, since: float) -> str | None:
    """Find the Alertmanager post for this alert, and wait for it if need be.

    Matching is on the alertname in the message body or embed, plus "posted
    recently" and "has no thread yet". Alertmanager groups by alertname +
    namespace, so two firing groups with the same alertname in the same
    namespace would be one message anyway.
    """
    deadline = time.monotonic() + FIND_TIMEOUT_S
    ns = (labels or {}).get("namespace", "")
    while time.monotonic() < deadline:
        s, msgs = await call("GET", f"/channels/{CHANNEL}/messages?limit=15")
        if s == 200 and isinstance(msgs, list):
            for m in msgs:
                if m.get("thread"):
                    continue                      # already has a thread
                ts = m.get("timestamp") or ""
                blob = (m.get("content") or "")
                for e in m.get("embeds") or []:
                    blob += " " + (e.get("title") or "") + " " + (e.get("description") or "")
                if not _mentions(blob, alertname, labels):
                    continue
                if ns and ns not in blob:
                    # namespace is in Alertmanager's title; if it disagrees this
                    # is a different group with the same alertname.
                    continue
                if "[RESOLVED]" in blob:
                    continue
                return m["id"]
        await asyncio.sleep(3)
    return None


async def open_thread(message_id: str, name: str) -> str | None:
    s, body = await call("POST", f"/channels/{CHANNEL}/messages/{message_id}/threads",
                         {"name": name[:100], "auto_archive_duration": AUTO_ARCHIVE_MIN})
    if s in (200, 201):
        return (body or {}).get("id")
    if s == 400 and body and "already has a thread" in str(body).lower():
        s2, m = await call("GET", f"/channels/{CHANNEL}/messages/{message_id}")
        if s2 == 200:
            return ((m or {}).get("thread") or {}).get("id")
    log.warning("thread create failed %s: %s", s, str(body)[:200])
    return None


async def me() -> str | None:
    s, body = await call("GET", "/users/@me")
    return (body or {}).get("id") if s == 200 else None


class ThreadGone(Exception):
    """The thread no longer exists — deleted, or its channel was cleared.

    Distinct from "nothing new was said", which is an empty list. A deleted
    thread used to be indistinguishable from a quiet one: 404 returned [], the
    incident stayed in the poll set, and the agent re-polled a dead id every
    tick for the life of the process. Clearing the channel left one such loop
    running for days, burning Discord's rate limit on a thread nobody could
    read.
    """


async def new_messages(thread_id: str, after: str | None, self_id: str) -> list[dict]:
    """Human messages in a thread since `after`, oldest first.

    Raises ThreadGone if the thread has been deleted, so the caller can stop
    watching it rather than polling forever.
    """
    q = f"/channels/{thread_id}/messages?limit=20" + (f"&after={after}" if after else "")
    s, msgs = await call("GET", q)
    if s == 404:
        raise ThreadGone(thread_id)
    if s != 200 or not isinstance(msgs, list):
        log.warning("thread poll failed %s on %s", s, thread_id)
        return []
    out = []
    for m in reversed(msgs):                      # Discord returns newest first
        if (m.get("author") or {}).get("id") == self_id:
            continue
        if m.get("webhook_id"):                   # Alertmanager, not a person
            continue
        if (m.get("author") or {}).get("bot"):
            continue
        if (m.get("type") or 0) not in (0, 19):   # default + reply
            continue
        out.append({"id": m["id"], "author": (m.get("author") or {}).get("username", "?"),
                    "content": (m.get("content") or "").strip()})
    return out


async def archive_thread(thread_id: str) -> None:
    """Close a thread out when its incident resolves.

    Two jobs. It reads as finished in the client, and it drops out of the
    guild's ACTIVE thread list — so a restart does not re-adopt a thread whose
    incident is over and start appending the next outage to it. Posting to an
    archived thread reopens it (see post), so an operator reply still works.
    """
    s, _ = await call("PATCH", f"/channels/{thread_id}", {"archived": True})
    if s >= 300:
        log.warning("could not archive thread %s: %s", thread_id, s)


async def active_threads() -> list[dict]:
    """Threads already open on our channel, so a restart rejoins its incidents."""
    s, ch = await call("GET", f"/channels/{CHANNEL}")
    guild = (ch or {}).get("guild_id") if s == 200 else None
    if not guild:
        return []
    s, body = await call("GET", f"/guilds/{guild}/threads/active")
    if s != 200:
        return []
    return [t for t in (body or {}).get("threads", []) if t.get("parent_id") == CHANNEL]
