"""The Discord side of the agent: one forum post per alert.

Alertmanager posts every alert to #cluster-alerts with its webhook, on its own,
so an alert is visible even when the agent is dead. The agent works in a
separate FORUM channel (DISCORD_CHANNEL_ID, #agent-actions): each alert it
receives opens its own post, and the post is the incident record: the alert,
the diagnosis, every action (posted by the gateway), the operator's replies
and the outcome, contained in one place that can be read, answered and deleted
as a unit. Tags on the post say where it stands (firing, investigating, fixed,
operator-needed, resolved), and a resolved post is archived.

An alert in #cluster-alerts with no matching post here means the agent is not
working; that is the health signal, so the agent never posts in that channel.
"""
import asyncio, logging, os, re, time

import httpx

log = logging.getLogger("agent.discord")

API = "https://discord.com/api/v10"
E = os.environ.get
TOKEN = E("DISCORD_BOT_TOKEN", "")
CHANNEL = E("DISCORD_CHANNEL_ID", "")
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


async def post(channel_or_thread: str, text: str, mention: list[str] | None = None) -> str | None:
    """Post, chunked. Returns the id of the first message, or None.

    Nothing in the text can ping anyone unless its user id is in `mention`:
    a report quoting "@everyone" from a log must not page a server.
    """
    first = None
    allowed = {"parse": [], "users": list(mention or [])[:100]}
    for i in range(0, len(text), LIMIT):
        s, body = await call("POST", f"/channels/{channel_or_thread}/messages",
                             {"content": text[i:i + LIMIT],
                              "allowed_mentions": allowed})
        if s == 404 or s == 403:
            # Most likely an archived post: posting to one is refused until it
            # is reopened, and a week-old incident that comes back is exactly
            # when you want the old context, not a fresh post.
            await call("PATCH", f"/channels/{channel_or_thread}", {"archived": False})
            s, body = await call("POST", f"/channels/{channel_or_thread}/messages",
                                 {"content": text[i:i + LIMIT],
                                  "allowed_mentions": allowed})
            if s == 404:
                raise ThreadGone(channel_or_thread)
        if s >= 300:
            log.warning("discord post failed %s: %s", s, str(body)[:200])
            return first
        first = first or (body or {}).get("id")
    return first


async def edit(thread_id: str, message_id: str, text: str) -> None:
    """Replace one of our messages (the live progress line)."""
    s, _ = await call("PATCH", f"/channels/{thread_id}/messages/{message_id}",
                      {"content": text[:LIMIT], "allowed_mentions": {"parse": []}})
    if s == 404:
        raise ThreadGone(thread_id)


# Forum tags, by normalised name ("🔥 firing" and "firing" are the same tag).
TAGS: dict[str, str] = {}
TAG_NAMES: dict[str, str] = {}


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9-]", "", (name or "").lower())


async def load_tags() -> None:
    """Read the forum's tags, so posts can be tagged by name."""
    s, ch = await call("GET", f"/channels/{CHANNEL}")
    if s != 200 or not ch:
        log.error("cannot read channel %s: HTTP %s", CHANNEL, s)
        return
    if ch.get("type") != 15:
        log.error("DISCORD_CHANNEL_ID %s is not a forum channel (type %s)", CHANNEL, ch.get("type"))
    for t in ch.get("available_tags") or []:
        TAGS[_norm(t["name"])] = t["id"]
        TAG_NAMES[t["id"]] = _norm(t["name"])
    log.info("forum tags: %s", ", ".join(sorted(TAGS)) or "(none)")


def tag_ids(names) -> list[str]:
    """Known tags only, at most 5 (Discord's limit per post)."""
    out = [TAGS[_norm(n)] for n in names if _norm(n) in TAGS]
    return list(dict.fromkeys(out))[:5]


def tag_names(ids) -> set[str]:
    return {TAG_NAMES[i] for i in (ids or []) if i in TAG_NAMES}


async def create_post(title: str, text: str, tags) -> str | None:
    """Open a forum post. Returns its id (a thread id), or None."""
    s, body = await call("POST", f"/channels/{CHANNEL}/threads", {
        "name": title[:100],
        "auto_archive_duration": AUTO_ARCHIVE_MIN,
        "applied_tags": tag_ids(tags),
        "message": {"content": text[:LIMIT], "allowed_mentions": {"parse": []}},
    })
    if s not in (200, 201) or not body:
        log.warning("post create failed %s: %s", s, str(body)[:200])
        return None
    tid = body.get("id")
    if tid and len(text) > LIMIT:
        await post(tid, text[LIMIT:])
    return tid


async def set_tags(thread_id: str, names) -> None:
    s, body = await call("PATCH", f"/channels/{thread_id}", {"applied_tags": tag_ids(names)})
    if s == 400 and "archived" in str(body).lower():
        # An archived post can only be edited while reopening it.
        s, body = await call("PATCH", f"/channels/{thread_id}",
                             {"archived": False, "applied_tags": tag_ids(names)})
    if s == 404:
        raise ThreadGone(thread_id)
    if s >= 300:
        log.warning("could not tag %s: %s %s", thread_id, s, str(body)[:120])


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


async def channel(channel_id: str) -> dict | None:
    """One channel/thread, or None if it is gone. 404 here means deleted."""
    s, body = await call("GET", f"/channels/{channel_id}")
    if s == 404:
        raise ThreadGone(channel_id)
    return body if s == 200 else None


async def history(thread_id: str, limit: int = 60) -> list[dict]:
    """A thread's messages, oldest first, EVERYONE's — including our own.

    new_messages() deliberately filters down to human replies since a marker;
    this is the opposite job. Rebuilding an incident days later means replaying
    what was actually said, both halves of it, which is the only record of that
    investigation once the process that ran it is long gone.
    """
    s, msgs = await call("GET", f"/channels/{thread_id}/messages?limit={min(limit, 100)}")
    if s == 404:
        raise ThreadGone(thread_id)
    if s != 200 or not isinstance(msgs, list):
        return []
    out = []
    for m in reversed(msgs):                      # Discord returns newest first
        text = (m.get("content") or "").strip()
        if not text:
            continue
        a = m.get("author") or {}
        out.append({"id": m["id"],
                    "author": a.get("username", "?"),
                    "mine": bool(a.get("id") and a.get("id") == _SELF.get("id")),
                    "bot": bool(a.get("bot") or m.get("webhook_id")),
                    "content": text})
    return out


_SELF: dict = {}          # filled by remember_self(), so history() can tell our own voice


def remember_self(user_id: str) -> None:
    _SELF["id"] = user_id


async def archive_thread(thread_id: str) -> None:
    """Close a post out when its incident resolves.

    Two jobs. It reads as finished in the client, and it drops out of the
    guild's ACTIVE thread list — so a restart does not re-adopt a post whose
    incident is over and start appending the next outage to it. Posting to an
    archived post reopens it (see post), so an operator reply still works.
    """
    s, _ = await call("PATCH", f"/channels/{thread_id}", {"archived": True})
    if s >= 300:
        log.warning("could not archive thread %s: %s", thread_id, s)


async def active_threads() -> list[dict]:
    """Posts still open in the forum, so a restart rejoins its incidents."""
    s, ch = await call("GET", f"/channels/{CHANNEL}")
    guild = (ch or {}).get("guild_id") if s == 200 else None
    if not guild:
        return []
    s, body = await call("GET", f"/guilds/{guild}/threads/active")
    if s != 200:
        return []
    return [t for t in (body or {}).get("threads", []) if t.get("parent_id") == CHANNEL]
