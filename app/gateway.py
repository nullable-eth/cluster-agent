"""The Discord gateway socket: presence, and replies the instant they are typed.

Polling already answers within fifteen seconds and needs nothing kept alive,
which is why it stays. What polling cannot do is make the bot appear ONLINE:
presence exists only for a live socket, so "is the agent connected and watching"
is a question the member list can answer only if we hold one. That turned out to
be worth the socket on its own — an agent that is quietly dead looks exactly like
an agent with nothing to say.

So both: the socket delivers replies immediately and carries the presence, and
the poller stays as the safety net underneath it. Every reply path is idempotent
(see Incident.seen), so a message arriving twice is free, and a socket that dies
degrades to "offline dot, fifteen-second replies" rather than to silence.

Hand-rolled rather than discord.py: this needs four opcodes and a heartbeat, and
the library is a large dependency for a service that is otherwise 300 lines. The
parts that actually bite — zombie connections, resume vs re-identify, backoff —
are handled below, deliberately and visibly.
"""
import asyncio, json, logging, os, random

import httpx
import websockets

log = logging.getLogger("agent.gateway")

API = "https://discord.com/api/v10"
# GUILDS (thread/channel state) | GUILD_MESSAGES | MESSAGE_CONTENT. Nothing
# else: no presence of others, no member list, no typing.
INTENTS = (1 << 0) | (1 << 9) | (1 << 15)

_state = {"connected": False, "status": "starting"}


def connected() -> bool:
    return _state["connected"]


def status() -> str:
    return _state["status"]


class _Session:
    def __init__(self, token: str, on_message, presence, on_delete=None):
        self.token, self.on_message, self.presence = token, on_message, presence
        self.on_delete = on_delete
        self.seq = None
        self.session_id = None
        self.resume_url = None
        self.ws = None
        self.acked = True

    async def _send(self, op: int, d):
        if self.ws:
            await self.ws.send(json.dumps({"op": op, "d": d}))

    async def _heartbeat(self, interval_ms: int):
        # First beat jittered, per Discord's instruction, so a fleet of bots
        # reconnecting after an outage does not arrive in lockstep.
        await asyncio.sleep(interval_ms / 1000 * random.random())
        while True:
            if not self.acked:
                # No ACK since the last beat: the socket is a zombie — it will
                # accept writes forever and deliver nothing. Drop it.
                log.warning("no heartbeat ACK; reconnecting")
                await self.ws.close(code=4000)
                return
            self.acked = False
            await self._send(1, self.seq)
            await asyncio.sleep(interval_ms / 1000)

    async def set_presence(self, text: str):
        await self._send(3, {"since": None, "afk": False, "status": "online",
                             "activities": [{"name": text, "type": 3}]})   # 3 = Watching

    async def run_once(self) -> bool:
        """One connection. Returns True if the session may be resumed."""
        url = self.resume_url if self.session_id else None
        if not url:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(f"{API}/gateway/bot",
                                headers={"Authorization": f"Bot {self.token}"})
                r.raise_for_status()
                url = r.json()["url"]
        async with websockets.connect(f"{url}/?v=10&encoding=json",
                                      max_size=8 * 1024 * 1024) as ws:
            self.ws = ws
            hello = json.loads(await ws.recv())
            interval = hello["d"]["heartbeat_interval"]
            self.acked = True
            beat = asyncio.create_task(self._heartbeat(interval))
            try:
                if self.session_id:
                    await self._send(6, {"token": self.token,
                                         "session_id": self.session_id, "seq": self.seq})
                else:
                    await self._send(2, {
                        "token": self.token, "intents": INTENTS,
                        "properties": {"os": "linux", "browser": "cluster-agent",
                                       "device": "cluster-agent"},
                        "presence": {"since": None, "afk": False, "status": "online",
                                     "activities": [{"name": self.presence(), "type": 3}]}})
                async for raw in ws:
                    msg = json.loads(raw)
                    op = msg.get("op")
                    if msg.get("s") is not None:
                        self.seq = msg["s"]
                    if op == 11:
                        self.acked = True
                    elif op == 1:
                        await self._send(1, self.seq)
                    elif op == 7:
                        log.info("gateway asked us to reconnect")
                        return True
                    elif op == 9:
                        resumable = bool(msg.get("d"))
                        log.info("session invalidated (resumable=%s)", resumable)
                        if not resumable:
                            self.session_id = None
                        await asyncio.sleep(1 + random.random() * 4)
                        return resumable
                    elif op == 0:
                        t = msg.get("t")
                        if t == "READY":
                            self.session_id = msg["d"]["session_id"]
                            self.resume_url = msg["d"].get("resume_gateway_url")
                            _state["connected"] = True
                            _state["status"] = "online"
                            log.info("gateway ready as %s",
                                     msg["d"].get("user", {}).get("username"))
                        elif t == "RESUMED":
                            _state["connected"] = True
                            _state["status"] = "online (resumed)"
                            log.info("gateway resumed")
                        elif t == "MESSAGE_CREATE":
                            try:
                                await self.on_message(msg["d"])
                            except Exception:
                                log.exception("message handler failed")
                        elif t in ("THREAD_DELETE", "CHANNEL_DELETE"):
                            # These already arrived — the GUILDS intent asks for
                            # them — and were being dropped on the floor, so a
                            # deleted post was only noticed lazily, on the next
                            # 404. Deleting a post is an explicit "stop", and it
                            # should take effect when it is made.
                            if self.on_delete:
                                try:
                                    await self.on_delete(msg["d"])
                                except Exception:
                                    log.exception("delete handler failed")
            finally:
                beat.cancel()
                _state["connected"] = False
        return True


_current: "_Session | None" = None


async def set_presence(text: str) -> None:
    """Update what the bot says it is doing. Silent no-op while disconnected."""
    if _current and _state["connected"]:
        try:
            await _current.set_presence(text)
        except Exception as exc:
            log.warning("presence update failed: %s", exc)


async def run(token: str, on_message, presence, on_delete=None) -> None:
    """Stay connected. Forever, through anything."""
    global _current
    sess = _Session(token, on_message, presence, on_delete)
    _current = sess
    backoff = 1
    while True:
        try:
            resumable = await sess.run_once()
            if not resumable:
                sess.session_id = None
            backoff = 1
        except Exception as exc:
            _state["connected"] = False
            _state["status"] = f"reconnecting ({type(exc).__name__})"
            log.warning("gateway connection lost: %s; retrying in %ss", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            # A resume is only valid for a short while; past a minute of being
            # away, ask for a fresh session rather than a rejected one.
            if backoff >= 60:
                sess.session_id = None
