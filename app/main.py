"""cluster-agent — alert-triggered autonomous reconciler.

Flow: Alertmanager POSTs /alert -> queue -> ONE call to llm-expert (the gateway
behind that Service runs the tool loop: kubectl, Home Assistant, memory search,
silences) -> the answer is posted to the agent's own Discord webhook.

This process owns no tools. It used to carry its own copy of the kubectl guard,
the HA client, the memory client, a tool loop and a compactor; the gateway has
run all of that since 2026-09-12, so every one of those paths was unreachable
code that still had to be kept in step with the gateway's version of the same
thing. One implementation, in the gateway, is the point. What is left here is
the part only this process does: take the webhook, rate-limit it, ask the
question, and make sure the answer — or the reason there isn't one — always
reaches the channel.

Safety lives in the gateway too (MODE, PROTECTED, the verb allowlist, the
silence caps). This process holds no cluster credentials.
"""
import asyncio, json, logging, os, time

import httpx
from fastapi import FastAPI, Request

log = logging.getLogger("agent")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

E = os.environ.get
CLUSTER   = E("CLUSTER_NAME", "the cluster")
LLM_URL   = E("LLM_URL", "http://llm:8000/v1")   # the gateway, not a model server
LLM_MODEL = E("LLM_MODEL", "default")
LLM_KEY   = E("LLM_API_KEY", "")
MODE      = E("MODE", "propose")            # reported in the prompt; enforced in the gateway
DISCORD   = E("DISCORD_WEBHOOK", "")        # the agent's own channel
MAX_TOKENS = int(E("LLM_MAX_TOKENS", "4000"))
COOLDOWN  = int(E("COOLDOWN_S", "900"))
# Must exceed the gateway's REQUEST_MAX_SECONDS (240) with room for the hop:
# below it, the caller abandons answers the gateway was about to give.
LLM_TIMEOUT_S = int(E("LLM_TIMEOUT_S", "420"))
# Waits between attempts when the brain is unreachable, in seconds. Sized to
# outlast an llm-expert rollout (~12 min observed: pod restart plus a 29GB GGUF
# load before llama.cpp binds its port), not to be polite about a blip.
BRAIN_BACKOFF = [int(s) for s in E("BRAIN_BACKOFF_S", "30,60,120,240,300").split(",") if s.strip()]


# ------------------------------------------------------------------ discord
async def discord_post(text: str) -> None:
    if not DISCORD:
        log.info("discord not configured; would have posted: %s", text[:200])
        return
    for i in range(0, len(text), 1900):
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                await c.post(DISCORD, json={"content": text[i:i + 1900]})
        except Exception as exc:
            log.warning("discord post failed (continuing): %s", exc)
            break


# ------------------------------------------------------------------ the ask
SYSTEM = f"""You are cluster-agent, the autonomous SRE for {CLUSTER} (Kubernetes, GitOps-managed).
An alert fired. The operator has already seen it in their alert channel; your job is to tell them
something they do not already know, and to act where policy allows.

1. FIRST, find out whether this is already known. Search the operator's memory for what THEY said
   about this alert — search the way they would have described the situation ("ethernet cable moved
   to another machine", "waiting on a part"), not only the alertname, because your own past alert
   reports are in that archive too and searching the alertname mostly finds those. Read the
   surrounding conversation before trusting a snippet.
2. If the operator has said this state is known and should be ignored, do not investigate it again:
   call silence_alert with their words and the hit you found them in, then finish() saying what you
   silenced, for how long, and on whose instruction.
3. Otherwise diagnose from evidence — pod status, events, logs — before concluding anything.
4. Mode is '{MODE}'. Mutations outside the allowlist are recorded as proposals, never run.
5. This cluster is GitOps (Flux). Config fixes are git changes: describe them exactly in proposals,
   never as kubectl apply/edit.
6. End with finish(). Say plainly what you could not determine.
"""


async def ask(payload: dict) -> str:
    """One request to the gateway. It does the tool work and returns the report.

    The retry policy exists for two different failures and must not treat them
    alike:

      * The brain is NOT THERE — connection refused, or the gateway answering
        502/503 because its upstream is. That is a model rollout, and llama.cpp
        does not bind its port until a 29GB GGUF is resident. The old policy
        retried over ~10 seconds and gave up; on 14 Sep every alert that fired
        during a routine llm-expert rollout was reported as "LLM error at step
        1" and never investigated. BRAIN_BACKOFF spans a rollout instead.

      * The brain is SLOW — a read timeout. Retrying is actively harmful there:
        the gateway's loop is still running the work we walked away from, and
        the retry queues behind it for the same llama.cpp slots, so each attempt
        is slower than the last. Three of those in a row is how a single alert
        burned fifteen minutes and still reported nothing. Fail once, say so.
    """
    body = {"model": LLM_MODEL, "max_tokens": MAX_TOKENS,
            "messages": [{"role": "system", "content": SYSTEM},
                         {"role": "user", "content":
                          "Alertmanager payload:\n" + json.dumps(payload, indent=1)[:6000]}]}
    last_exc: Exception | None = None
    for attempt in range(len(BRAIN_BACKOFF) + 1):
        try:
            async with httpx.AsyncClient(timeout=LLM_TIMEOUT_S) as c:
                r = await c.post(f"{LLM_URL}/chat/completions",
                                 headers={"Authorization": f"Bearer {LLM_KEY}"}, json=body)
            r.raise_for_status()
            msg = r.json()["choices"][0]["message"]
            return (msg.get("content") or "").strip() or "(the model returned an empty answer)"
        except httpx.TimeoutException:
            log.warning("LLM timed out after %ds; not retrying", LLM_TIMEOUT_S)
            raise
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            if exc.response.status_code not in (500, 502, 503, 504):
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


async def handle_alert(payload: dict) -> None:
    name = payload.get("groupLabels", {}).get("alertname", "unknown-alert")
    log.info("agent run start: %s", name)
    t0 = time.monotonic()
    report = await ask(payload)
    log.info("agent run done: %s in %ds", name, int(time.monotonic() - t0))
    await discord_post(f"**[cluster-agent] {name}** (mode={MODE}, "
                       f"{int(time.monotonic() - t0)}s)\n{report[:1800]}")


# ------------------------------------------------------------------ service
app = FastAPI(title="cluster-agent")
QUEUE: asyncio.Queue = asyncio.Queue(maxsize=50)
_last_run: dict[str, float] = {}


async def worker() -> None:
    while True:
        payload = await QUEUE.get()
        name = payload.get("groupLabels", {}).get("alertname", "unknown-alert")
        try:
            await handle_alert(payload)
        except Exception as exc:
            log.exception("agent run failed")
            # Never fail quietly: the alert is already in the operator's channel,
            # so the absence of a report has to be explained there too.
            await discord_post(f"**[cluster-agent] {name}** NOT investigated — "
                               f"`{type(exc).__name__}: {exc}`")
        finally:
            QUEUE.task_done()


@app.on_event("startup")
async def _start() -> None:
    asyncio.create_task(worker())
    log.info("cluster-agent up: mode=%s llm=%s timeout=%ds backoff=%s",
             MODE, LLM_URL, LLM_TIMEOUT_S, BRAIN_BACKOFF)


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "mode": MODE, "queued": QUEUE.qsize()}


@app.post("/alert")
async def alert(req: Request) -> dict:
    payload = await req.json()
    status = payload.get("status", "firing")
    key = payload.get("groupKey") or json.dumps(payload.get("groupLabels", {}), sort_keys=True)
    name = payload.get("groupLabels", {}).get("alertname", "unknown-alert")
    now = time.time()
    if status == "resolved":
        _last_run.pop(key, None)          # allow the next firing to run immediately
        return {"queued": False, "reason": "resolved"}
    # Every drop is announced. A silent drop means the alert channel shows an
    # alert and this channel shows nothing, which reads as "the agent is broken"
    # and was indistinguishable from it.
    if now - _last_run.get(key, 0) < COOLDOWN:
        left = int(COOLDOWN - (now - _last_run.get(key, 0)))
        await discord_post(f"**[cluster-agent] {name}** not investigated: already ran for "
                           f"this group, cooling down for {left}s more.")
        return {"queued": False, "reason": f"cooldown ({COOLDOWN}s) for {key}"}
    try:
        QUEUE.put_nowait(payload)
        _last_run[key] = now
    except asyncio.QueueFull:
        await discord_post(f"**[cluster-agent] {name}** NOT investigated: queue full "
                           f"({QUEUE.qsize()} waiting). Something is backing up.")
        return {"queued": False, "reason": "queue full"}
    return {"queued": True}
