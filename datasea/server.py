"""DataSea wrapper backend.

    DATASEA_ADMIN_PASSWORD=... uv run uvicorn datasea.server:app --port 8700

Browser -> this backend -> EnterpriseOps MCP server(s) -> seeded DB.
Every tool call is proxied and logged here; the browser never talks to MCP directly.

Auth (minimal, pilot-grade):
  * Workers authenticate with a per-worker secret link (/w/<token>); the token is sent as a Bearer header
    and only its SHA-256 is stored. Workers can only see their assigned tasks and their own sessions.
  * /admin and /api/admin/* require HTTP Basic auth with DATASEA_ADMIN_PASSWORD. If that variable is unset,
    admin is reachable from loopback only.

Session modes:
  * onboarding: after submit the worker sees pass/fail and may retry from a fresh seed.
  * production: the first attempt is immutable. If it fails the worker may make one correction attempt that
    continues from the failed final DB state; it is stored as a separate linked session.
"""

import asyncio
import json
import logging
import os
import secrets
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import DATASEA_DIR, RUNTIME_DIR
from .env import TaskEnvironment
from .export import export as run_export
from .provenance import repo_info
from .store import ERROR, FAILED, FLAGGED, IN_PROGRESS, PASSED, Store, now
from .tasks import load_catalog, worker_view

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("datasea")

CORRECTION_WINDOW_SECONDS = 30 * 60
ADMIN_PASSWORD = os.environ.get("DATASEA_ADMIN_PASSWORD")

app = FastAPI(title="DataSea demo collector", docs_url=None, redoc_url=None, openapi_url=None)
store = Store()
live: Dict[str, TaskEnvironment] = {}                 # session_id -> env, sessions accepting tool calls
pending_correction: Dict[str, Dict[str, Any]] = {}    # failed production session_id -> {"env", "deadline"}
_locks: Dict[str, asyncio.Lock] = {}

STATIC = os.path.join(DATASEA_DIR, "static")
app.mount("/static", StaticFiles(directory=STATIC), name="static")
_basic = HTTPBasic(auto_error=False)


# ---------------------------------------------------------------- auth

def require_worker(request: Request) -> Dict[str, Any]:
    auth = request.headers.get("authorization", "")
    token = auth[7:] if auth.lower().startswith("bearer ") else ""
    worker = store.worker_by_token(token) if token else None
    if not worker:
        raise HTTPException(401, "Invalid or missing worker link")
    return worker


def require_admin(request: Request, creds: Optional[HTTPBasicCredentials] = Depends(_basic)) -> None:
    if ADMIN_PASSWORD:
        if creds and secrets.compare_digest(creds.password.encode(), ADMIN_PASSWORD.encode()):
            return
        raise HTTPException(401, "Admin login required", headers={"WWW-Authenticate": 'Basic realm="datasea-admin"'})
    if request.client and request.client.host in ("127.0.0.1", "::1"):
        return
    raise HTTPException(403, "Admin is loopback-only until DATASEA_ADMIN_PASSWORD is set")


def _own_session(session_id: str, worker: Dict[str, Any]) -> Dict[str, Any]:
    s = store.get_session(session_id)
    if not s or s["worker_id"] != worker["worker_id"]:
        raise HTTPException(404, "Unknown session")
    return s


# ---------------------------------------------------------------- models

class StartReq(BaseModel):
    task_id: str


class CallReq(BaseModel):
    tool_name: str
    arguments: Dict[str, Any] = {}


class FinishReq(BaseModel):
    outcome: str  # "submit" | "unclear"
    final_response: str = ""
    worker_note: str = ""


class WorkerCreateReq(BaseModel):
    worker_id: str
    mode: str
    task_ids: list = []


class WorkerUpdateReq(BaseModel):
    mode: Optional[str] = None
    task_ids: Optional[list] = None
    active: Optional[bool] = None


# ---------------------------------------------------------------- pages

@app.get("/")
def worker_page():
    return FileResponse(os.path.join(STATIC, "worker.html"))


@app.get("/w/{token}")
def worker_link(token: str):
    return FileResponse(os.path.join(STATIC, "worker.html"))


@app.get("/admin", dependencies=[Depends(require_admin)])
def admin_page():
    return FileResponse(os.path.join(STATIC, "admin.html"))


# ---------------------------------------------------------------- worker API

def _task_status(worker: Dict[str, Any], task_id: str) -> Dict[str, Any]:
    sessions = store.sessions_for(worker["worker_id"], task_id, worker["mode"])
    if worker["mode"] == "production":
        # crashed/abandoned sessions (status error) never consume the single first attempt
        sessions = [s for s in sessions if s["status"] != ERROR]
    firsts = [s for s in sessions if s["attempt_kind"] == "first"]
    return {"attempts": len(sessions), "done": bool(firsts) and worker["mode"] == "production",
            "last_pass": None if worker["mode"] == "production" or not sessions else sessions[-1]["verifier_pass"]}


def _active_session(worker: Dict[str, Any]) -> Optional[str]:
    for sid in live:
        s = store.get_session(sid)
        if s and s["worker_id"] == worker["worker_id"]:
            return sid
    return None


@app.get("/api/me")
async def me(worker=Depends(require_worker)):
    await _expire_pending()
    catalog = load_catalog()
    pool = "onboarding" if worker["mode"] == "onboarding" else "canary"
    ids = worker["assigned_task_ids"] or [t for t, e in catalog.items() if e.get("pool") == pool]
    tasks = []
    for t in ids:
        e = catalog.get(t)
        if e:
            tasks.append({"task_id": t, "domain": e["domain"], "preview": e["config"]["user_prompt"][:160],
                          **_task_status(worker, t)})
            if worker["mode"] == "production" and not tasks[-1]["done"]:
                break  # production tasks unlock one at a time, in assigned order
    active = _active_session(worker)
    pending = next((sid for sid in pending_correction
                    if store.get_session(sid)["worker_id"] == worker["worker_id"]), None)
    return {"worker_id": worker["worker_id"], "mode": worker["mode"], "tasks": tasks,
            "active_session": _session_payload(active) if active else None,
            "pending_correction": pending}


def _session_payload(session_id: str) -> Dict[str, Any]:
    s = store.get_session(session_id)
    entry = load_catalog()[s["task_id"]]
    steps = store.get_steps(session_id)
    if s["parent_session_id"]:
        steps = store.get_steps(s["parent_session_id"]) + steps
    return {"session_id": session_id, "mode": s["mode"], "attempt_kind": s["attempt_kind"],
            "task": worker_view(entry), "tools": s["available_tools"],
            "history": [{"tool_name": x["tool_name"], "arguments": x["arguments"], "error": x["error"],
                         "result": x["result"], "display": _content_text((x["result"] or {}).get("result")),
                         "from_previous_attempt": x["session_id"] != session_id} for x in steps]}


def _new_session_row(session_id: str, worker: Dict[str, Any], entry: Dict[str, Any], env: TaskEnvironment,
                     attempt_kind: str, parent: Optional[str] = None) -> None:
    view = worker_view(entry)
    store.create_session({
        "session_id": session_id, "worker_id": worker["worker_id"], "task_id": entry["task_id"],
        "domain": entry["domain"], "pilot_only": entry["pilot_only"], "system_prompt": view["policy"],
        "user_prompt": view["instruction"], "selected_tools": entry["config"].get("selected_tools") or [],
        "available_tools": env.tools, "environment": env.environment_info,
        "provenance": {**repo_info(), "task_source": entry["source"], "task_mode": entry["mode"],
                       "task_pool": entry.get("pool")},
        "mode": worker["mode"], "attempt_kind": attempt_kind, "parent_session_id": parent,
    })


@app.post("/api/sessions")
async def start_session(req: StartReq, worker=Depends(require_worker)):
    if _active_session(worker):
        raise HTTPException(409, "Finish your current task first")
    entry = load_catalog().get(req.task_id)
    allowed = {t["task_id"] for t in (await me(worker))["tasks"]}
    if not entry or req.task_id not in allowed:
        raise HTTPException(404, "Task not assigned to you")
    status = _task_status(worker, req.task_id)
    if status["done"]:
        raise HTTPException(409, "You already completed this task")
    attempt_kind = "first" if status["attempts"] == 0 else "retry"

    env = TaskEnvironment(entry["config"])
    try:
        await env.start()
    except Exception as e:
        await env.reset()
        logger.exception("environment start failed")
        raise HTTPException(502, f"Could not start environment: {e}")
    session_id = f"sess_{uuid.uuid4().hex[:16]}"
    _new_session_row(session_id, worker, entry, env, attempt_kind)
    live[session_id] = env
    _locks[session_id] = asyncio.Lock()
    return _session_payload(session_id)


@app.post("/api/sessions/{session_id}/call")
async def call_tool(session_id: str, req: CallReq, worker=Depends(require_worker)):
    _own_session(session_id, worker)
    env = live.get(session_id)
    if env is None:
        raise HTTPException(409, "This task is no longer active")
    async with _locks[session_id]:
        ts = now()
        t0 = time.monotonic()
        try:
            res = await env.call_tool(req.tool_name, req.arguments)
            error = None
            if not res.get("success"):
                error = str(res.get("error"))
            elif res.get("error"):
                error = res["error"] if isinstance(res["error"], str) else json.dumps(res["error"])
            elif isinstance(res.get("result"), dict) and res["result"].get("isError"):
                error = _content_text(res["result"]) or "tool reported isError"
        except Exception as e:
            res, error = {"success": False, "error": str(e)}, str(e)
        ms = int((time.monotonic() - t0) * 1000)
        step = store.add_step(session_id, req.tool_name, req.arguments, res, error, ms, ts)
    return {"step": step, "timestamp": ts, "tool_name": req.tool_name, "arguments": req.arguments,
            "result": res, "display": _content_text(res.get("result")) if res.get("result") else None,
            "error": error, "duration_ms": ms}


@app.post("/api/sessions/{session_id}/finish")
async def finish(session_id: str, req: FinishReq, worker=Depends(require_worker)):
    s = _own_session(session_id, worker)
    env = live.get(session_id)
    if env is None:
        raise HTTPException(409, "This task is no longer active")
    async with _locks[session_id]:
        steps = store.get_steps(session_id)
        if s["parent_session_id"]:
            steps = store.get_steps(s["parent_session_id"]) + steps
        tool_results = [{"tool_name": x["tool_name"], "arguments": x["arguments"]} for x in steps]
        try:
            verifier = await env.verify(req.final_response, tool_results)
            if req.outcome == "unclear":
                status = FLAGGED
            else:
                status = PASSED if verifier["overall_success"] else FAILED
        except Exception as e:
            logger.exception("verifier failed")
            verifier, status = {"overall_success": False, "error": str(e)}, ERROR
        store.finish_session(session_id, status, req.final_response, req.worker_note, verifier)
        live.pop(session_id, None)

        can_correct = (s["mode"] == "production" and s["attempt_kind"] == "first" and status == FAILED)
        if can_correct:
            pending_correction[session_id] = {"env": env, "deadline": time.time() + CORRECTION_WINDOW_SECONDS}
        else:
            await env.reset()
            store.mark_reset(session_id)
            if s["parent_session_id"]:
                store.mark_reset(s["parent_session_id"])

    summary = verifier.get("verification_summary") or {}
    # Counts only: verifier names/queries describe the expected end state and must not reach workers.
    return {"recorded": True, "status": status, "passed": status == PASSED,
            "checks_passed": summary.get("passed"), "checks_total": summary.get("total"), "can_correct": can_correct}


@app.post("/api/sessions/{session_id}/correct")
async def start_correction(session_id: str, worker=Depends(require_worker)):
    parent = _own_session(session_id, worker)
    pend = pending_correction.pop(session_id, None)
    if pend is None:
        raise HTTPException(409, "Correction is not available for this task")
    env = pend["env"]
    entry = load_catalog()[parent["task_id"]]
    sid = f"sess_{uuid.uuid4().hex[:16]}"
    _new_session_row(sid, worker, entry, env, "correction", parent=session_id)
    live[sid] = env
    _locks[sid] = asyncio.Lock()
    return _session_payload(sid)


@app.post("/api/sessions/{session_id}/skip_correction")
async def skip_correction(session_id: str, worker=Depends(require_worker)):
    _own_session(session_id, worker)
    pend = pending_correction.pop(session_id, None)
    if pend:
        await pend["env"].reset()
        store.mark_reset(session_id)
    return {"ok": True}


async def _expire_pending() -> None:
    for sid, pend in list(pending_correction.items()):
        if time.time() > pend["deadline"]:
            pending_correction.pop(sid, None)
            await pend["env"].reset()
            store.mark_reset(sid)


# ---------------------------------------------------------------- admin API

@app.get("/api/admin/workers", dependencies=[Depends(require_admin)])
def admin_workers():
    out = []
    for w in store.list_workers():
        w.pop("token_sha256", None)
        out.append(w)
    return out


@app.post("/api/admin/workers", dependencies=[Depends(require_admin)])
def admin_create_worker(req: WorkerCreateReq, request: Request):
    catalog = load_catalog()
    bad = [t for t in req.task_ids if t not in catalog]
    if bad:
        raise HTTPException(400, f"Unknown task ids: {bad}")
    if store.get_worker(req.worker_id):
        raise HTTPException(409, "worker_id exists")
    token = store.create_worker(req.worker_id.strip(), req.mode, req.task_ids)
    return {"worker_id": req.worker_id, "link_path": f"/w/{token}"}


@app.post("/api/admin/workers/{worker_id}", dependencies=[Depends(require_admin)])
def admin_update_worker(worker_id: str, req: WorkerUpdateReq):
    if not store.get_worker(worker_id):
        raise HTTPException(404)
    store.update_worker(worker_id, req.mode, req.task_ids, req.active)
    return {"ok": True}


@app.post("/api/admin/workers/{worker_id}/rotate", dependencies=[Depends(require_admin)])
def admin_rotate(worker_id: str):
    if not store.get_worker(worker_id):
        raise HTTPException(404)
    return {"worker_id": worker_id, "link_path": f"/w/{store.rotate_token(worker_id)}"}


@app.get("/api/admin/tasks", dependencies=[Depends(require_admin)])
def admin_tasks():
    return [{"task_id": t, "domain": e["domain"], "pool": e.get("pool")} for t, e in load_catalog().items()]


@app.get("/api/admin/sessions", dependencies=[Depends(require_admin)])
def admin_sessions():
    rows = store.list_sessions()
    for r in rows:
        r["duration_seconds"] = (None if not r["ended_at"] else round(
            datetime.fromisoformat(r["ended_at"]).timestamp() - datetime.fromisoformat(r["started_at"]).timestamp(), 1))
        r["live"] = r["session_id"] in live
        r["awaiting_correction"] = r["session_id"] in pending_correction
    return rows


@app.get("/api/admin/sessions/{session_id}", dependencies=[Depends(require_admin)])
def admin_session(session_id: str):
    s = store.get_session(session_id)
    if not s:
        raise HTTPException(404)
    return {"session": s, "steps": store.get_steps(session_id)}


@app.post("/api/admin/sessions/{session_id}/reset", dependencies=[Depends(require_admin)])
async def admin_reset(session_id: str):
    """Abandon a live session or pending correction: keep the trajectory, drop the database."""
    if session_id in live:
        env = live.pop(session_id)
        store.finish_session(session_id, ERROR, "", "abandoned via admin reset", None)
    elif session_id in pending_correction:
        env = pending_correction.pop(session_id)["env"]
    else:
        raise HTTPException(404, "Nothing live for this session")
    await env.reset()
    store.mark_reset(session_id)
    return {"reset": True}


@app.post("/api/admin/export", dependencies=[Depends(require_admin)])
def admin_export():
    out = os.path.join(RUNTIME_DIR, "exports")
    return {"out_dir": out, "counts": run_export(out)}


# ---------------------------------------------------------------- lifecycle

@app.on_event("startup")
async def _recover_orphans():
    """Sessions left live by a crash/restart: mark as error and drop their databases."""
    from benchmark.mcp_client import delete_database

    for r in store.list_sessions():
        s = store.get_session(r["session_id"])
        if s["reset_at"]:
            continue
        if s["status"] == IN_PROGRESS:
            store.finish_session(s["session_id"], ERROR, "", "server restarted before submit", None)
        for e in s["environment"] or []:
            await asyncio.to_thread(delete_database, e["mcp_server_url"], e["database_id"])
        store.mark_reset(s["session_id"])


@app.on_event("shutdown")
async def _cleanup():
    for sid, env in list(live.items()):
        store.finish_session(sid, ERROR, "", "server shutdown before submit", None)
        await env.reset()
        store.mark_reset(sid)
    for sid, pend in list(pending_correction.items()):
        await pend["env"].reset()
        store.mark_reset(sid)


def _content_text(result: Any) -> Optional[str]:
    """Pull the human-readable text out of an MCP tool result."""
    if not isinstance(result, dict):
        return None
    parts = [c.get("text", "") for c in result.get("content", []) if isinstance(c, dict) and c.get("type") == "text"]
    return "\n".join(parts) if parts else None
