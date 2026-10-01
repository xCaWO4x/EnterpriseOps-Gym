"""SQLite persistence for workers, sessions, and trajectory steps. Nothing is ever deleted.

Session `mode`:
    onboarding   practice; worker sees pass/fail and may retry. Never exported for SFT.
    production   real collection; the first attempt is immutable.
    engineering  internal test sessions (incl. pre-mode milestone-1 rows). Never exported for SFT.

Session `attempt_kind`:
    first        first attempt by this worker on this task in this mode
    retry        onboarding re-attempt from a fresh seed
    correction   production follow-up that continues from a failed first attempt's final DB state;
                 stored as its own session linked by parent_session_id, never overwriting the parent
"""

import hashlib
import json
import os
import secrets
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import RUNTIME_DIR

DB_PATH = os.path.join(RUNTIME_DIR, "datasea.sqlite")

# Session status values.
IN_PROGRESS = "in_progress"
PASSED = "passed"
FAILED = "failed"
FLAGGED = "flagged_unclear"
ERROR = "error"

MODES = ("onboarding", "production", "engineering")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    token_sha256 TEXT NOT NULL UNIQUE,
    mode TEXT NOT NULL,
    assigned_task_ids_json TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    pilot_only INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    system_prompt TEXT NOT NULL,
    user_prompt TEXT NOT NULL,
    selected_tools_json TEXT NOT NULL,
    available_tools_json TEXT NOT NULL,
    environment_json TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    final_response TEXT,
    worker_note TEXT,
    verifier_json TEXT,
    verifier_pass INTEGER,
    reset_at TEXT
);
CREATE TABLE IF NOT EXISTS steps (
    session_id TEXT NOT NULL,
    step INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments_json TEXT NOT NULL,
    result_json TEXT,
    error TEXT,
    duration_ms INTEGER,
    PRIMARY KEY (session_id, step)
);
"""

# Columns added after milestone 1; existing rows predate session modes and were internal test runs.
_MIGRATIONS = [
    ("sessions", "mode", "TEXT NOT NULL DEFAULT 'engineering'"),
    ("sessions", "attempt_kind", "TEXT NOT NULL DEFAULT 'first'"),
    ("sessions", "parent_session_id", "TEXT"),
]

_JSON_COLS = {
    "selected_tools_json": "selected_tools",
    "available_tools_json": "available_tools",
    "environment_json": "environment",
    "provenance_json": "provenance",
    "verifier_json": "verifier",
    "arguments_json": "arguments",
    "result_json": "result",
    "assigned_task_ids_json": "assigned_task_ids",
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class Store:
    def __init__(self, path: str = DB_PATH):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            for table, col, ddl in _MIGRATIONS:
                cols = {r[1] for r in self._conn.execute(f"PRAGMA table_info({table})")}
                if col not in cols:
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
            self._conn.commit()

    def _exec(self, sql: str, params=()):
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur

    def _all(self, sql: str, params=()) -> List[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    @staticmethod
    def _decode(row: sqlite3.Row) -> Dict[str, Any]:
        out = {}
        for k in row.keys():
            v = row[k]
            if k in _JSON_COLS:
                out[_JSON_COLS[k]] = json.loads(v) if v is not None else None
            else:
                out[k] = v
        return out

    # ------------------------------------------------------------ workers

    def create_worker(self, worker_id: str, mode: str, assigned_task_ids: List[str]) -> str:
        if mode not in ("onboarding", "production"):
            raise ValueError("worker mode must be onboarding or production")
        token = secrets.token_urlsafe(24)
        self._exec(
            "INSERT INTO workers (worker_id, token_sha256, mode, assigned_task_ids_json, created_at) VALUES (?,?,?,?,?)",
            (worker_id, hash_token(token), mode, json.dumps(assigned_task_ids), now()),
        )
        return token

    def rotate_token(self, worker_id: str) -> str:
        token = secrets.token_urlsafe(24)
        self._exec("UPDATE workers SET token_sha256=? WHERE worker_id=?", (hash_token(token), worker_id))
        return token

    def update_worker(self, worker_id: str, mode: Optional[str] = None, assigned_task_ids: Optional[List[str]] = None,
                      active: Optional[bool] = None) -> None:
        if mode is not None:
            self._exec("UPDATE workers SET mode=? WHERE worker_id=?", (mode, worker_id))
        if assigned_task_ids is not None:
            self._exec("UPDATE workers SET assigned_task_ids_json=? WHERE worker_id=?", (json.dumps(assigned_task_ids), worker_id))
        if active is not None:
            self._exec("UPDATE workers SET active=? WHERE worker_id=?", (int(active), worker_id))

    def worker_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        rows = self._all("SELECT * FROM workers WHERE token_sha256=? AND active=1", (hash_token(token),))
        return self._decode(rows[0]) if rows else None

    def get_worker(self, worker_id: str) -> Optional[Dict[str, Any]]:
        rows = self._all("SELECT * FROM workers WHERE worker_id=?", (worker_id,))
        return self._decode(rows[0]) if rows else None

    def list_workers(self) -> List[Dict[str, Any]]:
        return [self._decode(r) for r in self._all("SELECT * FROM workers ORDER BY created_at")]

    # ------------------------------------------------------------ sessions

    def create_session(self, s: Dict[str, Any]) -> None:
        self._exec(
            """INSERT INTO sessions (session_id, worker_id, task_id, domain, pilot_only, status, started_at,
               system_prompt, user_prompt, selected_tools_json, available_tools_json, environment_json, provenance_json,
               mode, attempt_kind, parent_session_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                s["session_id"], s["worker_id"], s["task_id"], s["domain"], int(s["pilot_only"]), IN_PROGRESS,
                now(), s["system_prompt"], s["user_prompt"], json.dumps(s["selected_tools"]),
                json.dumps(s["available_tools"]), json.dumps(s["environment"]), json.dumps(s["provenance"]),
                s["mode"], s["attempt_kind"], s.get("parent_session_id"),
            ),
        )

    def add_step(self, session_id: str, tool_name: str, arguments: Dict[str, Any], result: Any,
                 error: Optional[str], duration_ms: int, timestamp: str) -> int:
        with self._lock:
            n = self._conn.execute("SELECT COALESCE(MAX(step), 0) FROM steps WHERE session_id=?", (session_id,)).fetchone()[0] + 1
            self._conn.execute(
                "INSERT INTO steps VALUES (?,?,?,?,?,?,?,?)",
                (session_id, n, timestamp, tool_name, json.dumps(arguments), json.dumps(result), error, duration_ms),
            )
            self._conn.commit()
        return n

    def finish_session(self, session_id: str, status: str, final_response: str, worker_note: str,
                       verifier: Optional[Dict[str, Any]]) -> None:
        """Write the outcome once. A finished session can never be finished again."""
        cur = self._exec(
            """UPDATE sessions SET status=?, ended_at=?, final_response=?, worker_note=?, verifier_json=?, verifier_pass=?
               WHERE session_id=? AND ended_at IS NULL""",
            (
                status, now(), final_response, worker_note, json.dumps(verifier) if verifier is not None else None,
                None if verifier is None else int(bool(verifier.get("overall_success"))), session_id,
            ),
        )
        if cur.rowcount != 1:
            raise RuntimeError(f"session {session_id} already finished; outcomes are immutable")

    def mark_reset(self, session_id: str) -> None:
        self._exec("UPDATE sessions SET reset_at=? WHERE session_id=? AND reset_at IS NULL", (now(), session_id))

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        rows = self._all("SELECT * FROM sessions WHERE session_id=?", (session_id,))
        return self._decode(rows[0]) if rows else None

    def get_steps(self, session_id: str) -> List[Dict[str, Any]]:
        return [self._decode(r) for r in self._all("SELECT * FROM steps WHERE session_id=? ORDER BY step", (session_id,))]

    def sessions_for(self, worker_id: str, task_id: str, mode: str) -> List[Dict[str, Any]]:
        return [dict(r) for r in self._all(
            "SELECT session_id, status, attempt_kind, verifier_pass FROM sessions WHERE worker_id=? AND task_id=? AND mode=? ORDER BY started_at",
            (worker_id, task_id, mode))]

    def list_sessions(self) -> List[Dict[str, Any]]:
        rows = self._all(
            """SELECT s.session_id, s.worker_id, s.task_id, s.domain, s.pilot_only, s.status, s.mode, s.attempt_kind,
                      s.parent_session_id, s.started_at, s.ended_at, s.verifier_pass, s.reset_at, s.worker_note,
                      (SELECT COUNT(*) FROM steps t WHERE t.session_id = s.session_id) AS num_tool_calls,
                      (SELECT COUNT(*) FROM steps t WHERE t.session_id = s.session_id AND t.error IS NOT NULL) AS num_tool_errors
               FROM sessions s ORDER BY s.started_at DESC"""
        )
        return [dict(r) for r in rows]
