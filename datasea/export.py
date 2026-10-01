"""Export trajectories.

    python -m datasea.export [--out_dir datasea/runs/exports]

Writes:
    raw_all.jsonl           every session (all modes, all statuses) with its raw steps; nothing dropped
    clean_passed.jsonl      production first attempts that passed the verifier with zero failed tool calls
    recovery_passed.jsonl   production trajectories that passed but contain failed tool calls, and passed
                            correction attempts (parent's failed trajectory + correction steps, one record)
    failed.jsonl            production attempts that failed, were flagged unclear, or errored

The three curated files contain production sessions only; onboarding and engineering sessions appear only
in raw_all.jsonl. Records from public benchmark tasks keep pilot_only=true in metadata.
"""

import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from . import RUNTIME_DIR
from .store import PASSED, Store

DEFAULT_FINAL = "Task completed."


def _tool_message_content(step: Dict[str, Any]) -> str:
    # Matches orchestrators/react.py: ToolMessage(content=json.dumps(tool_result.get("result", {}))),
    # falling back to the error text when the MCP call itself failed.
    res = step.get("result") or {}
    if res.get("result") is not None:
        return json.dumps(res["result"])
    return json.dumps({"error": step.get("error") or res.get("error") or "unknown error"})


def _duration_seconds(session: Dict[str, Any]) -> Optional[float]:
    if not session.get("ended_at"):
        return None
    return (datetime.fromisoformat(session["ended_at"]) - datetime.fromisoformat(session["started_at"])).total_seconds()


def to_record(session: Dict[str, Any], steps: List[Dict[str, Any]],
              parent: Optional[Dict[str, Any]] = None, parent_steps: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Tool-calling message format. For a correction, the parent's steps come first."""
    all_steps = (parent_steps or []) + steps
    messages = [
        {"role": "system", "content": session["system_prompt"]},
        {"role": "user", "content": session["user_prompt"]},
    ]
    for i, s in enumerate(all_steps, 1):
        call_id = f"call_{i}"
        messages.append({"role": "assistant", "content": None, "tool_calls": [
            {"id": call_id, "type": "function",
             "function": {"name": s["tool_name"], "arguments": json.dumps(s["arguments"])}}]})
        messages.append({"role": "tool", "tool_call_id": call_id, "name": s["tool_name"], "content": _tool_message_content(s)})
    messages.append({"role": "assistant", "content": (session.get("final_response") or "").strip() or DEFAULT_FINAL})

    tools = [
        {"type": "function", "function": {"name": t["name"], "description": t.get("description", ""), "parameters": t.get("inputSchema", {})}}
        for t in session["available_tools"]
    ]
    verifier = session.get("verifier") or {}
    return {
        "messages": messages,
        "tools": tools,
        "metadata": {
            "session_id": session["session_id"],
            "task_id": session["task_id"],
            "worker_id": session["worker_id"],
            "domain": session["domain"],
            "mode": session["mode"],
            "attempt_kind": session["attempt_kind"],
            "parent_session_id": session.get("parent_session_id"),
            "parent_status": parent["status"] if parent else None,
            "parent_final_response": parent.get("final_response") if parent else None,
            "num_parent_steps": len(parent_steps or []),
            "status": session["status"],
            "pilot_only": bool(session["pilot_only"]),
            "verifier_pass": bool(session.get("verifier_pass")),
            "verification_summary": verifier.get("verification_summary"),
            "worker_note": session.get("worker_note"),
            "duration_seconds": _duration_seconds(session),
            "num_tool_calls": len(all_steps),
            "num_tool_errors": sum(1 for s in all_steps if s.get("error")),
            "started_at": session["started_at"],
            "ended_at": session["ended_at"],
            "provenance": session["provenance"],
            "environment": session["environment"],
        },
    }


def classify(session: Dict[str, Any], num_errors: int) -> Optional[str]:
    """Curated bucket for a finished session, or None if it only belongs in raw_all."""
    if session["mode"] != "production" or session["status"] == "in_progress":
        return None
    if session["status"] != PASSED:
        return "failed"
    if session["attempt_kind"] == "correction" or num_errors:
        return "recovery_passed"
    return "clean_passed"


def export(out_dir: str) -> Dict[str, int]:
    store = Store()
    os.makedirs(out_dir, exist_ok=True)
    buckets: Dict[str, list] = {"raw_all": [], "clean_passed": [], "recovery_passed": [], "failed": []}
    for row in reversed(store.list_sessions()):
        session = store.get_session(row["session_id"])
        steps = store.get_steps(row["session_id"])
        buckets["raw_all"].append({"session": session, "steps": steps})
        parent = parent_steps = None
        if session.get("parent_session_id"):
            parent = store.get_session(session["parent_session_id"])
            parent_steps = store.get_steps(session["parent_session_id"])
        rec = to_record(session, steps, parent, parent_steps)
        bucket = classify(session, rec["metadata"]["num_tool_errors"])
        if bucket:
            buckets[bucket].append(rec)

    for name, items in buckets.items():
        with open(os.path.join(out_dir, f"{name}.jsonl"), "w") as f:
            for r in items:
                f.write(json.dumps(r) + "\n")
    return {k: len(v) for k, v in buckets.items()}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out_dir", default=os.path.join(RUNTIME_DIR, "exports"))
    args = p.parse_args()
    for k, v in export(args.out_dir).items():
        print(f"{k}: {v}")
    print(f"written to {args.out_dir}")


if __name__ == "__main__":
    main()
