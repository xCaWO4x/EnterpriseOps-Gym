"""End-to-end check of auth, onboarding retry, production correction/skip, and export against a running server.

Use a throwaway runtime; it creates workers w_on, w_pr, w_x:
    DATASEA_RUNTIME_DIR=/tmp/datasea_t DATASEA_ADMIN_PASSWORD=testpw uv run uvicorn datasea.server:app --port 8701
    DATASEA_URL=http://127.0.0.1:8701 DATASEA_ADMIN_PASSWORD=testpw uv run python -m datasea.smoke_flow
"""

import os

import httpx

B = os.environ.get("DATASEA_URL", "http://127.0.0.1:8701")
A = ("admin", os.environ.get("DATASEA_ADMIN_PASSWORD", ""))
c = httpx.Client(timeout=120)

tasks = c.get(f"{B}/api/admin/tasks", auth=A).json()
onb = [t for t in tasks if t["pool"] == "onboarding" and t["domain"] == "teams"][0]["task_id"]
can = [t["task_id"] for t in tasks if t["pool"] == "canary" and t["domain"] in ("teams", "calendar")][:2]

def mk(wid, mode, ids):
    r = c.post(f"{B}/api/admin/workers", auth=A, json={"worker_id": wid, "mode": mode, "task_ids": ids}).json()
    return {"Authorization": "Bearer " + r["link_path"].split("/w/")[1]}

on, pr, other = mk("w_on", "onboarding", []), mk("w_pr", "production", can), mk("w_x", "production", can)

me = c.get(f"{B}/api/me", headers=on).json()
print("onboarding sees", len(me["tasks"]), "tasks (pool):", {t["domain"] for t in me["tasks"]})
me = c.get(f"{B}/api/me", headers=pr).json()
print("production sees", [t["task_id"][-8:] for t in me["tasks"]])
print("prod start unassigned:", c.post(f"{B}/api/sessions", headers=pr, json={"task_id": onb}).status_code)

# onboarding: fail, then retry
s = c.post(f"{B}/api/sessions", headers=on, json={"task_id": onb}).json()
print("onb leak check keys:", sorted(s), sorted(s["task"]))
r = c.post(f"{B}/api/sessions/{s['session_id']}/finish", headers=on, json={"outcome": "submit"}).json()
print("onb finish:", r)
s2 = c.post(f"{B}/api/sessions", headers=on, json={"task_id": onb}).json()
print("onb retry kind:", s2["attempt_kind"])
c.post(f"{B}/api/sessions/{s2['session_id']}/finish", headers=on, json={"outcome": "unclear", "worker_note": "test"})

# production: one bad call + one good call, fail, correct
s = c.post(f"{B}/api/sessions", headers=pr, json={"task_id": can[0]}).json()
sid = s["session_id"]
print("other worker call:", c.post(f"{B}/api/sessions/{sid}/call", headers=other, json={"tool_name": "x"}).status_code)
print("second concurrent start:", c.post(f"{B}/api/sessions", headers=pr, json={"task_id": can[1]}).status_code)
bad = c.post(f"{B}/api/sessions/{sid}/call", headers=pr, json={"tool_name": "no_such_tool", "arguments": {}}).json()
print("bad call error:", bad["error"])
lt = [t["name"] for t in s["tools"] if t["name"].startswith(("list", "get"))][0]
ok = c.post(f"{B}/api/sessions/{sid}/call", headers=pr, json={"tool_name": lt, "arguments": {}}).json()
print("lookup", lt, "error:", ok["error"])
r = c.post(f"{B}/api/sessions/{sid}/finish", headers=pr, json={"outcome": "submit", "final_response": "done"}).json()
print("prod finish:", r)
print("finish twice:", c.post(f"{B}/api/sessions/{sid}/finish", headers=pr, json={"outcome": "submit"}).status_code)
print("restart same task:", c.post(f"{B}/api/sessions", headers=pr, json={"task_id": can[0]}).status_code)
print("me pending:", c.get(f"{B}/api/me", headers=pr).json()["pending_correction"] == sid)
print("other cannot correct:", c.post(f"{B}/api/sessions/{sid}/correct", headers=other).status_code)
cs = c.post(f"{B}/api/sessions/{sid}/correct", headers=pr).json()
print("correction kind:", cs["attempt_kind"], "history carried:", len(cs["history"]), [h["from_previous_attempt"] for h in cs["history"]])
print("correct twice:", c.post(f"{B}/api/sessions/{sid}/correct", headers=pr).status_code)
c.post(f"{B}/api/sessions/{cs['session_id']}/call", headers=pr, json={"tool_name": lt, "arguments": {}})
r = c.post(f"{B}/api/sessions/{cs['session_id']}/finish", headers=pr, json={"outcome": "submit"}).json()
print("correction finish:", r)

# production: fail then skip
s = c.post(f"{B}/api/sessions", headers=pr, json={"task_id": can[1]}).json()
r = c.post(f"{B}/api/sessions/{s['session_id']}/finish", headers=pr, json={"outcome": "submit"}).json()
print("2nd prod finish can_correct:", r["can_correct"], "skip:", c.post(f"{B}/api/sessions/{s['session_id']}/skip_correction", headers=pr).json())
print("me after:", [(t["task_id"][-8:], t["done"]) for t in c.get(f"{B}/api/me", headers=pr).json()["tasks"]])

rows = c.get(f"{B}/api/admin/sessions", auth=A).json()
for x in rows:
    print(" ", x["worker_id"], x["mode"], x["attempt_kind"], x["status"], "parent" if x["parent_session_id"] else "", "reset" if x["reset_at"] else "NOT RESET", x["num_tool_calls"], x["num_tool_errors"])
print("export:", c.post(f"{B}/api/admin/export", auth=A).json())
