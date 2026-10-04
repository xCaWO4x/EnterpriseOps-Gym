"""Manage worker accounts and their secret links.

    python -m datasea.workers add --worker_id w01 --mode production --task_id <id> [--task_id <id> ...]
    python -m datasea.workers add --worker_id w01 --mode onboarding          # sees the whole onboarding pool
    python -m datasea.workers set --worker_id w01 --mode production --task_id ...
    python -m datasea.workers rotate --worker_id w01                          # new link, old one stops working
    python -m datasea.workers disable --worker_id w01
    python -m datasea.workers list
    python -m datasea.workers plan --worker_id a b c d                        # canary assignment -> runs/canary_plan.json
    python -m datasea.workers promote --worker_id a                           # production mode with planned tasks

Production workers see their assigned tasks one at a time, in order.

The link token is shown once; only its SHA-256 is stored.
"""

import argparse
import json
import os

from . import RUNTIME_DIR
from .store import Store
from .tasks import load_catalog

BASE_URL = os.environ.get("DATASEA_BASE_URL", "http://127.0.0.1:8700")
PLAN_PATH = os.path.join(RUNTIME_DIR, "canary_plan.json")


def canary_plan(worker_ids):
    """4 workers x 4 canary tasks: each worker gets one task per domain, ordered easy -> medium -> medium -> hard,
    with domains rotated so every domain contributes 1 easy, 2 distinct mediums, 1 hard and no task is shared."""
    from .manifest import PILOT_DOMAINS, build_manifest

    rows = [r for r in build_manifest() if r["pool"] == "canary"]
    tier = {(d, t): [r["task_id"] for r in rows if r["domain"] == d and r["difficulty"] == t]
            for d in PILOT_DOMAINS for t in ("easy", "medium", "hard")}
    plan = {}
    for i, w in enumerate(worker_ids):
        dom = [PILOT_DOMAINS[(i + k) % 4] for k in range(4)]
        plan[w] = [tier[(dom[0], "easy")][0], tier[(dom[1], "medium")][0],
                   tier[(dom[2], "medium")][1], tier[(dom[3], "hard")][0]]
    return plan


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("add", "set"):
        a = sub.add_parser(name)
        a.add_argument("--worker_id", required=True)
        a.add_argument("--mode", choices=("onboarding", "production"), required=(name == "add"))
        a.add_argument("--task_id", action="append", default=None)
    for name in ("rotate", "disable", "enable"):
        sub.add_parser(name).add_argument("--worker_id", required=True)
    sub.add_parser("list")
    sub.add_parser("plan").add_argument("--worker_id", nargs=4, required=True)
    sub.add_parser("promote").add_argument("--worker_id", required=True)
    args = p.parse_args()

    if args.cmd == "plan":
        plan = canary_plan(args.worker_id)
        with open(PLAN_PATH, "w") as f:
            json.dump(plan, f, indent=2)
        for w, ids in plan.items():
            print(w, ids)
        print(f"written to {PLAN_PATH}")
        return
    if args.cmd == "promote":
        with open(PLAN_PATH) as f:
            ids = json.load(f)[args.worker_id]
        Store().update_worker(args.worker_id, mode="production", assigned_task_ids=ids)
        print(f"{args.worker_id} -> production: {ids}")
        return

    store = Store()
    catalog = load_catalog()
    if getattr(args, "task_id", None):
        unknown = [t for t in args.task_id if t not in catalog]
        if unknown:
            raise SystemExit(f"Unknown task ids: {unknown}")
    if args.cmd == "add":
        token = store.create_worker(args.worker_id, args.mode, args.task_id or [])
        print(f"{args.worker_id} ({args.mode}): {BASE_URL}/w/{token}")
    elif args.cmd == "set":
        store.update_worker(args.worker_id, args.mode, args.task_id)
        print("updated")
    elif args.cmd == "rotate":
        print(f"{args.worker_id}: {BASE_URL}/w/{store.rotate_token(args.worker_id)}")
    elif args.cmd in ("disable", "enable"):
        store.update_worker(args.worker_id, active=(args.cmd == "enable"))
        print(args.cmd + "d")
    else:
        for w in store.list_workers():
            tasks = w["assigned_task_ids"] or ["<whole pool>"]
            print(w["worker_id"], w["mode"], "active" if w["active"] else "disabled", ",".join(tasks))


if __name__ == "__main__":
    main()
