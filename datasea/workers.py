"""Manage worker accounts and their secret links.

    python -m datasea.workers add --worker_id w01 --mode production --task_id <id> [--task_id <id> ...]
    python -m datasea.workers add --worker_id w01 --mode onboarding          # sees the whole onboarding pool
    python -m datasea.workers set --worker_id w01 --mode production --task_id ...
    python -m datasea.workers rotate --worker_id w01                          # new link, old one stops working
    python -m datasea.workers disable --worker_id w01
    python -m datasea.workers list

The link token is shown once; only its SHA-256 is stored.
"""

import argparse
import os

from .store import Store
from .tasks import load_catalog

BASE_URL = os.environ.get("DATASEA_BASE_URL", "http://127.0.0.1:8700")


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
    args = p.parse_args()

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
