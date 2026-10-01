"""Pilot task catalog.

Tasks are imported from the official HuggingFace dataset at a pinned revision
and converted into the exact config dicts that upstream `evaluate.py` writes
before calling `load_config`. Every imported public task is marked
`pilot_only = true` so it can never be counted as untouched evaluation data.
Tasks listed in known_broken.jsonl (see datasea/audit.py) are refused.

Usage:
    python -m datasea.tasks import --domain email --pool canary --task_id <id> [--task_id <id> ...]
    python -m datasea.tasks list
"""

import argparse
import functools
import json
import os
from typing import Any, Dict, List, Optional

from . import DATASEA_DIR

HF_DATASET = "ServiceNow-AI/EnterpriseOps-Gym"
CATALOG_PATH = os.path.join(DATASEA_DIR, "tasks", "pilot_tasks.jsonl")
BROKEN_PATH = os.path.join(DATASEA_DIR, "tasks", "known_broken.jsonl")
POOLS = ("onboarding", "canary")

# Same conversion as evaluate.py (--hf_dataset branch).
_JSON_STRING_FIELDS = {"gym_servers_config", "verifiers"}
_HF_ONLY_FIELDS = {"task_id", "domain"}


def _hf_revision_sha(dataset: str, revision: str) -> str:
    from huggingface_hub import HfApi

    return HfApi().dataset_info(dataset, revision=revision).sha


def hf_row_to_config(row: Dict[str, Any]) -> Dict[str, Any]:
    config = {}
    for k, v in row.items():
        if k in _HF_ONLY_FIELDS:
            continue
        if k in _JSON_STRING_FIELDS and isinstance(v, str):
            v = json.loads(v)
        if hasattr(v, "tolist"):  # numpy arrays from parquet
            v = v.tolist()
        config[k] = v
    return config


def load_known_broken() -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    if os.path.exists(BROKEN_PATH):
        with open(BROKEN_PATH) as f:
            for line in f:
                if line.strip():
                    b = json.loads(line)
                    out.setdefault(b["task_id"], []).append(b)
    return out


def import_tasks(domain: str, task_ids: List[str], pool: str, mode: str = "oracle",
                 revision: str = "main") -> List[Dict[str, Any]]:
    from .audit import load_split

    broken = load_known_broken()
    if not broken:
        raise SystemExit("No known_broken.jsonl found; run `python -m datasea.audit` first.")
    refused = [t for t in task_ids if t in broken]
    if refused:
        raise SystemExit("Refusing known-broken tasks:\n" + "\n".join(
            f"  {t}: {', '.join(sorted({d['defect'] for d in broken[t]}))}" for t in refused))

    sha = _hf_revision_sha(HF_DATASET, revision)
    wanted = set(task_ids)
    entries = []
    for row in load_split(domain, sha, mode):
        if row["task_id"] not in wanted:
            continue
        entries.append(
            {
                "task_id": row["task_id"],
                "domain": row["domain"],
                "mode": mode,
                "pool": pool,
                "pilot_only": True,
                "source": {"type": "official_public", "hf_dataset": HF_DATASET, "hf_revision": sha, "hf_config": mode},
            }
        )
    missing = wanted - {e["task_id"] for e in entries}
    if missing:
        raise SystemExit(f"Task ids not found in {domain}/{mode}: {sorted(missing)}")
    return entries


@functools.lru_cache(maxsize=None)
def _split_configs(domain: str, revision: str, mode: str) -> Dict[str, Dict[str, Any]]:
    from .audit import load_split

    return {r["task_id"]: hf_row_to_config(r) for r in load_split(domain, revision, mode)}


def load_catalog() -> Dict[str, Dict[str, Any]]:
    """Catalog entries with `config` resolved from the pinned HF revision (task data is not copied into the repo)."""
    if not os.path.exists(CATALOG_PATH):
        return {}
    with open(CATALOG_PATH) as f:
        entries = [json.loads(line) for line in f if line.strip()]
    for e in entries:
        src = e["source"]
        e["config"] = _split_configs(e["domain"], src["hf_revision"], src["hf_config"])[e["task_id"]]
    return {e["task_id"]: e for e in entries}


def save_catalog(catalog: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(CATALOG_PATH), exist_ok=True)
    with open(CATALOG_PATH, "w") as f:
        for e in catalog.values():
            f.write(json.dumps({k: v for k, v in e.items() if k != "config"}) + "\n")


def worker_view(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Only what an agent under evaluation would see: policy + request. No verifiers, seeds, or tokens."""
    cfg = entry["config"]
    return {
        "task_id": entry["task_id"],
        "domain": entry["domain"],
        "instruction": cfg["user_prompt"],
        "policy": cfg["system_prompt"],
    }


def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    return load_catalog().get(task_id)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    imp = sub.add_parser("import")
    imp.add_argument("--domain", required=True)
    imp.add_argument("--pool", required=True, choices=POOLS)
    imp.add_argument("--task_id", action="append", default=[], required=True)
    imp.add_argument("--mode", default="oracle")
    imp.add_argument("--revision", default="main")
    rm = sub.add_parser("remove")
    rm.add_argument("--task_id", action="append", required=True)
    sub.add_parser("list")
    args = p.parse_args()

    catalog = load_catalog()
    if args.cmd == "import":
        for e in import_tasks(args.domain, args.task_id, args.pool, args.mode, args.revision):
            catalog[e["task_id"]] = e
            print(f"imported {e['domain']}/{e['task_id']} pool={e['pool']} pilot_only=true rev={e['source']['hf_revision'][:10]}")
        save_catalog(catalog)
    elif args.cmd == "remove":
        for t in args.task_id:
            catalog.pop(t, None)
        save_catalog(catalog)
    else:
        for e in catalog.values():
            print(e["domain"], e.get("pool", "?"), e["task_id"], "pilot_only=" + str(e["pilot_only"]).lower())


if __name__ == "__main__":
    main()
