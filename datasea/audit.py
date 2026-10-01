"""Audit official EnterpriseOps tasks for verifier defects. Upstream data is never modified.

    uv run python -m datasea.audit [--live email calendar drive teams]

Static checks run on every domain. Live checks seed each task's DB on its
running MCP server and evaluate the upstream verifiers against untouched seed state.

Writes (pinned to one HF revision):
    datasea/tasks/known_broken.jsonl   one line per (task, defect); importer refuses these tasks
    datasea/tasks/audit_facts.jsonl    per-task facts used to build the pilot manifest

Defect codes:
    numeric_string_expected_vs_integer   verifier expects e.g. '1' (str) with comparison 'equals' while the
                                         query returns an integer; upstream VerifierEngine compares with ==,
                                         so 1 == '1' is False and the verifier can never pass.
    verifier_user_mismatch               verifier filters on a user_id different from the user the task's
                                         auth token acts as, so a correct solution lands on the wrong user.
    verifier_sql_error_at_seed           verifier query errors against the task's own seed DB.
    selected_tool_missing                a selected_tools entry is not exposed by the MCP server.
    all_verifiers_pass_at_seed           every verifier already passes before any action: task is vacuous.
    seed_failed                          the seed DB could not be created.
"""

import argparse
import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Dict, List

from huggingface_hub import hf_hub_download

from . import DATASEA_DIR
from .tasks import HF_DATASET, _hf_revision_sha, hf_row_to_config

ALL_DOMAINS = ["email", "calendar", "drive", "teams", "csm", "itsm", "hr", "hybrid"]
BROKEN_PATH = os.path.join(DATASEA_DIR, "tasks", "known_broken.jsonl")
FACTS_PATH = os.path.join(DATASEA_DIR, "tasks", "audit_facts.jsonl")
_NUMERIC = re.compile(r"-?\d+(\.\d+)?")


def load_split(domain: str, revision: str, mode: str = "oracle") -> List[Dict[str, Any]]:
    import pandas as pd

    path = hf_hub_download(HF_DATASET, f"{mode}/{domain}-00000-of-00001.parquet", repo_type="dataset", revision=revision)
    return pd.read_parquet(path).to_dict("records")


def static_defects(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for v in config["verifiers"]:
        vc = v.get("validation_config", {})
        exp = vc.get("expected_value")
        if (v["verifier_type"] == "database_state" and isinstance(exp, str) and _NUMERIC.fullmatch(exp.strip())
                and vc.get("comparison_type", "equals") == "equals"):
            out.append({
                "defect": "numeric_string_expected_vs_integer",
                "verifier": v.get("name"),
                "expected_value": exp,
                "comparison_type": "equals",
                "query": vc.get("query"),
                "reason": f"expected_value is the string {exp!r} but the query returns an integer; "
                          f"benchmark/verifier.py compares with ==, and {int(float(exp))} == {exp!r} is False.",
            })
    return out


async def live_audit(config: Dict[str, Any]) -> Dict[str, Any]:
    from .env import TaskEnvironment

    env = TaskEnvironment(config)
    defects, facts = [], {}
    t0 = time.monotonic()
    try:
        await env.start()
    except Exception as e:
        await env.reset()
        return {"defects": [{"defect": "seed_failed", "reason": str(e)}], "facts": {}}
    try:
        facts["seed_seconds"] = round(time.monotonic() - t0, 2)
        available = {t["name"] for t in env.tools}
        facts["tools_available"] = len(available)
        for name in config.get("selected_tools") or []:
            if name not in available:
                defects.append({"defect": "selected_tool_missing", "tool": name,
                                "reason": f"selected tool {name!r} is not exposed by the MCP server"})

        v = await env.verify("", [])
        seed_results = []
        for name, r in v["verification_results"].items():
            seed_results.append({"verifier": name, "passed_at_seed": bool(r.get("passed")), "actual_at_seed": r.get("actual")})
            if r.get("error"):
                defects.append({"defect": "verifier_sql_error_at_seed", "verifier": name, "reason": r["error"],
                                "query": r.get("query")})
        facts["seed_verifier_results"] = seed_results
        if seed_results and all(s["passed_at_seed"] for s in seed_results):
            defects.append({"defect": "all_verifiers_pass_at_seed",
                            "reason": "every verifier passes on the untouched seed DB, so doing nothing passes"})

        for gym in env.executor.gym_configs:
            token = (gym.get("context") or {}).get("x-email-user-token")
            if not token:
                continue
            res = await env.executor.verifier_engine._execute_sql_query(
                f"SELECT id FROM users WHERE token = '{token}'", gym["database_id"], gym.get("context"), gym["mcp_server_name"])
            rows = (res.get("result") or {}).get("data") or []
            if not rows:
                continue
            token_user = rows[0]["id"]
            facts.setdefault("acting_user", {})[gym["mcp_server_name"]] = token_user
            for ver in config["verifiers"]:
                if ver.get("gym_name") not in (None, gym["mcp_server_name"]):
                    continue
                users = set(re.findall(r"user_id\s*=\s*'([^']+)'", ver["validation_config"].get("query", "")))
                if users and token_user not in users:
                    defects.append({"defect": "verifier_user_mismatch", "verifier": ver.get("name"),
                                    "token_user": token_user, "verifier_users": sorted(users),
                                    "reason": f"task acts as {token_user} but verifier checks {sorted(users)}"})
    finally:
        await env.reset()
    return {"defects": defects, "facts": facts}


def basic_facts(row: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    tools = list(config.get("selected_tools") or [])
    lookup = [t for t in tools if re.match(r"(list|get|search|find|query|read|describe)", t)]
    return {
        "num_selected_tools": len(tools),
        "selected_tools": tools,
        "num_lookup_tools": len(lookup),
        "num_verifiers": len(config["verifiers"]),
        "verifier_types": sorted({v["verifier_type"] for v in config["verifiers"]}),
        "gyms": [g["mcp_server_name"] for g in config["gym_servers_config"]],
        "cross_app": len(config["gym_servers_config"]) > 1,
        "prompt_chars": len(config["user_prompt"]),
        "seed_database_file": config["gym_servers_config"][0]["seed_database_file"],
    }


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--revision", default="main")
    p.add_argument("--live", nargs="*", default=[], help="Domains whose MCP server is running locally")
    args = p.parse_args()
    logging.basicConfig(level=logging.WARNING)
    for noisy in ("benchmark", "httpx", "evaluate", "datasea"):
        logging.getLogger(noisy).setLevel(logging.ERROR)

    sha = _hf_revision_sha(HF_DATASET, args.revision)
    broken, facts_out = [], []
    for domain in ALL_DOMAINS:
        rows = load_split(domain, sha)
        n_broken = set()
        for i, row in enumerate(rows):
            config = hf_row_to_config(row)
            defects = [dict(d, detected_by="static") for d in static_defects(config)]
            facts = basic_facts(row, config)
            if domain in args.live:
                live = await live_audit(config)
                defects += [dict(d, detected_by="live_preflight") for d in live["defects"]]
                facts.update(live["facts"])
                print(f"\r  {domain}: {i + 1}/{len(rows)}", end="", flush=True)
            for d in defects:
                broken.append({"task_id": row["task_id"], "domain": domain, "hf_dataset": HF_DATASET,
                               "hf_revision": sha, "upstream_unmodified": True, **d})
                n_broken.add(row["task_id"])
            facts_out.append({"task_id": row["task_id"], "domain": domain, "hf_revision": sha,
                              "live_audited": domain in args.live, "defects": sorted({d["defect"] for d in defects}), **facts})
        print(f"\r{domain}: {len(rows)} tasks, {len(n_broken)} with known defects"
              f"{' (static + live)' if domain in args.live else ' (static only)'}")

    os.makedirs(os.path.dirname(BROKEN_PATH), exist_ok=True)
    with open(BROKEN_PATH, "w") as f:
        for b in broken:
            f.write(json.dumps(b) + "\n")
    with open(FACTS_PATH, "w") as f:
        for x in facts_out:
            f.write(json.dumps(x, default=str) + "\n")
    print(f"{len({b['task_id'] for b in broken})} broken tasks / {len(broken)} defect records -> {BROKEN_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
