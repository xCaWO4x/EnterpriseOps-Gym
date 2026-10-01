# DataSea human-demonstration layer for EnterpriseOps-Gym

Nontechnical workers complete EnterpriseOps-Gym tasks in a browser while every
MCP tool call is proxied, logged, verified with the upstream verifiers, and
exported for SFT.

```
Browser ──> DataSea backend (datasea/server.py) ──> EnterpriseOps MCP server (docker) ──> per-session seeded DB
```

Everything lives in `datasea/`. Upstream code is untouched except for one
optional-dependency group added to `pyproject.toml` (`datasea`).

What is reused from upstream, unmodified:

| Concern | Upstream piece |
|---|---|
| Task config parsing | `evaluate.load_config` (same HF→config conversion as `evaluate.py`) |
| Fresh DB per session | `benchmark.mcp_client.create_database_from_file` |
| MCP protocol | `benchmark.mcp_client.MCPClient` |
| Tool discovery + `selected_tools` filtering | `BenchmarkExecutor._discover_and_merge_tools` |
| Verification | `BenchmarkExecutor._run_verifiers` → `VerifierEngine` |
| Reset | `benchmark.mcp_client.delete_database` |

## 1. Start EnterpriseOps (Email domain)

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/), and a Docker runtime.
On Apple Silicon the images are amd64-only; Colima with Rosetta works:

```bash
brew install colima docker
colima start --vm-type vz --vz-rosetta --cpu 4 --memory 8

unzip gym_dbs.zip                                   # seed SQL snapshots
uv sync --extra openai --extra datasea              # openai extra only needed for smoke test / LLM-judge verifiers

docker pull --platform linux/amd64 shivakrishnareddyma225/enterpriseops-gym-mcp-email:latest
docker run -d --name eog-email --platform linux/amd64 -p 8004:8005 \
    shivakrishnareddyma225/enterpriseops-gym-mcp-email:latest
```

Containers listen on 8005 internally (calendar: 8003); map them to the host
ports in `conf.example/ray/domain_conf.json` (email 8004, teams 8002, csm 8001,
calendar 8003, itsm 8006, hr 8008, drive 8009), because task configs hard-code those URLs.

Check the upstream pipeline end-to-end without an LLM (scripted orchestrator
plugged into the unmodified `BenchmarkExecutor.execute_benchmark()`):

```bash
uv run python -m datasea.smoke_upstream \
    --task_id task_20260106_054515_137_1628b966_fe1068d4 \
    --script datasea/tasks/scripts/task_20260106_054515_137_1628b966_fe1068d4.json
```

For the four pilot domains, also run calendar (`-p 8003:8003`), drive (`-p 8009:8005`) and teams (`-p 8002:8005`).

## 2. Known-broken tasks, pilot tasks, manifest

```bash
uv run python -m datasea.audit --live email calendar drive teams   # -> tasks/known_broken.jsonl, tasks/audit_facts.jsonl
uv run python -m datasea.manifest import                           # rebuild catalog: 20 canary + 4 onboarding tasks
uv run python -m datasea.manifest write                            # -> tasks/manifest.md, tasks/manifest.jsonl
uv run python -m datasea.tasks list
```

**Blacklist.** `datasea.audit` checks every task in the pinned HF revision. It runs
static checks on all domains, and also seeds and checks each task live in the domains
passed to `--live`. Every defect gets one record in `tasks/known_broken.jsonl`, giving
the task, defect code, exact reason, and the offending verifier, query and expected value.
Defect codes:

- `numeric_string_expected_vs_integer`: the verifier expects `"1"` but the SQL returns `1`, and `VerifierEngine` compares with `==`, so the task can never pass.
- `verifier_user_mismatch`: the task acts as one user, but the verifier checks another.
- `selected_tool_missing`: a tool in `selected_tools` isn't exposed by the MCP server.
- `verifier_sql_error_at_seed`, `all_verifiers_pass_at_seed`, `seed_failed`.

Broken tasks are **excluded, never patched**: `datasea.tasks import` refuses them.
For benchmarking, either use upstream exactly as released, or report any corrected
subset under a separate name. Never present such a subset as "EnterpriseOps-Gym".

**Pools.** Each task in `tasks/pilot_tasks.jsonl` has a `pool`:
- `onboarding` holds practice tasks.
- `canary` holds production pilot tasks.

Tasks come from `ServiceNow-AI/EnterpriseOps-Gym` (oracle split) at a pinned revision SHA.
The catalog stores only IDs, pool and provenance. Task configs are loaded at runtime from the
locally cached HF parquet at that revision, so no task data is copied or modified in this repo.
Every public task is stamped `pilot_only: true`; it must never be part of a claimed untouched
evaluation set. The tool mode is oracle (each task's `selected_tools`).

## 3. Start the human wrapper

```bash
DATASEA_ADMIN_PASSWORD='<long random>' uv run uvicorn datasea.server:app --port 8700   # binds 127.0.0.1
uv run python -m datasea.workers add --worker_id w01 --mode onboarding
uv run python -m datasea.workers add --worker_id w01p --mode production --task_id <id> --task_id <id> ...
```

**Auth (pilot-grade).**
- Each worker gets a secret link, `/w/<token>`. It's printed once by `datasea.workers` or the admin page. Only the token's SHA-256 is stored, and `rotate` revokes the old link.
- Workers can only see their own assigned tasks (or the whole pool for their mode) and their own sessions.
- `/admin` and `/api/admin/*` require HTTP Basic auth with `DATASEA_ADMIN_PASSWORD`. If that variable is unset, admin works from loopback only.
- For remote workers, keep uvicorn on 127.0.0.1 and put it behind an HTTPS reverse proxy or tunnel. Never serve the links over plain HTTP.

**Modes** (set per worker; stored on each session as `mode`, plus `attempt_kind` and `parent_session_id`):

- `onboarding`: after submit, the worker sees pass/fail and how many checks passed, but never which checks. They can retry from a fresh seed (`attempt_kind=retry`).
- `production`: one first attempt per worker per task, and it is immutable once submitted. If it fails, the worker sees "N of M checks passed" and can choose:
  - **Try to fix it**: a new `correction` session linked by `parent_session_id`. It continues on the same, un-reset database and is verified on its own. The original session is never modified.
  - **Skip**: the database is reset. Unanswered fix offers expire after 30 minutes.
- `engineering`: internal/test sessions, including sessions recorded before modes existed.

Workers see exactly what an agent under evaluation sees: the task's system prompt (as
"Assistant policy"), the user request, and the task's MCP tools. They never see
verifiers, check names, expected values, seed files, or auth tokens.

Worker and admin flow check (throwaway runtime): see `datasea/smoke_flow.py`.

Forms are generated from each tool's MCP `inputSchema` (`static/schema_form.js`):
enums → dropdowns, booleans → Yes/No, arrays/objects → repeatable/nested groups,
`$ref`/`anyOf`/nullable unions resolved, regex `pattern` validated, and time-like
fields get a UTC date picker that can emit ISO 8601, epoch ms, or epoch seconds.

## 4. Session lifecycle and reset

1. **Start**: seed a brand-new database from the task's SQL snapshot (unique `database_id`).
2. **Each action**: the backend calls MCP, then appends a step (timestamp, tool, exact args, exact result, error, latency) to SQLite.
3. **Submit / Unclear**: run the upstream verifiers on the final DB state, store the full verifier output, and set the status to `passed`, `failed`, or `flagged_unclear`.
4. **Reset**: delete the session database. The next session always starts from a fresh seed.

Abandoned live sessions can be reset from the admin page; they get status `error` and
the trajectory is kept. On server shutdown, all live session databases are deleted. On
startup, sessions orphaned by a crash are marked `error` and their databases are deleted.
A finished session's outcome can't be overwritten. Nothing is ever removed from
`datasea/runs/datasea.sqlite`.

## 5. Export

```bash
uv run python -m datasea.export
```

Writes to `datasea/runs/exports/`:

| File | Contents |
|---|---|
| `raw_all.jsonl` | every session in every mode and status, with raw steps; nothing dropped |
| `clean_passed.jsonl` | production first attempts that passed with **zero** failed tool calls |
| `recovery_passed.jsonl` | production passes that contain failed tool calls, plus passed corrections (parent steps followed by correction steps) |
| `failed.jsonl` | production attempts that failed, were flagged unclear, or errored |

The curated files contain only production sessions. Each record has three parts:

- `messages`: OpenAI-style. System, then user, then an assistant `tool_calls` message and a `tool` message for each action, then the final reply.
- `tools`: the list of tools offered for the task.
- `metadata`: mode, attempt_kind, parent, status, pilot_only, verifier summary, counts, repo commit, HF revision, docker digest, and seed SHA-256.

Tool message content is serialized the same way as in `orchestrators/react.py`.

## Files

```
datasea/
  env.py            live per-session environment (upstream seeding/MCP/verifier/reset)
  server.py         FastAPI backend: auth, worker + admin APIs, modes, logging proxy
  store.py          SQLite workers + sessions + steps
  workers.py        worker accounts / secret links CLI
  audit.py          known-broken task audit (static + live)
  manifest.py       canary/onboarding selection, task manifest
  tasks.py          HF import → pilot catalog (refuses blacklisted), worker-safe view
  export.py         raw_all / clean_passed / recovery_passed / failed export
  provenance.py     git commit, docker digest, seed hashes
  smoke_upstream.py upstream pipeline check (scripted orchestrator)
  smoke_flow.py     auth / modes / correction / export flow check
  static/           worker.html, admin.html, schema_form.js, style.css
  tasks/            pilot_tasks.jsonl, known_broken.jsonl, audit_facts.jsonl, manifest.{md,jsonl}, scripts/
  runs/             (gitignored) sqlite DB + exports
```
