# Large-Scale Evaluation Harness — Plan

A single-host Python harness that runs CFL/DFL/hybrid federated-learning benchmarks at
varying scale (Tiers: 5/10/15/30 Nodes), sweeping Configs per Tier, and writing per-Run
result CSVs. See `edgefl/harness/README.md` for the vocabulary (Node/Run/Config/Tier/
Harness/Benchmarker) and the runbook.

> **STATUS: implemented and validated at tiers 5/10/15.** Steps 1–8 below are built
> (`edgefl/harness/`); CFL and DFL runs completed end-to-end on Apple Silicon against
> the AnyLog backend at all three tiers, with timing CSVs collected. Step 9 (hybrid) is
> coded but never executed. See `edgefl/harness/README.md` for the runbook, current
> validation status, backend notes (AnyLog primary; EdgeLake backend has a known
> operator-bootstrap gap), and the handoff list.

Branch: `fl-evals` (off `dfl`). Preserves the existing error-handling module; reuses the
benchmark team's `benchmarking/` module rather than their server rewrite.

## Settled decisions

- **Node** = 1:1:1 stack (node_server + EdgeLake operator + Postgres). Shared-operator is
  a sanctioned fallback; data isolation is per-table either way (accuracy may share a table).
- **Scale**: single-host is the primary model. Tiers 5/10/15 run on one machine; 30 is the
  stretch (fits only the M5 Max with reduced per-node footprint, or multi-person later).
  Multi-person distributed is a config-driven extension, NOT built now.
- **Metrics**: reuse benchmark team's `benchmarking/benchmarker.py` (cherry-picked), wrap
  its call-sites in our own error handling. We instrument **timing** ourselves; **accuracy**
  is the accuracy team's deliverable, consumed via the shared `fl_benchmarks` table.
- **Config** = typed Python dataclass + thin CLI (`--tier`, `--dry-run`, single-run overrides).
  Sweeps built by comprehension.
- **Host profile** = gitignored `host_profile.py` from committed `host_profile.example.py`.
  Harness renders host profile + derived per-node identity + Config into a
  **generated `.env` per Node** (gitignored build artifact) and launches via `dotenv run`.
- **Process model** = `subprocess.Popen` per server, handles tracked, per-Node logs captured.
  EdgeLake operators/master stay Docker (existing Makefile / `deploy_docker_containers.py`).
- **Completion** = poll the blockchain. CFL: aggregator RoundStart hits `total_rounds`.
  DFL: all N Nodes published a submodel at `total_rounds` (unanimity). Wall-clock timeout
  escape hatch records a partial, flagged result.
- **Teardown** = configurable. Light (fresh index per Run + reset run-state) is default;
  Full (rebuild containers/Postgres/blockchain) is opt-in. Forced-Full when crossing a Tier
  or on any mid-run failure — not Config-disableable.
- **Mid-run failure** = process death | completion timeout | no-progress stall. Mark FAILED,
  write partial CSV, retry once with fresh Full rebuild, then hard-fail. Thresholds fixed now.
- **Hybrid** = `aggregation_mode="hybrid"` + `hybrid_split` (e.g. 4/6). Cohorts interact
  (one shared model). Per-cohort completion; done when both satisfied. Built LAST.
- **Output** = raw `fl_benchmarks` rows to CSV per Run. That is the harness's full output
  responsibility this phase.

## Implementation order

1. **Cherry-pick `benchmarking/` module** from `benchmark/integration`; wrap its logging/
   call-sites in our error handling. (Does NOT pull their node_server/aggregator rewrite.)
2. **Host profile + env generation** — `host_profile.example.py`, the template renderer,
   derived per-node identity (ports/names/tables), generated `.env` writer.
3. **Process model** — Popen launch/track/teardown of master+operators (Docker) and
   node_server/aggregator (Python), with per-Node log capture and readiness gates.
4. **Single-Run lifecycle** — start → readiness-gate → init → train → blockchain completion
   poll → collect CSV → teardown. Validate on a 5-Node **pure CFL** Run first.
5. **Timing instrumentation** — add our `record_metric` timing call-sites into our
   `node_server.py` / `aggregator_server.py` (training/polling/total, aggregation/straggler).
6. **Pure DFL** Run path + DFL completion predicate; validate 5-Node DFL.
7. **Config sweep + Tiers** — dataclass, CLI, suite loop, tiered teardown, fresh-index logic.
8. **Failure detection + one-retry** across the suite.
9. **Hybrid** — split config, per-cohort completion. (Prereq note: in a hybrid round
   both the central aggregator's RoundStart and each DFL node's RoundStart exist
   on-chain at once; a listener takes whichever the blockchain query returns first,
   nondeterministically. Submodel aggregation itself is unaffected — both cohorts
   write to the same round/index either way. Accepted as-is for this eval; see the
   determinism fix noted below.)

## Future directions (handoff for the next team)

- **Plotting / visualization** — out of scope this phase by decision. CSVs are long-format
  (`node, training_index, round_number, metric_name, metric_value, time`) specifically so a
  later pandas+matplotlib layer can read past suites without re-running. Headline
  comparisons to target: CFL-vs-DFL convergence, wall-clock-vs-scale across Tiers, drift-mode
  tradeoffs. The benchmark team's `visualizer.py` is referenced in BENCHMARKING.md but does
  not exist yet — this is where it should land.
- **Configurable failure thresholds** — timeout / no-progress / retry-count are fixed now;
  expose as Config knobs when configs need per-Run tuning.
- **Multi-person distributed runs** — the path to 30+ and the eventual 200-Node harness. One
  person runs the master; others run Node slices pointed at it (shared wifi; different
  networks need firewall config). Achieved by config (master endpoint + this-host's node
  index range + port base) + human coordination, NOT by the harness driving remote machines.
- **Hybrid RoundStart determinism** — if hybrid results need reproducibility, add a
  deterministic tiebreak (DFL nodes prefer node_type=aggregator, else lowest node_id).
