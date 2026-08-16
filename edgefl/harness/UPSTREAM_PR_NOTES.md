# Upstream PR readiness notes

Context for submitting the DFL work on `fl-evals` to `royshadmon/EdgeFL` (`origin`),
written against [PR #105 "Rollback&benchmarking"](https://github.com/royshadmon/EdgeFL/pull/105)
(open, `ivanarzola`, base `main`). Both branches touch the same few files. This is
what's actually different between them and what to do about it.

## Summary

`fl-evals` adds decentralized FL — nodes self-aggregate, no central aggregator — plus
an evaluation harness (`edgefl/harness/`, all new, no overlap with PR #105). PR #105
adds a rollback feature: revert a node to an earlier round's weights. The two features
don't conflict conceptually — a rolled-back node can rejoin either a centralized or
decentralized round fine. The actual pain is that both PRs rewrite the same functions,
and the benchmarking instrumentation got built twice: once in each PR, same metric
names, neither aware of the other.

## File by file

### `benchmarking/__init__.py` — easy fix

The original cherry-pick and PR #105 both leave this empty (a bare package marker).
I gave it real content this session: a lazy `get_benchmarker()` singleton, built from
`BENCHMARK_ENABLED`/`BENCHMARK_REST_CONN`. PR #105 doesn't use it — its two server
files each construct their own `Benchmarker` eagerly at import time instead:

```python
bench = Benchmarker(_bench_endpoint, enabled=_bench_enabled)  # PR #105, both files
```

Pick one. I'd keep `get_benchmarker()` — PR #105's version raises at import time if
`BENCHMARK_REST_CONN`/`EXTERNAL_IP` isn't set, which means the module can't even be
imported for tooling without that env var present. The harness launches servers as
subprocesses, so that's a real problem for it specifically.

### `aggregator/aggregator_server.py` — duplicate instrumentation

My diff here is small and separable from the DFL work (`git diff HEAD` is 37 lines,
and DFL doesn't touch this file at all). Both my diff and PR #105 add the same three
metrics to `listen_for_update_agg`, built separately:

| metric | mine | PR #105 |
|---|---|---|
| `aggregation_time_s` | timed around `aggregate_model_params` | same |
| `first_to_last_arrival_s` | tracked via an `arrival_ts` dict keyed by submodel link | tracked via `param_arrival_ts` + `link_to_node` — same idea, different names |
| `straggling_node_id` | parsed from the last-arriving link's node name | same, via `link_to_node.get(...)` |

Keep one, drop the other. PR #105's is slightly more precise (timestamps on first
appearance in the poll loop rather than at fetch). Recording both would double-insert
rows under the same `metric_name`s and corrupt any averages downstream.

### `node/node_server.py` — duplicate instrumentation, plus a real conflict

Same duplicate-metrics story as above for `polling_time_s` / `training_time_s` /
`total_round_time_s` around `train_model_params()` — identical names, identical spot,
built twice. Same fix: keep one.

The real conflict is structural. PR #105 is based on pre-DFL `main`, so its version of
`listen_for_start_round` has no DFL branch, no peer-aggregation thread, no
`SELF_START`/`COLD_START`, no drift handling. `fl-evals`'s version of that same
function is substantially rewritten for all of that. This isn't something `git` can
auto-resolve — it's one function grown in two directions from a common ancestor.
Whoever merges second has to manually re-apply PR #105's rollback hooks
(`_node_ready.wait()`/`.clear()`, the `skip_download` flag, the auto-rollback check
after `current_round += 1`) into the DFL-aware version, not take either diff whole.

The four new endpoints PR #105 adds (`/rollback`, `/rollback/config`,
`/rollback/history`, `/accuracy-report`) are additive and should merge clean —
`fl-evals` doesn't touch that part of the file.

### `node/node.py` — a signature break

PR #105 changes `train_model_params()`'s return type from a bare path string to a
dict:

```python
# main / fl-evals now:
return file_name

# PR #105:
return {'model_path': file_name, 'initial_accuracy': initial_accuracy, 'final_accuracy': final_accuracy}
```

`main` has one caller to update. `fl-evals` has more — DFL's `dfl_aggregate_round` and
`run_self_start` both call `train_model_params()` too, and those call sites don't
exist in PR #105's world since it predates DFL. Anyone merging PR #105's `node.py`
changes needs to find and fix every call site in `fl-evals`, not just the one PR #105
itself updated. This is the one to actually watch — it fails at runtime mid-round, not
as a merge conflict marker, if a call site gets missed.

Worth knowing: this is also what fills the accuracy-metric gap `HARNESS_PLAN.md`
deferred to "the accuracy team." The harness's CSV output already has a
`round_accuracy` slot waiting for exactly this.

### `benchmarking/{benchmarker.py, postmvp_benchmarker.py, PLAN.md}` — low risk

Both branches cherry-picked these from `benchmark/integration` and neither modified
them further. Should be identical or close — diff to confirm, but not a hand-reconcile job.

## Everything else `fl-evals` changes

No overlap with PR #105, safe to merge independently:

- `edgefl/harness/` — the evaluation harness, entirely new this session.
- DFL core: `base_fl_participant.py` (new, shared base class pulled out of
  `node.py`/`aggregator.py`) plus the restructuring of both around it.
- `lib/logger/error_handling/` — predates this session.
- Setup docs: `AnylogNetworkSetup/AnyLog-Setup.md`, `EdgeLake-Setup.md`,
  `DFLReadme.md`, README.md restructuring.
- `edgefl/data/mnist/store_data.py`, `env_files/mnist/*.env` — minor config changes.
  PR #105 touches the same env files differently — diff before merging, but these are
  values, not logic.

## Suggested merge order

1. Land `fl-evals` first (bigger, more validated — see evidence below), or coordinate
   with `ivanarzola` to rebase PR #105 onto `fl-evals` instead of `main` so the
   DFL-aware `node_server.py` is the base to build on rather than something to
   reconcile afterward.
2. Either way, the four files above need a human to resolve them by hand — none are
   safe to take wholesale from either side.
3. Re-run the harness's tier-5 CFL+DFL validation after merging (~2 min) as a smoke test.

## Test / demo evidence

Six clean runs across three tiers, all `COMPLETED`, no failures or retries:

| tier | mode | duration | metric rows | run dir |
|---|---|---|---|---|
| 5  | CFL | 95s / 110s (two runs) | 43 / 37 | `harness/runs/*mnist_cen_n5_*` |
| 5  | DFL | 85s | 113 | `harness/runs/*mnist_dec_n5_*` |
| 10 | CFL | 110s | 67 | `harness/runs/*mnist_cen_n10_*` |
| 10 | DFL | 90s | 229 | `harness/runs/*mnist_dec_n10_*` |
| 15 | CFL | 135s | 93 | `harness/runs/*mnist_cen_n15_*` |
| 15 | DFL | 85s | 302 | `harness/runs/*mnist_dec_n15_*` |

CFL duration grows with node count (the aggregator waits on every submodel each
round); DFL stays flat (unanimity-based completion, no central bottleneck) — a small
piece of the CFL-vs-DFL comparison the harness exists to produce. Tier 30 (the plan's
stretch tier) untried; 5/10/15 cover what `HARNESS_PLAN.md` scoped as primary.

**Inference check:** the six runs above only prove rounds published to the
blockchain — the harness wipes `file_write`/`tmp_dir` right after each run, so nothing
confirms the trained weights actually predict anything. Ran one more 5-round CFL pass
and called `/inference/{index}` on each node before teardown: node1–node5 scored
28.0% / 38.0% / 38.0% / 38.0% / 52.0% on real MNIST test data — well above the 10%
random baseline, so training genuinely works end to end. (Accuracy is modest because
this is toy-scale: 5 rounds × 50 images × 1 epoch. Also: `store_data.py`'s seeding is
deterministic and identical across every operator, so all nodes train and test on the
same data — the spread here is init/training noise, not real per-node heterogeneity.)
The harness has no accuracy instrumentation of its own yet — this was a manual check,
not something `python -m harness run` does automatically. PR #105's
`train_model_params()` change is what would make that automatic.

Runbook, troubleshooting, and infra gotchas: `edgefl/harness/README.md`.
