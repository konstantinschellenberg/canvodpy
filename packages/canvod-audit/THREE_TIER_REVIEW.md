# Three-Tier Audit Review (Tier 0, Tier 1, Tier 2)

Date: 2026-03-25
Reviewer: Codex
Scope: code review of `packages/canvod-audit` Tier 0/1/2 scripts, runners, shared comparison engine, and Typst report path.

## Executive Summary

The tier framework is close to operationally useful, but there are two cross-cutting correctness risks that affect interpretation of all tiers:

1. `ComparisonResult.passed` is computed from exact equality only and can be `True` while `failures` is non-empty.
2. Tier 1 scripts bypass the runner-level tolerance/variable policy and therefore do not enforce the same scientific contract as the runner APIs.

Practical impact:
- Tier 0 equivalence checks are conceptually correct for your stated goal (legacy truth identity), but status semantics are still fragile.
- Tier 1 conclusions can drift depending on whether scripts or runners are used.
- Tier 2 can report "all passed" with zero actual comparisons in some skip/empty-checkpoint paths.

## Scope and Files Reviewed

- Tier 0:
  - `scripts/run_tier0_vod.py`
  - `src/canvod/audit/runners/api_levels.py`
  - `src/canvod/audit/runners/round_trip.py`
- Tier 1:
  - `scripts/run_tier1_sbf_vs_rinex.py`
  - `scripts/run_tier1_broadcast_vs_agency.py`
  - `src/canvod/audit/runners/sbf_vs_rinex.py`
  - `src/canvod/audit/runners/ephemeris.py`
- Tier 2:
  - `scripts/run_tier2_freeze.py`
  - `scripts/run_tier2_regression.py`
  - `src/canvod/audit/runners/regression.py`
- Shared logic:
  - `src/canvod/audit/core.py`
  - `src/canvod/audit/tolerances.py`
  - `src/canvod/audit/runners/common.py`
  - `src/canvod/audit/reporting/typst.py`

## Findings (Prioritized)

### F1 (High): `passed` can disagree with `failures`

Evidence:
- `passed` is exact-only: `core.py:528-531`
- failures collected independently: `core.py:474-500`
- summary/report displays both status and failures, allowing contradictory output.

Impact:
- A run can show `PASSED` while annotations contain hard comparison issues.
- This affects Tier 0/1/2 equally.

Recommendation:
- Define explicit semantics and encode them in fields:
  - `strict_pass` (bit-identical)
  - `tolerance_pass` (within tolerances)
  - or make `passed` include `not failures`.
- Update summary/Typst wording to match chosen semantics.

### F2 (High): Tier 1 scripts do not enforce the same science policy as Tier 1 runners

Evidence:
- Tier 1 script (SBF vs RINEX) calls `compare_datasets(..., tier=SCIENTIFIC)` without runner overrides: `run_tier1_sbf_vs_rinex.py:137-142`
- Tier 1 runner has explicit per-variable tolerances: `runners/sbf_vs_rinex.py:36-77`, used at `:117-123`.
- Tier 1 broadcast script also uses plain SCIENTIFIC, no runner overrides: `run_tier1_broadcast_vs_agency.py:71-76`.
- Ephemeris runner defines stricter domain tolerances + variable whitelist: `runners/ephemeris.py:37-73`, used at `:127-133`.

Impact:
- Script output can differ materially from runner output for the same data.
- Tier 1 scientific interpretation is not stable across entry points.

Recommendation:
- Make scripts call runner APIs (`audit_sbf_vs_rinex`, `audit_ephemeris_sources`) directly.
- If scripts remain custom, replicate the same tolerance overrides and variable whitelists explicitly.

### F3 (Medium): Tier 2 can report success with zero comparisons

Evidence:
- `AuditResult.passed` uses `all(...)` on results: `runners/common.py:39-41`.
- For empty `results`, this evaluates to `True`.
- `audit_regression` returns empty `AuditResult()` when no checkpoints are found: `runners/regression.py:131-134`.
- `run_tier2_regression.py` prints all-passed banner based on `result.passed`: `scripts/run_tier2_regression.py:83-86`.

Impact:
- False confidence in CI/manual runs when checkpoint discovery fails or all are skipped.

Recommendation:
- Treat `n_total == 0` as indeterminate/fail-fast.
- In scripts, track comparisons performed and exit non-zero if none were executed.

### F4 (Medium): Tier 0 script intent vs comparison scope is implicit

Evidence:
- Docstring says compare VOD/phi/theta: `run_tier0_vod.py:3-6`.
- Actual `compare_datasets` call does not pass `variables=[...]`: `run_tier0_vod.py:67-73`.
- Script does now drop sid metadata coords and includes explanatory notes in Typst: `run_tier0_vod.py:58-65`, `:80-95`.

Impact:
- If additional shared data variables appear later, Tier 0 scope changes silently.

Recommendation:
- Pass `variables=["vod", "phi", "theta"]` explicitly and assert they exist in both datasets.

### F5 (Medium): Tier 1 deep-dive blocks can crash on missing variables

Evidence:
- Hard-coded variable loops with direct indexing:
  - `run_tier1_sbf_vs_rinex.py:165-167`
  - `run_tier1_broadcast_vs_agency.py:97-99`

Impact:
- Non-portable scripts across store variants where some vars are absent.

Recommendation:
- Filter by `if var in ds.data_vars` before indexing or derive from validated comparison variable list.

### F6 (Low): Documentation mismatch in NUMERICAL tolerance

Evidence:
- Enum doc says `atol=1e-12`: `tolerances.py:51-53`.
- Actual default uses `atol=1e-6`: `tolerances.py:68-70`.

Impact:
- Misleads maintainers and downstream agents about expected sensitivity.

Recommendation:
- Update docs or code to align.

### F7 (Low): Duplicate epoch removal is silent preprocessing with scientific implications

Evidence:
- `load_group` removes duplicate epochs automatically: `runners/common.py:92-107`.

Impact:
- Potentially hides source data integrity issues; affects all tiers.

Recommendation:
- Keep behavior, but surface duplicate counts in structured metadata and reports.

## Tier-by-Tier Assessment

## Tier 0 (Legacy truth identity)

Status: Methodologically valid for your stated purpose (strict identity against legacy pipeline).

Strengths:
- Uses `EXACT` comparison (`run_tier0_vod.py:70`).
- Generates Typst/PDF with methodological notes (`run_tier0_vod.py:80-95`, `typst.py:258-264`).

Risks:
- `passed` semantics issue (F1).
- Scope still implicit unless variable list is fixed (F4).

## Tier 1 (Internal consistency)

Status: Scientifically reasonable at runner level; inconsistent at script level.

Strengths:
- Runner tolerances are domain-justified for SBF vs RINEX and ephemeris comparisons.

Risks:
- Scripts bypass runner tolerances and variable policy (F2).
- Deep-dive sections assume variable presence (F5).

## Tier 2 (Regression freeze/check)

Status: Good baseline mechanism; needs guardrails for null execution paths.

Strengths:
- Freeze captures version/hash/time metadata (`run_tier2_freeze.py:65-82`, `runners/regression.py:80-87`).

Risks:
- Possible "green with zero checks" outcome (F3).

## Typst Report Path

Status: Useful and now includes methodological notes.

Strengths:
- Coverage and alignment sections are clearly represented.
- Explicit notes section exists (`typst.py:258-264`).

Risks:
- Annotation section still labels failures as informational (`typst.py:270-272`) while core may encode structural/contract issues.
- No dedicated tests for report rendering content/contract.

## Recommended Work Plan for Next Agent

1. Unify pass semantics in `core.py`.
2. Convert Tier 1 scripts to call runner APIs, or copy runner tolerance/variable policy exactly.
3. Lock Tier 0 variable scope explicitly (`vod`, `phi`, `theta`) with required-variable checks.
4. Make Tier 2 fail-fast when no comparisons are executed.
5. Align NUMERICAL tolerance documentation with implementation.
6. Add tests for:
   - contradictory `passed` vs `failures`
   - Tier 1 script/runner parity
   - Tier 2 empty-checkpoint behavior
   - Typst notes + annotation semantics

## Notes for Handoff

- I did not execute full tier scripts against `/Volumes/ExtremePro/...` stores in this review.
- Prior package test run in this workspace reported `60 passed` for `packages/canvod-audit/tests`.
- This review is code-contract and methodology focused, intended to guide implementation fixes and reproducibility hardening.

## Round 2 Addendum (2026-03-25)

### New / Reconfirmed Findings

1. **High** — `EXACT` can still pass when `exact_match=False` for all-NaN variables with structural mismatches.
   - Reproduced locally: `passed=True`, `exact_match=False`, `failures={}`.
   - Why: `n_compared==0` yields `max_abs_diff=NaN` in stats, so structural check gate `vs.max_abs_diff == 0.0` is not entered.
   - References: `src/canvod/audit/core.py:517`, `src/canvod/audit/core.py:543`, `src/canvod/audit/stats.py:158-170`.

2. **High** — `scripts/run_tier0_vs_gnssvodpy.py` imports a non-existent symbol.
   - Script imports `audit_vs_gnssvodpy`, but runners export `audit_vs_gnssvod`.
   - References: `scripts/run_tier0_vs_gnssvodpy.py:27`, `src/canvod/audit/runners/__init__.py:36`, `src/canvod/audit/runners/__init__.py:48`.

3. **Medium** — Tier 0 VOD script still has implicit variable scope.
   - `compare_datasets()` is called without explicit `variables=["vod","phi","theta"]`.
   - References: `scripts/run_tier0_vod.py:67-73`.

4. **Medium** — Requested-but-missing variables are silently ignored in `compare_datasets`.
   - This can hide data-contract regressions when callers pass explicit variable lists.
   - Reference: `src/canvod/audit/core.py:480-482`.

5. **Low** — Typst annotation wording says failures are informational, but `passed` now depends on failures.
   - References: `src/canvod/audit/reporting/typst.py:271`, `src/canvod/audit/core.py:543`.

6. **Low** — Coverage finite-mask semantics still differ from stats semantics.
   - Coverage uses `np.isnan`; stats validity uses `np.isfinite`.
   - References: `src/canvod/audit/core.py:522-527`, `src/canvod/audit/stats.py:70-72`.

### Validation Snapshot

- Test run: `uv run pytest packages/canvod-audit/tests -q`
- Result: `60 passed` (warnings present; no test failures)
