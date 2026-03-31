---
title: Repository Maintenance Review — March 2026
description: Findings from diagram review, README audit, and repository rules review across canvodpy, canvodpy-demo, canvodpy-test-data
date: 2026-03-31
---

# Repository Maintenance Review — March 2026

## 1. Mermaid Diagram Review (docs/diagrams/)

Reviewed 12 `.mmd` source files and key inline diagrams in Zensical docs.

### Bugs fixed

| File | Issue | Fix applied |
|---|---|---|
| `04-vod-workflow.mmd` | VOD formula missing `cos(θ)` and transmissivity step | Added `T = 10^(ΔSNR/10)` + `τ = -ln(T) · cos(θ)` |
| `10-complete-logical-flow.mmd` | Used deprecated `PairDataDirMatcher` | Replaced with `FilenameMapper → DataDirectoryValidator` |
| `01-package-structure.mmd` | `canvod-audit` missing | Added to InfraPkg subgraph |
| `03-dependencies.mmd` | `canvod-audit` missing | Added to Consumer Layer |
| `06-software-architecture.mmd` | `canvod-audit` missing | Added VERIFY subgraph |
| `07-factory-extensibility.mmd` | Duplicate `07-` prefix with `07-api-levels.mmd` | Renamed to `13-factory-extensibility.mmd` |

### Structural improvements

| File | Issue | Fix applied |
|---|---|---|
| `07-api-levels.mmd` | L2 missing `.grid()` / `.vod()` steps; L4 missing `assign_grid_cells()` / `write_to_store()`; `VOD` and `STORE` shared components unconnected from L2/L3/L4 | Added missing steps and wired shared components |
| `12-preprocessing-pipeline.mmd` | Only 4 nodes — missing Signal Selection and QC steps | Expanded to 7 nodes: raw → signal selection → QC → temporal aggregate → grid assignment → PipelineResult → store |
| `08-reproducibility.mmd` | `canvod-store-metadata` absent from Provenance section | Added node with DataCite/ACDD/STAC standards label |

### Remaining known issues (not fixed — require design decision)

| File | Issue |
|---|---|
| `06-software-architecture.mmd` and `06-ephemeris-sources.mmd` | Both use `06-` prefix — pre-existing numbering collision |
| `05-gnss-t-methodology.mmd` | SBF receiver path absent (only RINEX shown) |
| `09-multi-receiver-scs.mmd` | Node labels cramped; SCS expansion logic not visible |
| `11-naming-recipe-flow.mmd` | `DataDirectoryValidator` pre-flight gate not shown |
| `07-api-levels.mmd` | L3 `site.vod` compute path abbreviated |

---

## 2. README Review

### canvodpy (root README)

Status: **Reviewed and up to date** — badge reorganisation, five-column technology table, and content restructuring was completed prior to this session.

Minor note: line 16 contains a commented-out older VODnet badge variant kept for reference. Can be deleted once the active badge is confirmed stable.

### canvodpy-demo

Status: **Created** — no README existed. New README documents:
- 20 marimo notebooks with topic descriptions
- `data/` structure (Rosalia site, DOY 2025001, canopy + reference receivers)
- Usage via `just open-notebook` / `just app-notebook`
- marimo molab browser link

### canvodpy-test-data

Status: **Created** — no README existed. New README documents:
- `valid/` structure: RINEX `.25o` files (348), SP3 + CLK auxiliary files, Icechunk test stores
- `corrupted/` and `edge_cases/` placeholders (currently empty)
- File naming convention with example
- Usage via `just test-package canvod-readers`

---

## 3. Repository Rules Review

### canvodpy

#### Branch rulesets

Two active rulesets: `main` and `develop`.

**`main` ruleset:**

| Rule | Status | Assessment |
|---|---|---|
| Deletion blocked | ✅ | Good |
| Force push (non_fast_forward) blocked | ✅ | Good |
| PR required — 1 approving review | ✅ | Good |
| Required status checks (strict) | ✅ 8 checks | Good |
| `require_code_owner_review` | ❌ false | Gap — CODEOWNERS exists but not enforced |
| `dismiss_stale_reviews_on_push` | ❌ false | Gap — stale approvals survive new commits |
| `required_review_thread_resolution` | ❌ false | Gap — open threads don't block merge |
| `enforce_admins` | ❌ false | Admin users can bypass all rules |
| Merge methods | merge + squash + rebase | Consider restricting to squash for clean history |

**`develop` ruleset:**

| Rule | Status | Assessment |
|---|---|---|
| Deletion blocked | ✅ | Good |
| Force push blocked | ✅ | Good |
| PR required — 0 approving reviews | ✅ | Intentional for solo dev; Copilot review compensates |
| Required status checks (strict) | ✅ 8 checks | Good |
| Copilot code review on push | ✅ | Good |
| `require_code_owner_review` | ❌ false | Gap |
| `dismiss_stale_reviews_on_push` | ❌ false | Gap |

#### Status checks gap

`type_consistency` is a required check but runs with `continue-on-error: true` — always reports green.
`type_budget` (the actual enforcer with `TY_MAX_DIAGNOSTICS=0`) is NOT a required check and cannot block merges.

This is likely intentional during Phase 2/3 of the ty rollout, but should be revisited once the budget reaches zero.

#### CODEOWNERS

`CODEOWNERS` file exists at `.github/CODEOWNERS` with `* @nfb2021`, but `require_code_owner_review` is false on both rulesets. The file has no effect until this is enabled.

### canvodpy-demo / canvodpy-test-data

**No rulesets configured.** Direct pushes to `main` are unrestricted. These are data-only submodule repos, but basic protections (no force push, no deletion) are recommended.

---

## Recommended actions

### Immediate (low risk, reversible)

| Action | Repo | Priority |
|---|---|---|
| Enable `require_code_owner_review: true` on `main` | canvodpy | Medium |
| Enable `required_review_thread_resolution: true` on `main` | canvodpy | Medium |
| Enable `dismiss_stale_reviews_on_push: true` on `main` | canvodpy | Medium |
| Add minimal ruleset (no deletion, no force push on `main`) | canvodpy-demo | Low |
| Add minimal ruleset (no deletion, no force push on `main`) | canvodpy-test-data | Low |

### Deferred (requires ty Phase 3 completion)

| Action | Repo | Note |
|---|---|---|
| Add `type_budget` as required status check | canvodpy | Only after `TY_MAX_DIAGNOSTICS` budget reaches zero |
| Restrict merge methods to squash-only on `main` | canvodpy | Breaking change for any existing rebase-merge workflows |

---

## 4. Dependabot (Task #4 — pending)

Not reviewed in this session — see Task #4.
