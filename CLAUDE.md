# canvodpy — Claude Code Instructions

## Skills — install before contributing

Contributors (and their AI agents) should have these skills available for the
best development experience. Apply all skills automatically whenever their
domain is relevant — do not wait to be asked.

### Required (built-in, auto-apply)

These ship with Claude Code. No installation needed.

| Skill | Apply when |
|---|---|
| `xarray` | Working with `xarray.Dataset` / `DataArray`, coordinates, dims, attrs |
| `zarr-python` | Working with Zarr stores, Icechunk, chunking, encoding |
| `pydantic` | Working with Pydantic models, validators, `BaseModel` |
| `python-testing-patterns` | Writing or reviewing `pytest` tests |
| `uv-package-manager` | Running `uv`, editing `pyproject.toml`, managing deps |
| `marimo-notebook` | Writing/editing marimo notebooks (`.py` marimo files) |
| `beautiful-mermaid` | Rendering Mermaid diagrams to SVG/PNG from `.mmd` sources |
| `scientific-writing` | Writing scientific manuscripts (IMRAD structure, citations) |
| `mermaid-diagrams` | Creating software diagrams (architecture, flows, ERDs) |
| `docs-as-code` | Documentation pipeline automation |

### Required (install these)

```bash
# Context-mode plugin — keeps large outputs out of the context window
# (enabled in .claude/settings.json, install via plugin marketplace)

# NotebookLM — generate podcasts, reports, quizzes from project sources
pip install notebooklm-py && notebooklm skill install

# Polars — used by SatelliteCatalog for DataFrame operations
npx skills add davila7/claude-code-templates@polars
```

| Skill | Install | Purpose |
|---|---|---|
| `context-mode` | plugin (pre-configured) | Prevents context window flooding on large outputs |
| `notebooklm` | `pip install notebooklm-py` | Generate scientific content from project docs |
| `polars` | `npx skills add davila7/claude-code-templates@polars` | DataFrame operations (SatelliteCatalog, audit comparisons) |

### Recommended (optional but useful)

| Skill | Install | Purpose |
|---|---|---|
| `python-code-style` | `npx skills add wshobson/agents@python-code-style` | Ruff-aware Python style guide (3K installs) |
| `pytest-advanced` | `npx skills add laurigates/claude-plugins@pytest-advanced` | Advanced pytest patterns, fixtures, parametrize |
| `git-commit` | `npx skills add fvadicamo/dev-agent-skills@git-commit` | Conventional commit messages (commitizen-compatible) |
| `geospatial-data-pipeline` | `npx skills add erichowens/some_claude_skills@geospatial-data-pipeline` | Geospatial data processing patterns |
| `mkdocs` | `npx skills add vamseeachanta/workspace-hub@mkdocs` | MkDocs documentation workflows (we use MkDocs Material via Zensical) |
| `simplify` | built-in | Review changed code for reuse, quality, and efficiency |

## Scientific context — GNSS-T and Vegetation Optical Depth

> This section provides the domain knowledge needed to work on this codebase.
> Read it before making changes to scientific logic.

### What is GNSS Transmissometry (GNSS-T)?

GNSS-T is a remote sensing technique that uses existing GNSS satellite signals
(L-band microwaves) to estimate vegetation properties. As signals travel from
a satellite to a ground-based receiver, they are scattered and absorbed by the
vegetation canopy.

The experimental setup uses **two receivers**:
- **Reference receiver** — placed in the open or above the canopy (unobstructed)
- **Canopy receiver** — placed underneath the vegetation

By comparing the **Signal-to-Noise Ratio (SNR)** at both locations for the same
satellite, the system calculates **transmittance (T)** — the ratio of signal
power reaching the below-canopy receiver vs. the unobstructed reference.

### What is VOD?

**Vegetation Optical Depth (VOD)** quantifies canopy signal attenuation using
the Tau-Omega Radiative Transfer Model:

    VOD = -ln(T) · cos(θ)

where T is transmittance and θ is the zenith angle. VOD is a proxy for
**vegetation biomass and fuel moisture content**. Unlike optical sensors (NDVI),
L-band signals penetrate the entire canopy — invaluable for monitoring forest
health, carbon stocks, and drought stress.

### Key domain concepts for developers

| Concept | What it means | In the code |
|---|---|---|
| **SNR** | Signal-to-Noise Ratio (dB-Hz), the primary observable | SBF: 0.25 dB quantization; RINEX: ~0.001 dB |
| **SID** | Signal ID: `SV\|Band\|Code` (e.g. `G01\|L1\|C`) | Unique key identifying satellite + frequency + tracking code |
| **PRN** | Satellite identifier (e.g. `G01`) | Used by external tools; canvodpy uses SID internally |
| **Zenith angle (θ)** | Angle from vertical to satellite (0°=overhead, 90°=horizon) | Used in VOD formula; internally prefer zenith over elevation |
| **Azimuth (φ)** | Compass direction to satellite (0°=N, 90°=E) | Used for hemispheric gridding |
| **Ephemeris** | Satellite orbital data for position computation | Agency final (SP3/CLK, ~3 cm, 12-18 day latency) or broadcast (~1-2 m, real-time) |
| **Constellations** | GPS (G), Galileo (E), GLONASS (R), BeiDou (C) | System prefix in SID string |
| **Fresnel zone** | Elliptical signal footprint on canopy/ground | Determines spatial sensitivity of each observation |
| **Epoch** | Timestamp of a GNSS observation | GPS Time → UTC conversion with leap-second offset |
| **ECEF** | Earth-Centered Earth-Fixed coordinates | Satellite positions before conversion to receiver-relative spherical |

### Processing pipeline

```
RINEX/SBF files → Reader → xarray.Dataset(epoch, sid)
    → Ephemeris augmentation (SP3/CLK or broadcast)
    → Coordinate transform (ECEF → spherical: r, θ, φ)
    → Hemispheric gridding (EqualArea grid cells)
    → VOD retrieval (align canopy & reference by epoch+SID)
    → Icechunk/Zarr store (versioned, cloud-native)
```

## Project architecture

### Monorepo packages

| Package | Namespace | Role |
|---|---|---|
| `canvod-readers` | `canvod.readers` | RINEX v2/v3 and SBF binary readers → `xarray.Dataset` |
| `canvod-store` | `canvod.store` | Icechunk/Zarr storage layer (`MyIcechunkStore`) |
| `canvod-store-metadata` | `canvod.store_metadata` | Rich DataCite/ACDD/STAC metadata (11 sections, ~90 fields) |
| `canvod-vod` | `canvod.vod` | VOD retrieval algorithms |
| `canvod-grids` | `canvod.grids` | Spatial grid operations (EqualArea hemigrid) |
| `canvod-auxiliary` | `canvod.auxiliary` | Ephemeris, troposphere, auxiliary data pipeline |
| `canvod-utils` | `canvod.utils` | Config models (Pydantic), shared utilities |
| `canvod-viz` | `canvod.viz` | Visualization and store viewer |
| `canvod-ops` | `canvod.ops` | Operational pipeline (streaming, monitoring) |
| `canvod-virtualiconvname` | `canvod.virtualiconvname` | GNSS filename convention parsing and validation |
| `canvod-audit` | `canvod.audit` | Three-tier verification suite (canvodpy vs gnssvodpy vs gnssvod) |
| `canvodpy` | `canvodpy` | Orchestrator, API levels (L1-L4), VodComputer |

### API levels

| Level | Style | Entry point | Use case |
|---|---|---|---|
| L1 | Convenience | `canvodpy.read()`, `canvodpy.vod()` | Quick exploration, notebooks |
| L2 | Fluent | `FluentWorkflow().read().augment().grid().vod()` | Scripted workflows |
| L3 | Site pipeline | `site.process()`, `site.vod` | Full site processing with config |
| L4 | Functional | `canvodpy.functional.*` | Custom pipelines, testing |

### Data contracts

- **All datasets**: dimensions `(epoch, sid)`, attribute `"File Hash"` required
- **SID format**: `SV|Band|Code` (e.g. `G01|L1|C`)
- **Naming convention**: `{SIT}{T}{NN}{AGC}_R_{YYYY}{DOY}{HHMM}_{PERIOD}_{SAMPLING}_{CONTENT}.{TYPE}`
- **Store guardrails**: three-layer dedup (hash match, temporal overlap, intra-batch overlap)

## Tooling

| Tool | Command | Purpose |
|---|---|---|
| `uv` | `uv sync`, `uv run` | Package manager, workspace orchestration, virtual env |
| `ruff` | `uv run ruff check`, `uv run ruff format` | Linting and formatting (replaces flake8/black/isort) |
| `ty` | `uv run ty check` | Type checking (Astral's type checker) |
| `pytest` | `uv run pytest` | Test runner; `-m "not integration"` for fast suite |
| `beautiful-mermaid` | `npx beautiful-mermaid render ...` | Render `.mmd` diagrams to SVG/PNG |
| `gfzrnx` | `/usr/local/bin/gfzrnx` | IGS RINEX toolkit (obs type filtering, splicing) — used by `RinexTrimmer` |
| `Zensical` | `uv run zensical build` | Rust+Python MkDocs Material wrapper for docs |
| `commitizen` | pre-commit hook | Enforces conventional commit messages |
| `pre-commit` | auto on `git commit` | Runs ruff, trim whitespace, large file check, private key detection |

### Common commands

```bash
uv sync                                  # Install all workspace deps
uv run pytest                            # Run all unit tests
uv run pytest -m "not integration"       # Skip integration tests (fast)
uv run pytest packages/canvod-readers    # Test a single package
uv run ruff check --fix                  # Lint and auto-fix
uv run ruff format                       # Format all code
uv run ty check                          # Type check
uv run zensical build                    # Build documentation site
```

## Conventions

- Monorepo managed with `uv` workspaces — all packages share one `.venv` at root
- Pydantic models use `frozen=False` with `@cached_property` for lazy computation
- Config: Pydantic models in `canvod.utils.config.models` (centralized, ~900 lines)
- Commits: conventional commits enforced by commitizen (`feat:`, `fix:`, `chore:`, etc.)
- Generated files: do NOT commit `*.png`, `*.svg` (except `docs/assets/logo.svg`),
  `*.lcov`, `*.db`, `node_modules/`, `package.json`, `package-lock.json`

## Guardrails — what NOT to change without understanding

> These areas involve scientific correctness or data integrity. Do not modify
> them without understanding the underlying science and running the audit suite.

- **VOD formula** (`canvod-vod`) — Tau-Omega radiative transfer model
- **Coordinate transforms** (`canvod-auxiliary`) — ECEF ↔ spherical, deg/rad conversions
- **Store dedup logic** (`canvod-store`) — hash + temporal overlap + intra-batch guards
- **Naming convention parser** (`canvod-virtualiconvname`) — IGS/RINEX standard
- **Ephemeris interpolation** (`canvod-auxiliary`) — Hermite spline on SP3 data
- **SID construction** (`canvod-readers`) — must match across readers and store

After changes to any of the above, run: `uv run pytest -m "not integration"`

## Diagram rendering

Use **[lukilabs/beautiful-mermaid](https://github.com/lukilabs/beautiful-mermaid)** for
rendering Mermaid diagrams to SVG/PNG. Source files live in `docs/diagrams/` (`.mmd`).
Do not commit generated images (`*.png`, `*.svg` except `docs/assets/logo.svg`),
`node_modules/`, or `package*.json`.

## Key documentation

For deeper context, read these docs (in order of importance):

1. `docs/architecture.md` — system architecture and data flow
2. `docs/principles.md` — design principles and philosophy
3. `docs/guides/api-levels.md` — the four API levels explained
4. `docs/guides/getting-started.md` — setup and first run
5. `docs/findings/` — scientific comparison results and findings
6. `docs/packages/*/overview.md` — per-package documentation
