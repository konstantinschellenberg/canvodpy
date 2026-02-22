# Versioning Strategy

canVODpy uses **unified semantic versioning** across all packages in the monorepo — one version number covers all eight packages, released together.

---

## Why Unified Versioning?

<div class="grid cards" markdown>

-   :fontawesome-solid-microscope: &nbsp; **Scientific Reproducibility**

    ---

    A single version number enables unambiguous citation in papers.
    `pip install canvodpy==0.2.0` recreates a known-compatible environment.
    Supports FAIR principles for scientific software.

-   :fontawesome-solid-layer-group: &nbsp; **Sollbruchstellen**

    ---

    Independent during development; coherent at release time.
    Develop and test packages independently, release them together with one tag.

</div>

---

## Semantic Versioning

Following [Semantic Versioning 2.0.0](https://semver.org/):

```
MAJOR.MINOR.PATCH
```

| Increment | Trigger | Example |
|-----------|---------|---------|
| `MAJOR` | Breaking API changes | 0.9.0 → 1.0.0 |
| `MINOR` | New features, backwards compatible | 0.1.0 → 0.2.0 |
| `PATCH` | Bug fixes, backwards compatible | 0.1.0 → 0.1.1 |

Pre-release identifiers: `0.2.0-alpha.1` · `0.2.0-beta.1` · `0.2.0-rc.1`

---

## Creating Releases

```bash
just release 0.2.0    # minor release
just release 0.1.1    # patch release
just release 1.0.0    # major release
```

Or by bump type:

```bash
just bump minor       # 0.1.0 → 0.2.0
just bump patch       # 0.1.0 → 0.1.1
just bump major       # 0.1.0 → 1.0.0
```

`just release` runs tests → generates CHANGELOG → bumps version in all packages → creates a git tag.

---

## Version Files

All `pyproject.toml` files are kept in sync by commitizen:

```toml
[tool.commitizen]
version = "0.1.0"
version_files = [
    "canvodpy/pyproject.toml:version",
    "packages/canvod-readers/pyproject.toml:version",
    "packages/canvod-auxiliary/pyproject.toml:version",
    "packages/canvod-grids/pyproject.toml:version",
    "packages/canvod-vod/pyproject.toml:version",
    "packages/canvod-store/pyproject.toml:version",
    "packages/canvod-viz/pyproject.toml:version",
    "packages/canvod-utils/pyproject.toml:version",
]
```

Git tags follow `vMAJOR.MINOR.PATCH` — e.g. `v0.1.0`, `v1.0.0`, `v0.2.0-beta.1`.

---

## Deprecation Policy

1. **Version N** — Feature deprecated with `DeprecationWarning`
2. **Version N+1** — Louder warning, migration guide in docs
3. **Version N+2** — Feature removed, MAJOR version bump

---

## Citation

```bibtex
@software{canvodpy2026,
  author  = {Bader, Nicolas and Contributors},
  title   = {canvodpy: GNSS Vegetation Optical Depth Analysis},
  version = {0.2.0},
  year    = {2026},
  url     = {https://github.com/nfb2021/canvodpy},
  doi     = {10.5281/zenodo.XXXXXXX}
}
```

> "Analysis was performed using canvodpy v0.2.0 (Bader et al., 2026)."

---

[:octicons-arrow-right-24: Release Process](RELEASING.md) &nbsp;&nbsp; [:octicons-arrow-right-24: Conventional Commits](https://www.conventionalcommits.org/)
