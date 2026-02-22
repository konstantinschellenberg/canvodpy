# Contributing

Contributions of all kinds are welcome — bug reports, feature implementations, documentation improvements, and feedback.

---

## Ways to Contribute

<div class="grid cards" markdown>

-   :fontawesome-solid-bug: &nbsp; **Report Bugs**

    ---

    Open an issue at [github.com/nfb2021/canvodpy/issues](https://github.com/nfb2021/canvodpy/issues).
    Include your OS, Python version, and steps to reproduce.

-   :fontawesome-solid-wrench: &nbsp; **Fix Bugs**

    ---

    Issues labelled **"bug"** and **"help wanted"** are open for PRs.
    Comment on the issue first so nobody duplicates work.

-   :fontawesome-solid-star: &nbsp; **Implement Features**

    ---

    Issues labelled **"enhancement"** and **"help wanted"** are open.
    Keep scope narrow — one feature per PR.

-   :fontawesome-solid-book: &nbsp; **Improve Documentation**

    ---

    Improvements to docs, docstrings, or external articles are always
    appreciated. See the [Development Guide](guides/DEVELOPMENT.md).

</div>

---

## Required Tools

Two tools must be installed before `uv sync` (not managed by it):

=== "macOS"

    ```bash
    brew install uv just
    ```

=== "Linux"

    ```bash
    # uv
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # just
    curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh \
        | bash -s -- --to ~/.local/bin
    ```

=== "Windows"

    ```powershell
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    winget install Casey.Just
    ```

Verify both are available:

```bash
just check-dev-tools
```

---

## Contribution Workflow

```bash
# 1. Fork on GitHub, then clone your fork
git clone git@github.com:YOUR_USERNAME/canvodpy.git
cd canvodpy

# 2. Install dependencies + hooks
git submodule update --init --recursive
uv sync
just hooks

# 3. Create a feature branch
git checkout -b feature/my-feature

# 4. Make changes
# packages/<pkg>/src/canvod/<pkg>/  ← implementation
# packages/<pkg>/tests/             ← tests

# 5. Verify
just test && just check

# 6. Commit with Conventional Commits
git add packages/<pkg>/src/... packages/<pkg>/tests/...
git commit -m "feat(readers): add RINEX 4.0 support"

# 7. Push + open PR
git push origin feature/my-feature
```

---

## Commit Message Format

```
<type>(<scope>): <subject>
```

**Types:** `feat` · `fix` · `docs` · `refactor` · `test` · `chore` · `perf` · `ci`

**Scopes:** `readers` · `aux` · `grids` · `vod` · `store` · `viz` · `utils` · `docs` · `ci` · `deps`

```bash
git commit -m "feat(vod): add tau-omega calculator"
git commit -m "fix(readers): handle empty RINEX files"
git commit -m "docs: update installation instructions"
git commit -m "feat(viz)!: redesign 3D plotting API"   # ! = breaking change
```

See [Conventional Commits](https://www.conventionalcommits.org/) for the full specification.

---

## Pull Request Guidelines

!!! tip "Before opening a PR"
    1. `just test && just check` must pass with no errors.
    2. Include tests for all new functionality.
    3. Update documentation if adding or changing public API.
    4. Target Python 3.13+.

---

## Code Quality

```bash
just check          # lint + format + type-check (run before committing)
```

| Tool | Role |
|------|------|
| **ruff** | Linting + formatting |
| **ty** | Type checking |
| **pytest** | Testing with coverage |
| **pre-commit** | Automatic checks on `git commit` |

---

## Deploying (Maintainers)

```bash
just bump minor     # bump version in all pyproject.toml files
git push
git push --tags     # triggers GitHub Actions → publish to PyPI
```
