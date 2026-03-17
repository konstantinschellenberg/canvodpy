# Development Guide

Quick reference for day-to-day canVODpy development.

---

## Initial Setup

```bash
git clone https://github.com/nfb2021/canvodpy.git
cd canvodpy
git submodule update --init --recursive   # test data + demo data
uv sync                                    # install all packages (editable)
just hooks                                 # install pre-commit hooks
just test                                  # verify everything works
```

!!! info "Submodules"
    Two data repositories are pulled as submodules:

    - **`packages/canvod-readers/tests/test_data`** — falsified/corrupted RINEX files for validation tests
    - **`demo`** — clean real-world data for demos and documentation

    Tests that depend on these datasets are automatically skipped if submodules are not initialised.

---

## Prerequisites

Two tools must be installed outside `uv`:

| Tool | Install | Purpose |
|------|---------|---------|
| **uv** | `brew install uv` | Python + dependency management |
| **just** | `brew install just` | Task runner |

```bash
just check-dev-tools   # verify both are present
```

[:octicons-arrow-right-24: Full installation guide](getting-started.md)

---

## Configuration Management

canVODpy uses three YAML files in `config/`:

| File | Purpose |
|------|---------|
| `sites.yaml` | Research sites, receiver definitions, data root paths, VOD analysis pairs |
| `processing.yaml` | Processing parameters, NASA Earthdata credentials, storage strategies, Icechunk config |
| `sids.yaml` | Signal ID filtering — `all`, a named `preset`, or `custom` list |

Each file has a `.example` template in the same directory.

=== "First-time setup"

    ```bash
    just config-init        # copy .example templates → YAML files
    # edit config/sites.yaml and config/processing.yaml
    just config-validate    # check for errors
    just config-show        # print resolved config
    ```

=== "Daily use"

    ```bash
    just config-validate    # after editing
    just config-show        # inspect current values
    ```

---

## Testing

```bash
just test                            # all tests
just test-package canvod-readers     # specific package
just test-coverage                   # with HTML coverage report
```

Tests live in `packages/<pkg>/tests/`. Integration tests are marked
`@pytest.mark.integration` and excluded from the default run.

---

## Code Quality

```bash
just check          # lint + format + type-check (run before committing)
just check-lint     # ruff linting only
just check-format   # ruff formatting only
```

| Tool | Purpose |
|------|---------|
| **ruff** | Linting + formatting (replaces flake8, black, isort) |
| **ty** | Type checking (replaces mypy) |
| **pytest** | Testing with coverage |

---

## Keeping Your Fork in Sync

If you are working from a fork (rather than a direct clone), periodically pull updates from the upstream repository:

```bash
git checkout main
git fetch upstream
git merge upstream/main
git push origin main

# Then update your feature branch:
git checkout feature/my-feature
git rebase main
```

If `upstream` is not configured, add it once:

```bash
git remote add upstream git@github.com:nfb2021/canvodpy.git
```

[:octicons-arrow-right-24: Detailed fork sync guide](getting-started.md#12-keeping-your-fork-up-to-date)

---

## Pre-Commit Hooks

`just hooks` installs Git hooks that run **automatically on every commit**. If any hook fails, the commit is rejected — your changes stay staged but no commit is created.

### What runs

| Hook | Stage | Effect |
|------|-------|--------|
| **ruff check --fix** | `pre-commit` | Lints and auto-fixes Python code |
| **ruff format** | `pre-commit` | Formats Python code |
| **uv-lock** | `pre-commit` | Verifies `uv.lock` matches `pyproject.toml` |
| **trailing-whitespace** | `pre-commit` | Strips trailing whitespace |
| **check-added-large-files** | `pre-commit` | Blocks large files from being committed |
| **detect-private-key** | `pre-commit` | Prevents accidental secret commits |
| **end-of-file-fixer** | `pre-commit` | Ensures files end with one newline |
| **commitizen** | `commit-msg` | Validates Conventional Commits format |

### When your commit is rejected

```bash
# Most common: ruff auto-fixed your code. Stage the fixes and retry:
git add -u && git commit -m "feat(readers): your message"

# If commitizen rejects your message, use the correct format:
git commit -m "type(scope): description"
# types: feat, fix, docs, refactor, test, chore, perf, ci
# scopes: readers, aux, grids, vod, store, viz, utils, naming, ops, docs, ci, deps

# If uv-lock is out of date:
uv sync && git add uv.lock && git commit -m "feat(readers): your message"
```

[:octicons-arrow-right-24: Full troubleshooting guide](getting-started.md#14-pre-commit-hooks-and-why-your-commit-may-be-rejected)

---

## Contributing Workflow

```bash
git checkout -b feature/my-feature
# … make changes in packages/<pkg>/src/ …
# … add tests in packages/<pkg>/tests/ …
just check && just test
git add packages/<pkg>/src/... packages/<pkg>/tests/...
git commit -m "feat(readers): add RINEX 4.0 support"
git push origin feature/my-feature
# → open pull request on GitHub
```

### Conventional Commit Scopes

`readers` · `aux` · `grids` · `vod` · `store` · `viz` · `utils` · `naming` · `ops` · `docs` · `ci` · `deps`

---

## Continuous Integration

Every push and PR triggers GitHub Actions workflows:

| Workflow | Checks |
|----------|--------|
| **Code Quality** | ruff lint, ruff format, ty type-check, lockfile consistency |
| **Test with Coverage** | pytest → coverage.lcov → [Coveralls](https://coveralls.io/github/nfb2021/canvodpy) |
| **Platform Tests** | Multi-OS, multi-Python-version test matrix |

Coverage reports are posted as PR comments and tracked on Coveralls over time. To run coverage locally:

```bash
just test-coverage    # generates HTML report
```

---

## All Just Commands

```bash
just                     # list all commands
just check               # lint + format + type-check
just test                # run all tests
just sync                # install/update dependencies
just clean               # remove build artifacts
just hooks               # install pre-commit hooks
just config-init         # initialize config files
just config-validate     # validate configuration
just config-show         # view resolved configuration
just docs                # preview documentation (localhost:3000)
just build-all           # build all packages
just deps-report         # dependency metrics report
just deps-graph          # mermaid dependency graph
```

---

## Troubleshooting

??? failure "`No module named 'canvod.X'`"
    Run `uv sync` to install/reinstall packages in editable mode.

??? failure "`Command not found: just`"
    Install just: `brew install just` (macOS) or see [getting-started](getting-started.md).

??? failure "Tests fail after dependency changes"
    ```bash
    uv sync --all-extras
    ```
