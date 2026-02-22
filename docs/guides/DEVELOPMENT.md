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

`readers` · `aux` · `grids` · `vod` · `store` · `viz` · `utils` · `docs` · `ci` · `deps`

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
