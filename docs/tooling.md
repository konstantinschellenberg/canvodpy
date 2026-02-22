---
title: Development Tooling
description: Tools used in canVODpy development
---

# Development Tooling

canVODpy uses a modern Python toolchain built almost entirely on the [Astral](https://astral.sh/) ecosystem.

<div class="grid cards" markdown>

-   :fontawesome-solid-box: &nbsp; **uv**

    ---

    Replaces pip, venv, pip-tools, and twine in a single binary.
    Manages Python versions, virtual environments, dependency resolution,
    and builds.

    [:octicons-arrow-right-24: astral.sh/uv](https://docs.astral.sh/uv/)

-   :fontawesome-solid-broom: &nbsp; **ruff**

    ---

    Linter + formatter in one tool. Implements 700+ rules from flake8,
    pylint, black, and isort at 10–100× their speed.

    [:octicons-arrow-right-24: astral.sh/ruff](https://docs.astral.sh/ruff/)

-   :fontawesome-solid-magnifying-glass-chart: &nbsp; **ty**

    ---

    Type checker replacing mypy. Early development (alpha) but already
    significantly faster for large codebases.

-   :fontawesome-solid-list-check: &nbsp; **just**

    ---

    Task runner with simpler syntax than Make. All common development
    tasks — test, check, docs, config — are `just <command>` away.

    [:octicons-arrow-right-24: github.com/casey/just](https://github.com/casey/just)

</div>

---

## Core Tools

=== "uv — Package Manager"

    ```bash
    uv sync                  # Install all dependencies (workspace-aware)
    uv add numpy             # Add a runtime dependency
    uv add --dev pytest      # Add a dev dependency
    uv run pytest            # Run in the managed environment
    uv build                 # Build a wheel
    uv publish               # Publish to PyPI
    ```

    Configuration in `pyproject.toml`:

    ```toml
    [project]
    dependencies = ["numpy>=2.0", "xarray>=2024.0"]

    [dependency-groups]
    dev = ["pytest>=8.0", "ruff>=0.14", "ty>=0.0.1"]
    ```

=== "uv_build — Build Backend"

    Builds wheel and sdist with native namespace package support:

    ```toml
    # packages/canvod-readers/pyproject.toml
    [build-system]
    requires      = ["uv_build>=0.9.17,<0.10.0"]
    build-backend = "uv_build"

    [tool.uv.build-backend]
    module-name = "canvod.readers"   # dot → namespace package
    ```

    !!! note
        `canvod-utils` uses `hatchling` as its build backend — the only
        exception in the monorepo.

=== "ruff — Linter + Formatter"

    ```bash
    ruff check .          # Lint
    ruff check . --fix    # Lint with auto-fix
    ruff format .         # Format
    ```

    Configuration:

    ```toml
    [tool.ruff]
    line-length = 88

    [tool.ruff.lint]
    select = ["E", "F", "W", "I", "N", "UP", "B", "SIM", "C4", "RUF", "PIE", "PT"]
    ```

=== "ty — Type Checker"

    ```bash
    uv run ty check packages/canvod-readers/src/canvod/readers/
    uv run ty check canvodpy/src/canvodpy/
    ```

    Configured per-package in `pyproject.toml`:

    ```toml
    [tool.ty]
    python = "3.13"
    ```

---

## Supporting Tools

=== "just — Task Runner"

    All common tasks are single commands:

    ```bash
    just test             # Run the full test suite
    just check            # Lint + format + type-check
    just hooks            # Install pre-commit hooks
    just docs             # Build and serve documentation locally
    just config-init      # Initialize YAML config from templates
    just config-validate  # Validate config files
    just --list           # Show all available commands
    ```

=== "pytest — Testing"

    ```bash
    uv run pytest                        # All tests
    uv run pytest --cov=canvod           # With coverage
    uv run pytest -m "not integration"   # Skip integration tests
    uv run pytest packages/canvod-readers/tests/
    ```

    Integration tests are marked `@pytest.mark.integration` and excluded
    from the default run.

=== "pre-commit — Git Hooks"

    ```bash
    just hooks    # Install hooks (run once after clone)
    ```

    Configured in `.pre-commit-config.yaml`:

    ```yaml
    repos:
      - repo: https://github.com/astral-sh/ruff-pre-commit
        hooks:
          - id: ruff-check
            args: [--fix]
          - id: ruff-format
    ```

    Hooks run on every `git commit` — failures block the commit and
    auto-fix what they can.

---

## Tool Comparison

| Task | Traditional stack | canVODpy |
|------|------------------|----------|
| Package management | pip | uv |
| Virtual environments | venv / virtualenv | uv (built-in) |
| Linting | flake8 + pylint | ruff |
| Formatting | black + isort | ruff |
| Type checking | mypy | ty |
| Building | setuptools | uv_build |
| Publishing | twine | uv |
| Task runner | make / tox | just |
| Documentation | Sphinx | Zensical (MkDocs) |
