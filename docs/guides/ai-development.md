---
title: Developing with Claude Code
description: How canVODpy uses AI-assisted development with Claude Code, skills, and CLAUDE.md
---

# Developing with Claude Code

canVODpy is developed with [Claude Code](https://docs.anthropic.com/en/docs/claude-code),
Anthropic's agentic coding CLI. This page documents how we use it and how contributors
can reproduce the same setup.

---

## What is Claude Code?

Claude Code is a terminal-based AI assistant that reads your codebase, runs commands,
edits files, and executes multi-step engineering tasks. It operates inside your local
environment — no code leaves your machine except as API calls to Anthropic.

```bash
# Install
npm install -g @anthropic-ai/claude-code

# Run in the project root
cd canvodpy
claude
```

---

## CLAUDE.md — Project Instructions

The file `CLAUDE.md` at the repository root provides persistent instructions that
Claude Code loads automatically in every session. It contains:

- **Active skills** — domain-specific knowledge modules Claude applies automatically
- **Project overview** — package structure and key components
- **Conventions** — tooling, testing, linting, dataset structure

```markdown title="CLAUDE.md (excerpt)"
## Skills — always apply automatically

| Skill | Apply when |
|---|---|
| `xarray` | Working with xarray.Dataset / DataArray |
| `zarr-python` | Working with Zarr stores, Icechunk |
| `pydantic` | Working with Pydantic models, validators |
| `python-testing-patterns` | Writing or reviewing pytest tests |
| `uv-package-manager` | Running uv, editing pyproject.toml |
| `marimo-notebook` | Writing or editing marimo notebooks |

## Conventions

- Monorepo managed with `uv` workspaces
- Type checking: `uv run ty check`
- Linting/formatting: `uv run ruff check` / `uv run ruff format`
- Tests: `uv run pytest`; integration tests marked `@pytest.mark.integration`
- Dataset structure: `(epoch, sid)` dims, `"File Hash"` attr required
```

!!! tip "Contributors"

    You do not need Claude Code to contribute. The `CLAUDE.md` file is simply a
    markdown file — it also serves as a human-readable summary of project conventions.

---

## Skills

Skills are domain-specific knowledge modules that Claude Code applies contextually.
They provide deep expertise in specific libraries and patterns without needing to
re-explain conventions each session.

### Active skills in this project

| Skill | What it provides |
|---|---|
| `xarray` | Correct usage of dims, coords, attrs, `.sel()`, `.where()`, chunking with Dask |
| `zarr-python` | Zarr v3 store operations, Icechunk transactions, encoding, compression |
| `pydantic` | BaseModel patterns, validators, `ConfigDict`, `model_dump()`, frozen models |
| `python-testing-patterns` | pytest fixtures, parametrize, mocking, `tmp_path`, assertion patterns |
| `uv-package-manager` | `uv run`, `uv add`, workspace management, `pyproject.toml` editing |
| `marimo-notebook` | Marimo cell structure, reactive execution, `mo.ui` widgets |

### Installing skills

Skills are installed globally and available across all projects:

```bash
# From the Claude Code CLI
claude /install-skill <skill-name>
```

---

## Memory

Claude Code maintains a persistent memory directory per project at
`.claude/projects/<project-hash>/memory/`. This stores:

- Architectural decisions confirmed across sessions
- Key file paths and package structure
- Solutions to recurring problems
- User preferences

Memory is automatically consulted at the start of each session, so Claude does not
re-discover the same patterns repeatedly.

---

## Typical workflows

### Code generation

```
> Add a new reader for NMEA format that follows the GNSSDataReader ABC

Claude Code will:
1. Read the base class (GNSSDataReader)
2. Study an existing reader (Rnxv3Obs or SbfReader) for patterns
3. Generate the new reader with proper Pydantic model, to_ds(), iter_epochs()
4. Write tests following existing test patterns
5. Register the reader in the factory
```

### Documentation

```
> Document the coordinate transform pipeline in canvod-auxiliary

Claude Code will:
1. Read the source code for compute_spherical_coordinates()
2. Trace the ECEF → ENU → spherical conversion
3. Write a documentation page with correct formulas and code examples
4. Add Navipedia references for GNSS-specific terms
```

### Debugging

```
> The store write fails with "Could not serialize object of type _HLGExprSequence"

Claude Code will:
1. Search for the error in the codebase
2. Identify it comes from calling .load() on a Dask-backed dataset
3. Trace the call path to _prepare_store_for_overwrite()
4. Suggest .compute() or rechunking before serialisation
```

---

## Diagram rendering

Mermaid diagrams are rendered to SVG and PNG using
[beautiful-mermaid](https://github.com/lukilabs/beautiful-mermaid). Source files
live in `docs/diagrams/*.mmd`.

```bash
# Claude Code renders via the beautiful-mermaid skill:
> Render docs/diagrams/01-package-structure.mmd with tokyo-night theme
```

---

## Guidelines for AI-assisted contributions

1. **Review all output.** AI-generated code and documentation must be reviewed by a
   human before merging. The contributor submitting the change is responsible for
   verifying its correctness.

2. **Run the test suite.** Always run `uv run pytest` after AI-assisted changes.
   AI can introduce subtle bugs that pass a quick read but fail under test.

3. **Check scientific claims.** AI may hallucinate references, formulas, or numerical
   values. Cross-check against primary sources (IGS documentation, Navipedia,
   peer-reviewed literature).

4. **Commit attribution.** Commits with significant AI assistance include:
   ```
   Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
   ```

5. **No secrets.** Never paste API keys, credentials, or private data into an AI
   tool. Claude Code operates locally but sends prompts to Anthropic's API.
