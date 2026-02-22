# Getting Started

This guide walks you through everything you need — from creating a GitHub account to running your first test — so you can start contributing to canVODpy even if you have never used Git, GitHub, or Python tooling before.

<div class="grid cards" markdown>

-   :fontawesome-brands-github: &nbsp; **Steps 1–5** &nbsp; GitHub + Git

    ---

    GitHub account · Homebrew · Git install · SSH keys · Git identity

-   :fontawesome-solid-gear: &nbsp; **Steps 6–9** &nbsp; Tools + Repo

    ---

    Install `uv` + `just` · Fork + clone · Submodules · Dev environment

-   :fontawesome-solid-sliders: &nbsp; **Step 10** &nbsp; Configuration

    ---

    Initialize YAML config files · Set paths and credentials · Validate

-   :fontawesome-solid-circle-check: &nbsp; **Steps 11–13** &nbsp; Verify + Contribute

    ---

    Run tests · Quality checks · Your first pull request

</div>

---

## 1. Create a GitHub account

GitHub is a website that hosts code and lets teams collaborate on software projects. If you don't already have an account, sign up at [github.com/signup](https://github.com/signup).

---

## 2. Install Homebrew (macOS only)

Homebrew is the standard package manager for macOS — it lets you install developer tools with a single command. Open **Terminal** (press ++cmd+space++, type "Terminal", press ++enter++) and run:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Follow the on-screen instructions. When it finishes, it will tell you to run two commands to add Homebrew to your PATH — **copy and run them**. Then verify:

```bash
brew --version
```

!!! note
    If you already have Homebrew installed, run `brew update` to make sure it's current.

---

## 3. Install Git

Git is the version-control system that tracks changes in the codebase. Install it for your operating system:

=== "macOS"

    ```bash
    brew install git
    ```

=== "Linux (Debian/Ubuntu)"

    ```bash
    sudo apt update && sudo apt install git
    ```

=== "Linux (Fedora)"

    ```bash
    sudo dnf install git
    ```

=== "Windows"

    Download and run the installer from [git-scm.com](https://git-scm.com/download/win).
    Accept the default options during installation. Afterwards, open **Git Bash** (installed with Git) to run the commands in this guide.

Verify the installation:

```bash
git --version
```

You should see something like `git version 2.x.x`.

---

## 4. Set up an SSH key for GitHub

SSH keys let you securely connect to GitHub without typing your password every time.

### Generate a key

=== "macOS / Linux"

    ```bash
    ssh-keygen -t ed25519 -C "your_email@example.com"
    ```

    Press ++enter++ three times to accept the defaults (default file location, no passphrase).

=== "Windows (Git Bash)"

    ```bash
    ssh-keygen -t ed25519 -C "your_email@example.com"
    ```

    Press ++enter++ three times to accept the defaults.

### Add the key to the SSH agent

=== "macOS"

    ```bash
    eval "$(ssh-agent -s)"
    ssh-add ~/.ssh/id_ed25519
    ```

=== "Linux"

    ```bash
    eval "$(ssh-agent -s)"
    ssh-add ~/.ssh/id_ed25519
    ```

=== "Windows (Git Bash)"

    ```bash
    eval "$(ssh-agent -s)"
    ssh-add ~/.ssh/id_ed25519
    ```

### Copy the public key

=== "macOS"

    ```bash
    pbcopy < ~/.ssh/id_ed25519.pub
    ```

=== "Linux"

    ```bash
    cat ~/.ssh/id_ed25519.pub
    ```

    Select and copy the output.

=== "Windows (Git Bash)"

    ```bash
    clip < ~/.ssh/id_ed25519.pub
    ```

### Add the key to GitHub

1. Go to [github.com/settings/keys](https://github.com/settings/keys).
2. Click **New SSH key**.
3. Give it a title (e.g. "My Laptop"), paste the key, and click **Add SSH key**.

### Test the connection

```bash
ssh -T git@github.com
```

You should see: `Hi <username>! You've successfully authenticated...`

---

## 5. Configure your Git identity

Tell Git who you are (this information appears in your commits):

```bash
git config --global user.name "Your Name"
git config --global user.email "your_email@example.com"
```

Use the same email you registered on GitHub.

---

## 6. Install development tools

canVODpy uses two command-line tools to manage the project:

- **uv** — a fast Python package manager that handles dependencies and virtual environments.
- **just** — a command runner (like a simplified Makefile) that provides shortcuts for common tasks.

### Install uv

=== "macOS"

    ```bash
    brew install uv
    ```

=== "Linux"

    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

=== "Windows"

    ```powershell
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

### Install just

=== "macOS"

    ```bash
    brew install just
    ```

=== "Linux (any distro)"

    This works on all Linux distributions (Ubuntu, Mint, Fedora, etc.):

    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to ~/.local/bin
    ```

    Make sure `~/.local/bin` is on your PATH. Add this to your `~/.bashrc` or `~/.zshrc` if needed:

    ```bash
    export PATH="$HOME/.local/bin:$PATH"
    ```

=== "Linux (Ubuntu 23.04+ / Debian 13+)"

    ```bash
    sudo apt install just
    ```

    !!! warning
        This does **not** work on Linux Mint or older Ubuntu versions.
        Use the "Linux (any distro)" tab instead.

=== "Windows"

    ```powershell
    winget install Casey.Just
    ```

### Verify both tools

```bash
uv --version
just --version
```

---

## 7. Fork and clone the repository

A **fork** is your own copy of the project on GitHub. You make changes in your fork and then propose them back to the original project via a "pull request."

### Fork on GitHub

1. Go to [github.com/nfb2021/canvodpy](https://github.com/nfb2021/canvodpy).
2. Click the **Fork** button in the top-right corner.
3. GitHub will create a copy at `github.com/YOUR_USERNAME/canvodpy`.

### Clone your fork

```bash
git clone git@github.com:YOUR_USERNAME/canvodpy.git
cd canvodpy
```

Replace `YOUR_USERNAME` with your actual GitHub username.

### Add the upstream remote

This lets you pull in updates from the original repository later:

```bash
git remote add upstream git@github.com:nfb2021/canvodpy.git
```

Verify your remotes:

```bash
git remote -v
```

You should see `origin` (your fork) and `upstream` (the original).

---

## 8. Initialize submodules

The repository uses two Git submodules that contain test data and demo data. Initialize them after cloning:

```bash
git submodule update --init --recursive
```

This pulls:

- **`packages/canvod-readers/tests/test_data`** — validation test data (falsified/corrupted RINEX files for testing)
- **`demo`** — clean real-world data for demos and documentation

If you skip this step, tests that require these datasets will be automatically skipped.

---

## 9. Set up the development environment

From inside the `canvodpy` directory, run:

```bash
# Verify required tools are available
just check-dev-tools

# Install all Python dependencies into a virtual environment
uv sync

# Install pre-commit hooks (automatic code checks before each commit)
just hooks
```

---

## 10. Configure the project

canVODpy uses three YAML configuration files in the `config/` directory:

| File | Purpose |
|------|---------|
| `sites.yaml` | Defines research sites: data root paths, receiver definitions (name, type, directory), and VOD analysis pairs. Each receiver's `directory` is the full relative path from the site data root to the raw RINEX date folders (e.g. `01_reference/01_GNSS/01_raw`). |
| `processing.yaml` | Processing parameters: metadata, credentials (NASA Earthdata), auxiliary data settings (agency, product type), time aggregation, compression, Icechunk storage, and store strategies. |
| `sids.yaml` | Signal ID (SID) filtering: choose `all`, a named `preset` (e.g. `gps_galileo`), or list `custom` SIDs to keep. |

Each file has a corresponding `.example` template. To initialize them:

```bash
just config-init
```

After editing, validate your configuration:

```bash
just config-validate
```

To view the resolved configuration:

```bash
just config-show
```

---

## 11. Verify everything works

Run the test suite:

```bash
just test
```

Run code-quality checks (linting, formatting, type checking):

```bash
just check
```

If both commands complete without errors, your environment is ready.

---

## 12. Working in teams

During a hackathon or collaborative sprint, each team works on its own topic (often aligned with a package like `canvod-grids` or `canvod-readers`). A shared **develop branch** (e.g. `develop/hackathon2026`) acts as the integration point — no one commits to it directly. Instead, each team gets its own **team branch**.

### Branch structure

```
main
└── develop/hackathon2026              ← integration branch (shared by all teams)
    ├── team-grids/                    ← Team A
    │   ├── (direct commits)           ← Workflow A
    │   └── team-grids/add-healpix     ← Workflow B feature branches
    ├── team-readers/                  ← Team B
    └── team-vod/                      ← Team C
```

### Set up the team branch

The **team lead** creates the branch once:

```bash
git checkout develop/hackathon2026
git checkout -b team-grids
git push -u origin team-grids
```

**All other team members** fetch and switch to it:

```bash
git fetch origin
git checkout team-grids
```

From here, choose the workflow that fits your team:

=== "Workflow A: Push to the team branch directly"

    Best for small teams (2–3 people) or when members work on separate files.

    Everyone commits and pushes to the team branch:

    ```bash
    # Make your changes, then:
    git add <files you changed>
    git commit -m "feat(grids): add new grid type"
    git push origin team-grids
    ```

    If someone else pushed before you, pull first:

    ```bash
    git pull --rebase origin team-grids
    git push origin team-grids
    ```

=== "Workflow B: Individual feature branches with PRs"

    Best for larger teams or when you want lightweight review within the team.

    Create a feature branch off the team branch:

    ```bash
    git checkout team-grids
    git checkout -b team-grids/add-healpix
    ```

    Work, commit, and push your feature branch:

    ```bash
    git add <files you changed>
    git commit -m "feat(grids): add HEALPix support"
    git push -u origin team-grids/add-healpix
    ```

    Then open a pull request on GitHub targeting `team-grids`.

    To stay up to date with teammates' merged work:

    ```bash
    git checkout team-grids
    git pull origin team-grids
    git checkout team-grids/add-healpix
    git rebase team-grids
    ```

### Merge the team's work into the develop branch

When your team's feature is ready, open **one pull request** from `team-grids` into `develop/hackathon2026` on GitHub. This is where the maintainer reviews the team's combined work.

---

## 13. Your first contribution

### Make your changes

Edit files with your favorite text editor or IDE.

### Run quality checks

```bash
just test
just check
```

### Stage and commit

```bash
git add <files you changed>
git commit -m "feat(grids): add new grid type"
```

Commit messages follow the [Conventional Commits](https://www.conventionalcommits.org/) format: `type(scope): description`. Common types: `feat`, `fix`, `docs`, `test`, `refactor`.

### Push

Push to your team branch or feature branch (see [Working in teams](#10-working-in-teams) above).

### Open a pull request

1. Go to your fork on GitHub — you'll see a banner suggesting to open a pull request.
2. Click **Compare & pull request**.
3. Set the **base** branch to your team branch (or the develop branch, depending on your workflow).
4. Add a title and description, then click **Create pull request**.

---

## 14. Common commands cheat sheet

!!! tip "Print this"

    | Command                | What it does                                      |
    | ---------------------- | ------------------------------------------------- |
    | `just test`            | Run all tests                                     |
    | `just check`           | Lint, format, and type-check all code             |
    | `just hooks`           | Install pre-commit hooks                          |
    | `just check-dev-tools` | Verify uv, just, and python3 are installed        |
    | `just config-init`     | Initialize configuration files from templates     |
    | `just config-validate` | Validate the current configuration                |
    | `just config-show`     | Show the resolved configuration                   |
    | `just docs`            | Preview documentation locally                     |
    | `just test-coverage`   | Run tests with coverage report                    |
    | `just clean`           | Remove build artifacts and caches                 |
    | `uv sync`              | Install/update Python dependencies                |

---

??? question "15. Troubleshooting"

    **"command not found" for uv, just, or git**
    :   The tool is not installed or not on your `PATH`. Re-run the installation step and, if needed, open a new terminal window so your shell picks up the updated `PATH`.

    **"Permission denied (publickey)" when pushing or cloning**
    :   Your SSH key is not set up correctly. Go back to [step 4](#4-set-up-an-ssh-key-for-github) and make sure the key is added to both the SSH agent and your GitHub account.

    **`uv sync` fails with a Python version error**
    :   canVODpy requires Python 3.13 or 3.14. Install a supported version with `uv python install 3.13` and try again.

    **Pre-commit hook fails on commit**
    :   Run `just check` — it will auto-fix most linting and formatting issues. Stage the fixed files and commit again.

    **"push rejected" or "failed to push"**
    :   Your branch is behind the remote. Pull the latest changes first:

        ```bash
        git pull --rebase origin my-feature
        ```

    **Windows: `just` says "could not find the shell" or "system cannot find the path"**
    :   The Justfile expects Git Bash at `C:\Program Files\Git\bin\bash.exe` (the default Git for Windows location). If Git is installed elsewhere, find its location by running in PowerShell:

        ```powershell
        where.exe bash
        ```

        Then update the path in the first lines of the Justfile:

        ```
        set windows-shell := ["C:/YOUR/ACTUAL/PATH/TO/bash.exe", "-c"]
        ```

        !!! warning
            Do **not** commit this change — it's specific to your machine. The default path works for most installations and for CI.

    **Windows: uv install fails with "execution of scripts is disabled"**
    :   PowerShell's default execution policy blocks scripts. Use the bypass flag:

        ```powershell
        powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
        ```

    **Windows: line ending warnings (`LF will be replaced by CRLF`)**
    :   Configure Git to keep Unix-style line endings:

        ```bash
        git config --global core.autocrlf input
        ```
