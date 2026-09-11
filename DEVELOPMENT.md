# The basics

This is a multiplatform project supporting both Windows and Linux. Development of the Linux
functionality takes place in a Windows Subsystem for Linux (WSL) virtual machine.

See `rust-toolchain.toml` for the required stable Rust toolchain version. The `nightly` toolchain
is also required for some development tooling.

# Development environment setup (Windows)

Prerequisites:

* Windows 11
* Visual Studio 2022 with workload "Desktop development with C++"
* Visual Studio Code with extensions:
    * C/C++
    * rust-analyzer
    * vscode-just
    * WSL
* PowerShell 7
* Node.js with npm (used to install and run the Azurite Azure Blob emulator for `just test-azurite`)
* `rustup toolchain install` to install Rust development tools based on `rust-toolchain.toml`
* `cargo install just`
* (Only if publishing releases) GitHub CLI + `gh auth login`

Setup:

1. Clone the repo to a directory of your choosing.
1. Open a terminal in the repo root.
1. Execute `git config --local include.path ./.gitconfig` to attach the repo-specific Git configuration.
1. Execute `just install-tools` to install development tools.

Validation:

1. Open repo directory in Visual Studio code.
1. Execute from task palette (F1):
    * `Tasks: Run Build Task`
    * `Tasks: Run Test Task`
1. Execute `just validate-local` in terminal.

# Development environment setup (Linux)

Prerequisites:

* Ubuntu 24 installed in WSL
* `sudo apt install -y git git-lfs build-essential cmake gcc make curl libssl-dev pkg-config valgrind`
* Git LFS setup: `git lfs install`
* [PowerShell 7](https://learn.microsoft.com/en-us/powershell/scripting/install/install-ubuntu?view=powershell-7.5):
  ```bash
  # Download and install Microsoft package repository
  wget -q "https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb"
  sudo dpkg -i packages-microsoft-prod.deb
  sudo apt update
  sudo apt install -y powershell
  ```
* `rustup toolchain install` to install Rust development tools based on `rust-toolchain.toml`
* `cargo install just`
* Node.js with npm (used to install and run the Azurite Azure Blob emulator for `just test-azurite`)
* If first time Git setup, execute `git config --global credential.helper "/mnt/c/Program\ Files/Git/mingw64/bin/git-credential-manager.exe"` to setup authentication flow

Setup:

1. Navigate to repo shared with Windows host (under `/mnt/c/`). Do not create a separate clone of the repo for Linux.
1. Execute `just install-tools` to install development tools.
1. Open Visual Studio code via `code .`
1. If first time setup, install required Visual Studio Code extensions:
    * C/C++
    * rust-analyzer

Validation:

1. Execute from task palette (F1):
    * `Tasks: Run Build Task`
    * `Tasks: Run Test Task`
1. Execute `just validate-local` in terminal.

# Scheduled deep validation and Local App remediation

The [scheduled-validation chapter](docs/scheduled-validation.md) describes nightly
deep checks, readable failure reports, issue triage and repair PR follow-up.
Humans and Local App agents use the same GitHub issues, claims and linked PRs.
Setup is reproducible from `.github\prompts\setup-scheduled-remediation.prompt.md`;
entries remain disabled until explicitly enabled by the operator.

`just validate-local` always runs shallow validation.
`just package="foo bar" validate-deep-local` always runs deep validation on the
current platform. PR/push workflows stay shallow, while scheduled workflows own
recurring deep checks. Repair authors link relevant deep-check results for review.
The ordinary `just test-scripts` and `just validate-scripts` commands cover the
workflow helpers. The App uses supported native operations and no local
coordination database. See the chapter for setup and external-service safeguards.

# Testing Azure functionality

The `cargo-bench-history` package has an Azure Blob storage backend. Its tests are
special: they are **not** exercised by `just test`, because they need
either a local storage emulator or a real cloud account that an ordinary test run cannot assume
is present. Under `just test` these tests self-skip (no emulator is started), so they never break
a normal test run. To actually exercise them, use one of the dedicated recipes below.

There are two flavours:

* **Azurite (emulator) tests** run against a local [Azurite](https://github.com/Azure/Azurite)
  Blob emulator. `just install-tools` installs Azurite (via npm), and `just test-azurite` starts
  the emulator, runs the tests against it, and stops the emulator afterward — including on failure.
  If an emulator is already running on the default port, the recipe reuses it and leaves it running.
* **Real-Azure tests** run against a real Azure Storage account to exercise the Microsoft Entra ID
  authentication path that the emulator does not cover. Run them with `just test-azure` after
  `az login`. They are opt-in and self-skip unless explicitly enabled.

For account provisioning and the gating environment variables, see `infra/azure-bench-history-test/` and
the `packages/cargo-bench-history/AGENTS.md` testing notes.