# dure

Detachable Windows console sessions that outlive the terminal.

`dure` starts an interactive console process under a per-app supervisor, so the
session keeps running after the terminal that launched it goes away — a closed
window, a dropped SSH connection, or a killed foreground process. A later
terminal reattaches to that same session.

`dure` runs only on Windows. Other targets build, so the workspace stays
buildable everywhere, but the resulting binary does nothing except report that
the platform is unsupported.

## Install

```text
cargo binstall dure
```

This downloads a prebuilt binary for `x86_64-pc-windows-msvc` and
`aarch64-pc-windows-msvc`. Every other target, Windows or not, transparently
falls back to a source build — and off Windows that build produces only the
unsupported-platform refusal, so there is nothing to install there.

Start sessions from the installed `dure.exe`, not through a wrapper such as
`cargo run`. Wrappers of that kind confine what they launch to a Windows job
object that forbids breakaway and is killed when the wrapper exits, so the
supervisor could not outlive them; `dure run` refuses such a launcher outright
rather than starting a session that would die with it. Ordinary shells,
including the one an SSH session provides, are fine.

## Usage

```text
dure [--verbose] run -- <command> [args...]
dure [--verbose] resume [<id>]
dure [--verbose] list
dure [--verbose] kill <id>
```

`run` always starts a new session in the current directory and attaches
immediately; it never reconnects to an existing one. The command is executed
directly rather than through a shell, and it inherits the working directory and
environment of the `dure run` process.

`resume` attaches to a live session. Without an id it auto-detects: a single
live session launched from the current directory is taken, and anything else
prints the session list and asks which id to take. `list` is where those ids
come from.

`kill <id>` abruptly terminates the session's supervisor; the app and its
ordinary descendants die with it.

Closing the foreground `dure` process detaches — the supervisor and app keep
running. A new attach displaces any older client, which is also how a wedged
client is taken over. Reattaching does not replay what the app printed earlier:
the new client starts on an empty screen and then sees live output.

`--verbose` explains on stderr what a command inspected and decided — where the
session store is, which records were read, why each was judged live or dead, and
why `resume` chose a session or fell through to the list. Reach for it when
session selection or liveness does not go the way you expected.

## A session across two terminals

```text
> dure run -- pwsh.exe
session 1
...work in the shell, then close the terminal window...
```

The app keeps running. From a new terminal:

```text
> dure list
ID  ATTACHED  SUPERVISOR PID  AGE  LAUNCH DIRECTORY  COMMAND
1   no        12345           4m   C:\work           pwsh.exe

> dure resume
```

`resume` finds session 1 because it was launched from `C:\work`; from anywhere
else, name it explicitly with `dure resume 1`. When the work is done, exit the
app normally, or discard the session from another terminal with `dure kill 1`.
