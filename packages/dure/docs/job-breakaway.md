# Job breakaway

How a supervisor gets out of the boundary its launcher put it in, and what it
can honestly say about having done so. Part of the `dure`
[implementation guide](implementation.md).

## Two places, two policies

Breakaway is evaluated against the job a process is directly in, so the policy
matters in two places and the process PAL takes it as an explicit parameter
rather than a fixed choice.

The **session job permits breakaway**, so a nested `dure run` inside the app can
create an independent inner supervisor; other apps that deliberately request
breakaway receive the same Windows behavior.

The **launcher's job is not ours to choose**. A launcher that forbids breakaway
would kill the supervisor along with itself, and `CreateProcessW` refuses the
spawn outright, so `dure run` reports that as a startup failure. `cargo run` is
such a launcher, so `dure` is exercised as an installed binary, not through
cargo.

Diagnosing a denied breakaway needs care, because Windows reports it as a plain
access-denied failure from `CreateProcessW`. Only that error code, and only
while the job the caller is in withholds the breakaway permission, is reported
as denied breakaway; every other failure keeps its own identity instead of being
misattributed to a job policy. The resulting message names the job as the cause
and tells the user to launch `dure.exe` directly.

## What the supervisor can establish

Because breakaway only leaves the *immediate* job, a permissive job nested inside
a restrictive one lets `CreateProcessW` succeed while the supervisor stays a
member of the outer job. Whether the supervisor escaped is therefore confirmed
rather than assumed, and confirmed by the supervisor, because Windows reports a
process's job membership only to that process itself and offers no way to
enumerate a chain of ancestor jobs.

What the supervisor asks is not whether it is in a job but whether the job it is
in would kill it: only kill-on-close ties its lifetime to the launcher.
Terminals and remote-session hosts routinely place every process in an ambient
job without that limit, and treating those as doomed would condemn nearly every
real session.

The inspection has three outcomes, and all three are carried to the client
unchanged rather than collapsed into a yes or no:

| Outcome | Means | Client says |
| --- | --- | --- |
| confirmed tie | the immediate job is kill-on-close | it will not survive the launcher, and why |
| unknown | the job could not be inspected | that nothing was established |
| no tie detected | no immediate job would end it | nothing |

Collapsing "unknown" into "tied" would name a cause that was never established,
and collapsing it into "durable" would promise one. Neither is honest, and the
third state costs one protocol value.

Even the last row is not a durability guarantee. The check inspects the
immediate job, so a harmless job nested inside a killing one still reads as no
tie detected. Detecting that would require walking the job chain, which Windows
does not expose. It is a narrow gap: the launchers that impose kill-on-close do
not nest a second job below it.

## Why it is a warning

The supervisor cannot know why it was launched that way, and a session that is
merely non-durable still does everything else the user asked for; a build or
test harness that wraps `dure` in a job wants exactly that. So the startup
response carries the assessment, and the client — which owns the user's console
— prints the warning and continues.

This is also what lets the integration tests run: `cargo test` places its test
binaries in a kill-on-close job that forbids breakaway, so no session started
under cargo is ever durable.

## Testing

Tests would otherwise never see either path: a test that builds its own job to
host a child naturally gives it the permissive policy, which is more permissive
than any real launcher. The integration harness therefore builds job chains, and
the regression tests spawn `dure run` both inside a job that forbids breakaway
and inside a permissive job nested in one that forbids it.
