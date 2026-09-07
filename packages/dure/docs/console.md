# Console

The two consoles a session involves: the pseudoconsole the supervisor gives the
app, and the local console the client hands over for the relay. Part of the
`dure` [implementation guide](implementation.md).

A *console* is the Windows console-host API — handles, modes, and window size. A
*terminal* is the visible emulator that renders VT. This document is about
consoles; the terminal is whatever the user is already sitting at.

## The app's pseudoconsole

The app is attached with `CreatePseudoConsole` and
`PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE`. From the app's side this is a console;
from the supervisor's side it is a pair of pipes plus resize. Raw anonymous
pipes as the child's stdin/stdout are not an alternative: a TUI would not see a
console.

The app is spawned with explicitly invalid standard handles. `CreateProcessW`
otherwise passes the supervisor's own standard handle values on to the child,
and the pseudoconsole attribute does not displace values that arrive that way,
so the child would come up on pipes rather than on a console.

The terminal the user sits at already presents a console: Windows Terminal
through the console host, an SSH session through a pseudoconsole. `dure` adds a
second one around the app. For a VT TUI such as Copilot CLI, that extra layer is
not expected to remove features the same app already has in that terminal
without `dure`. HWND-based console features and terminal graphics protocols
remain unavailable, as they already are in those terminals. Occasional resize or
line-wrap glitches are in the same class as any terminal plus a pseudoconsole.

Console close on the client does not propagate a kill into the supervisor's
pseudoconsole.

### Ending it in two steps

Teardown closes the lifetime job before the pseudoconsole. Descendants of the
app stay attached to the pseudoconsole until the job ends them, and closing a
pseudoconsole waits for its attached clients, so the other order can stall on a
grandchild that outlived the app.

The pseudoconsole then ends in two steps, because the app's last bytes and the
handles that carry them have different lifetimes. Finishing closes the
pseudoconsole itself, which flushes what it still holds into the output pipe and
drops the host's end of that pipe; reads keep succeeding until the pipe is empty
and then report the end of the stream. Only once the output pump has run to that
end are the handles released. Releasing them in one step instead would cancel
whatever read was in flight, and bytes the app had already written would be lost
in favour of its exit status. Closing on its own remains the abandon path, used
when a session is torn down before it ever served anyone.

Reaching the end of the app's output and failing to read it are different
outcomes, and the pseudoconsole reports them differently. A failed read leaves
the output incomplete, and treating it as a clean end would silently truncate
what the app said.

Only the input side is cancelled by finishing. A relay blocked writing to an app
that is gone has nothing left to deliver, while the output side still holds
bytes worth reading. The pseudoconsole handle is taken out of the table under
the lock and closed outside it, since closing waits for attached clients and
those clients only make progress while the output pump keeps emptying the pipe.

## The client's console

Terminal pass-through (`design.md`, "Terminal pass-through") is not a feature
built on top of the relay; it is what the relay does once the client's console
is in the right mode and the size is carried as its own message.

### One takeover, one hand-back

The client takes the console over as one operation that returns a lease, and
hands it back through that lease. The console is process-wide state, so a second
lease is refused rather than sharing the first one's saved state and restoring
it out from under a live relay.

The lease records what each step actually changed, so a takeover that fails
halfway is handed back exactly as far as it got, and every recorded change is
attempted on the way out even when an earlier one fails: leaving a console
half-raw is worse than reporting an error sooner.

Handing back is explicit and fallible on every ordinary return, so a failure is
something the caller learns about rather than something a drop guard swallows.
`Drop` remains as best-effort cleanup for an unwind, where there is nobody left
to tell.

Ctrl+C suppression is part of that takeover, and is an owned handler rather than
the process-wide ignore flag. The flag cannot be un-set to whatever the caller
had before; removing an owned handler restores exactly the previous policy. The
handler consumes Ctrl+C and Ctrl+Break only — close, logoff, and shutdown run
their course.

### Modes

The modes the takeover sets are the whole pass-through mechanism.

On input, `ENABLE_ECHO_INPUT`, `ENABLE_LINE_INPUT`, and `ENABLE_PROCESSED_INPUT`
are cleared so keystrokes reach the app as they are typed instead of being
echoed locally, buffered until a newline, or turned into a Ctrl+C signal for the
client. `ENABLE_VIRTUAL_TERMINAL_INPUT` is set so the console encodes keys that
are not characters — arrows, function keys, modifier chords — as the VT
sequences an app already knows how to read, which is why no per-key mapping
exists anywhere in this crate. `ENABLE_WINDOW_INPUT` is cleared so window
changes do not enter the byte reader's queue; size is observed independently,
as described under [Window size](#window-size).

On output, `ENABLE_VIRTUAL_TERMINAL_PROCESSING`, `ENABLE_PROCESSED_OUTPUT`, and
`ENABLE_WRAP_AT_EOL_OUTPUT` are set so the local console host renders the
sequences the app wrote through its pseudoconsole. Nothing in the relay inspects
that stream, so cursor addressing, colors, the alternate screen buffer, scroll
regions, and title sequences all work by not being touched.

Bits other than these are preserved from the console's own mode and restored
along with them.

### Encoding

A pseudoconsole produces and consumes UTF-8. A console, in contrast, applies a
code page to the bytes crossing its reads and writes, and both console code
pages default to the machine's OEM one — 437 on a US-English install. The relay
hands the client's console raw pseudoconsole bytes, so under an OEM code page
every multi-byte UTF-8 sequence is decoded as several unrelated glyphs: a TUI's
frame comes out as unreadable text, and because each sequence expands to more
cells than it should, the line wraps early and the right edge of the screen
breaks up. ASCII survives because it is identical in both, which is why plain
text looks fine while everything else does not.

The takeover therefore converts both code pages to UTF-8 alongside the modes,
and the hand-back restores the saved values with the saved modes. Code pages are
per-console and outlive the process that changed them, so leaving them converted
would change how a shell sharing that console behaves after the session. The
input direction needs the same treatment: without it a non-ASCII keystroke is
encoded under the OEM code page and reaches the app as something else.

An integration test covers this end to end: the helper prints box-drawing
characters through the supervisor's pseudoconsole, and the test asserts they
arrive intact in the outer pseudoconsole's screen, which they do not if either
code page is left at its default. The input direction is covered the same way,
by typing non-ASCII text in and requiring the helper to echo back what it
received.

### Reading input

One blocking `ReadFile` path owns the console input queue. Under VT input mode,
the console host exposes keys and terminal reports that have a VT representation
through that byte stream. This includes mouse and focus reports when the app has
negotiated them and the local console presents them as VT input. The relay does
not inspect, classify, or consume individual input records.

Window changes have no VT byte representation. They are deliberately kept out
of the input queue and observed through the console's current size instead.
Keeping those mechanisms independent avoids a check-then-read race in which a
window record can arrive after the queue is inspected and be consumed without
being reported by `ReadFile`.

A blocking read outlives the relay unless something ends it, and a console
handed back while a read is still outstanding would take the next thing the user
types. The relay therefore cancels the read and joins its thread before handing
the console back. The reader waits on the console input handle and a dedicated
cancellation event together. Some input records produce no bytes and leave that
read waiting; cancellation also cancels any such read. A sticky cancellation
flag and a published read-active flag close the handoff between those two paths:
a read starts only after checking the sticky flag, and the canceller retries for
a bounded scheduler handoff and waits for an accepted cancellation to retire
the read. If either step fails, the relay reports that failure without joining a
reader it cannot prove was woken; the command can then leave instead of waiting
indefinitely.

### Window size

Size travels on the same connection as bytes but as its own message, because it
is console state rather than console content: the attach message carries the
size the client starts with, and a dedicated watcher samples the attached
console at a short fixed cadence. It sends a resize message only when the
observed size differs from the last size sent. Both messages end at the
pseudoconsole's resize, which is what makes the app observe a console resize
rather than receive bytes describing one.

Size observation is state convergence rather than event forwarding. Several
changes within one polling interval may collapse to the latest geometry, and
ordering against input bytes is not defined. Neither distinction changes the
app's result: it receives the current geometry shortly after the terminal
settles, and a missed observation is corrected by the next sample.

The attach size is applied only once the connection owns the client slot. The
app redraws in response to a size change, and that redraw belongs to the client
that asked for the size, so applying it earlier would paint the previous
client's screen. Until the first attach the pseudoconsole runs at a default
geometry, which exists only so the app has some size during the window between
spawn and attach.

A watcher stops if a size observation fails or the connection can no longer
accept a resize. The input reader and receive loop own relay lifetime, so this
secondary observer does not race them to decide how the relay ends. A
pseudoconsole resize failure is likewise ignored: the app wait and output pump
observe that teardown and end the session through the paths that own it.
