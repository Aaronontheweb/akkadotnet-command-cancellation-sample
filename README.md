# Akka.NET Command Cancellation Sample

A runnable example of how to handle per-request cancellation (`CancellationToken`)
in an Akka.NET event-sourced aggregate whose command has already been handed to
the actor for processing.

This is a common question when wiring `CancellationToken`s from an HTTP request into
an actor that persists events: the token cancels the caller's `Ask` wait, but it
doesn't stop the aggregate from persisting once the command is in flight.

## The design (four levers)

The sample demonstrates four ways to make an in-flight command behave well under
cancellation:

1. **Carry the cancellation signal, gate at the boundary** — the aggregate checks
   the signal before it writes events, so a cancelled command is rejected before
   anything is persisted. Best-effort: the signal can fire a moment after the check.
2. **Return the persisted event identifiers** — the aggregate replies with the
   sequence number of the events it wrote, so "did it happen or not?" is resolvable
   rather than just tolerated.
3. **Idempotency on command id** — re-sending the same command id returns
   "already applied" instead of persisting a second time, so an indeterminate
   outcome can't double-apply.
4. **Compensating events** — to undo a command that genuinely landed, append a
   retraction event that the aggregate folds in, unwinding real domain state.
   This is **not** a rollback: the journal is append-only, so a retraction is a
   modelled domain fact, not a deletion.

## Why a `CancellationToken` can't be threaded into `Persist`/`PersistAll`

The journal write is all-or-nothing and in order per `persistenceId`, and the actor
must end in a known state. A write that's already been handed to the journal can't
be safely aborted mid-flight — there's no path to know whether part of it committed.
So the sample models cancellation at the command boundary and uses idempotency +
compensating events to make the outcome safe instead.

## Layout

- `src/CommandCancellationSample/` — console host with an `ActorSystem` that runs
  the apply / cancel / idempotency / retract flow.
- `tests/CommandCancellationSample.Tests/` — `Akka.Hosting.TestKit` tests using an
  in-memory journal, including a test that proves a retraction event reverts the
  aggregate's balance to its original value after journal replay.

## Building

```bash
dotnet build CommandCancellationSample.slnx
dotnet test
```
