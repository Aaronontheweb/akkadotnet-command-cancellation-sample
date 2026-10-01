# PoC Plan — Akka.NET Command Cancellation & Compensating Events

## Goal

Build a runnable, self-contained, **anonymized** proof of concept that answers a
real Akka.NET support question:

> When an HTTP request is cancelled, how do I stop an event-sourced aggregate from
> persisting an in-flight command's events — and what do I do when the events
> already landed?

The PoC must demonstrate, in code, the design conclusions from the support
analysis. It must not contain any customer PII or proprietary business details.

## Background / constraints (the "why")

The customer (kept anonymous) uses a command dispatcher that does
`await router.Ask(command, cancellationToken)`, a router that `Forward`s the
command to an event-sourced aggregate, and an aggregate that generates events and
writes them via `PersistAll`.

Key facts established in the support analysis:

1. **`CancellationToken` only cancels the caller's local wait on the `Ask`.** It
   never travels with the forwarded command, so the aggregate never sees it.
2. **Cancellation in .NET is cooperative and racy.** A token is a signal at a
   checkpoint, not a kill switch. Even EF Core `SaveChangesAsync(token)` does not
   guarantee an in-flight insert won't commit.
3. **`PersistAll` is not cancellable.** There is no token overload and no abort
   seam. Once events are handed to the journal the write is atomic going forward;
   there is no inverse.
4. **The journal is append-only.** You cannot roll back or scrub events. A
   "reversal" is a *new* compensating event, not a rollback. It does not make the
   original event disappear from history.
5. **The "did it persist or not?" answer is inherently indeterminate** from the
   caller's seat because of the persist + cancellation race. Persistence is what
   turns a "cancelled request" into a real consistency risk: state advanced on
   disk while the caller believes it didn't.
6. **If the customer uses Akka.Cluster, a `CancellationToken` cannot be
   serialized.** The cancellation signal must be typeless/index-based so it can
   cross the wire.

## Design conclusions (what the PoC proves)

The honest answers, in order of preference:

### Lever 1 — carry the token, gate at the boundary
The command message carries a cancellation signal. The aggregate checks
`token.IsCancellationRequested` at its control points **before** it generates /
persists events, inside the actor's serialized context. Best-effort: the token
can fire a microsecond after the check. This is the only real "stop it before it
starts" gate.

### Lever 2 — return the persisted event identifiers
The command result (via `Ask`) returns the sequence / event IDs that were written.
This closes the telemetry gap: "did it happen?" becomes "exactly these events were
persisted," so the caller's uncertainty is resolvable rather than tolerated.

### Lever 3 — idempotency on command id (the safety net)
Because the persist/cancel outcome is indeterminate, re-sends must be safe. The
aggregate dedupes on a stable command id so a retried command returns "already
applied" instead of double-persisting. This is the real guard against the
"caller thinks it didn't happen but it did" case corrupting state.

### Lever 4 — compensating events (append-only, honest framing)
When a command genuinely landed and must be *semantically retracted*, append a
compensating event that the aggregate's state machine understands ("applied, then
retracted"). This is NOT a rollback. It:
- does not delete the original event,
- is a second, separate write (so it is itself racy relative to the original),
- must be a modelled domain state, not a scrubbing mechanism.

### Cluster consideration (documented, not necessarily built in PoC)
For a cluster, the cancel signal becomes a serializable `Cancel(commandId)`
message rather than a local `CancellationToken`. The PoC keeps everything
in-process, but models the cancel signal as an index-based message so the design
lifts to a cluster without change.

## Deliverables

1. A runnable console host (`src/CommandCancellationSample`) using an in-memory
   journal (self-contained; no external DB required).
2. An event-sourced aggregate that:
   - checks a cancellation signal before persisting,
   - returns the persisted event IDs on success,
   - dedupes re-sends on command id,
   - can append a compensating "retract" event.
3. A dispatcher + router that model the customer flow but anonymized.
4. A short demonstration in `Program.cs` (or a test) that exercises:
   - command completes normally → event IDs returned,
   - cancellation fires before persist → no events written,
   - same command re-sent → no double apply (idempotent),
   - commanded retract → compensating event appended.
5. README / docs explaining each lever with references to the code.

## Reference design (pseudo-code / spec)

The code follows the Akka.NET Bootcamp "Effective Actor Messaging" conventions:
messages organized as an effects system (`Commands` / `Events` / `Queries`),
all messages are `record` types, and no mutable collections cross actor
boundaries. State is decoupled from the actor as a pure, event-folded model.

### 1. Message hierarchy (effects system)

```csharp
// ---- Commands (instructions to DO something) ----
public static class AggregateCommands
{
    // carries an index-based cancel signal so it survives a cluster (no CTS)
    public sealed record ApplyCommand(Guid CommandId, Guid AggregateId,
                                      string Payload, bool Cancelled);
    public sealed record CancelCommand(Guid CommandId);
    public sealed record RetractCommand(Guid CommandId);
}

// ---- Events (facts that happened; folded into state) ----
public static class AggregateEvents
{
    public sealed record CommandApplied(Guid CommandId, Guid AggregateId,
                                        string Payload, long SequenceNr);
    public sealed record CommandCancelled(Guid CommandId, Guid AggregateId);
    // compensating event (NOT a rollback - this is a modelled retraction)
    public sealed record CommandRetracted(Guid CommandId, Guid AggregateId);
}
```

### 2. Decoupled state model (pure, no actor coupling)

```csharp
// Pure state. Reconstruction = fold all events in sequence order.
public sealed class AggregateState
{
    public Guid AggregateId { get; private set; }
    public HashSet<Guid> AppliedCommandIds { get; private set; } = new();
    public HashSet<Guid> RetractedCommandIds { get; private set; } = new();

    // Fold: applied command moves to Applied; retract moves it to Retracted
    public static AggregateState Fold(AggregateState state, object evt) =>
        evt switch {
            CommandApplied a      => state with { AppliedCommandIds.Add(a.CommandId) },
            CommandRetracted r    => state with { RetractedCommandIds.Add(r.CommandId) },
            _                     => state
        };
}
```

### 3. Aggregate actor (PersistentActor)

```csharp
public sealed class AggregateActor : ReceivePersistentActor
{
    private AggregateState _state = new();
    public override string PersistenceId => _state.AggregateId.ToString();

    public AggregateActor(Guid aggregateId)
    {
        _state = new AggregateState { AggregateId = aggregateId };

        Recover<CommandApplied>(e   => _state = AggregateState.Fold(_state, e));
        Recover<CommandRetracted>(e => _state = AggregateState.Fold(_state, e));

        Command<ApplyCommand>(cmd =>
        {
            // Lever 1: gate before persist (cooperative, best-effort)
            if (cmd.Cancelled)
            {
                Persist(new CommandCancelled(cmd.CommandId, _state.AggregateId),
                        _ => Sender.Tell(new Status.Failure(new CommandCancelledException(cmd.CommandId))));
                return;
            }

            // Lever 3: idempotency - already applied or retracted? short-circuit
            if (_state.AppliedCommandIds.Contains(cmd.CommandId) ||
                _state.RetractedCommandIds.Contains(cmd.CommandId))
            {
                Sender.Tell(new AlreadyApplied(cmd.CommandId));   // no double-persist
                return;
            }

            Persist(new CommandApplied(cmd.CommandId, _state.AggregateId,
                                       cmd.Payload, LastSequenceNr + 1), applied =>
            {
                _state = AggregateState.Fold(_state, applied);
                // Lever 2: return the persisted event id to the caller
                Sender.Tell(new CommandAppliedAck(applied.CommandId, applied.SequenceNr));
            });
        });

        Command<CancelCommand>(cmd =>
            Sender.Tell(new CancelAck(cmd.CommandId, IsCancelled(cmd))));

        Command<RetractCommand>(cmd =>
        {
            if (!_state.AppliedCommandIds.Contains(cmd.CommandId)) { /* no-op */ }
            else
                Persist(new CommandRetracted(cmd.CommandId, _state.AggregateId),
                        retracted =>
                {
                    _state = AggregateState.Fold(_state, retracted);
                    Sender.Tell(new RetractedAck(cmd.CommandId));
                });
        });
    }
}
```

### 4. Dispatcher (models the customer flow)

```csharp
// The Ask waits locally; when the HTTP token fires, this returns to the caller.
// Lever 2 makes the persisted event id available even if the response races cancel.
var result = await router.Ask<CommandAppliedAck>(
    new ApplyCommand(commandId, aggregateId, payload, requestToken.IsCancellationRequested),
    requestToken);

// Lever 3: on retry with the same commandId, the aggregate replies AlreadyApplied.
```

## Technical choices

- **Test harness:** use **`Akka.Hosting.TestKit`** (`AkkaHostingTest` base class).
  This tests the real hosted `ActorSystem` with its actual DI/service setup and
  HOCON config — which is exactly how the sample wires actors up — so the tests
  exercise the true production wiring rather than a hand-built `ActorSystem`.
- **Persistence (journal plugin):** use **`Akka.Persistence.TestKit`** alongside
  `Akka.Hosting.TestKit` for the **in-memory journal**, so the tests and the
  runnable host need no external DB or file. Keep it as a **test-only** reference;
  the aggregate does not hard-code the provider, so it can be swapped.
- **File-backed variant (not built in PoC):** [`Akka.Persistence.Sqlite`](https://www.nuget.org/packages/Akka.Persistence.Sqlite)
  would be the drop-in if a durable journal is wanted — same aggregate code, just
  a different journal config. Not required for this PoC.
- **Packages:** `Akka.Hosting` (already pinned 1.5.71). Add `Akka.Persistence`
  (core) + `Akka.Hosting.TestKit` + `Akka.Persistence.TestKit` (test-only) to
  `Directory.Packages.props` central management. Note `Akka.Hosting.TestKit`
  1.5.71 transitively requires xunit v3, so tests should reference the xunit v3
  metapackage and the `xunit.runner.visualstudio` 3.x adapter (not the v2 adapter).
- **Project:** keep `src/CommandCancellationSample` console host. Add
  `AggregateActor`, `RouterActor`, `Dispatcher`, shared message types
  (`Messages.cs`), and the journal config. Put the tests in a `tests/` project
  under a `CommandCancellationSample.Tests` class library.

## Tests (validate the reversal)

All tests derive from `AkkaHostingTest` (`Akka.Hosting.TestKit`), so they use the
real hosted `ActorSystem`, DI, and the in-memory journal configured via
`Akka.Persistence.TestKit`.

- **Cancel before persist → nothing written:** send `ApplyCommand(..., Cancelled=true)`,
  assert the actor replies `Status.Failure` and the journal has **no** `CommandApplied`.
- **Idempotent re-send:** apply the same `CommandId` twice, assert the second returns
  `AlreadyApplied` and the journal still has exactly one `CommandApplied`.
- **Reversal (the core ask):** apply a command, then `RetractCommand`, then assert
  the folded `AggregateState` reverts to pre-command (the retraction event is
  folded and the command is now in `RetractedCommandIds`).
- **Replay after retract:** recover the aggregate from the journal and assert the
  post-retraction state is consistent (proves it's durable across replay).

## Out of scope for the PoC

- Real cluster / remote deployment (in-process only; cluster concern documented).
- A real SQL/EventStore journal (in-memory is sufficient to prove the design).
- The actual reply email to the customer (separate task).
