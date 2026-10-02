# CommandCancellationSample

A small Akka.NET console host that demonstrates how to handle per-request
cancellation (`CancellationToken`) in an event-sourced aggregate whose command is
already in flight.

## What it shows

The host starts an `ActorSystem` with an in-memory persistence journal and runs
the four-lever flow from the repo root README:

1. **Boundary gating** — a cancelled command is rejected before any event is written.
2. **Persisted event identifiers** — the aggregate replies with the sequence number
   of the events it wrote.
3. **Idempotency** — re-sending the same command id returns "already applied"
   instead of persisting twice.
4. **Compensating events** — a `RetractCommand` appends a retraction event that
   unwinds the aggregate's balance to its original value.

Run it with:

```bash
dotnet run --project src/CommandCancellationSample
```
