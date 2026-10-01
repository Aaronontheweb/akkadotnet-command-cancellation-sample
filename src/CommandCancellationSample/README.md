# Akka.NET Command Cancellation Sample

A runnable, anonymized reproduction of a common support question: how to honor an
HTTP request's `CancellationToken` inside an Akka.NET event-sourced aggregate when
the command has already been handed to the actor for processing.

This repo is **scaffolded** from the
[`akkadotnet/build-system-template`](https://github.com/akkadotnet/build-system-template)
. The build system and the actor-shell project are in place; the actual
cancellation / compensating-transaction design is intentionally **not yet filled in**.

## Intent

The scenario we are modeling (with all customer details removed):

1. An HTTP request arrives with a `CancellationToken` tied to that request.
2. A command dispatcher validates the request, then forwards the command to a
   router which routes it to an event-sourced aggregate actor.
3. The aggregate processes the command, generates events, and persists them via
   `PersistAll`.
4. If the caller cancels the HTTP request, the token fires — but by then the
   command may already be in flight in the aggregate.

This sample explores the options: best-effort boundary cancellation, a
serializable `Cancel(commandId)` message, and a compensating-transaction shape.
