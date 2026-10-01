# Akka.NET Command Cancellation Sample

An anonymized, runnable reproduction of an Akka.NET support scenario: honoring
per-request cancellation (`CancellationToken`) inside an event-sourced aggregate
whose command is already in flight.

Scaffolded from the [`akkadotnet/build-system-template`](https://github.com/akkadotnet/build-system-template)
using the standard Akka.NET build system (`global.json`, central package
management, `.slnx`, GitHub Actions).

## Status

The build shell is in place (see `src/CommandCancellationSample`). The
cancellation / compensating-transaction design is being added incrementally.

## Layout

- `src/CommandCancellationSample/` — console app with an `ActorSystem` host.

## Building

```bash
dotnet build CommandCancellationSample.slnx
```
