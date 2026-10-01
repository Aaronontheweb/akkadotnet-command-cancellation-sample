namespace CommandCancellationSample.Messages;

/// <summary>
/// Commands are instructions to DO something. They are replayed as-is into the
/// aggregate's serialized mailbox context. The cancel signal is carried as a plain
/// <c>bool</c> rather than a <see cref="System.Threading.CancellationToken"/> so the
/// message stays serializable and lifts to a clustered deployment unchanged.
/// </summary>
public static class AggregateCommands
{
    /// <summary>
    /// Applies a domain command to the aggregate.
    /// <paramref name="Cancelled"/> is the index-based cancellation signal captured at
    /// the command boundary (Lever 1). It is NOT a live token inside the actor.
    /// </summary>
    public sealed record ApplyCommand(
        Guid CommandId,
        Guid AggregateId,
        string Payload,
        bool Cancelled);

    /// <summary>Query-style command: is this command's state cancellable (i.e. not applied)?</summary>
    public sealed record CancelCommand(Guid CommandId);

    /// <summary>Appends a compensating event that semantically retracts an applied command.</summary>
    public sealed record RetractCommand(Guid CommandId, Guid AggregateId);
}

/// <summary>
/// Events are facts that happened. They are folded into <see cref="AggregateState"/>.
/// The journal is append-only, so a "reversal" is a NEW event, never a rollback.
/// </summary>
public static class AggregateEvents
{
    public sealed record CommandApplied(
        Guid CommandId,
        Guid AggregateId,
        string Payload,
        long SequenceNr);

    /// <summary>Records that a command was cancelled at the boundary (audit-only, not folded).</summary>
    public sealed record CommandCancelled(Guid CommandId, Guid AggregateId);

    /// <summary>
    /// Compensating event (NOT a rollback). It is a second, separate write that the
    /// state machine understands as "applied, then retracted".
    /// </summary>
    public sealed record CommandRetracted(Guid CommandId, Guid AggregateId);
}

/// <summary>
/// Read-side queries against the aggregate's current folded state.
/// </summary>
public static class AggregateQueries
{
    public sealed record GetState(Guid AggregateId);

    public sealed record StateSnapshot(
        Guid AggregateId,
        IReadOnlySet<Guid> AppliedCommandIds,
        IReadOnlySet<Guid> RetractedCommandIds);
}

/// <summary>
/// Acknowledgements and results returned to the dispatcher / caller.
/// </summary>
public static class CommandAcks
{
    /// <summary>Lever 2: the persisted event identifiers, so "did it happen?" becomes resolvable.</summary>
    public sealed record CommandAppliedAck(Guid CommandId, long SequenceNr);

    /// <summary>The command id was already applied (or retracted) - no double-persist (Lever 3).</summary>
    public sealed record AlreadyApplied(Guid CommandId);

    public sealed record RetractedAck(Guid CommandId);

    public sealed record CancelAck(Guid CommandId, bool IsCancelled);
}

/// <summary>
/// Raised when a command is rejected at the boundary because the cancellation
/// signal was already set. Surfaced as a <see cref="Akka.Actor.Status.Failure"/>.
/// </summary>
public sealed class CommandCancelledException : Exception
{
    public Guid CommandId { get; }

    public CommandCancelledException(Guid commandId)
        : base($"Command '{commandId}' was cancelled before it was persisted.")
    {
        CommandId = commandId;
    }
}
