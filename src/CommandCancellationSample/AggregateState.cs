using static CommandCancellationSample.Messages.AggregateEvents;

namespace CommandCancellationSample;

/// <summary>
/// Pure, event-folded domain state. Deliberately decoupled from the actor: it knows
/// nothing about persistence, mailboxes, or the actor's identity. The actor simply
/// feeds it events via <see cref="Fold"/> and reads its properties.
/// </summary>
public sealed class AggregateState
{
    public AggregateState()
    {
    }

    public AggregateState(Guid aggregateId)
    {
        AggregateId = aggregateId;
    }

    public static readonly AggregateState Empty = new(Guid.Empty);

    public Guid AggregateId { get; set; }

    /// <summary>Command ids that have been applied (and not retracted).</summary>
    public HashSet<Guid> AppliedCommandIds { get; private set; } = new();

    /// <summary>Command ids that were applied and later retracted via a compensating event.</summary>
    public HashSet<Guid> RetractedCommandIds { get; private set; } = new();

    public bool IsApplied(Guid commandId) => AppliedCommandIds.Contains(commandId);

    public bool IsRetracted(Guid commandId) => RetractedCommandIds.Contains(commandId);

    /// <summary>
    /// Pure fold: an applied command moves to <see cref="AppliedCommandIds"/>; a
    /// retracting event moves it to <see cref="RetractedCommandIds"/>. Returns a NEW
    /// state instance; never mutates the input.
    /// </summary>
    public static AggregateState Fold(AggregateState state, object evt)
    {
        var next = new AggregateState(state.AggregateId)
        {
            AppliedCommandIds = new HashSet<Guid>(state.AppliedCommandIds),
            RetractedCommandIds = new HashSet<Guid>(state.RetractedCommandIds),
        };

        switch (evt)
        {
            case CommandApplied applied:
                next.AppliedCommandIds.Add(applied.CommandId);
                break;

            case CommandRetracted retracted:
                next.AppliedCommandIds.Remove(retracted.CommandId);
                next.RetractedCommandIds.Add(retracted.CommandId);
                break;
        }

        return next;
    }
}
