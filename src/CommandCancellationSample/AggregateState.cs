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

    /// <summary>
    /// The aggregate's real domain state. Every <see cref="CommandApplied"/> folds its
    /// signed <see cref="Messages.AggregateEvents.CommandApplied.Amount"/> into this
    /// balance; a <see cref="CommandRetracted"/> folds the amount back out.
    /// </summary>
    public decimal Balance { get; private set; }

    /// <summary>
    /// Command ids that have been applied (and not retracted), mapped to the amount
    /// each command contributed. A retract looks the amount up here to unwind it and
    /// removes the entry.
    /// </summary>
    public Dictionary<Guid, decimal> AppliedAmounts { get; private set; } = new();

    /// <summary>Command ids that were applied and later retracted via a compensating event.</summary>
    public HashSet<Guid> RetractedCommandIds { get; private set; } = new();

    public bool IsApplied(Guid commandId) => AppliedAmounts.ContainsKey(commandId);

    public bool IsRetracted(Guid commandId) => RetractedCommandIds.Contains(commandId);

    /// <summary>The amount a previously applied command contributed, for compensation.</summary>
    public decimal AppliedAmount(Guid commandId) => AppliedAmounts.GetValueOrDefault(commandId);

    /// <summary>
    /// Pure fold. An <see cref="CommandApplied"/> adds its amount to the balance and
    /// records the command as applied; a <see cref="CommandRetracted"/> subtracts the
    /// amount back out (returning the balance towards its pre-command value) and marks
    /// the command as retracted. Returns a NEW state instance; never mutates the input.
    /// </summary>
    public static AggregateState Fold(AggregateState state, object evt)
    {
        var next = new AggregateState(state.AggregateId)
        {
            Balance = state.Balance,
            AppliedAmounts = new Dictionary<Guid, decimal>(state.AppliedAmounts),
            RetractedCommandIds = new HashSet<Guid>(state.RetractedCommandIds),
        };

        switch (evt)
        {
            case CommandApplied applied:
                next.Balance += applied.Amount;
                next.AppliedAmounts[applied.CommandId] = applied.Amount;
                break;

            case CommandRetracted retracted:
                // Compensating event: unwind the amount the command applied.
                next.Balance -= retracted.Amount;
                next.AppliedAmounts.Remove(retracted.CommandId);
                next.RetractedCommandIds.Add(retracted.CommandId);
                break;
        }

        return next;
    }
}
