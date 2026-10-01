using Akka.Actor;
using Akka.Persistence;
using CommandCancellationSample.Messages;
using static CommandCancellationSample.Messages.AggregateCommands;
using static CommandCancellationSample.Messages.AggregateEvents;

namespace CommandCancellationSample;

/// <summary>
/// Event-sourced aggregate. Recover folds persisted events into <see cref="AggregateState"/>;
/// each command is gated on the boundary before any write (Lever 1), deduped by command id
/// (Lever 3), and returns the persisted sequence number on success (Lever 2). A retract
/// appends a compensating <see cref="CommandRetracted"/> event (Lever 4) - never a rollback.
/// </summary>
public sealed class AggregateActor : ReceivePersistentActor
{
    private AggregateState _state = AggregateState.Empty;

    public Guid AggregateId { get; }

    public override string PersistenceId => AggregateId.ToString();

    public AggregateActor(Guid aggregateId)
    {
        AggregateId = aggregateId;
        _state = new AggregateState { AggregateId = aggregateId };

        Recover<CommandApplied>(e => _state = AggregateState.Fold(_state, e));
        Recover<CommandRetracted>(e => _state = AggregateState.Fold(_state, e));

        // Lever 1: gate BEFORE persist (cooperative, best-effort).
        // Lever 3: idempotency - dedupe on command id.
        Command<ApplyCommand>(Apply);
        Command<CancelCommand>(cm => Sender.Tell(new CommandAcks.CancelAck(cm.CommandId, !_state.IsApplied(cm.CommandId))));
        Command<RetractCommand>(Retract);

        // Read-side query for the tests / host.
        Command<AggregateQueries.GetState>(q =>
            Sender.Tell(new AggregateQueries.StateSnapshot(AggregateId, _state.AppliedCommandIds, _state.RetractedCommandIds)));
    }

    private void Apply(ApplyCommand cmd)
    {
        // Lever 1: cancellation signal checked in the actor's serialized context,
        // before any journal write. Best-effort: the signal may fire microseconds after.
        if (cmd.Cancelled)
        {
            // Audit the rejection as a CommandCancelled fact; do NOT write a CommandApplied.
            Persist(new AggregateEvents.CommandCancelled(cmd.CommandId, AggregateId),
                _ => Sender.Tell(new Status.Failure(new CommandCancelledException(cmd.CommandId))));
            return;
        }

        // Lever 3: already applied or retracted? Short-circuit, no double-persist.
        if (_state.IsApplied(cmd.CommandId) || _state.IsRetracted(cmd.CommandId))
        {
            Sender.Tell(new CommandAcks.AlreadyApplied(cmd.CommandId));
            return;
        }

        // Lever 2: persist and return the sequence number (the event id) to the caller.
        var seqNr = LastSequenceNr + 1;
        Persist(new AggregateEvents.CommandApplied(cmd.CommandId, AggregateId, cmd.Payload, seqNr),
            applied =>
            {
                _state = AggregateState.Fold(_state, applied);
                Sender.Tell(new CommandAcks.CommandAppliedAck(applied.CommandId, applied.SequenceNr));
            });
    }

    private void Retract(RetractCommand cmd)
    {
        if (!_state.IsApplied(cmd.CommandId))
        {
            // Nothing was applied (or already retracted), so there's nothing to retract.
            Sender.Tell(new CommandAcks.AlreadyApplied(cmd.CommandId));
            return;
        }

        // Lever 4: append a compensating event. This is a separate write; it does NOT
        // delete the original CommandApplied event from the journal.
        Persist(new AggregateEvents.CommandRetracted(cmd.CommandId, AggregateId),
            retracted =>
            {
                _state = AggregateState.Fold(_state, retracted);
                Sender.Tell(new CommandAcks.RetractedAck(cmd.CommandId));
            });
    }
}
