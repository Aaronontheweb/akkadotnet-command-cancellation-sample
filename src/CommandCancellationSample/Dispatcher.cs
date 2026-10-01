using Akka.Actor;
using Akka.Pattern;
using CommandCancellationSample.Messages;

namespace CommandCancellationSample;

/// <summary>
/// Models the customer's command dispatcher: it validates an inbound request, then does
/// <c>await router.Ask(command, cancellationToken)</c>. The token only cancels the local
/// wait on the Ask; it never travels to the aggregate. So the dispatcher captures whether
/// the token had already fired (<c>token.IsCancellationRequested</c>) at the boundary and
/// stamps it onto the command message - the only real "stop it before it starts" gate.
/// </summary>
public sealed class Dispatcher
{
    private readonly IActorRef _router;
    private readonly TimeSpan _timeout;

    public Dispatcher(IActorRef router, TimeSpan timeout)
    {
        _router = router;
        _timeout = timeout;
    }

    /// <summary>
    /// Submit an <see cref="ApplyCommand"/> with a signed <paramref name="amount"/>. Returns
    /// the <see cref="CommandAcks.CommandAppliedAck"/> (with persisted sequence number and
    /// amount) on success. The ask is bounded by <paramref name="token"/>.
    /// </summary>
    public async Task<object> ApplyAsync(Guid aggregateId, decimal amount, CancellationToken token)
    {
        var commandId = Guid.NewGuid();
        // Lever 1: capture the cancel signal at the boundary - this is what the aggregate sees.
        var cmd = new AggregateCommands.ApplyCommand(commandId, aggregateId, amount, token.IsCancellationRequested);

        // Lever 2: Ask resolves to the persisted event id. If cancellation fires mid-flight,
        // the Ask throws (OperationCanceledException) but the events may still have landed -
        // idempotency on commandId (Lever 3) makes a retry safe.
        return await _router.Ask<object>(cmd, _timeout);
    }

    /// <summary>Append a compensating retract event for a previously applied command.</summary>
    public async Task<object> RetractAsync(Guid aggregateId, Guid commandId, CancellationToken token)
    {
        var cmd = new AggregateCommands.RetractCommand(commandId, aggregateId);
        return await _router.Ask<object>(cmd, _timeout);
    }
}
