using Akka.Actor;
using Akka.Actor.Dsl;
using CommandCancellationSample.Messages;
using static CommandCancellationSample.Messages.AggregateCommands;
using static CommandCancellationSample.Messages.AggregateQueries;

namespace CommandCancellationSample;

/// <summary>
/// Routes commands to the aggregate identified in the message. Uses <see cref="IActorRef.Forward"/>
/// so the aggregate's reply goes straight back to the original sender (the dispatcher's Ask),
/// bypassing the router.
/// </summary>
public sealed class RouterActor : ReceiveActor
{
    private readonly Dictionary<Guid, IActorRef> _aggregates = new();

    public RouterActor()
    {
        Receive<ApplyCommand>(cmd =>
        {
            var agg = GetOrCreate(cmd.AggregateId);
            agg.Forward(cmd);
        });

        Receive<RetractCommand>(cmd =>
        {
            if (_aggregates.TryGetValue(cmd.AggregateId, out var agg))
                agg.Forward(cmd);
        });

        Receive<AggregateQueries.GetState>(query =>
        {
            if (_aggregates.TryGetValue(query.AggregateId, out var agg))
                agg.Forward(query);
        });
    }

    private IActorRef GetOrCreate(Guid aggregateId)
    {
        if (!_aggregates.TryGetValue(aggregateId, out var existing))
        {
            existing = Context.ActorOf(
                Props.Create(() => new AggregateActor(aggregateId)),
                $"aggregate-{aggregateId:N}");
            _aggregates[aggregateId] = existing;
        }

        return existing;
    }
}
