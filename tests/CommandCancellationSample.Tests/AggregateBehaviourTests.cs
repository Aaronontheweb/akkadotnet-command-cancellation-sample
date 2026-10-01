using Akka.Actor;
using Akka.Hosting.TestKit;
using Akka.Persistence;
using Akka.TestKit;
using CommandCancellationSample;
using CommandCancellationSample.Messages;
using Xunit;

namespace CommandCancellationSample.Tests;

/// <summary>
/// Validates the aggregate's cancellation / compensating-event behaviour through the
/// public actor contract (Ask replies) and journal replay. Derives from the
/// Akka.Hosting.TestKit <c>PersistenceTestKit</c> base, which wires a real hosted
/// ActorSystem with an in-memory <c>TestJournal</c>.
/// </summary>
public class AggregateBehaviourTests : PersistenceTestKit
{
    public AggregateBehaviourTests(ITestOutputHelper output) : base(output: output)
    {
    }

    private IActorRef? _router;

    private IActorRef Router => _router ??= Sys.ActorOf(Props.Create<RouterActor>(), "router");

    private Guid NewAggregateId() => Guid.NewGuid();

    [Fact]
    public async Task Apply_command_persists_and_returns_the_sequence_number()
    {
        var aggId = NewAggregateId();
        var cmdId = Guid.NewGuid();

        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, "payload-a", Cancelled: false), TestActor);

        var ack = await ExpectMsgAsync<CommandAcks.CommandAppliedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, ack.CommandId);
        Assert.True(ack.SequenceNr >= 1, "Persisted event should get a sequence number (Lever 2).");
    }

    [Fact]
    public async Task Cancel_before_persist_rejects_and_writes_no_applied_event()
    {
        var aggId = NewAggregateId();
        var cmdId = Guid.NewGuid();

        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, "payload-b", Cancelled: true), TestActor);

        var failure = await ExpectMsgAsync<Status.Failure>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.IsType<CommandCancelledException>(failure.Cause);

        // Replaying a fresh aggregate from the journal must show the command as NOT applied.
        // If a CommandApplied had been written, this retry would report AlreadyApplied.
        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, "payload-b", Cancelled: false), TestActor);
        var ack = await ExpectMsgAsync<CommandAcks.CommandAppliedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, ack.CommandId);
    }

    [Fact]
    public async Task Idempotent_resend_of_same_command_does_not_double_apply()
    {
        var aggId = NewAggregateId();
        var cmdId = Guid.NewGuid();

        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, "payload", Cancelled: false), TestActor);
        var first = await ExpectMsgAsync<CommandAcks.CommandAppliedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, first.CommandId);

        // Same command id again -> AlreadyApplied, no second CommandApplied.
        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, "payload", Cancelled: false), TestActor);
        var second = await ExpectMsgAsync<CommandAcks.AlreadyApplied>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, second.CommandId);
    }

    [Fact]
    public async Task Retract_reverts_applied_state_after_replay()
    {
        var aggId = NewAggregateId();
        var cmdId = Guid.NewGuid();

        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, "payload", Cancelled: false), TestActor);
        var ack = await ExpectMsgAsync<CommandAcks.CommandAppliedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, ack.CommandId);

        // Confirm the command is applied in live state.
        Router.Tell(new AggregateQueries.GetState(aggId), TestActor);
        var before = await ExpectMsgAsync<AggregateQueries.StateSnapshot>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Contains(cmdId, before.AppliedCommandIds);
        Assert.DoesNotContain(cmdId, before.RetractedCommandIds);

        // Retract via a compensating event.
        Router.Tell(new AggregateCommands.RetractCommand(cmdId, aggId), TestActor);
        var retracted = await ExpectMsgAsync<CommandAcks.RetractedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, retracted.CommandId);

        // Replay a fresh aggregate from the journal: the compensating event must fold
        // so the command is no longer applied and is recorded as retracted.
        var replay = Sys.ActorOf(Props.Create(() => new AggregateActor(aggId)), $"replay-{aggId:N}");
        replay.Tell(new AggregateQueries.GetState(aggId), TestActor);
        var after = await ExpectMsgAsync<AggregateQueries.StateSnapshot>(cancellationToken: TestContext.Current.CancellationToken);

        Assert.DoesNotContain(cmdId, after.AppliedCommandIds);
        Assert.Contains(cmdId, after.RetractedCommandIds);

        Sys.Stop(replay);
    }
}
