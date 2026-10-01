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

    private static decimal Amount => 100m;

    [Fact]
    public async Task Apply_command_persists_and_returns_the_sequence_number()
    {
        var aggId = NewAggregateId();
        var cmdId = Guid.NewGuid();

        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, Amount, Cancelled: false), TestActor);

        var ack = await ExpectMsgAsync<CommandAcks.CommandAppliedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, ack.CommandId);
        Assert.Equal(Amount, ack.Amount);
        Assert.True(ack.SequenceNr >= 1, "Persisted event should get a sequence number (Lever 2).");
    }

    [Fact]
    public async Task Cancel_before_persist_rejects_and_writes_no_applied_event()
    {
        var aggId = NewAggregateId();
        var cmdId = Guid.NewGuid();

        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, Amount, Cancelled: true), TestActor);

        var failure = await ExpectMsgAsync<Status.Failure>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.IsType<CommandCancelledException>(failure.Cause);

        // Replaying a fresh aggregate from the journal must show the command as NOT applied.
        // If a CommandApplied had been written, this retry would report AlreadyApplied.
        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, Amount, Cancelled: false), TestActor);
        var ack = await ExpectMsgAsync<CommandAcks.CommandAppliedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, ack.CommandId);
    }

    [Fact]
    public async Task Idempotent_resend_of_same_command_does_not_double_apply()
    {
        var aggId = NewAggregateId();
        var cmdId = Guid.NewGuid();

        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, Amount, Cancelled: false), TestActor);
        var first = await ExpectMsgAsync<CommandAcks.CommandAppliedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, first.CommandId);
        Assert.Equal(Amount, first.Amount);

        // Same command id again -> AlreadyApplied, no second CommandApplied.
        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, Amount, Cancelled: false), TestActor);
        var second = await ExpectMsgAsync<CommandAcks.AlreadyApplied>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, second.CommandId);
    }

    [Fact]
    public async Task Retract_reverts_balance_to_original_value_after_replay()
    {
        var aggId = NewAggregateId();
        var cmdId = Guid.NewGuid();

        // Start at a non-zero opening balance so the reversal is non-trivial.
        Router.Tell(new AggregateCommands.ApplyCommand(Guid.NewGuid(), aggId, 500m, Cancelled: false), TestActor);
        await ExpectMsgAsync<CommandAcks.CommandAppliedAck>(cancellationToken: TestContext.Current.CancellationToken);

        // Apply a second command that should later be exclusively retracted.
        Router.Tell(new AggregateCommands.ApplyCommand(cmdId, aggId, Amount, Cancelled: false), TestActor);
        var ack = await ExpectMsgAsync<CommandAcks.CommandAppliedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, ack.CommandId);

        // Confirm the balance reflects both commands before the retract.
        Router.Tell(new AggregateQueries.GetState(aggId), TestActor);
        var before = await ExpectMsgAsync<AggregateQueries.StateSnapshot>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(600m, before.Balance);
        Assert.Contains(cmdId, before.AppliedAmounts.Keys);

        // Retract ONLY the second command via a compensating event.
        Router.Tell(new AggregateCommands.RetractCommand(cmdId, aggId), TestActor);
        var retracted = await ExpectMsgAsync<CommandAcks.RetractedAck>(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(cmdId, retracted.CommandId);
        Assert.Equal(Amount, retracted.Amount);

        // Replay a fresh aggregate from the journal: the compensating event must fold
        // so the balance returns to its pre-retract value and the command is recorded retracted.
        var replay = Sys.ActorOf(Props.Create(() => new AggregateActor(aggId)), $"replay-{aggId:N}");
        replay.Tell(new AggregateQueries.GetState(aggId), TestActor);
        var after = await ExpectMsgAsync<AggregateQueries.StateSnapshot>(cancellationToken: TestContext.Current.CancellationToken);

        // The domain state actually unwound: 600 - 100 = 500, back to the opening balance.
        Assert.Equal(500m, after.Balance);
        Assert.DoesNotContain(cmdId, after.AppliedAmounts.Keys);
        Assert.Contains(cmdId, after.RetractedCommandIds);

        Sys.Stop(replay);
    }
}
