using Akka.Actor;
using Akka.Hosting;
using Akka.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using CommandCancellationSample;
using CommandCancellationSample.Messages;

var builder = new HostBuilder();

builder.ConfigureServices((context, services) =>
{
    services.AddAkka("CommandCancellationSystem", (akkaBuilder, sp) =>
    {
        // In-memory persistence journal for the PoC - zero external DB.
        // Swappable to Akka.Persistence.Sqlite / EventStore by changing this config.
        akkaBuilder.WithActors((system, registry, resolver) =>
        {
            var router = system.ActorOf(Props.Create<RouterActor>(), "router");
            registry.Register<RouterActor>(router);
        });
    });
});

var host = builder.Build();
await host.StartAsync();

var actorSystem = host.Services.GetRequiredService<ActorSystem>();
var router = host.Services.GetRequiredService<IActorRegistry>().Get<RouterActor>();

Console.WriteLine("Akka.NET Command Cancellation & Compensating Events PoC");
Console.WriteLine("------------------------------------------------------");

var dispatcher = new Dispatcher(router, TimeSpan.FromSeconds(10));
var aggregateId = Guid.NewGuid();
const decimal appliedAmount = 100m;

// 1. Apply a command with an already-fired token -> rejected at the boundary (Lever 1).
using var cancelled = new CancellationTokenSource();
cancelled.Cancel();
try
{
    await dispatcher.ApplyAsync(aggregateId, appliedAmount, cancelled.Token);
}
catch (Exception ex)
{
    Console.WriteLine($"[1] Cancelled command rejected before persist: {ex.GetType().Name}");
}

// 2. Apply a valid command -> returns the persisted event sequence (Lever 2).
var applied = await dispatcher.ApplyAsync(aggregateId, appliedAmount, CancellationToken.None);
Console.WriteLine($"[2] Applied {appliedAmount} -> {applied}");

// 3. Demonstrate idempotency (Lever 3): send the SAME ApplyCommand message (same CommandId)
// to the router twice. The second send must return AlreadyApplied (no double-persist).
var cmdId = Guid.NewGuid();
var demoAggregateId = Guid.NewGuid();
try
{
    var raw = await router.Ask<object>(new AggregateCommands.ApplyCommand(cmdId, demoAggregateId, appliedAmount, Cancelled: false), TimeSpan.FromSeconds(10));
    Console.WriteLine($"[3] First send of {cmdId:N} -> {raw}");
    var dup = await router.Ask<object>(new AggregateCommands.ApplyCommand(cmdId, demoAggregateId, appliedAmount, Cancelled: false), TimeSpan.FromSeconds(10));
    Console.WriteLine($"[3] Re-send of {cmdId:N} -> {dup} (expected AlreadyApplied)");
}
catch (Exception ex)
{
    Console.WriteLine($"[3] Error during idempotency check: {ex.GetType().Name}");
}

// 4. Retract the applied command -> compensating event unwinds the amount (Lever 4).
try
{
    var before = await router.Ask<object>(new AggregateQueries.GetState(demoAggregateId), TimeSpan.FromSeconds(10));
    Console.WriteLine($"[4] Balance before retract: {before}");
    var retracted = await dispatcher.RetractAsync(demoAggregateId, cmdId, CancellationToken.None);
    Console.WriteLine($"[4] Retract -> {retracted}");
    var after = await router.Ask<object>(new AggregateQueries.GetState(demoAggregateId), TimeSpan.FromSeconds(10));
    Console.WriteLine($"[4] Balance after retract: {after} (compensating event unwound the amount)");
}
catch (Exception ex)
{
    Console.WriteLine($"[4] Error during retract: {ex.GetType().Name}");
}

Console.WriteLine("PoC complete. Stopping ActorSystem.");
await actorSystem.Terminate();
