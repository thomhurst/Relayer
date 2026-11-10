using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Serialization;

namespace Relayer.Nats.Tests;

public class NatsExecutionTests
{
    [Test]
    public async Task Hosted_pipeline_retries_and_confirms_poison_copy_before_acknowledging_original()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var events = TestContext.Current!.Isolation.GetIsolatedName("events");
        var orders = TestContext.Current!.Isolation.GetIsolatedName("orders");
        var deadSubject = TestContext.Current!.Isolation.GetIsolatedName("dead");
        var control = new ExecutionControl();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(control);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Handler<ExecutionHandler>();
            relayer.Nats("execution", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    options.Streams.Add(new(events) { Configuration = new(events, [orders, deadSubject]) });
                    options.Consumers.Add(new(events, "worker") { Configuration = new("worker") { FilterSubject = orders } });
                });
                transport.Send<ExecutionMessage>(orders);
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = events;
                    consumer.Options.Settings = new() { MaxMsgs = 1, Expires = TimeSpan.FromSeconds(1), DrainOnCancel = true };
                    consumer.Options.Failure = new() { RetryDelays = new[] { TimeSpan.Zero }, DeadLetterEndpoint = deadSubject };
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateConsumerAsync(events, new("dead_reader") { FilterSubject = deadSubject }, token);
        var poison = new byte[] { 1, 2, 3 };
        var poisonHeaders = new NatsPublishHeaders(false)
        {
            ["original"] = "preserved",
            ["Nats-Msg-Id"] = "original-poison-id",
            TraceParent = "00-11111111111111111111111111111111-2222222222222222-01",
        };
        var ack = await js.PublishAsync(orders, new ReadOnlyMemory<byte>(poison), poisonHeaders,
            headers: poisonHeaders, cancellationToken: token);
        ack.EnsureSuccess();
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new ExecutionMessage(42),
            options => options.Nats(headers => headers["custom"] = "native"), token);
        await control.Completed.Task.WaitAsync(token);
        var dead = await js.GetConsumerAsync(events, "dead_reader", token);
        var copy = await dead.NextAsync(NatsRawSerializer<byte[]>.Default, cancellationToken: token);
        await Assert.That(copy).IsNotNull();
        await Assert.That(copy!.Data).IsEquivalentTo(poison);
        await Assert.That(copy.Headers!["original"].ToString()).IsEqualTo("preserved");
        await Assert.That(copy.Headers.ContainsKey("Nats-Msg-Id")).IsFalse();
        await Assert.That(copy.Headers["traceparent"].ToString()).IsEqualTo("00-11111111111111111111111111111111-2222222222222222-01");
        await copy.AckAsync(new() { DoubleAck = true }, token);
        await host.StopAsync(token);
        var settled = await js.GetConsumerAsync(events, "worker", token);
        await Assert.That(settled.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(settled.Info.AckFloor.ConsumerSeq).IsEqualTo(2UL);
        await Assert.That(control.Attempts).IsEqualTo(2);
    }
    [Test]
    public async Task Native_failure_stops_sibling_receivers_and_cleanup_closes_the_owned_connection()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var events = TestContext.Current!.Isolation.GetIsolatedName("events");
        var orders = TestContext.Current!.Isolation.GetIsolatedName("orders");
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.Nats("failure", transport =>
        {
            transport.Options.Configure(options =>
            {
                options.ConnectionOptions = broker.Options();
                options.Streams.Add(new(events) { Configuration = new(events, [orders]) });
                options.Consumers.Add(new(events, "worker") { Configuration = new("worker") });
            });
            transport.Send<ExecutionMessage>(orders);
        }));
        await using var provider = services.BuildServiceProvider();
        var adapter = (NatsNativeTransport)provider.GetRequiredService<Relayer.Transports.ITransport>();
        await adapter.ApplyTopologyAsync(TopologyMode.Provision, token);
        NatsConnection? ownedConnection = null;
        var worker = adapter.AddConsumer(events, "worker", new() { MaxMsgs = 1, DrainOnCancel = true, Expires = TimeSpan.FromSeconds(1) }, 2);
        worker.Receive = (delivery, _) =>
        {
            ownedConnection = (NatsConnection)delivery.Message.Connection!;
            return ValueTask.FromException<Disposition>(new InvalidOperationException("injected native callback failure"));
        };
        await adapter.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        var ack = await js.PublishAsync(orders, new byte[] { 42 }, NatsRawSerializer<byte[]>.Default, cancellationToken: token);
        ack.EnsureSuccess();
        await Assert.That(async () => await worker.Completion.WaitAsync(token)).Throws<InvalidOperationException>();
        await Assert.That(async () => await adapter.DisposeAsync()).Throws<InvalidOperationException>();
        await Assert.That(ownedConnection).IsNotNull();
        await Assert.That(async () => await ownedConnection!.PingAsync(token)).Throws<ObjectDisposedException>();
        await native.PingAsync(token);
    }

}

[Message("nats.execution")]
public sealed record ExecutionMessage(int Value);
public sealed class ExecutionControl
{
    public int Attempts;
    public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}
public sealed class ExecutionHandler(ExecutionControl control) : IHandler<ExecutionMessage>
{
    public ValueTask HandleAsync(ExecutionMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        var native = context.Nats();
        if (message.Value != 42 || native.Message.Headers!["custom"].ToString() != "native")
            throw new InvalidOperationException("Native payload/header mapping changed.");
        if (++control.Attempts == 1) throw new InvalidOperationException("Expected in-process retry.");
        control.Completed.TrySetResult();
        return default;
    }
}
[JsonSerializable(typeof(ExecutionMessage))]
internal partial class NatsExecutionJson : JsonSerializerContext;
