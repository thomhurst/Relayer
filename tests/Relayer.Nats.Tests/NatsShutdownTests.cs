using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Nats.Tests;

public sealed class NatsShutdownTests
{
    [Test]
    public async Task Host_stop_with_expired_token_closes_send_only_nats()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var stream = TestContext.Current!.Isolation.GetIsolatedName("shutdown");
        var subject = TestContext.Current!.Isolation.GetIsolatedName("output");
        await using var native = new NatsConnection(broker.Options());
        var jetStream = new NatsJSContext(native);
        await jetStream.CreateStreamAsync(new(stream, [subject]), token);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsShutdownJson.Default);
            relayer.Nats("nats", transport =>
            {
                transport.TopologyMode = TopologyMode.None;
                transport.Options.Configure(options => options.ConnectionOptions = broker.Options());
                transport.Send<Output>(subject);
            });
        });

        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var scope = host.Services.CreateAsyncScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new Output(42), cancellationToken: token);

        try
        {
            await host.StopAsync(new CancellationToken(true));
        }
        catch (OperationCanceledException)
        {
            // Cleanup must still happen when host stop reports cancellation.
        }

        await Assert.That(async () => await sender.SendAsync(new Output(43), cancellationToken: token))
            .Throws<InvalidOperationException>().WithMessageContaining("Start the NATS transport before sending.");
        await Assert.That((await jetStream.GetStreamAsync(stream, cancellationToken: token)).Info.State.Messages)
            .IsEqualTo(1L);
    }

    [Test]
    public async Task Concurrent_host_stop_keeps_send_only_nats_open_for_buffered_handler_sends()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var stream = TestContext.Current!.Isolation.GetIsolatedName("shutdown");
        var subject = TestContext.Current!.Isolation.GetIsolatedName("output");
        await using var native = new NatsConnection(broker.Options());
        var jetStream = new NatsJSContext(native);
        await jetStream.CreateStreamAsync(new(stream, [subject]), token);

        var gate = new StopGate();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.Configure<HostOptions>(options => options.ServicesStopConcurrently = true);
        builder.Services.AddSingleton(gate);
        // The host invokes the StopAsync methods in reverse order, so the gate releases after NATS enters StopAsync.
        builder.Services.AddSingleton<IHostedService>(gate);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsShutdownJson.Default);
            relayer.Nats("nats", transport =>
            {
                transport.TopologyMode = TopologyMode.None;
                transport.Options.Configure(options => options.ConnectionOptions = broker.Options());
                transport.Send<Output>(subject);
            });
            relayer.InMemory("input", transport =>
            {
                transport.Send<Input>("input");
                transport.Consume("input", consumer => consumer.Handler<Forward>());
            });
        });

        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        var transport = host.Services.GetRequiredKeyedService<NatsNativeTransport>("nats");
        await host.StartAsync(token);

        try
        {
            await host.Services.GetRequiredService<ISender>().SendAsync(new Input(42), cancellationToken: token);
            await gate.Buffered.Task.WaitAsync(token);
            await Assert.That((await jetStream.GetStreamAsync(stream, cancellationToken: token)).Info.State.Messages).IsEqualTo(0L);
        }
        finally
        {
            await host.StopAsync(token);
        }

        await Assert.That((await jetStream.GetStreamAsync(stream, cancellationToken: token)).Info.State.Messages).IsEqualTo(1L);
        await Assert.That(transport.Connection).IsNull();
    }

    [Message("nats.shutdown.input")]
    public sealed record Input(int Value);

    [Message("nats.shutdown.output")]
    public sealed record Output(int Value);

    public sealed class Forward(ISender sender, StopGate gate) : IHandler<Input>
    {
        public async ValueTask HandleAsync(Input message, MessageContext context, CancellationToken cancellationToken)
        {
            await sender.SendAsync(new Output(message.Value), cancellationToken: cancellationToken);
            gate.Buffered.TrySetResult();
            await gate.Stopping.Task.WaitAsync(cancellationToken);
        }
    }

    public sealed class StopGate : IHostedService
    {
        internal TaskCompletionSource Buffered
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Stopping
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Stopping.TrySetResult();
            return Task.CompletedTask;
        }
    }
}

[JsonSerializable(typeof(NatsShutdownTests.Input))]
[JsonSerializable(typeof(NatsShutdownTests.Output))]
internal partial class NatsShutdownJson : JsonSerializerContext;
