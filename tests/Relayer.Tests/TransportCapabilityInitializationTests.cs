using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Transports;

namespace Relayer.Tests;

public class TransportCapabilityInitializationTests
{
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Sender_resolved_before_startup_uses_initialized_capabilities_for_selected_destination(bool concurrent, bool sendBeforeStartup)
    {
        var transport = new InitializingTransport { ReportConservativeCapabilities = true };
        using var host = CreateHost(transport, concurrent, native =>
            native.Send<Notice>(["orders", "billing"], static notice => notice.Destination));
        await using var lifetime = (IAsyncDisposable)host;
        using var scope = host.Services.CreateScope();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();

        await Assert.That(transport.Lookups).IsEqualTo(0);

        if (sendBeforeStartup)
        {
            await sender.SendAsync(new Notice("orders"), timeout.Token);
            await Assert.That(transport.LastDestination).IsEqualTo("orders");
        }

        transport.Release.TrySetResult();
        await host.StartAsync(timeout.Token);

        try
        {
            await sender.SendAsync(new Notice("billing"), options => options.OrderingKey = "west", timeout.Token);
            await Assert.That(transport.LastDestination).IsEqualTo("billing");
            await Assert.That(transport.LastOrderingKey).IsEqualTo("west");
            await Assert.That(transport.Lookups).IsEqualTo(sendBeforeStartup ? 4 : 2);
        }
        finally
        {
            await host.StopAsync(timeout.Token);
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Native_capabilities_finish_before_validation_and_route_composition_even_with_concurrent_startup(bool concurrent)
    {
        var transport = new InitializingTransport();
        using var host = CreateHost(transport, concurrent);
        await using var lifetime = (IAsyncDisposable)host;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var starting = host.StartAsync(timeout.Token);
        await transport.Entered.Task.WaitAsync(timeout.Token);

        try
        {
            await Assert.That(starting.IsCompleted).IsFalse();
            await Assert.That(transport.Lookups).IsEqualTo(0);
        }
        finally
        {
            transport.Release.TrySetResult();
        }

        await starting;
        await Assert.That(transport.Initializations).IsEqualTo(1);
        await Assert.That(transport.Lookups).IsEqualTo(1);
        await Assert.That(transport.Mode).IsEqualTo(TopologyMode.None);
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendWithoutOutboxAsync(new Sample(), cancellationToken: timeout.Token);
        await host.StopAsync(timeout.Token);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task The_same_host_can_retry_after_capability_initialization_is_cancelled(bool concurrent)
    {
        var transport = new InitializingTransport();
        using var host = CreateHost(transport, concurrent);
        await using var lifetime = (IAsyncDisposable)host;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var cancelStart = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var starting = host.StartAsync(cancelStart.Token);
        await transport.Entered.Task.WaitAsync(timeout.Token);
        await cancelStart.CancelAsync();
        await Assert.That(async () => await starting.WaitAsync(timeout.Token)).Throws<OperationCanceledException>();
        await Assert.That(transport.Initializations).IsEqualTo(1);
        await Assert.That(transport.Lookups).IsEqualTo(0);

        transport.Release.TrySetResult();
        await host.StartAsync(timeout.Token);
        await Assert.That(transport.Initializations).IsEqualTo(2);
        await Assert.That(transport.Lookups).IsEqualTo(1);
        await host.StopAsync(timeout.Token);
    }

    private static IHost CreateHost(InitializingTransport transport, bool concurrent,
        Action<TransportBuilder<Options, Options>>? configure = null)
    {
        return new HostBuilder().ConfigureServices(services =>
        {
            services.Configure<HostOptions>(options => options.ServicesStartConcurrently = concurrent);
            services.AddSingleton<ITransport>(transport);
            services.AddRelayer(builder =>
            {
                builder.AddMessageTypesFromThisAssembly();
                builder.AddTransport<Options, Options>("native", native =>
                {
                    native.TopologyMode = TopologyMode.None;

                    if (configure is null)
                    {
                        native.Send<Sample>("orders", route => route.SendOptions = new() { RequiredDeliveryDelay = TimeSpan.FromMinutes(1) });
                    }
                    else
                    {
                        configure(native);
                    }
                });
            });
        }).Build();
    }

    public sealed class Options;

    [Message("capabilities.initialization.sample")]
    public sealed record Sample;

    [Message("capabilities.initialization.notice")]
    public sealed record Notice(string Destination);

    private sealed class InitializingTransport : ITransport, ITransportCapabilityInitializer
    {
        internal readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Initializations;
        internal int Lookups;
        internal TopologyMode Mode;
        private bool _initialized;

        public string Name => "native";

        internal bool ReportConservativeCapabilities
        {
            get; init;
        }

        internal string? LastDestination
        {
            get; private set;
        }

        internal string? LastOrderingKey
        {
            get; private set;
        }

        public async ValueTask InitializeCapabilitiesAsync(TopologyMode mode, CancellationToken token)
        {
            Initializations++;
            Mode = mode;
            Entered.TrySetResult();
            await Release.Task.WaitAsync(token);
            _initialized = true;
        }

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            Lookups++;

            if (!_initialized)
            {
                if (ReportConservativeCapabilities)
                {
                    return new();
                }

                throw new InvalidOperationException("Native capability evidence is pending.");
            }

            return new()
            {
                MaxDeliveryDelay = TimeSpan.FromHours(1),
                OrderedByKey = true
            };
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token)
        {
            return new(new TopologyReport([]));
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken token)
        {
            LastDestination = message.Destination;
            LastOrderingKey = message.OrderingKey.ToString();
            return ValueTask.CompletedTask;
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token)
        {
            return ValueTask.CompletedTask;
        }

        public Task StartAsync(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
