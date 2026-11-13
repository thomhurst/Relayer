using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Transports;

namespace Relayer.Tests;

public class TransportCapabilityInitializationTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Native_capabilities_finish_before_validation_and_route_composition_even_with_concurrent_startup(bool concurrent)
    {
        var transport = new InitializingTransport();
        using var host = new HostBuilder().ConfigureServices(services =>
        {
            services.Configure<HostOptions>(options => options.ServicesStartConcurrently = concurrent);
            services.AddSingleton<ITransport>(transport);
            services.AddRelayer(builder =>
            {
                builder.AddMessageTypesFromThisAssembly();
                builder.AddTransport<Options, Options>("native", native =>
                {
                    native.TopologyMode = TopologyMode.None;
                    native.Send<Sample>("orders", route => route.SendOptions = new() { RequiredDeliveryDelay = TimeSpan.FromMinutes(1) });
                });
            });
        }).Build();
        await using var lifetime = (IAsyncDisposable)host;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var starting = host.StartAsync(timeout.Token);
        await transport.Entered.Task.WaitAsync(timeout.Token);
        try
        {
            await Assert.That(starting.IsCompleted).IsFalse();
            await Assert.That(transport.Lookups).IsEqualTo(0);
        }
        finally { transport.Release.TrySetResult(); }
        await starting;
        await Assert.That(transport.Initializations).IsEqualTo(1);
        await Assert.That(transport.Lookups).IsEqualTo(1);
        await Assert.That(transport.Mode).IsEqualTo(TopologyMode.None);
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new Sample(), cancellationToken: timeout.Token);
        await host.StopAsync(timeout.Token);
    }

    public sealed class Options;
    [Message("capabilities.initialization.sample")]
    public sealed record Sample;

    private sealed class InitializingTransport : ITransport, ITransportCapabilityInitializer
    {
        internal readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Initializations;
        internal int Lookups;
        internal TopologyMode Mode;
        private bool _initialized;
        public string Name => "native";
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
            if (!_initialized) throw new InvalidOperationException("Native capability evidence is pending.");
            return new() { MaxDeliveryDelay = TimeSpan.FromHours(1) };
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token) => new(new TopologyReport([]));
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken token) => ValueTask.CompletedTask;
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token) => ValueTask.CompletedTask;
        public Task StartAsync(CancellationToken token) => Task.CompletedTask;
        public Task StopAsync(CancellationToken token) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
