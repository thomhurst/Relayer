using Relayer.Transports;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.InMemory;
using Relayer.Serialization;

namespace Relayer.Tests;

public class TransportSerializerTests
{
    [Test]
    public async Task Named_serializer_isolated_from_fallback_and_duplicate_selection_rejected()
    {
        var custom = new PreparedSerializer();
        var services = new ServiceCollection().AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("custom", transport =>
            {
                transport.UseSerializer(_ => custom);
                Assert.Throws<InvalidOperationException>(() => transport.UseSerializer(_ => custom));
            });
            builder.InMemory("default", _ => { });
        });
        await using var provider = services.BuildServiceProvider();
        await Assert.That(provider.GetMessageSerializer("custom")).IsSameReferenceAs(custom);
        await Assert.That(provider.GetMessageSerializer("default")).IsSameReferenceAs(provider.GetRequiredService<IMessageSerializer>());
        foreach (var service in provider.GetServices<IHostedService>())
            await service.StartAsync(default);
        await Assert.That(custom.Initializations).IsEqualTo(1);
        foreach (var service in provider.GetServices<IHostedService>().Reverse())
            await service.StopAsync(default);
    }

    [Test]
    public async Task All_named_serializers_skip_unused_json_metadata_and_prepare_before_concurrent_service_start()
    {
        var custom = new PreparedSerializer();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.Configure<HostOptions>(options => options.ServicesStartConcurrently = true);
        builder.Services.AddHostedService(_ => new StartupProbe(custom));
        builder.Services.AddRelayer(relayer =>
        {
            relayer.Infrastructure().MessageTypes.Add<NoJsonMetadata>("manual");
            relayer.InMemory("first", transport => transport.UseSerializer(_ => custom));
            relayer.InMemory("second", transport => transport.UseSerializer(_ => custom));
        });
        using var host = builder.Build();
        await host.StartAsync();
        await Assert.That(custom.Initializations).IsEqualTo(1);
        await host.StopAsync();
    }

    [Test]
    public async Task Preparation_failure_prevents_any_receiver_start()
    {
        var serializer = new PreparedSerializer { Fail = true };
        var probe = new StartupProbe(serializer);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddHostedService(_ => probe);
        builder.Services.AddRelayer(relayer => relayer.InMemory("broken", transport =>
            transport.UseSerializer(_ => serializer)));
        using var host = builder.Build();
        await Assert.That(async () => await host.StartAsync()).Throws<InvalidOperationException>().WithMessageContaining("registry unavailable");
        await Assert.That(probe.Started).IsFalse();
    }

    private sealed record NoJsonMetadata(int Number);

    private sealed class PreparedSerializer : IMessageSerializer, IMessageSerializerInitializer
    {
        public int Initializations;
        public bool Fail;
        public string ContentType => "application/test";
        public async ValueTask InitializeAsync(CancellationToken cancellationToken)
        {
            await Task.Yield();
            if (Fail) throw new InvalidOperationException("registry unavailable");
            Interlocked.Increment(ref Initializations);
        }
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }

    private sealed class StartupProbe(PreparedSerializer serializer) : IHostedService
    {
        public bool Started;
        public Task StartAsync(CancellationToken cancellationToken)
        {
            Started = true;
            return serializer.Initializations == 1 ? Task.CompletedTask
                : throw new InvalidOperationException("Serializer was not prepared before service startup.");
        }
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
