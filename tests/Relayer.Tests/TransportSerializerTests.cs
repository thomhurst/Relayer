using System.Buffers;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.InMemory;
using Relayer.Pipeline;
using Relayer.Serialization;
using Relayer.Transports;

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
            builder.InMemory("default", _ =>
            {
            });
        });
        await using var provider = services.BuildServiceProvider();
        await Assert.That(provider.GetMessageSerializer("custom")).IsSameReferenceAs(custom);
        await Assert.That(provider.GetMessageSerializer("default")).IsSameReferenceAs(provider.GetRequiredService<IMessageSerializer>());

        foreach (var service in provider.GetServices<IHostedService>())
        {
            await service.StartAsync(default);
        }

        await Assert.That(custom.Initializations).IsEqualTo(1);

        foreach (var service in provider.GetServices<IHostedService>().Reverse())
        {
            await service.StopAsync(default);
        }
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
    [Arguments(false)]
    [Arguments(true)]
    public async Task Host_disposes_named_serializers_once_per_instance(bool shareInstance)
    {
        var shared = new DisposableSerializer();
        var independent = new DisposableSerializer();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.Infrastructure().MessageTypes.Add<NoJsonMetadata>("manual");
            relayer.InMemory("first", transport => transport.UseSerializer(_ => shared).Send<NoJsonMetadata>("first"));

            if (shareInstance)
            {
                relayer.InMemory("second", transport => transport.UseSerializer(_ => shared).Send<NoJsonMetadata>("second"));
            }

            relayer.InMemory("independent", transport => transport.UseSerializer(_ => independent).Send<NoJsonMetadata>("independent"));
        });

        var host = builder.Build();

        await using ((IAsyncDisposable)host)
        {
            await host.StartAsync();
            await Assert.That(host.Services.GetMessageSerializer("first")).IsSameReferenceAs(shared);
            await Assert.That(host.Services.GetMessageSerializer("independent")).IsSameReferenceAs(independent);

            if (shareInstance)
            {
                await Assert.That(host.Services.GetMessageSerializer("second")).IsSameReferenceAs(shared);
            }

            await Assert.That(shared.Initializations).IsEqualTo(1);
            await Assert.That(independent.Initializations).IsEqualTo(1);
            await host.StopAsync();
            await Assert.That(shared.Disposals).IsEqualTo(0);
            await Assert.That(independent.Disposals).IsEqualTo(0);
        }

        await Assert.That(shared.Disposals).IsEqualTo(1);
        await Assert.That(independent.Disposals).IsEqualTo(1);
    }

    [Test]
    public async Task Default_json_transport_accepts_missing_metadata_for_custom_only_contracts()
    {
        var custom = new PreparedSerializer();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.Infrastructure().MessageTypes.Add<NoJsonMetadata>("manual");
            relayer.AddMessageTypesFromThisAssembly();
            relayer.InMemory("custom", transport =>
            {
                transport.UseSerializer(_ => custom);
                transport.Send<NoJsonMetadata>("binary");
                transport.Consume("binary", consumer => consumer.Handler<ManualHandler>());
            });
            relayer.InMemory("default", transport => transport.Send<JsonValue>("json"));
        });
        using var host = builder.Build();
        await host.StartAsync();

        var serializer = host.Services.GetMessageSerializer("default");
        var buffer = new ArrayBufferWriter<byte>();
        serializer.Serialize(new JsonValue(42), buffer);
        await Assert.That(serializer.Deserialize<JsonValue>(new ReadOnlySequence<byte>(buffer.WrittenMemory)))
            .IsEqualTo(new JsonValue(42));
        await Assert.That(custom.Initializations).IsEqualTo(1);
        await host.StopAsync();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Default_json_transport_requires_metadata_for_shared_contracts(bool receiveOnly)
    {
        var custom = new PreparedSerializer();
        var probe = new StartupProbe(custom);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.Configure<HostOptions>(options => options.ServicesStartConcurrently = true);
        builder.Services.AddHostedService(_ => probe);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.Infrastructure().MessageTypes.Add<NoJsonMetadata>("manual");
            relayer.InMemory("custom", transport =>
            {
                transport.UseSerializer(_ => custom);
                transport.Send<NoJsonMetadata>("binary");
            });
            relayer.InMemory("default", transport =>
            {
                if (receiveOnly)
                {
                    transport.Consume("json", consumer => consumer.Handler<ManualHandler>());
                }
                else
                {
                    transport.Send<NoJsonMetadata>("json");
                }
            });
        });
        using var host = builder.Build();
        await Assert.That(() => host.StartAsync()).Throws<InvalidOperationException>()
            .WithMessageContaining(nameof(NoJsonMetadata));
        await Assert.That(probe.Started).IsFalse();
    }

    [Test]
    public async Task Default_json_consumer_requires_metadata_for_upcast_sources()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.Infrastructure().MessageTypes.Add<NoJsonMetadata>("versioned").Add<JsonValue>("versioned", 2);
            relayer.Infrastructure().Upcasters.Add(new ManualUpgrade());
            relayer.UseJsonSerializer(RelayerGeneratedJsonContext.Default);
            relayer.InMemory("default", transport => transport.Consume("json", consumer => consumer.Handler<JsonValueHandler>()));
        });
        using var host = builder.Build();
        await Assert.That(() => host.StartAsync()).Throws<InvalidOperationException>()
            .WithMessageContaining(nameof(NoJsonMetadata));
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Pipeline_callback_requires_json_metadata_for_upcast_sources(bool includeSourceMetadata)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            var infrastructure = relayer.Infrastructure();
            infrastructure.MessageTypes.Add<NoJsonMetadata>("versioned").Add<JsonValue>("versioned", 2)
                .Add<JsonOrder>("orders");
            infrastructure.Upcasters.Add(new ManualUpgrade());
            relayer.UseJsonSerializer(RelayerGeneratedJsonContext.Default);

            if (includeSourceMetadata)
            {
                relayer.UseJsonSerializer(UpcastSourceJsonContext.Default);
            }

            new ConsumerBuilder<object>(relayer.Services, infrastructure.Handlers, "manual").Handler<JsonValueHandler>();
            relayer.InMemory("default", transport => transport.Consume("json", consumer =>
            {
                consumer.Handler<JsonOrderHandler>();
                consumer.ConfigurePipeline(pipeline => pipeline.Handler(infrastructure.Handlers.GetHandler<JsonValue, JsonValueHandler>()));
            }));
        });

        using var host = builder.Build();

        if (includeSourceMetadata)
        {
            await host.StartAsync();
            await host.StopAsync();
        }
        else
        {
            await Assert.That(() => host.StartAsync()).Throws<InvalidOperationException>()
                .WithMessageContaining(nameof(NoJsonMetadata));
        }
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

    public sealed record NoJsonMetadata(int Number);

    public sealed class ManualHandler : IHandler<NoJsonMetadata>
    {
        public ValueTask HandleAsync(NoJsonMetadata message, MessageContext context, CancellationToken cancellationToken)
        {
            return default;
        }
    }

    public sealed class JsonValueHandler : IHandler<JsonValue>
    {
        public ValueTask HandleAsync(JsonValue message, MessageContext context, CancellationToken cancellationToken)
        {
            return default;
        }
    }

    public sealed class JsonOrderHandler : IHandler<JsonOrder>
    {
        public ValueTask HandleAsync(JsonOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            return default;
        }
    }

    private sealed class ManualUpgrade : IUpcaster<NoJsonMetadata, JsonValue>
    {
        public JsonValue Upcast(NoJsonMetadata message)
        {
            return new JsonValue(message.Number);
        }
    }

    private class PreparedSerializer : IMessageSerializer, IMessageSerializerInitializer
    {
        public int Initializations;
        public bool Fail;

        public string ContentType => "application/test";

        public async ValueTask InitializeAsync(CancellationToken cancellationToken)
        {
            await Task.Yield();

            if (Fail)
            {
                throw new InvalidOperationException("registry unavailable");
            }

            Interlocked.Increment(ref Initializations);
        }

        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        {
            throw new NotSupportedException();
        }

        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source)
        {
            throw new NotSupportedException();
        }
    }

    private sealed class DisposableSerializer : PreparedSerializer, IAsyncDisposable
    {
        public int Disposals;

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return ValueTask.CompletedTask;
        }
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

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}

[JsonSerializable(typeof(TransportSerializerTests.NoJsonMetadata))]
internal partial class UpcastSourceJsonContext : JsonSerializerContext;
