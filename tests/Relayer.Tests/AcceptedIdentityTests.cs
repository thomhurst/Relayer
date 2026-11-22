using System.Buffers;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.InMemory;
using Relayer.Pipeline;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Tests;

public class AcceptedIdentityTests
{
    [Test]
    public async Task Accepted_identities_dispatch_to_the_handler_and_report_the_name_that_arrived()
    {
        await using var provider = Provider();
        var pipeline = Builder(provider).Accepts<RsvpChanged>(new MessageType("rsvp.updated", 1), new MessageType("rsvp.withdrawn", 2))
            .Handler(Handler(provider)).Build(1);

        foreach (var identity in new[] { new MessageType("rsvp.changed", 1), new("rsvp.updated", 1), new("rsvp.withdrawn", 2) })
        {
            using var context = pipeline.RentContext(Message(provider, identity));
            await pipeline.ProcessAsync(context);
        }

        var observed = provider.GetRequiredService<Channel<(MessageType, RsvpChanged)>>().Reader;
        var received = new List<(MessageType, RsvpChanged)>();

        while (observed.TryRead(out var item))
        {
            received.Add(item);
        }

        await Assert.That(received).IsEquivalentTo(new[]
        {
            (new MessageType("rsvp.changed", 1), new RsvpChanged("guest")),
            (new MessageType("rsvp.updated", 1), new RsvpChanged("guest")),
            (new MessageType("rsvp.withdrawn", 2), new RsvpChanged("guest")),
        });
    }

    [Test]
    public async Task Native_receive_resolves_accepted_utf8_identities_and_other_names_stay_unknown()
    {
        await using var provider = Provider();
        var failures = new List<Exception>();
        var pipeline = Builder(provider, (_, error, _) =>
        {
            failures.Add(error);
            return default;
        })
            .Accepts<RsvpChanged>(new MessageType("rsvp.updated", 1)).Handler(Handler(provider)).Build(1);
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new(), 1);
        var body = Message(provider, default).Body;
        await receive.ProcessAsync(new(body, "rsvp.updated;v=1"u8.ToArray()));
        await receive.ProcessAsync(new(body, "rsvp.updated;v=2"u8.ToArray()));
        await Assert.That(provider.GetRequiredService<Channel<(MessageType, RsvpChanged)>>().Reader.Count).IsEqualTo(1);
        await Assert.That(failures.Single()).IsTypeOf<UnknownMessageTypeException>();
    }

    [Test]
    public async Task An_accepted_identity_may_belong_to_a_type_the_consumer_does_not_handle()
    {
        await using var provider = Provider();
        var pipeline = Builder(provider).Accepts<RsvpChanged>(new MessageType("rsvp.recorded", 1)).Handler(Handler(provider)).Build(1);
        using var context = pipeline.RentContext(Message(provider, new("rsvp.recorded", 1)));
        await pipeline.ProcessAsync(context);
        await Assert.That(provider.GetRequiredService<Channel<(MessageType, RsvpChanged)>>().Reader.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Build_rejects_accepted_identities_without_a_handler_or_already_handled()
    {
        await using var provider = Provider();
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        await Assert.That(() => Builder(provider).Accepts<Recorded>(new MessageType("rsvp.updated", 1)).Handler(Handler(provider)).Build())
            .Throws<InvalidOperationException>().WithMessageContaining("declares no handler");
        await Assert.That(() => Builder(provider).Accepts<RsvpChanged>(new MessageType("rsvp.changed", 1)).Handler(Handler(provider)).Build())
            .Throws<InvalidOperationException>().WithMessageContaining("already handles 'rsvp.changed;v=1'");
        await Assert.That(() => Builder(provider).Accepts<RsvpChanged>(new MessageType("rsvp.recorded", 1)).Handler(Handler(provider))
            .Handler(handlers.GetHandler<Recorded, RecordedHandler>()).Build())
            .Throws<InvalidOperationException>().WithMessageContaining("already handles 'rsvp.recorded;v=1'");
        await Assert.That(() => Builder(provider).Accepts<RsvpChanged>()).Throws<ArgumentException>();
        await Assert.That(() => Builder(provider).Accepts<RsvpChanged>(new MessageType("rsvp.updated", 0))).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Batch_consumers_accept_extra_identities()
    {
        await using var provider = Provider();
        var definition = provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
        var failures = new List<Exception>();
        var pipeline = Builder(provider, (_, error, _) =>
        {
            failures.Add(error);
            return default;
        })
            .Accepts<RsvpChanged>(new MessageType("rsvp.updated", 1))
            .BuildBatch(provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<RsvpChanged, BatchHandler>(), definition);
        using var primary = pipeline.RentContext(Message(provider, new("rsvp.changed", 1)));
        using var accepted = pipeline.RentContext(Message(provider, new("rsvp.updated", 1)));
        using var unknown = pipeline.RentContext(Message(provider, new("rsvp.withdrawn", 1)));
        await pipeline.ProcessBatchAsync(new[] { primary, accepted, unknown });
        await Assert.That(provider.GetRequiredService<Channel<(MessageType, RsvpChanged)>>().Reader.Count).IsEqualTo(2);
        await Assert.That(failures.Single()).IsTypeOf<UnknownMessageTypeException>();
    }

    [Test]
    public async Task One_host_publishes_two_types_that_one_handler_consumes_by_name()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Channel.CreateUnbounded<(MessageType, RsvpChanged)>());
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.InMemory("local", transport =>
            {
                transport.Send<Recorded>("rsvps");
                transport.Send<Updated>("rsvps");
                transport.Consume("rsvps", consumer => consumer.Handler<Changes>().Accepts<RsvpChanged>("rsvp.recorded", "rsvp.updated"));
            });
        });
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToArray();

        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>())
        {
            await lifecycle.StartingAsync(default);
        }

        foreach (var service in hosted)
        {
            await service.StartAsync(default);
        }

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendAsync(new Recorded("ada"));
            await sender.SendAsync(new Updated("grace"));
            var reader = provider.GetRequiredService<Channel<(MessageType, RsvpChanged)>>().Reader;
            var received = new HashSet<(MessageType, RsvpChanged)>();

            for (var i = 0; i < 2; i++)
            {
                received.Add(await reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)));
            }

            await Assert.That(received).IsEquivalentTo(new[]
            {
                (new MessageType("rsvp.recorded", 1), new RsvpChanged("ada")),
                (new MessageType("rsvp.updated", 1), new RsvpChanged("grace")),
            });
        }
        finally
        {
            foreach (var service in hosted.Reverse())
            {
                await service.StopAsync(default);
            }
        }
    }

    private static ServiceProvider Provider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Channel.CreateUnbounded<(MessageType, RsvpChanged)>());
        services.AddSingleton<ITransport>(new InMemoryTransport("native", new Dictionary<string, InMemoryEndpointOptions> { ["rsvps"] = new() }));
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<Changes>().Handler<RecordedHandler>();
            r.AddTransport<Options, Options>("test", t => t.Consume("rsvps", c => c.Handler<BatchHandler>().Batch(5, TimeSpan.FromSeconds(1))));
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static HandlerDescriptor Handler(ServiceProvider provider)
    {
        return provider.GetRequiredService<HandlerRegistry>().GetHandler<RsvpChanged, Changes>();
    }

    private static ConsumePipelineBuilder Builder(ServiceProvider provider, ConsumeFailureDelegate? failure = null)
    {
        return new(provider.GetRequiredService<IServiceScopeFactory>(), provider.GetRequiredService<MessageTypeRegistry>(),
                    provider.GetRequiredService<IMessageSerializer>(), failure ?? (static (_, error, _) => ValueTask.FromException(error)));
    }

    private static ConsumeMessage Message(ServiceProvider provider, MessageType identity)
    {
        var buffer = new ArrayBufferWriter<byte>();
        provider.GetRequiredService<IMessageSerializer>().Serialize(new RsvpChanged("guest"), buffer);
        return new()
        {
            MessageId = "rsvp",
            MessageType = identity,
            Body = new(buffer.WrittenMemory)
        };
    }

    [Message("rsvp.changed")]
    public sealed record RsvpChanged(string Guest);

    [Message("rsvp.recorded")]
    public sealed record Recorded(string Guest);

    [Message("rsvp.updated")]
    public sealed record Updated(string Guest);

    public sealed class Options;

    public sealed class Changes(Channel<(MessageType, RsvpChanged)> observed) : IHandler<RsvpChanged>
    {
        public ValueTask HandleAsync(RsvpChanged message, MessageContext context, CancellationToken cancellationToken)
        {
            return observed.Writer.WriteAsync((context.MessageType, message), cancellationToken);
        }
    }

    public sealed class RecordedHandler : IHandler<Recorded>
    {
        public ValueTask HandleAsync(Recorded message, MessageContext context, CancellationToken cancellationToken)
        {
            return default;
        }
    }

    public sealed class BatchHandler(Channel<(MessageType, RsvpChanged)> observed) : IBatchHandler<RsvpChanged>
    {
        public ValueTask<BatchResult> HandleAsync(Batch<RsvpChanged> batch, CancellationToken cancellationToken)
        {
            for (var i = 0; i < batch.Count; i++)
            {
                observed.Writer.TryWrite((batch.Items[i].Context.MessageType, batch.Items[i].Message));
            }

            return new(BatchResult.Success);
        }
    }

    private sealed record Incoming(ReadOnlySequence<byte> Body, byte[] Type);

    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message)
        {
            return message.Body;
        }

        public static int? GetDeliveryCount(Incoming message)
        {
            return 1;
        }

        public static string? GetPartitionId(Incoming message)
        {
            return null;
        }

        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            value = name switch
            {
                "ce_type" => new(message.Type),
                "ce_id" => new("rsvp"),
                _ => default
            };
            return !value.IsEmpty;
        }
    }
}
