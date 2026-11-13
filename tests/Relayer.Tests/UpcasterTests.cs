using Relayer.Pipeline;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Relayer.InMemory;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Tests;

public class UpcasterTests
{
    [Test]
    public async Task Single_and_chained_upcasts_deserialize_the_wire_contract_and_keep_metadata()
    {
        await using var provider = Provider();
        var pipeline = Builder(provider).Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<V3, Handler>()).Build(1);
        foreach (var version in new[] { 1, 2, 3 })
        {
            using var context = pipeline.RentContext(Message(provider, version));
            await pipeline.ProcessAsync(context);
        }
        var observed = provider.GetRequiredService<Observed>();
        await Assert.That(observed.Messages).IsEquivalentTo(new[] { new V3(42, "GBP"), new V3(42, "GBP"), new V3(42, "GBP") });
        await Assert.That(observed.Versions).IsEquivalentTo(new[] { 1, 2, 3 });
        await Assert.That(observed.DisposedScopes).IsEqualTo(3);
    }

    [Test]
    public async Task Native_receive_resolves_old_utf8_identities_and_unknown_versions_dead_letter()
    {
        await using var provider = Provider();
        var failures = new List<(Exception Error, FailureAction Action)>();
        var pipeline = Builder(provider, (_, error, action) => { failures.Add((error, action)); return default; })
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<V3, Handler>()).Build(1);
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new(), 1);
        var old = Message(provider, 1);
        await receive.ProcessAsync(new(old.Body, "upgrade;v=1"u8.ToArray()));
        await receive.ProcessAsync(new(old.Body, "upgrade;v=99"u8.ToArray()));
        await receive.ProcessAsync(new(old.Body, "unrecognized;v=1"u8.ToArray()));
        await receive.ProcessAsync(new(new("bad json"u8.ToArray()), "upgrade;v=1"u8.ToArray()));
        await receive.ProcessAsync(new(Message(provider, 3).Body, "upgrade;v=3"u8.ToArray()));
        await receive.ProcessAsync(new(new("null"u8.ToArray()), "upgrade;v=3"u8.ToArray()));
        await Assert.That(provider.GetRequiredService<Observed>().Messages.Count).IsEqualTo(2);
        await Assert.That(failures.Count).IsEqualTo(4);
        await Assert.That(failures.All(f => f.Action == FailureAction.DeadLetter)).IsTrue();
        await Assert.That(failures[0].Error).IsTypeOf<UnknownMessageTypeException>();
        await Assert.That(failures[0].Error.Message).Contains("upgrade;v=99");
        await Assert.That(failures[1].Error).IsTypeOf<UnknownMessageTypeException>();
        await Assert.That(failures[2].Error).IsTypeOf<MessageDeserializationException>();
        await Assert.That(failures[3].Error).IsTypeOf<MessageDeserializationException>();
    }

    [Test]
    public async Task Batch_accepts_mixed_versions_and_isolates_unknown_and_malformed_items()
    {
        await using var provider = Provider();
        var failures = new List<FailureAction>();
        var definition = provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
        var pipeline = Builder(provider, (_, _, action) => { failures.Add(action); return default; })
            .BuildBatch(provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<V3, BatchHandler>(), definition);
        using var first = pipeline.RentContext(Message(provider, 1));
        using var second = pipeline.RentContext(Message(provider, 2));
        using var third = pipeline.RentContext(Message(provider, 3));
        using var unknown = pipeline.RentContext(Message(provider, 3) with { MessageType = new("upgrade", 99) });
        using var invalid = pipeline.RentContext(Message(provider, 1) with { Body = new("invalid"u8.ToArray()) });
        await pipeline.ProcessBatchAsync(new[] { first, second, third, unknown, invalid });
        await Assert.That(provider.GetRequiredService<Observed>().Messages.Count).IsEqualTo(3);
        await Assert.That(failures).IsEquivalentTo(new[] { FailureAction.DeadLetter, FailureAction.DeadLetter });
    }

    [Test]
    public async Task Upcaster_errors_use_retry_classification_in_single_and_batch_consumers()
    {
        var error = new ApplicationException("upgrade failed");
        await using var provider = Provider(new ThrowingUpcaster(error));
        var failures = new List<(Exception Error, FailureAction Action)>();
        ConsumeFailureDelegate failure = (_, exception, action) => { failures.Add((exception, action)); return default; };
        var pipeline = Builder(provider, failure).Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<V3, Handler>()).Build();
        using (var context = pipeline.RentContext(Message(provider, 1))) await pipeline.ProcessAsync(context);
        var batch = Builder(provider, failure).BuildBatch(provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<V3, BatchHandler>(),
            provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0]);
        using (var context = batch.RentContext(Message(provider, 1))) await batch.ProcessBatchAsync(new[] { context });
        await Assert.That(failures.Count).IsEqualTo(2);
        await Assert.That(failures.All(f => ReferenceEquals(f.Error, error) && f.Action == FailureAction.Retry)).IsTrue();
    }

    [Test]
    public async Task Startup_rejects_missing_contracts_name_changes_and_non_increasing_versions()
    {
        await Assert.That(() => new ServiceCollection().AddRelayer(r => r.Infrastructure().Upcasters.Add(new FirstUpgrade())))
            .Throws<InvalidOperationException>();
        foreach (var identity in new[] { new MessageType("other", 2), new MessageType("upgrade", 1), new MessageType("upgrade", 2) })
        {
            await Assert.That(() => new ServiceCollection().AddRelayer(r =>
            {
                r.Infrastructure().MessageTypes.Add<V1>("upgrade", 2).Add<V2>(identity.Name, identity.Version);
                r.Infrastructure().Upcasters.Add(new FirstUpgrade());
            })).Throws<InvalidOperationException>();
        }
    }

    [Test]
    public async Task Registration_rejects_duplicate_sources_and_late_mutation()
    {
        var registry = new UpcasterRegistry().Add(new FirstUpgrade());
        await Assert.That(() => registry.Add(new FirstUpgrade())).Throws<InvalidOperationException>();
        await using var provider = Provider();
        await Assert.That(() => provider.GetRequiredService<UpcasterRegistry>().Add(new FirstUpgrade()))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Consumer_rejects_ambiguous_handlers_and_does_not_upgrade_to_an_unselected_handler()
    {
        await using var provider = Provider();
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        await Assert.That(() => Builder(provider).Handler(handlers.GetHandler<V1, OldHandler>())
            .Handler(handlers.GetHandler<V3, Handler>()).Build()).Throws<InvalidOperationException>();
        var failures = new List<Exception>();
        var pipeline = Builder(provider, (_, error, _) => { failures.Add(error); return default; })
            .Handler(handlers.GetHandler<V1, OldHandler>()).Build();
        using var unknown = pipeline.RentContext(Message(provider, 2));
        await pipeline.ProcessAsync(unknown);
        await Assert.That(failures.Single()).IsTypeOf<UnknownMessageTypeException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Durable_retry_repeats_the_original_upgrade_and_unknown_versions_have_native_dead_letter_reasons(bool batch)
    {
        var converter = new RetryUpgrade();
        await using var provider = Provider(converter);
        var failures = new FailurePipeline(provider, "native", "versions", "upgrade-consumer",
            new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 });
        var builder = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), provider.GetRequiredService<IMessageSerializer>(), failures);
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        var pipeline = batch
            ? builder.BuildBatch(handlers.GetBatchHandler<V3, BatchHandler>(), provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0])
            : builder.Handler(handlers.GetHandler<V3, Handler>()).Build();
        using (var context = pipeline.RentContext(Message(provider, 1)))
        {
            if (batch) await pipeline.ProcessBatchAsync(new[] { context });
            else await pipeline.ProcessAsync(context);
        }
        await Assert.That(converter.Inputs).IsEquivalentTo(new[] { new V1(42), new V1(42) });
        await Assert.That(provider.GetRequiredService<Observed>().Messages.Single()).IsEqualTo(new V3(42, "GBP"));
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new(), 1);
        var unknown = new Incoming(Message(provider, 1).Body, "upgrade;v=99"u8.ToArray());
        Disposition disposition;
        if (batch)
        {
            var dispositions = new Disposition[1];
            await receive.ProcessBatchAsync(new[] { unknown }, dispositions);
            disposition = dispositions[0];
        }
        else disposition = await receive.ProcessAsync(unknown);
        await Assert.That(disposition.Kind).IsEqualTo(DispositionKind.DeadLetter);
        await Assert.That(disposition.DeadLetter.Description).Contains("upgrade;v=99");
    }

    [Test]
    public async Task Incomplete_chains_do_not_guess_a_missing_upgrade()
    {
        var services = new ServiceCollection();
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            r.Infrastructure().Upcasters.Add(new FirstUpgrade());
            new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<Handler>();
        });
        await using var provider = services.BuildServiceProvider();
        var failures = new List<FailureAction>();
        var pipeline = Builder(provider, (_, _, action) => { failures.Add(action); return default; })
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<V3, Handler>()).Build();
        using var context = pipeline.RentContext(Message(provider, 1));
        await pipeline.ProcessAsync(context);
        await Assert.That(failures.Single()).IsEqualTo(FailureAction.DeadLetter);
    }

    internal static ServiceProvider Provider(IUpcaster<V1, V2>? first = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<Observed>();
        services.AddScoped<Scope>();
        services.AddSingleton<ITransport>(new InMemoryTransport("native", new Dictionary<string, InMemoryEndpointOptions>
        {
            ["versions"] = new(),
        }));
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            // Register in reverse order to prove composition follows types, not insertion order.
            r.Infrastructure().Upcasters.Add(new SecondUpgrade()).Add(first ?? new FirstUpgrade());
            new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<Handler>().Handler<OldHandler>();
            r.AddTransport<Options, Options>("test", t => t.Consume("versions", c => c.Handler<BatchHandler>().Batch(5, TimeSpan.FromSeconds(1))));
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static ConsumePipelineBuilder Builder(ServiceProvider provider, ConsumeFailureDelegate? failure = null)
        => new(provider.GetRequiredService<IServiceScopeFactory>(), provider.GetRequiredService<MessageTypeRegistry>(),
            provider.GetRequiredService<IMessageSerializer>(), failure ?? (static (_, error, _) => ValueTask.FromException(error)));

    private static ConsumeMessage Message(ServiceProvider provider, int version)
    {
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var buffer = new ArrayBufferWriter<byte>();
        switch (version)
        {
            case 1: serializer.Serialize(new V1(42), buffer); break;
            case 2: serializer.Serialize(new V2(42), buffer); break;
            default: serializer.Serialize(new V3(42, "GBP"), buffer); break;
        }
        return new() { MessageId = "original", MessageType = new("upgrade", version), Body = new(buffer.WrittenMemory) };
    }

    [Message("upgrade", Version = 1)] public readonly record struct V1(int Value);
    [Message("upgrade", Version = 2)] public readonly record struct V2(long Value);
    [Message("upgrade", Version = 3)] public readonly record struct V3(long Value, string Currency);
    public sealed class FirstUpgrade : IUpcaster<V1, V2> { public V2 Upcast(V1 message) => new(message.Value); }
    public sealed class SecondUpgrade : IUpcaster<V2, V3> { public V3 Upcast(V2 message) => new(message.Value, "GBP"); }
    private sealed class ThrowingUpcaster(Exception error) : IUpcaster<V1, V2> { public V2 Upcast(V1 message) => throw error; }
    private sealed class RetryUpgrade : IUpcaster<V1, V2>
    {
        public List<V1> Inputs { get; } = [];
        public V2 Upcast(V1 message)
        {
            Inputs.Add(message);
            if (Inputs.Count == 1) throw new InvalidOperationException("retry upgrade");
            return new(message.Value);
        }
    }
    public sealed class Options;
    public sealed class Observed
    {
        public List<V3> Messages { get; } = [];
        public List<int> Versions { get; } = [];
        public int DisposedScopes { get; set; }
    }
    public sealed class Scope(Observed observed) : IDisposable
    {
        public void Dispose() => observed.DisposedScopes++;
    }
    public sealed class Handler(Observed observed, Scope scope) : IHandler<V3>
    {
        public ValueTask HandleAsync(V3 message, MessageContext context, CancellationToken cancellationToken)
        {
            if (context.MessageId != "original" || !ReferenceEquals(context.Services.GetRequiredService<Scope>(), scope))
                throw new InvalidOperationException("Original metadata or scope was lost.");
            observed.Messages.Add(message);
            observed.Versions.Add(context.MessageType.Version);
            return default;
        }
    }
    public sealed class OldHandler : IHandler<V1>
    {
        public ValueTask HandleAsync(V1 message, MessageContext context, CancellationToken cancellationToken) => default;
    }
    public sealed class BatchHandler(Observed observed) : IBatchHandler<V3>
    {
        public ValueTask<BatchResult> HandleAsync(Batch<V3> batch, CancellationToken cancellationToken)
        {
            for (var i = 0; i < batch.Count; i++) observed.Messages.Add(batch.Items[i].Message);
            return new(BatchResult.Success);
        }
    }
    private sealed record Incoming(ReadOnlySequence<byte> Body, byte[] Type);
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => message.Body;
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static string? GetPartitionId(Incoming message) => null;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            value = name switch { "ce_type" => new(message.Type), "ce_id" => new("original"), _ => default };
            return !value.IsEmpty;
        }
    }
}
