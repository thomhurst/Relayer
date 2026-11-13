using Relayer.Transports;
using Relayer.Pipeline;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.Serialization;

internal static class UpcasterSmoke
{
    internal static async Task RunAsync()
    {
        var services = new ServiceCollection();
        services.AddSingleton<UpgradeHandler>();
        services.AddSingleton<UpgradeBatchHandler>();
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            r.Infrastructure().Upcasters.Add(new UpgradeFirst()).Add(new UpgradeSecond());
            r.AddTransport<object, object>("manual-1", transport => transport.Consume("source", consumer => consumer.Handler<UpgradeHandler>()));
            r.AddTransport<UpgradeOptions, UpgradeOptions>("upgrades", t =>
                t.Consume("versions", c => c.Handler<UpgradeBatchHandler>().Batch(2, TimeSpan.FromSeconds(1))));
        });
        await using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var body = new ArrayBufferWriter<byte>();
        serializer.Serialize(new UpgradeV1(42), body);
        ConsumePipelineBuilder Builder() => new(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), serializer, static (_, error, _) => ValueTask.FromException(error));
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        var single = Builder().Handler(handlers.GetHandler<UpgradeV3, UpgradeHandler>()).Build(1);
        var message = new ConsumeMessage { MessageId = "old", MessageType = new("smoke.upgrade", 1), Body = new(body.WrittenMemory) };
        using (var context = single.RentContext(message)) await single.ProcessAsync(context);
        var batch = Builder().BuildBatch(handlers.GetBatchHandler<UpgradeV3, UpgradeBatchHandler>(),
            provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0]);
        using (var context = batch.RentContext(message)) await batch.ProcessBatchAsync(new[] { context });
        if (provider.GetRequiredService<UpgradeHandler>().Value != 42 || provider.GetRequiredService<UpgradeBatchHandler>().Value != 42)
            throw new InvalidOperationException("Chained message upcasting failed under Native AOT.");
        Console.WriteLine("Single and batch upcaster chains passed under Native AOT.");
    }
}

[Message("smoke.upgrade", Version = 1)] public readonly record struct UpgradeV1(int Value);
[Message("smoke.upgrade", Version = 2)] public readonly record struct UpgradeV2(long Value);
[Message("smoke.upgrade", Version = 3)] public readonly record struct UpgradeV3(long Value);
public sealed class UpgradeFirst : IUpcaster<UpgradeV1, UpgradeV2> { public UpgradeV2 Upcast(UpgradeV1 message) => new(message.Value); }
public sealed class UpgradeSecond : IUpcaster<UpgradeV2, UpgradeV3> { public UpgradeV3 Upcast(UpgradeV2 message) => new(message.Value); }
public sealed class UpgradeOptions;
public sealed class UpgradeHandler : IHandler<UpgradeV3>
{
    public long Value { get; private set; }
    public ValueTask HandleAsync(UpgradeV3 message, MessageContext context, CancellationToken cancellationToken)
    {
        if (context.MessageType != new MessageType("smoke.upgrade", 1)) throw new InvalidOperationException("Wire version was lost.");
        Value = message.Value;
        return default;
    }
}
public sealed class UpgradeBatchHandler : IBatchHandler<UpgradeV3>
{
    public long Value { get; private set; }
    public ValueTask<BatchResult> HandleAsync(Batch<UpgradeV3> batch, CancellationToken cancellationToken)
    {
        Value = batch.Items[0].Message.Value;
        return new(BatchResult.Success);
    }
}
