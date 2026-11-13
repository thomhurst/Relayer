using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

// Shared setup only: measured calls still use the real sender or the native client directly.
internal sealed class DirectJsonSend : IAsyncDisposable
{
    private readonly ServiceProvider _provider;
    private readonly IServiceScope _scope;
    private readonly IMessageSerializer _serializer;
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private readonly MessageEnvelope _envelope;
    internal readonly DirectJsonOrder Order = new(42, "customer-1");
    internal ISender Sender { get; }
    internal string ContentType => _serializer.ContentType;

    internal DirectJsonSend(RelayerInstrumentation instrumentation, Action<IRelayerBuilder> configure,
        string prefix = "ce_")
    {
        var services = new ServiceCollection();
        services.AddSingleton(instrumentation);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            configure(builder);
        });
        _provider = services.BuildServiceProvider();
        _scope = _provider.CreateScope();
        Sender = _scope.ServiceProvider.GetRequiredService<ISender>();
        _serializer = _provider.GetMessageSerializer("benchmark-json");
        _envelope = new(new("benchmarks.direct-json", 1), "/", new(prefix));
    }

    internal async Task StartAsync()
    {
        foreach (var transport in _provider.GetServices<ITransport>()) await transport.StartAsync(default);
    }

    internal ReadOnlyMemory<byte> Serialize()
    {
        _buffer.Clear();
        _serializer.Serialize(Order, _buffer);
        return _buffer.WrittenMemory;
    }

    internal void WriteEnvelope<T, TWriter>(T native) where TWriter : IMessageHeaderWriter<T> =>
        _envelope.Write<T, TWriter>(native, MessageEnvelopeMetadata.Create(Activity.Current?.Context ?? default));

    public async ValueTask DisposeAsync()
    {
        if (_scope is IAsyncDisposable scope) await scope.DisposeAsync();
        else _scope.Dispose();
        await _provider.DisposeAsync();
    }
}

[Message("benchmarks.direct-json")]
public sealed record DirectJsonOrder(int OrderId, string Customer);
