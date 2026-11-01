using Azure.Messaging.ServiceBus;
using BenchmarkDotNet.Attributes;
using Relayer.AzureServiceBus;

namespace Relayer.Benchmarks;

/// <summary>Compares receive projection with equivalent direct, zero-copy native AMQP property reads.</summary>
[MemoryDiagnoser]
public class ServiceBusBindingBenchmarks
{
    private ProcessMessageEventArgs _incoming = null!;

    [GlobalSetup]
    public void Setup()
    {
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData(new byte[256]),
            properties: new Dictionary<string, object> { ["cloudEvents:type"] = "orders.placed;v=1" }, deliveryCount: 3);
        _incoming = new ProcessMessageEventArgs(message, new BenchmarkReceiver(), "benchmark", default);
    }

    [Benchmark(Baseline = true)]
    public int NativeAmqpProjection()
    {
        var native = _incoming.Message.GetRawAmqpMessage();
        native.Body.TryGetData(out var sections);
        var body = ((IReadOnlyList<ReadOnlyMemory<byte>>)sections!)[0];
        native.ApplicationProperties.TryGetValue("cloudEvents:type", out var type);
        return body.Length + ((string)type!).Length + _incoming.Message.DeliveryCount;
    }

    [Benchmark]
    public int RelayerBindingProjection()
    {
        var body = ServiceBusBinding.GetBody(_incoming);
        ServiceBusBinding.TryGetHeader(_incoming, "cloudEvents:type", out var type);
        return checked((int)body.Length) + type.Text!.Length + ServiceBusBinding.GetDeliveryCount(_incoming)!.Value;
    }

    private sealed class BenchmarkReceiver : ServiceBusReceiver;
}
