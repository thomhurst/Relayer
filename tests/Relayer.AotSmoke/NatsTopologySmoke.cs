using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer;
using Relayer.Nats;
using Relayer.Transports;

internal static class NatsTopologySmoke
{
    internal static async Task RunAsync()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Nats("nats-smoke", nats =>
        {
            nats.TopologyMode = TopologyMode.None;
            nats.Options.Configure(options =>
            {
                options.ConnectionOptions = new NatsOpts { Url = "nats://does-not-exist.invalid:4222" };
                options.Streams.Add(new("orders") { Configuration = new("orders", ["orders.>"]) });
                options.Consumers.Add(new("orders", "handler") { Configuration = new("handler") });
            });
        }));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<ITransport>();
        if ((await transport.ApplyTopologyAsync(TopologyMode.None, default)).Differences.Count != 0)
            throw new InvalidOperationException("NATS no-connect topology registration failed under Native AOT.");

        var buffer = new ArrayBufferWriter<byte>();
        NatsJSJsonSerializer<StreamConfig>.Default.Serialize(buffer, new StreamConfig("orders", ["orders.>"]));
        NatsJSJsonSerializer<ConsumerConfig>.Default.Serialize(buffer, new ConsumerConfig("handler"));
        if (buffer.WrittenCount == 0) throw new InvalidOperationException("Native NATS topology serialization failed under Native AOT.");
        Console.WriteLine("NATS topology registration and native configuration serialization passed under Native AOT.");
    }
}
