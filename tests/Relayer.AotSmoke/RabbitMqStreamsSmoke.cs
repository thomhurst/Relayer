using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using Relayer;
using Relayer.RabbitMQ.Streams;
using Relayer.Transports;

internal static class RabbitMqStreamsSmoke
{
    internal static async Task RunAsync()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQStreams("streams-smoke", streams => streams.Options.Configure(options =>
        {
            options.Configuration = new StreamSystemConfig { ClientProvidedName = "relayer-aot" };
            options.Streams.Add(new("orders") { Configuration = new("orders") });
        })));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<ITransport>();
        if ((await transport.ApplyTopologyAsync(TopologyMode.None, default)).Differences.Count != 0)
            throw new InvalidOperationException("RabbitMQ Streams no-connect registration failed under Native AOT.");
        var message = new Message(new byte[] { 1, 2, 3 })
        {
            ApplicationProperties = new ApplicationProperties { ["cloudEvents:type"] = "aot.smoke;v=1" },
        };
        if (message.Write(new byte[message.Size]) != message.Size)
            throw new InvalidOperationException("Native stream message serialization failed under Native AOT.");
        Console.WriteLine("RabbitMQ Streams registration and native message serialization passed under Native AOT.");
    }
}
