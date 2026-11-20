using System.Text.Json.Serialization;
using Dekaf.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer;
using Relayer.Kafka;
using Relayer.Transports;

public static class KafkaTransactionProbe
{
    public const string InputName = "tests.kafka.eos.input";
    public const string TransactionalIdSuffix = ".transaction";

    internal static async Task RunAsync(string bootstrap, string input, string output, bool commit)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(KafkaTransactionJson.Default);
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.None;
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(bootstrap);
                    options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                });
                kafka.Send<KafkaTransactionOutput>(output);
                kafka.Consume(input, consumer =>
                {
                    consumer.Options.Group = input;
                    consumer.Options.TransactionalId = input + TransactionalIdSuffix;
                    consumer.Options.Processing = new()
                    {
                        Ordering = PartitionedProcessingOrder.Key,
                        CommitPolicy = PartitionCommitPolicy.CommitCompletedInTransaction,
                        CommitInterval = commit ? TimeSpan.FromMilliseconds(200) : TimeSpan.FromHours(1),
                    };
                    consumer.Handler<KafkaTransactionHandler>();
                });
            });
        });

        using var host = builder.Build();
        await host.RunAsync();
    }
}

[Message(KafkaTransactionProbe.InputName)]
public sealed record KafkaTransactionInput(int Number);

[Message("tests.kafka.eos.output")]
public sealed record KafkaTransactionOutput(int Number);

internal sealed class KafkaTransactionHandler(ISender sender) : IHandler<KafkaTransactionInput>
{
    public ValueTask HandleAsync(KafkaTransactionInput message, MessageContext context, CancellationToken cancellationToken)
    {
        return sender.SendAsync(new KafkaTransactionOutput(message.Number), cancellationToken);
    }
}

[JsonSerializable(typeof(KafkaTransactionInput))]
[JsonSerializable(typeof(KafkaTransactionOutput))]
internal partial class KafkaTransactionJson : JsonSerializerContext;
