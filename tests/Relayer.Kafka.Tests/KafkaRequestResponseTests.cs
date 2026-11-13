using Dekaf.Admin;
using Dekaf.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

public class KafkaRequestResponseTests
{
    [Test]
    public async Task Requests_and_replies_use_native_headers_consumers_and_commits()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        await using var broker = new KafkaBuilder("apache/kafka:4.0.2")
            .WithVendor(KafkaVendor.ApacheSoftwareFoundation).AfterStartupCopy().Build();
        await broker.StartAsync(token);
        var bootstrap = broker.GetBootstrapAddress();
        var requests = "rpc-" + Guid.NewGuid().ToString("N");
        var replies = requests + "-replies";
        await using var admin = new AdminClientBuilder().WithBootstrapServers(bootstrap).Build();
        await admin.CreateTopicsAsync([
            new() { Name = requests, NumPartitions = 1, ReplicationFactor = 1 },
            new() { Name = replies, NumPartitions = 1, ReplicationFactor = 1 },
        ], cancellationToken: token);
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Logging.ClearProviders();
        hostBuilder.Services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.Kafka("rpc", transport =>
            {
                transport.TopologyMode = TopologyMode.None;
                transport.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(bootstrap);
                    // New groups must read requests/replies sent before their initial assignment completes.
                    options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                });
                transport.Request<KafkaRequest, KafkaResponse>(requests, replies,
                    request => request.Replies(consumer => consumer.Options.Group = replies));
                transport.Send<KafkaResponse>(replies);
                transport.Consume(requests, consumer =>
                {
                    consumer.Options.Group = requests;
                    consumer.Handler<KafkaRequestHandler>();
                });
            });
        });
        using var host = hostBuilder.Build();
        await host.StartAsync(token);
        try
        {
            using var scope = host.Services.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<KafkaRequest, KafkaResponse>>();
            var pending = Enumerable.Range(0, 8).Select(i => client.RequestAsync(new(i), TimeSpan.FromSeconds(30), token).AsTask()).ToArray();
            var responses = await Task.WhenAll(pending);
            await Assert.That(responses.Select(response => response.Value).SequenceEqual(Enumerable.Range(0, 8))).IsTrue();
        }
        finally { await host.StopAsync(token); }
    }
}

[Message("tests.kafka.request")]
public sealed record KafkaRequest(int Value);
[Message("tests.kafka.response")]
public sealed record KafkaResponse(int Value);
public sealed class KafkaRequestHandler : IHandler<KafkaRequest>
{
    public ValueTask HandleAsync(KafkaRequest message, MessageContext context, CancellationToken cancellationToken) =>
        context.ReplyAsync(new KafkaResponse(message.Value), cancellationToken);
}
