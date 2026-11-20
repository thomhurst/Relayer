using System.Text;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.AmazonSqs;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class SqsConsumerControlTests
{
    [Test]
    public async Task Deferred_consumers_start_and_stop_through_the_keyed_control()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        await using var provider = Provider(client, autoStart: false);
        var control = provider.GetRequiredKeyedService<IConsumerControl>("sqs");

        await ServiceBusReceiveTests.Start(provider);
        await Assert.That(client.Polls).IsEqualTo(0);

        // A worker issues its first long poll synchronously inside start.
        await control.StartAsync(default);
        await control.StartAsync(default);
        await Assert.That(client.Polls).IsEqualTo(1);

        await control.StopAsync(default);
        await control.StopAsync(default);
        await control.StartAsync(default);
        await Assert.That(client.Polls).IsEqualTo(2);
        await ServiceBusReceiveTests.Stop(provider);
        await Assert.That(async () => await control.StartAsync(default)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Control_fails_before_the_host_starts_and_after_it_stops()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        await using var provider = Provider(client, autoStart: false);
        var control = provider.GetRequiredKeyedService<IConsumerControl>("sqs");

        await Assert.That(async () => await control.StartAsync(default)).Throws<InvalidOperationException>();
        await ServiceBusReceiveTests.Start(provider);
        await ServiceBusReceiveTests.Stop(provider);
        await Assert.That(async () => await control.StartAsync(default)).Throws<InvalidOperationException>();
        await Assert.That(client.Polls).IsEqualTo(0);
    }

    [Test]
    public async Task Auto_started_consumers_can_be_paused_and_resumed()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        await using var provider = Provider(client);
        var control = provider.GetRequiredKeyedService<IConsumerControl>("sqs");

        await ServiceBusReceiveTests.Start(provider);
        await control.StartAsync(default);
        await Assert.That(client.Polls).IsEqualTo(1);
        await control.StopAsync(default);
        await control.StartAsync(default);
        await Assert.That(client.Polls).IsEqualTo(2);
        await ServiceBusReceiveTests.Stop(provider);
    }

    [Test]
    public async Task Floci_consumer_receives_only_after_the_deferred_start()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        using var client = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig
        {
            ServiceURL = await SharedBrokers.FlociUrlAsync(token),
            AuthenticationRegion = "us-east-1",
        });
        var source = TestContext.Current!.Isolation.GetIsolatedName("deferred");
        var dead = TestContext.Current!.Isolation.GetIsolatedName("deferred-dead");
        var control = new SqsReceiveFlociTests.Control();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(control);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AmazonSqs("sqs", sqs =>
            {
                sqs.TopologyMode = TopologyMode.Provision;
                sqs.Options.Configure(options =>
                {
                    options.SqsClient = client;
                    options.AutoStartConsumers = false;
                    options.Queues.Add(new(source));
                    options.Queues.Add(new(dead));
                });
                sqs.Consume(source, consumer =>
                {
                    consumer.Handler<SqsReceiveFlociTests.Handler>();
                    consumer.Options.ConfigureReceive = request => request.WaitTimeSeconds = 1;
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = dead
                    });
                });
            });
        });
        using var host = builder.Build();

        try
        {
            await host.StartAsync(token);
            var transport = host.Services.GetServices<ITransport>().Single(item => item.Name == "sqs");
            await transport.SendAsync(new(source, Encoding.UTF8.GetBytes("{\"Value\":2}"),
                new KeyValuePair<string, WireString>[] { new("ce_id", new(Guid.NewGuid().ToString("N"))), new("ce_type", new("sqs.receive;v=1")), new("content-type", new("application/json")) }), token);
            var url = (await client.GetQueueUrlAsync(source, token)).QueueUrl;
            var attributes = await client.GetQueueAttributesAsync(new GetQueueAttributesRequest(url, ["ApproximateNumberOfMessages"]), token);
            await Assert.That(attributes.ApproximateNumberOfMessages).IsEqualTo(1);
            await Assert.That(control.Healthy.Task.IsCompleted).IsFalse();

            await host.Services.GetRequiredKeyedService<IConsumerControl>("sqs").StartAsync(token);
            await control.Healthy.Task.WaitAsync(token);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    private static ServiceProvider Provider(SqsReceiveTests.ReceiveClient client, bool autoStart = true)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(new SqsReceiveFlociTests.Control());
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AmazonSqs("sqs", sqs =>
            {
                sqs.TopologyMode = TopologyMode.None;
                sqs.Options.Configure(options =>
                {
                    options.SqsClient = client;
                    options.AutoStartConsumers = autoStart;
                    options.Queues.Add(new("orders"));
                    options.Queues.Add(new("dead"));
                });
                sqs.Consume("orders", consumer =>
                {
                    consumer.Handler<SqsReceiveFlociTests.Handler>();
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = "dead"
                    });
                });
            });
        });
        return services.BuildServiceProvider();
    }
}
