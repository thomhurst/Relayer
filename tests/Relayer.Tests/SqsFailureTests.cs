using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.AmazonSqs;

namespace Relayer.Tests;

public sealed class SqsFailureTests
{
    [Test]
    public async Task Failed_poison_copy_never_deletes_the_original()
    {
        using var client = new SqsReceiveTests.ReceiveClient { SendFailure = new IOException("DLQ unavailable") };
        using var host = Host(client);
        await host.StartAsync();
        await client.Deliveries.Writer.WriteAsync(SqsReceiveTests.Message("{\"Value\":1}", 1));
        await client.NextPoll.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Deletes).IsEqualTo(0);
        await host.StopAsync();
    }

    [Test]
    public async Task Unsupported_native_delay_waits_for_durable_outbox_before_deletion()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        var scheduler = new Scheduler();
        using var host = Host(client, scheduler);
        await host.StartAsync();
        await client.Deliveries.Writer.WriteAsync(SqsReceiveTests.Message("{\"Value\":1}", 1));
        await scheduler.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Deletes).IsEqualTo(0);
        await Assert.That(scheduler.Transport).IsEqualTo("sqs");
        await Assert.That(scheduler.Destination).IsEqualTo("orders");
        await Assert.That(scheduler.Due > DateTimeOffset.UtcNow.AddHours(12)).IsTrue();
        await Assert.That(scheduler.Body).IsEqualTo("{\"Value\":1}");
        scheduler.Commit.SetResult();
        await client.NextPoll.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Deletes).IsEqualTo(1);
        await host.StopAsync();
    }

    private static IHost Host(SqsReceiveTests.ReceiveClient client, Scheduler? scheduler = null)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new SqsReceiveLocalStackTests.Control());
        if (scheduler is not null) builder.Services.AddSingleton<IFailureScheduler>(scheduler);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AmazonSqs("sqs", sqs =>
            {
                sqs.TopologyMode = TopologyMode.None;
                sqs.Options.Configure(options => { options.SqsClient = client; options.Queues.Add(new("orders")); options.Queues.Add(new("dead")); });
                sqs.Consume("orders", consumer =>
                {
                    consumer.Handler<SqsReceiveLocalStackTests.Handler>();
                    consumer.Options.Failure = new() { DeadLetterEndpoint = "dead", Jitter = 0,
                        RedeliveryDelays = scheduler is null ? default : new[] { TimeSpan.FromHours(13) } };
                });
            });
        });
        return builder.Build();
    }
    private sealed class Scheduler : IFailureScheduler
    {
        internal readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Commit = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal string? Transport;
        internal string? Destination;
        internal string? Body;
        internal DateTimeOffset? Due;
        public async ValueTask ScheduleAsync(string transportName, OutgoingMessage message, CancellationToken token)
        {
            Transport = transportName; Destination = message.Destination; Due = message.DeliverAt;
            Body = System.Text.Encoding.UTF8.GetString(message.Body.Span);
            Started.SetResult();
            await Commit.Task.WaitAsync(token);
        }
    }
}
