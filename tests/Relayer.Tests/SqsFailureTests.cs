using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.AmazonSqs;

namespace Relayer.Tests;

public sealed class SqsFailureTests
{
    [Test]
    [Arguments("ce_id", "ce_id", false)]
    [Arguments("ce_id", "custom_id", true)]
    [Arguments("missing_id", "ce_id", true)]
    public async Task Fifo_copy_uses_native_fallback_only_when_a_bound_identity_is_missing(string sourceHeader, string deadHeader, bool needsFallback)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var delivery = new SqsDelivery(client, "url");
        var message = SqsReceiveTests.Message("body", 1);
        message.Attributes["MessageDeduplicationId"] = "native-id";
        message.Attributes["MessageGroupId"] = "group";
        delivery.Prepare(message, TimeProvider.System.GetTimestamp(), null);
        var context = new CopyContext();
        context.Features.Set(new NativeMessage<SqsDelivery> { Message = delivery });
        var copy = SqsHostedService.Copy(context, sourceHeader, deadHeader);
        var state = new SqsSendState("0");
        state.Prepare(copy, "dead.fifo", true, deadHeader);
        await Assert.That(state.Request.MessageDeduplicationId).IsEqualTo(deadHeader == "ce_id" ? "id" : "native-id");
        if (needsFallback)
            await Assert.That(() => copy.ValidateForOutbox()).Throws<InvalidOperationException>();
        else copy.ValidateForOutbox();
    }

    [Test]
    [Arguments("Number", "ce_id", "ce_id")]
    [Arguments("String.eventId", "ce_id", "ce_id")]
    [Arguments("Number", "source_id", "ce_id")]
    [Arguments("String.eventId", "ce_id", "dead_id")]
    public async Task Fifo_copy_preserves_native_deduplication_for_typed_identity_attributes(string dataType, string sourceHeader, string deadHeader)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var delivery = new SqsDelivery(client, "url");
        var message = SqsReceiveTests.Message("body", 1);
        message.MessageAttributes[sourceHeader] = new() { DataType = dataType, StringValue = "42" };
        message.MessageAttributes[deadHeader] = new() { DataType = dataType, StringValue = "42" };
        message.Attributes["MessageDeduplicationId"] = "native-id";
        message.Attributes["MessageGroupId"] = "group";
        delivery.Prepare(message, TimeProvider.System.GetTimestamp(), null);
        var context = new CopyContext();
        context.Features.Set(new NativeMessage<SqsDelivery> { Message = delivery });
        var copy = SqsHostedService.Copy(context, sourceHeader, deadHeader);
        var state = new SqsSendState("0");
        state.Prepare(copy, "dead.fifo", true, deadHeader);
        await Assert.That(state.Request.MessageDeduplicationId).IsEqualTo("native-id");
        await Assert.That(state.Request.MessageAttributes[deadHeader].DataType).IsEqualTo(dataType);
        await Assert.That(state.Request.MessageAttributes[deadHeader].StringValue).IsEqualTo("42");
        await Assert.That(() => copy.ValidateForOutbox()).Throws<InvalidOperationException>();
    }

    private sealed class CopyContext : MessageContext
    {
        public override string MessageId => "id";
        public override MessageType MessageType => default;
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => throw new NotSupportedException();
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }

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
    public async Task Native_attribute_capacity_failure_retains_original_instead_of_dropping_metadata()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var host = Host(client);
        await host.StartAsync();
        var message = SqsReceiveTests.Message("{\"Value\":1}", 1);
        for (var index = 0; index < 6; index++)
            message.MessageAttributes.Add("application-" + index, new() { DataType = "String", StringValue = "value" });
        await client.Deliveries.Writer.WriteAsync(message);
        await client.NextPoll.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Deletes).IsEqualTo(0);
        await Assert.That(message.MessageAttributes.Count).IsEqualTo(8);
        await Assert.That(client.Sends).IsEqualTo(0);
        await host.StopAsync();
    }

    [Test]
    public async Task Invalid_native_receive_count_reaches_poison_copy_instead_of_failing_before_pipeline()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var host = Host(client);
        await host.StartAsync();
        var message = SqsReceiveTests.Message("{\"Value\":1}", 1);
        message.Attributes["ApproximateReceiveCount"] = "invalid";
        await client.Deliveries.Writer.WriteAsync(message);
        await client.NextPoll.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Deletes).IsEqualTo(1);
        await Assert.That(client.Sends).IsEqualTo(1);
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
