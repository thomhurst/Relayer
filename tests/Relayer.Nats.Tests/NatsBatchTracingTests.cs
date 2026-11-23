using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Relayer.Transports;

namespace Relayer.Nats.Tests;

public class NatsBatchTracingTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Batch_tracing_preserves_confirmed_publications_in_input_order(bool mixedSubjects)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var isolation = TestContext.Current!.Isolation;
        var streamName = isolation.GetIsolatedName("batch_events");
        var first = streamName + ".first";
        var second = streamName + ".second";
        string[] subjects = mixedSubjects ? [first, second, first] : [first, first];
        await using var native = new NatsConnection(broker.Options());
        var jetStream = new NatsJSContext(native);
        var stream = await jetStream.CreateStreamAsync(new(streamName, [first, second]), token);

        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Nats("nats", nats =>
        {
            nats.Options.Configure(options => options.ConnectionOptions = broker.Options());
            nats.Send<ExecutionMessage>([first, second], _ => first);
        }));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<ITransport>();
        var source = provider.GetRequiredService<MessageTracing>().Source;
        var spans = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = candidate => ReferenceEquals(candidate, source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        await transport.StartAsync(token);

        var messages = subjects.Select(subject => new OutgoingMessage(subject, "payload"u8.ToArray())).ToArray();
        await transport.SendBatchAsync(messages, token);

        var stored = await jetStream.GetStreamAsync(streamName, cancellationToken: token);
        await Assert.That(stored.Info.State.Messages).IsEqualTo((long)messages.Length);
        var completed = spans.ToArray();
        Activity[] creationSpans;

        if (mixedSubjects)
        {
            await Assert.That(completed.Select(span => span.OperationName)).IsEquivalentTo(subjects.Select(subject => $"send {subject}"));
            await Assert.That(completed.All(span => span.Kind == ActivityKind.Producer && !span.Links.Any())).IsTrue();
            creationSpans = completed;
        }
        else
        {
            var sends = completed.Where(span => span.OperationName == $"send {first}").ToArray();
            await Assert.That(sends.Length).IsEqualTo(1);
            var batch = sends[0];
            await Assert.That(batch.Kind).IsEqualTo(ActivityKind.Client);
            await Assert.That(batch.GetTagItem("messaging.batch.message_count")).IsEqualTo(messages.Length);
            creationSpans = completed.Where(span => span.OperationName == $"create {first}").ToArray();
            await Assert.That(creationSpans.Length).IsEqualTo(messages.Length);
            await Assert.That(creationSpans.All(span => span.Kind == ActivityKind.Producer)).IsTrue();
            await Assert.That(batch.Links.Select(link => link.Context.SpanId).SequenceEqual(creationSpans.Select(span => span.SpanId))).IsTrue();
        }

        for (int i = 0; i < subjects.Length; i++)
        {
            var message = (await stream.GetAsync(new()
            {
                Seq = (ulong)i + 1,
            }, token)).Message;
            await Assert.That(message.Subject).IsEqualTo(subjects[i]);
            var headers = Encoding.UTF8.GetString(Convert.FromBase64String(message.Hdrs!));
            await Assert.That(headers).Contains($"traceparent: {creationSpans[i].Id}");
        }
    }
}
