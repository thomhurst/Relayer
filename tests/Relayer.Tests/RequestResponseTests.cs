using System.Buffers;
using System.Text;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.InMemory;
using Relayer.AzureServiceBus;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Tests;

[Message("tests.rpc.request")]
public sealed record RpcRequest(int Value);
[Message("tests.rpc.response")]
public sealed record RpcResponse(int Value);

public sealed class RequestEchoHandler : IHandler<RpcRequest>
{
    public ValueTask HandleAsync(RpcRequest message, MessageContext context, CancellationToken cancellationToken) =>
        context.ReplyAsync(new RpcResponse(message.Value), cancellationToken);
}

public class RequestResponseTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);

    [Test]
    public async Task Hosted_native_in_memory_pipelines_deliver_concurrent_correlated_replies()
    {
        using var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", transport =>
            {
                transport.Request<RpcRequest, RpcResponse>("requests", "replies", configureReplies: c => c.Concurrency(4));
                transport.Send<RpcResponse>("replies");
                transport.Consume("requests", c => c.Handler<RequestEchoHandler>().Concurrency(4));
            });
        })).Build();
        await host.StartAsync();
        try
        {
            using var scope = host.Services.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<RpcRequest, RpcResponse>>();
            var requests = Enumerable.Range(0, 64).Select(i => client.RequestAsync(new(i), Limit).AsTask()).ToArray();
            var responses = await Task.WhenAll(requests).WaitAsync(Limit);
            await Assert.That(responses.Select(r => r.Value).SequenceEqual(Enumerable.Range(0, 64))).IsTrue();
        }
        finally { await host.StopAsync(); }
    }

    [Test]
    [Arguments("ce_")]
    [Arguments("cloudEvents:")]
    public async Task Extensions_follow_native_binding_and_duplicate_or_unmatched_replies_settle_normally(string prefix)
    {
        await using var fixture = new Fixture(prefix: prefix);
        var request = fixture.Client.RequestAsync(new(42), Limit).AsTask();
        var sent = await fixture.Next();
        await Assert.That(sent.Headers[fixture.Names.ReplyTo].ToString()).IsEqualTo("replies");
        var correlation = sent.Headers[fixture.Names.CorrelationId];
        await fixture.Reply(correlation, 42);
        await Assert.That((await request).Value).IsEqualTo(42);
        await fixture.Reply(correlation, 99);
        await fixture.Reply(new("not-a-correlation"), 99);
        await fixture.Reply(default, 99);
        // A reply removes its reservation; a new request can use the same bounded capacity.
        var next = fixture.Client.RequestAsync(new(43), Limit).AsTask();
        var nextSent = await fixture.Next();
        await fixture.Reply(nextSent.Headers[fixture.Names.CorrelationId], 43);
        await Assert.That((await next).Value).IsEqualTo(43);
    }

    [Test]
    public async Task Deadline_expires_without_sleep_and_late_reply_cannot_complete_reused_slot()
    {
        var clock = new InMemoryTestClock();
        await using var fixture = new Fixture(clock);
        var expired = fixture.Client.RequestAsync(new(1), TimeSpan.FromSeconds(1)).AsTask();
        var first = await fixture.Next();
        clock.Advance(TimeSpan.FromSeconds(1));
        await Assert.That(async () => await expired).Throws<TimeoutException>();
        var current = fixture.Client.RequestAsync(new(2), Limit).AsTask();
        var second = await fixture.Next();
        await fixture.Reply(first.Headers[fixture.Names.CorrelationId], 100);
        await Assert.That(current.IsCompleted).IsFalse();
        await fixture.Reply(second.Headers[fixture.Names.CorrelationId], 2);
        await Assert.That((await current).Value).IsEqualTo(2);
    }

    [Test]
    public async Task Caller_cancellation_releases_capacity_and_preserves_caller_token()
    {
        await using var fixture = new Fixture();
        using var cancellation = new CancellationTokenSource();
        var request = fixture.Client.RequestAsync(new(1), Limit, cancellation.Token).AsTask();
        var sent = await fixture.Next();
        cancellation.Cancel();
        var error = await Assert.That(async () => await request).Throws<OperationCanceledException>();
        await Assert.That(error!.CancellationToken).IsEqualTo(cancellation.Token);
        await fixture.Reply(sent.Headers[fixture.Names.CorrelationId], 100);
        var next = fixture.Client.RequestAsync(new(2), Limit).AsTask();
        var second = await fixture.Next();
        await fixture.Reply(second.Headers[fixture.Names.CorrelationId], 2);
        await Assert.That((await next).Value).IsEqualTo(2);
    }

    [Test]
    public async Task Send_failure_releases_capacity_even_after_a_reply_arrived()
    {
        await using var fixture = new Fixture();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Transport.Send = async token =>
        {
            await release.Task.WaitAsync(token);
            throw new InvalidOperationException("send failed");
        };
        var request = fixture.Client.RequestAsync(new(1), Limit).AsTask();
        var first = await fixture.Next();
        await fixture.Reply(first.Headers[fixture.Names.CorrelationId], 1);
        release.SetResult();
        await Assert.That(async () => await request).Throws<InvalidOperationException>();
        fixture.Transport.Send = null;
        var next = fixture.Client.RequestAsync(new(2), Limit).AsTask();
        var second = await fixture.Next();
        await fixture.Reply(second.Headers[fixture.Names.CorrelationId], 2);
        await Assert.That((await next).Value).IsEqualTo(2);
    }

    [Test]
    public async Task Deadline_includes_native_send_and_cancels_its_token()
    {
        var clock = new InMemoryTestClock();
        await using var fixture = new Fixture(clock);
        fixture.Transport.Send = token => new(Task.Delay(Timeout.Infinite, token));
        var request = fixture.Client.RequestAsync(new(1), TimeSpan.FromSeconds(1)).AsTask();
        await fixture.Next();
        clock.Advance(TimeSpan.FromSeconds(1));
        await Assert.That(async () => await request).Throws<TimeoutException>();
        fixture.Transport.Send = null;
        var next = fixture.Client.RequestAsync(new(2), Limit).AsTask();
        var second = await fixture.Next();
        await fixture.Reply(second.Headers[fixture.Names.CorrelationId], 2);
        await Assert.That((await next).Value).IsEqualTo(2);
    }

    [Test]
    public async Task Bounded_capacity_rejects_excess_requests_without_sending()
    {
        await using var fixture = new Fixture();
        var first = fixture.Client.RequestAsync(new(1), Limit).AsTask();
        var sent = await fixture.Next();
        await Assert.That(async () => await fixture.Client.RequestAsync(new(2), Limit)).Throws<InvalidOperationException>();
        await Assert.That(fixture.Transport.Sent.Reader.TryRead(out _)).IsFalse();
        await fixture.Reply(sent.Headers[fixture.Names.CorrelationId], 1);
        await first;
    }

    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    [Arguments(long.MaxValue)]
    public async Task Invalid_timeout_is_rejected_before_sending(long ticks)
    {
        await using var fixture = new Fixture();
        await Assert.That(async () => await fixture.Client.RequestAsync(new(1), TimeSpan.FromTicks(ticks))).Throws<ArgumentOutOfRangeException>();
        await Assert.That(fixture.Transport.Sent.Reader.TryRead(out _)).IsFalse();
    }

    [Test]
    public async Task Already_cancelled_request_is_not_sent()
    {
        await using var fixture = new Fixture();
        await Assert.That(async () => await fixture.Client.RequestAsync(new(1), Limit, new(true))).Throws<OperationCanceledException>();
        await Assert.That(fixture.Transport.Sent.Reader.TryRead(out _)).IsFalse();
    }

    [Test]
    public async Task Native_Service_Bus_application_properties_correlate_a_reply()
    {
        await using var fixture = new Fixture(prefix: "cloudEvents:");
        var request = fixture.Client.RequestAsync(new(5), Limit).AsTask();
        var sent = await fixture.Next();
        var correlation = sent.Headers[fixture.Names.CorrelationId].ToString();
        var native = ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("{\"Value\":5}"), deliveryCount: 1, properties: new Dictionary<string, object>
        {
            ["cloudEvents:id"] = "native-reply", ["cloudEvents:type"] = "tests.rpc.response;v=1",
            ["cloudEvents:correlationid"] = correlation,
        });
        var receive = new ReceivePipeline<ProcessMessageEventArgs, ServiceBusBinding>(
            fixture.Pipeline(new ReplyDescriptor<RpcRequest, RpcResponse>()), fixture.Names);
        await receive.ProcessAsync(new ProcessMessageEventArgs(native, new Receiver(), "rpc", default));
        await Assert.That((await request).Value).IsEqualTo(5);
    }

    [Test]
    [Arguments(0)]
    [Arguments(65537)]
    public async Task Invalid_pending_capacity_fails_registration(int capacity)
    {
        await Assert.That(() => new ServiceCollection().AddRelayer(builder =>
            builder.InMemory("local", transport => transport.Request<RpcRequest, RpcResponse>("requests", "replies",
                new() { MaxPendingRequests = capacity })))).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Startup_rejects_batched_or_shared_reply_endpoint(bool shared)
    {
        using var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", transport =>
            {
                transport.Request<RpcRequest, RpcResponse>("requests", "replies",
                    configureReplies: consumer => { if (!shared) consumer.Batch(2, TimeSpan.FromSeconds(1)); });
                if (shared) transport.Consume("replies", consumer => consumer.Handler<RequestEchoHandler>());
            });
        })).Build();
        await Assert.That(async () => await host.StartAsync()).Throws<Microsoft.Extensions.Options.OptionsValidationException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Startup_rejects_raw_or_outbox_required_request_routes(bool outbox)
    {
        using var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", transport =>
            {
                transport.SendDefaults = new()
                {
                    EnvelopeMode = outbox ? MessageEnvelopeMode.CloudEvents : MessageEnvelopeMode.Raw,
                    TransactionRequirement = outbox ? SendTransactionRequirement.Outbox : SendTransactionRequirement.None,
                };
                transport.Request<RpcRequest, RpcResponse>("requests", "replies");
            });
        })).Build();
        await Assert.That(async () => await host.StartAsync()).Throws<Microsoft.Extensions.Options.OptionsValidationException>();
    }

    [Test]
    public async Task Reply_uses_scoped_buffer_and_rejects_a_different_destination()
    {
        await using var fixture = new Fixture();
        var request = fixture.Client.RequestAsync(new(7), Limit).AsTask();
        var sent = await fixture.Next();
        var headers = new Dictionary<string, WireString>(sent.Headers);
        headers[fixture.Names.CorrelationId] = new(Encoding.UTF8.GetBytes(sent.Headers[fixture.Names.CorrelationId].ToString()));
        var pipeline = fixture.Pipeline(new EchoDescriptor(headers, false), durable: true);
        using (var context = pipeline.RentContext(fixture.Message(new RpcRequest(7), headers, "tests.rpc.request")))
            await pipeline.ProcessAsync(context);
        var response = await fixture.Next();
        await Assert.That(response.Headers[fixture.Names.CorrelationId].ToString()).IsEqualTo(sent.Headers[fixture.Names.CorrelationId].ToString());
        await fixture.Reply(response.Headers[fixture.Names.CorrelationId], 7);
        await Assert.That((await request).Value).IsEqualTo(7);
        headers[fixture.Names.ReplyTo] = new("untrusted-destination");
        pipeline = fixture.Pipeline(new EchoDescriptor(headers, false));
        using var rejected = pipeline.RentContext(fixture.Message(new RpcRequest(8), headers, "tests.rpc.request"));
        await Assert.That(async () => await pipeline.ProcessAsync(rejected)).Throws<InvalidOperationException>();
        await Assert.That(fixture.Transport.Sent.Reader.TryRead(out _)).IsFalse();
    }

    [Test]
    public async Task Reply_is_discarded_when_handler_fails_after_replying()
    {
        await using var fixture = new Fixture();
        var headers = new Dictionary<string, WireString>
        {
            [fixture.Names.ReplyTo] = new("replies"), [fixture.Names.CorrelationId] = new("borrowed-correlation"u8.ToArray()),
        };
        var pipeline = fixture.Pipeline(new EchoDescriptor(headers, true), durable: true);
        using var context = pipeline.RentContext(fixture.Message(new RpcRequest(1), headers, "tests.rpc.request"));
        await pipeline.ProcessAsync(context);
        await Assert.That(context.FailureDisposition!.Value.Kind).IsEqualTo(DispositionKind.DeadLetter);
        await Assert.That(fixture.Transport.Sent.Reader.TryRead(out _)).IsFalse();
    }

    private sealed class EchoDescriptor(Dictionary<string, WireString> headers, bool fail) : HandlerDescriptor<RpcRequest>(typeof(RequestEchoHandler)), IHandlerMarker
    {
        public override IHandlerMarker Resolve(IServiceProvider services) => this;
        public override async ValueTask DispatchAsync(IHandlerMarker handler, RpcRequest message, MessageContext context, CancellationToken cancellationToken)
        {
            await context.ReplyAsync(new RpcResponse(message.Value), cancellationToken);
            if (fail) throw new InvalidOperationException("handler failed");
            // The send must already own borrowed headers before native storage can be reused.
            if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(headers["ce_correlationid"].Utf8, out var data) && data.Array is not null) Array.Clear(data.Array, data.Offset, data.Count);
        }
    }

    private sealed class Fixture : IAsyncDisposable
    {
        internal Sink Transport { get; } = new();
        internal HeaderNames Names { get; }
        private readonly ServiceProvider _provider;
        private readonly IServiceScope _scope;
        private readonly ReceivePipeline<Incoming, Binding> _replies;
        internal IRequestClient<RpcRequest, RpcResponse> Client { get; }
        internal Fixture(TimeProvider? clock = null, string prefix = "ce_")
        {
            Names = new(prefix);
            var services = new ServiceCollection();
            services.AddSingleton<ITransport>(Transport);
            services.AddRelayer(builder =>
            {
                builder.AddMessageTypesFromThisAssembly();
                builder.AddTransport<Options, Options, Options>("sink", transport =>
                {
                    transport.SendDefaults = new() { HeaderNames = Names };
                    transport.Request<RpcRequest, RpcResponse>("requests", "replies", new() { MaxPendingRequests = 1, TimeProvider = clock ?? TimeProvider.System });
                    transport.Send<RpcResponse>("replies");
                });
            });
            _provider = services.BuildServiceProvider();
            _scope = _provider.CreateScope();
            Client = _scope.ServiceProvider.GetRequiredService<IRequestClient<RpcRequest, RpcResponse>>();
            _replies = new(Pipeline(new ReplyDescriptor<RpcRequest, RpcResponse>()), Names);
        }
        internal ConsumePipeline Pipeline(HandlerDescriptor descriptor, bool durable = false)
        {
            var scopes = _provider.GetRequiredService<IServiceScopeFactory>();
            var types = _provider.GetRequiredService<MessageTypeRegistry>();
            var serializer = _provider.GetRequiredService<IMessageSerializer>();
            var builder = durable
                ? new ConsumePipelineBuilder(scopes, types, serializer, new FailurePipeline(_provider, "sink", "requests", "rpc"))
                : new ConsumePipelineBuilder(scopes, types, serializer, static (_, error, _) => ValueTask.FromException(error));
            return builder.Handler(descriptor).Build(4);
        }

        internal ConsumeMessage Message<T>(T value, Dictionary<string, WireString> headers, string type)
        {
            var buffer = new ArrayBufferWriter<byte>();
            _provider.GetRequiredService<IMessageSerializer>().Serialize(value, buffer);
            return new() { MessageId = "id", MessageType = new(type, 1), Body = new(buffer.WrittenMemory), Headers = new Headers(headers) };
        }
        internal async Task Reply(WireString correlation, int value)
        {
            // Kafka delivers UTF-8 values. Exercise the same lazy representation for both header prefixes.
            var headers = new Dictionary<string, WireString>();
            if (!correlation.IsEmpty) headers[Names.CorrelationId] = new(Encoding.UTF8.GetBytes(correlation.ToString()));
            headers[Names.Id] = new("reply-id");
            headers[Names.Type] = new("tests.rpc.response;v=1");
            var message = Message(new RpcResponse(value), headers, "tests.rpc.response");
            var disposition = await _replies.ProcessAsync(new Incoming(message.Body, headers));
            await Assert.That(disposition.Kind).IsEqualTo(DispositionKind.Complete);
        }
        internal async Task<Sent> Next() => await Transport.Sent.Reader.ReadAsync().AsTask().WaitAsync(Limit);
        public async ValueTask DisposeAsync() { _scope.Dispose(); await _provider.DisposeAsync(); }
    }

    private sealed class Headers(Dictionary<string, WireString> values) : MessageHeaders
    {
        public override bool TryGetValue(string name, out WireString value) => values.TryGetValue(name, out value);
    }
    private sealed class Options;
    private sealed class Receiver : ServiceBusReceiver;
    private sealed record Incoming(ReadOnlySequence<byte> Body, Dictionary<string, WireString> Headers);
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => message.Body;
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static bool TryGetHeader(Incoming message, string name, out WireString value) => message.Headers.TryGetValue(name, out value);
    }
    private sealed record Sent(Dictionary<string, WireString> Headers);
    private sealed class Sink : ITransport
    {
        internal Channel<Sent> Sent { get; } = Channel.CreateUnbounded<Sent>();
        internal Func<CancellationToken, ValueTask>? Send { get; set; }
        public string Name => "sink";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { DeadLetter = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            var headers = new Dictionary<string, WireString>();
            foreach (var header in message.Headers.Span) headers[header.Key] = new(header.Value.ToString());
            Sent.Writer.TryWrite(new(headers));
            return Send?.Invoke(cancellationToken) ?? default;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
