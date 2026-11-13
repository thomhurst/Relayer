using Relayer.Transports;
using System.Buffers;
using System.Diagnostics;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Serialization;

namespace Relayer.Tests;

public class ServiceBusSendAllocationTests
{
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Preparation_adds_zero_allocations_over_equivalent_native_message_preparation(bool utf8ContentType, bool tracing)
    {
        using var fixture = new Fixture(utf8ContentType, tracing);
        var state = new ServiceBusSendState();
        var nativeData = new ReadOnlyMemory<byte>[1];
        var native = new ServiceBusMessage(new AmqpAnnotatedMessage(AmqpMessageBody.FromData(nativeData)));
        Measure(fixture, state, native, nativeData, prepare: false, 1000);
        Measure(fixture, state, native, nativeData, prepare: true, 1000);

        var nativePreparation = Measure(fixture, state, native, nativeData, prepare: false, 1000);
        var preparation = Measure(fixture, state, native, nativeData, prepare: true, 1000);
        Console.WriteLine($"Preparation: {preparation / 1000.0} B/send; raw SDK preparation: {nativePreparation / 1000.0} B/send; additional: {(preparation - nativePreparation) / 1000.0} B/send.");

        // D52 attributes SDK-required strings to native-client cost; preparation must add no allocations.
        // A lower preparation total satisfies the contract too: the native baseline can include
        // incidental runtime allocations (CI observed 8 extra bytes across all 1,000 sends).
        await Assert.That(nativePreparation).IsGreaterThan(0L);
        await Assert.That(preparation).IsLessThanOrEqualTo(nativePreparation);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Reuse_preserves_changing_envelopes_serializer_types_and_creation_context(bool utf8ContentType)
    {
        using var fixture = new Fixture(utf8ContentType, tracing: true);
        var state = new ServiceBusSendState();
        string? previousId = null;
        string? previousTime = null;
        for (var i = 0; i < 4; i++)
        {
            var outgoing = fixture.Next();
            state.Prepare(outgoing);
            var raw = state.Message.GetRawAmqpMessage();
            // Snapshot through the native encoder before reusing the caller's header buffers.
            var snapshot = AmqpAnnotatedMessage.FromBytes(raw.ToBytes());
            var id = (string)snapshot.ApplicationProperties["cloudEvents:id"]!;
            var time = (string)snapshot.ApplicationProperties["cloudEvents:time"]!;
            var contentType = state.Message.ContentType;
            await Assert.That(contentType).IsEqualTo(i % 2 == 0 ? "application/json" : "application/test");
            await Assert.That(state.Message.ApplicationProperties.ContainsKey("content-type")).IsFalse();
            await Assert.That(id).IsNotEqualTo(previousId);
            await Assert.That(time).IsNotEqualTo(previousTime);
            for (var headerIndex = 0; headerIndex < outgoing.Headers.Length; headerIndex++)
            {
                var header = outgoing.Headers.Span[headerIndex];
                if (header.Key != "content-type")
                    await Assert.That(state.Message.ApplicationProperties[header.Key]).IsEqualTo(header.Value.ToString());
            }

            await Assert.That(snapshot.ApplicationProperties["traceparent"]).IsEqualTo(Fixture.TraceParent);
            await Assert.That(snapshot.ApplicationProperties["tracestate"]).IsEqualTo("vendor=original");
            state.Clear();
            // Caller buffers can be reused after the native send's lease has been released.
            fixture.OverwriteId();
            await Assert.That(snapshot.ApplicationProperties["cloudEvents:id"]).IsEqualTo(id);
            await Assert.That(raw.ApplicationProperties.Count).IsEqualTo(0);
            await Assert.That(raw.Properties.ContentType).IsNull();
            await Assert.That(raw.Body.TryGetData(out var data)).IsTrue();
            await Assert.That(data!.Single().IsEmpty).IsTrue();
            previousId = id;
            previousTime = time;
        }
    }

    private static long Measure(Fixture fixture, ServiceBusSendState state, ServiceBusMessage native, ReadOnlyMemory<byte>[] nativeData, bool prepare, int count)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < count; i++)
        {
            var outgoing = fixture.Next();
            if (prepare)
            {
                state.Prepare(outgoing);
                state.Clear();
            }
            else
            {
                nativeData[0] = outgoing.Body;
                foreach (var header in outgoing.Headers.Span)
                {
                    if (header.Key == "content-type") native.ContentType = header.Value.ToString();
                    else native.ApplicationProperties[header.Key] = header.Value.ToString();
                }
                nativeData[0] = default;
                native.ApplicationProperties.Clear();
                native.ContentType = null;
            }
        }
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private sealed class Fixture : IDisposable
    {
        internal const string TraceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        private readonly ServiceProvider _provider;
        private readonly string[] _contentTypes;
        private readonly bool _utf8ContentType;
        private readonly SendHeaders _headers = new();
        private readonly MessageEnvelope _envelope = new(new("allocation.order", 1), "/orders", new("cloudEvents:"));
        private readonly ActivityContext _creation;
        private readonly byte[] _body = [42];
        private int _sequence;

        internal Fixture(bool utf8ContentType, bool tracing)
        {
            _utf8ContentType = utf8ContentType;
            _creation = tracing ? ActivityContext.Parse(TraceParent, "vendor=original") : default;
            var services = new ServiceCollection();
            services.AddRelayer(builder =>
            {
                builder.AddMessageTypesFromThisAssembly();
                builder.ServiceBus("json", _ => { });
                builder.ServiceBus("custom", bus => bus.UseSerializer(_ => new TestSerializer()));
            });
            _provider = services.BuildServiceProvider();
            _contentTypes = [_provider.GetMessageSerializer("json").ContentType, _provider.GetMessageSerializer("custom").ContentType];
        }

        internal OutgoingMessage Next()
        {
            var sequence = ++_sequence;
            _headers.Clear();
            _envelope.Write<SendHeaders, SendHeaders>(_headers,
                new(new Guid(sequence, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), DateTimeOffset.UnixEpoch.AddTicks(sequence), _creation));
            var contentType = _contentTypes[(sequence - 1) % _contentTypes.Length];
            if (_utf8ContentType) SendHeaders.SetHeader(_headers, "content-type", contentType);
            else _headers.Add("content-type", new(contentType));
            return new("orders", _body, _headers.Written);
        }

        internal void OverwriteId() => SendHeaders.SetHeader(_headers, "cloudEvents:id", "overwritten");

        public void Dispose() => _provider.Dispose();
    }

    private sealed class TestSerializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }
}
