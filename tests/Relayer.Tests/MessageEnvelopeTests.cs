using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Relayer.Tests;

public class MessageEnvelopeTests
{
    [Test]
    [Arguments(false, "ce_")]
    [Arguments(true, "ce_")]
    [Arguments(false, "cloudEvents:")]
    [Arguments(true, "cloudEvents:")]
    public async Task Envelope_round_trips_native_headers(bool utf8, string prefix)
    {
        var names = new HeaderNames(prefix);
        var envelope = new MessageEnvelope(new MessageType("billing.charge-card", 2), "/billing/東京", names);
        var message = new NativeMessage(utf8);
        var metadata = MessageEnvelopeMetadata.Create(CreateContext());

        envelope.Write<NativeMessage, NativeBinding>(message, metadata);
        var headers = new MessageHeaders<NativeMessage, NativeBinding>(message, names);

        await Assert.That(message.ReadCount).IsEqualTo(0);
        await Assert.That(headers.GetMessageType().ToString()).IsEqualTo("billing.charge-card;v=2");
        await Assert.That(Get(headers, names.Id)).IsEqualTo(metadata.MessageId.ToString("D"));
        await Assert.That(Get(headers, names.Source)).IsEqualTo("/billing/東京");
        await Assert.That(Get(headers, names.SpecVersion)).IsEqualTo("1.0");
        await Assert.That(Get(headers, names.Time)).IsEqualTo(metadata.Time.UtcDateTime.ToString("O", CultureInfo.InvariantCulture));
        var parsed = ActivityContext.TryParse(Get(headers, names.TraceParent), Get(headers, names.TraceState), true, out var context);
        await Assert.That(parsed).IsTrue();
        await Assert.That(context.TraceId).IsEqualTo(metadata.CreationContext.TraceId);
        await Assert.That(context.SpanId).IsEqualTo(metadata.CreationContext.SpanId);
        await Assert.That(context.TraceFlags).IsEqualTo(ActivityTraceFlags.Recorded);
        await Assert.That(context.TraceState).IsEqualTo("vendor=value");
    }

    [Test]
    public async Task Metadata_generates_unique_UUIDv7_ids_and_UTC_time()
    {
        var before = DateTimeOffset.UtcNow;
        var first = MessageEnvelopeMetadata.Create();
        var second = MessageEnvelopeMetadata.Create();
        var after = DateTimeOffset.UtcNow;

        await Assert.That(first.MessageId.Version).IsEqualTo(7);
        await Assert.That(second.MessageId.Version).IsEqualTo(7);
        await Assert.That(first.MessageId == second.MessageId).IsFalse();
        await Assert.That(first.Time >= before && first.Time <= after).IsTrue();
        await Assert.That(first.Time.Offset).IsEqualTo(TimeSpan.Zero);
    }

    [Test]
    public async Task Retry_preserves_metadata_and_overwrites_existing_attributes()
    {
        var names = new HeaderNames();
        var envelope = new MessageEnvelope(new MessageType("orders", 1), "/orders", names);
        var metadata = MessageEnvelopeMetadata.Create(CreateContext());
        var message = new NativeMessage(false);
        message.Values[names.Id] = new WireString("stale");
        envelope.Write<NativeMessage, NativeBinding>(message, metadata);
        var first = Get(new(message, names), names.Id);
        envelope.Write<NativeMessage, NativeBinding>(message, metadata);

        await Assert.That(Get(new(message, names), names.Id)).IsEqualTo(first);
        await Assert.That(message.Values.Count).IsEqualTo(7);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Missing_trace_context_removes_stale_headers(bool hasContext)
    {
        var names = new HeaderNames();
        var envelope = new MessageEnvelope(new MessageType("orders", 1), "/orders", names);
        var message = new NativeMessage(false);
        message.Values[names.TraceParent] = new WireString("stale");
        message.Values[names.TraceState] = new WireString("stale");
        var context = hasContext ? new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.None) : default;
        envelope.Write<NativeMessage, NativeBinding>(message, MessageEnvelopeMetadata.Create(context));

        await Assert.That(message.Values.ContainsKey(names.TraceState)).IsFalse();
        await Assert.That(message.Values.ContainsKey(names.TraceParent)).IsEqualTo(hasContext);
        if (hasContext)
        {
            await Assert.That(Get(new(message, names), names.TraceParent).EndsWith("-00", StringComparison.Ordinal)).IsTrue();
        }
    }

    [Test]
    public async Task Writer_uses_creation_context_instead_of_ambient_activity()
    {
        var names = new HeaderNames();
        var context = CreateContext();
        var metadata = MessageEnvelopeMetadata.Create(context);
        using var ambient = new Activity("dispatch").SetIdFormat(ActivityIdFormat.W3C).Start();
        var message = new NativeMessage(false);
        new MessageEnvelope(new MessageType("orders", 1), "/orders", names)
            .Write<NativeMessage, NativeBinding>(message, metadata);

        ActivityContext.TryParse(Get(new(message, names), names.TraceParent), null, out var written);
        await Assert.That(written.TraceId).IsEqualTo(context.TraceId);
        await Assert.That(written.SpanId).IsEqualTo(context.SpanId);
    }

    [Test]
    public async Task Raw_mode_writes_nothing_and_uses_explicit_type_without_reading_headers()
    {
        var names = new HeaderNames();
        var envelope = new MessageEnvelope(new MessageType("legacy", 1), "/legacy", names, MessageEnvelopeMode.Raw);
        var message = new NativeMessage(false);
        message.Values[names.Type] = new WireString("ignore;v=99");
        var headers = new MessageHeaders<NativeMessage, NativeBinding>(message, names);

        envelope.Write<NativeMessage, NativeBinding>(message, default);
        var type = headers.GetMessageType(MessageEnvelopeMode.Raw, new WireString("orders;v=1"));

        await Assert.That(type.ToString()).IsEqualTo("orders;v=1");
        await Assert.That(message.ReadCount).IsEqualTo(0);
        await Assert.That(message.WriteCount).IsEqualTo(0);
        await Assert.That(message.RemoveCount).IsEqualTo(0);
        await Assert.That(message.Values.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Missing_wire_type_requires_explicit_raw_mapping()
    {
        var headers = new MessageHeaders<NativeMessage, NativeBinding>(new NativeMessage(false), new HeaderNames());
        await Assert.That(() => headers.GetMessageType()).Throws<InvalidOperationException>().WithMessageContaining("raw mode");
        await Assert.That(() => headers.GetMessageType(MessageEnvelopeMode.Raw)).Throws<InvalidOperationException>().WithMessageContaining("mapping");
        await Assert.That(() => headers.GetMessageType((MessageEnvelopeMode)99)).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Native_header_lookup_is_lazy_and_preserves_original_storage()
    {
        var message = new NativeMessage(true);
        var bytes = Encoding.UTF8.GetBytes("héllo 東京");
        message.Values["custom"] = new WireString(bytes);
        var headers = new MessageHeaders<NativeMessage, NativeBinding>(message, new HeaderNames());
        await Assert.That(message.ReadCount).IsEqualTo(0);
        var found = headers.TryGetValue("custom", out var value);
        await Assert.That(found).IsTrue();
        await Assert.That(message.ReadCount).IsEqualTo(1);
        await Assert.That(value.Utf8.Equals(bytes.AsMemory())).IsTrue();
        await Assert.That(headers.TryGetValue("absent", out _)).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WireString_formats_unicode_into_caller_storage(bool utf8)
    {
        const string text = "héllo 東京 😀";
        var value = utf8 ? new WireString(Encoding.UTF8.GetBytes(text)) : new WireString(text);
        var formatted = Format(value);
        await Assert.That(formatted).IsEqualTo(text);
        await Assert.That(value.ToString()).IsEqualTo(text);
        await Assert.That(value.TryFormat(new char[2], out var written)).IsFalse();
        await Assert.That(written).IsEqualTo(0);
    }

    [Test]
    public async Task Unaccessed_headers_and_core_writes_allocate_zero_bytes()
    {
        var names = new HeaderNames();
        var message = new NativeMessage(false);
        var envelope = new MessageEnvelope(new MessageType("orders", 1), "/orders", names);
        var metadata = MessageEnvelopeMetadata.Create(CreateContext());
        var buffer = new HeaderBuffer();
        RunAllocationProbe(message, names, envelope, buffer, metadata);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var result = RunAllocationProbe(message, names, envelope, buffer, metadata);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        await Assert.That(allocated).IsEqualTo(0L);
        await Assert.That(message.ReadCount).IsEqualTo(0);
        await Assert.That(result).IsEqualTo(1000);
    }

    [Test]
    public async Task Empty_id_is_rejected_before_any_headers_are_written()
    {
        var envelope = new MessageEnvelope(new MessageType("orders", 1), "/orders", new HeaderNames());
        var message = new NativeMessage(false);
        await Assert.That(() => envelope.Write<NativeMessage, NativeBinding>(message, default))
            .Throws<ArgumentException>().WithMessageContaining("message ID");
        await Assert.That(message.WriteCount).IsEqualTo(0);
    }

    private static int RunAllocationProbe(NativeMessage message, HeaderNames names, MessageEnvelope envelope,
        HeaderBuffer buffer, MessageEnvelopeMetadata metadata)
    {
        var count = 0;
        for (var i = 0; i < 1000; i++)
        {
            var headers = new MessageHeaders<NativeMessage, NativeBinding>(message, names);
            if (!headers.GetMessageType(MessageEnvelopeMode.Raw, new WireString("orders;v=1")).IsEmpty)
            {
                count++;
            }
            envelope.Write<HeaderBuffer, BufferWriter>(buffer, metadata);
        }
        return count;
    }

    private static string Format(WireString value)
    {
        Span<char> buffer = stackalloc char[64];
        return value.TryFormat(buffer, out var written) ? new string(buffer[..written]) : "failed";
    }

    private static string Get(MessageHeaders<NativeMessage, NativeBinding> headers, string name) =>
        headers.TryGetValue(name, out var value) ? value.ToString() : throw new InvalidOperationException(name);

    private static ActivityContext CreateContext() => new(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded, "vendor=value");

    private sealed class NativeMessage(bool utf8)
    {
        public Dictionary<string, WireString> Values { get; } = new(StringComparer.Ordinal);
        public bool Utf8 { get; } = utf8;
        public int ReadCount { get; set; }
        public int WriteCount { get; set; }
        public int RemoveCount { get; set; }
    }

    private readonly struct NativeBinding : IMessageHeaderReader<NativeMessage>, IMessageHeaderWriter<NativeMessage>
    {
        public static bool TryGetHeader(NativeMessage message, string name, out WireString value)
        {
            message.ReadCount++;
            return message.Values.TryGetValue(name, out value);
        }

        public static void SetHeader(NativeMessage message, string name, ReadOnlySpan<char> value)
        {
            message.WriteCount++;
            if (message.Utf8)
            {
                var bytes = new byte[Encoding.UTF8.GetByteCount(value)];
                Encoding.UTF8.GetBytes(value, bytes);
                message.Values[name] = new WireString(bytes);
            }
            else
            {
                message.Values[name] = new WireString(new string(value));
            }
        }

        public static void RemoveHeader(NativeMessage message, string name)
        {
            message.RemoveCount++;
            message.Values.Remove(name);
        }
    }

    private sealed class HeaderBuffer
    {
        public char[] Buffer { get; } = new char[64];
    }

    private readonly struct BufferWriter : IMessageHeaderWriter<HeaderBuffer>
    {
        public static void SetHeader(HeaderBuffer message, string name, ReadOnlySpan<char> value) => value.CopyTo(message.Buffer);
        public static void RemoveHeader(HeaderBuffer message, string name) { }
    }
}
