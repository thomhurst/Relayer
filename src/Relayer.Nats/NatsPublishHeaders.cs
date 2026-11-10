using System.Buffers;
using Microsoft.Extensions.Primitives;
using NATS.Client.Core;

namespace Relayer.Nats;

// Native instrumentation injects its context before serialization. The native context-aware
// hook runs before header encoding, so restore stored creation headers here. Keep the payload's
// native generic type unchanged, avoiding larger native request state per publication.
internal sealed class NatsPublishHeaders(bool caseSensitive)
    : NatsHeaders(new Dictionary<string, StringValues>(16, caseSensitive ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase)),
      INatsSerializeWithContext<ReadOnlyMemory<byte>>
{
    internal bool IsFailureCopy;
    internal StringValues TraceParent;
    internal StringValues TraceState;
    public void Serialize(IBufferWriter<byte> writer, ReadOnlyMemory<byte> value) =>
        NatsRawSerializer<ReadOnlyMemory<byte>>.Default.Serialize(writer, value);
    public void Serialize(IBufferWriter<byte> writer, ReadOnlyMemory<byte> value, in NatsMsgContext context)
    {
        var headers = context.Headers ?? throw new InvalidOperationException("NATS publication requires a headers instance.");
        // Publish options can inject MsgId after native callbacks; clear it at serialization too.
        if (IsFailureCopy) headers.Remove("Nats-Msg-Id");
        if (TraceParent.Count > 0) headers["traceparent"] = TraceParent;
        else headers.Remove("traceparent");
        if (TraceState.Count > 0) headers["tracestate"] = TraceState;
        else headers.Remove("tracestate");
        Serialize(writer, value);
    }
    internal void Reset()
    {
        Clear();
        IsFailureCopy = false;
        TraceParent = default;
        TraceState = default;
    }
}
