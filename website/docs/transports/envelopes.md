---
title: Envelopes and headers
description: "The CloudEvents envelope Relayer writes, and how transports map header reads and writes to native messages."
---

The core owns the CloudEvents envelope. A transport maps individual header reads and writes to its native message; it does not copy the incoming header collection into a dictionary. Kafka, Service Bus sending and Google Pub/Sub use these primitives in their native adapters.

Configure the envelope once per route. Capture metadata once per logical message, then preserve it across retries, outbox dispatches, scheduled copies and dead-letter copies:

```csharp
using Relayer.Transports;
var names = new HeaderNames(); // ce_ binding, for example Kafka
var envelope = new MessageEnvelope(
    new MessageType("billing.charge-card", 2), "/billing", names);

// Per logical message; pass the creation span's context explicitly.
var metadata = MessageEnvelopeMetadata.Create(creationContext);
envelope.Write<NativeOutgoing, NativeHeaderWriter>(nativeMessage, metadata);
```

`MessageEnvelopeMetadata.Create` generates a UUIDv7 with `Guid.CreateVersion7()` and captures UTC time. The writer emits `ce_id`, `ce_type` (`billing.charge-card;v=2`), `ce_source`, `ce_time` (UTC round-trip format) and `ce_specversion` (`1.0`). `traceparent` and optional `tracestate` carry the supplied creation context, even when an unrelated dispatch activity is current. Without a valid creation context, the writer removes stale trace headers instead of propagating another message's context. With a context but no trace state, it removes stale `tracestate`.

Use `new HeaderNames("cloudEvents:")` for the AMQP attribute prefix and `new HeaderNames("ce-")` for [Google Pub/Sub](google-pubsub.md). Transports use the names for their protocol binding and implement the native property types where required. The Kafka and AMQP binary mappings are defined by the [Kafka binding](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/kafka-protocol-binding.md) and [AMQP binding](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/amqp-protocol-binding.md). Relayer carries W3C creation context in the unprefixed native `traceparent`/`tracestate` headers described in design §8.2.

A transport's reader is a struct implementing `IMessageHeaderReader<TIncoming>` with a static `TryGetHeader` method. Return `new WireString(nativeString)` or `new WireString(nativeUtf8Memory)` without decoding. Its writer implements `IMessageHeaderWriter<TOutgoing>` with static `SetHeader` and `RemoveHeader` methods. Set replaces an existing header; it must consume or copy the supplied character span before returning. UTF-8 transports encode directly into native storage. String-based clients create their required native strings at this boundary. Core formatting uses stack storage and creates no intermediate strings.

```csharp
using Relayer.Transports;
var headers = new MessageHeaders<NativeIncoming, NativeHeaderReader>(incoming, names);
var wireType = headers.GetMessageType();
if (headers.TryGetValue("tenant", out var tenant))
{
    // Access native text/memory, or decode only when needed.
    var text = tenant.ToString();
}
```

Constructing a `MessageHeaders` view reads nothing and allocates nothing. Each lookup reads only the requested native header. `WireString.Text` and `WireString.Utf8` expose the original storage; `TryFormat` copies or decodes into caller-owned character storage. `ToString()` returns the original UTF-16 string, or decodes UTF-8 on demand. Do not retain the view or its memory beyond the native message's lifetime. A binding that reuses receive buffers must keep those buffers alive while handlers use them.

For a legacy producer, raw mode leaves the native payload and headers untouched. Map the stable wire type explicitly, for example from a topic:

```csharp
var wireType = headers.GetMessageType(
    MessageEnvelopeMode.Raw, new WireString(typeMappedFromTopic));
```

Raw type resolution does not inspect `ce_type`, even when that header exists. A missing mapping throws an actionable error. To send without an envelope, configure the route's `MessageEnvelope` with `MessageEnvelopeMode.Raw`; `Write` then does nothing, including no trace header changes. Payload serialization remains separate from envelope handling.
