---
title: Apache Pulsar (planned)
description: "Pulsar transport status and native client prerequisites."
---

`Relayer.Pulsar` is planned under [#315](https://github.com/thomhurst/Relayer/issues/315). There is no Pulsar package implementation or registration API yet. Send, receive, settlement, topology, tracing, transport conformance and native-client performance comparisons remain unimplemented.

## Native client prerequisites

The client assessment checked the official [DotPulsar 5.3.4](https://github.com/apache/pulsar-dotpulsar/releases/tag/5.3.4), including its source and current `master`. Two upstream prerequisites prevent an adapter from meeting Relayer's current contracts:

- **UTF-8 message properties:** the native [`MessageMetadata` indexer](https://github.com/apache/pulsar-dotpulsar/blob/5.3.4/src/DotPulsar/MessageMetadata.cs) accepts only strings. Relayer's default CloudEvents path keeps changing values, including message IDs and trace context, in UTF-8 buffers. Converting those values into new strings would add allocations in Relayer's send path. [DotPulsar #301](https://github.com/apache/pulsar-dotpulsar/issues/301) requests a native byte-backed property API.
- **Native AOT and trimming:** a .NET 10 byte-array consumer probe still reaches reflection-based compression discovery without enabling producer compression or dynamic schemas. Native AOT publishing emits analysis warnings and fails when those warnings are treated as errors. [DotPulsar #288](https://github.com/apache/pulsar-dotpulsar/issues/288) tracks compatibility; it includes the 5.3.4 reproduction. This is a publish-time finding, not a claim that every untrimmed application fails.

The alternative [Pulsar.Client](https://github.com/fsprojects/pulsar-client-dotnet) also requires an `IReadOnlyDictionary<string, string>` for outgoing message properties in its [native producer API](https://github.com/fsprojects/pulsar-client-dotnet/blob/ebbf2851681d3ab14fbcb50afc277ced59802376/src/Pulsar.Client/Api/IProducer.fs). Switching clients therefore does not remove the property-conversion prerequisite.

Implementation can resume when a maintained native client provides a suitable property API and a verified Native AOT byte-message path. Relayer will reuse native recovery, flow control, ordering and acknowledgement rather than replace the native publish protocol. No Pulsar capabilities, conformance results or performance compliance are advertised before the adapter is implemented and tested against a real broker.
