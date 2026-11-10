---
title: Apache Pulsar (planned)
description: "Pulsar transport status and native client prerequisites."
---

`Relayer.Pulsar` is planned under [#315](https://github.com/thomhurst/Relayer/issues/315). There is no Pulsar package implementation or registration API yet. Send, receive, settlement, topology, tracing, transport conformance and native-client performance comparisons remain unimplemented.

## Native client prerequisites

The client assessment checked the official [DotPulsar 5.3.4](https://github.com/apache/pulsar-dotpulsar/releases/tag/5.3.4), including its source and current `master`. Native AOT compatibility remains the implementation gate:

- **Native AOT and trimming:** a .NET 10 byte-array consumer probe still reaches reflection-based compression discovery without enabling producer compression or dynamic schemas. Native AOT publishing emits analysis warnings and fails when those warnings are treated as errors. [DotPulsar #288](https://github.com/apache/pulsar-dotpulsar/issues/288) tracks compatibility; it includes the 5.3.4 reproduction. This is a publish-time finding, not a claim that every untrimmed application fails.

## Native property allocation policy

The native [`MessageMetadata` indexer](https://github.com/apache/pulsar-dotpulsar/blob/5.3.4/src/DotPulsar/MessageMetadata.cs) accepts only strings. Under [D52](https://github.com/thomhurst/Relayer/blob/main/docs/decisions.md#d52-allocations-a-native-api-requires-count-as-native-client-cost), conversions required by that signature may count as native-client cost when an equivalent raw-client comparison allocates at least as much. The adapter must name those conversions here and pin them with benchmarks or allocation tests; all other Relayer per-message allocations remain subject to the zero-allocation requirement. No such comparison or adapter is implemented yet.

[DotPulsar #301](https://github.com/apache/pulsar-dotpulsar/issues/301) requests a native byte-backed property API as an optimization, rather than an implementation prerequisite. The alternative [Pulsar.Client](https://github.com/fsprojects/pulsar-client-dotnet) also requires an `IReadOnlyDictionary<string, string>` in its [native producer API](https://github.com/fsprojects/pulsar-client-dotnet/blob/ebbf2851681d3ab14fbcb50afc277ced59802376/src/Pulsar.Client/Api/IProducer.fs); that signature alone does not disqualify a client under D52.

Implementation can resume when a maintained released native client provides a verified warning-clean Native AOT byte-message path. Relayer will reuse native recovery, flow control, ordering and acknowledgement rather than replace the native publish protocol. No Pulsar capabilities, conformance results or performance compliance are advertised before the adapter is implemented and tested against a real broker.
