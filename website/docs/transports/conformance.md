---
title: Transport conformance
description: "Run the reusable native transport contracts against a Relayer adapter."
---

`Relayer.Testing` provides `TransportConformanceSuite<TIncoming, TBinding>` for transport authors. It has no test-framework dependency. Each case creates a fresh fixture, runs the generated handler through the core receive pipeline, and calls the adapter's real send and settlement operations. Contract failures throw; missing endpoint capabilities return an explicit skip reason.

Add `Relayer.Testing` to your test project and implement `ITransportConformanceFixture<TIncoming>` using your native adapter. Use its existing `IMessageBinding<TIncoming>` as `TBinding`. The fixture receives the suite's isolated `MessageTracing` owner: pass it to the adapter's production tracing integration and enable the native client's instrumentation. The suite captures only that owner's `Relayer` source, so parallel fixtures do not share results.

```csharp
var suite = new TransportConformanceSuite<NativeDelivery, NativeBinding>(
    async (tracing, cancellationToken) =>
        await BrokerFixture.CreateAsync(tracing, cancellationToken),
    timeout: TimeSpan.FromMinutes(2));

foreach (var scenario in Enum.GetValues<TransportConformanceCase>())
{
    var result = await suite.RunAsync(scenario, cancellationToken);
    if (result.Skipped)
        Console.WriteLine($"Skipped {scenario}: {result.SkipReason}");
}
```

`NativeDelivery`, `NativeBinding` and `BrokerFixture` above are your adapter's types. Prefer a separate parameterized test for each enum value. Translate `result.Skipped` into your framework's skip mechanism, preserving `SkipReason`; let exceptions fail the test. A broker connection failure or missing tracing is a failure, never a capability skip.

Scenario failures identify the case and active phase, such as `send 2`, `receive completion` or `broker reconnect`, and retain the original exception as the inner exception. Transport shutdown receives a separate ten-second cancellation budget even when the scenario deadline or caller token has expired. If shutdown or fixture disposal also fails, an `AggregateException` retains both the scenario failure and cleanup failures. Fixture disposal must still release its owned resources; the shutdown budget does not impose a disposal deadline.

## Fixture responsibilities

- Provision an empty, isolated endpoint with capacity for at least 32 small messages. Supply its destination, CloudEvents `HeaderNames`, expected OpenTelemetry `messaging.system` and native completion operation name.
- Expose the production `ITransport`. `RegisterConsumer` connects the supplied receive callback to the native consumer without adding another process span. The suite composes generated dispatch, process tracing and settlement around the callback.
- Implement `SettleAsync` with the production native complete, redeliver and dead-letter operations. Use their existing settlement instrumentation; do not manufacture spans to satisfy the suite. Do not settle again when the callback returns `Disposition.SettledByHandler`.
- Read dead letters from the actual native destination and return an owned `ConformanceDeadLetter` snapshot. Preserve original headers and error reason/description.
- Make `DisconnectAsync` interrupt the broker connection and `ReconnectAsync` restore connectivity, allowing the adapter to recover. For a real broker, merely stopping and restarting a healthy consumer is insufficient. The in-memory fixture uses its native stop/start lifecycle because it has no connection.
- Observe cancellation in provisioning, receive waits, fault injection and lifecycle operations. Dispose receivers and all resources owned by this fixture, including after failures. The default scenario deadline is 30 seconds; increase it explicitly for container startup.

The reference fixture and executable tests live in [`tests/Relayer.Conformance.Tests`](https://github.com/thomhurst/Relayer/tree/main/tests/Relayer.Conformance.Tests). Queue and partitioned log modes both run every case. Separate tests verify capability skips and prove that missing send tracing or overwritten creation headers fail.

The Kafka adapter runs its supported cases against Kafka in Testcontainers, including sends and consumer shutdown while the broker process is stopped. Service Bus runs queue delivery, immediate redelivery, native dead-lettering, scheduling, tracing, creation-context copies and emulator restart recovery, plus native session ordering. Service Bus explicitly skips positive delayed redelivery because native abandon has no delay. Its receive recovery fixtures put owned endpoints in the emulator startup configuration: administration-created entities do not persist across an emulator restart. A separate provisioning test creates a queue through administration, proves it is missing after restart, reapplies `Provision`, and verifies native send/receive/complete on the restored queue. Startup-config persistence alone is not evidence of provisioning. `Provision` is reapplied through `ApplyTopologyAsync`; native connection recovery does not automatically recreate deleted entities. Kafka currently supports only `TopologyMode.None`, so its restart test preserves externally managed topics rather than claiming reprovisioning. See [Service Bus validation boundaries](service-bus.md#health).

## Contracts

| Case | Checks | Native capability required |
|---|---|---|
| `SendReceiveSettle` | Body/header ownership, delivery, completion and no replay after consumer restart | None |
| `Retry` | Redelivery, unchanged body/creation headers and increasing delivery count when the broker supplies one | `MaxRedeliveryDelay` is not null |
| `DeadLetter` | Original body/creation headers, reason/description and healthy-message progress | `DeadLetter` |
| `Delay` | Scheduled message arrives at or after its due time | Positive `MaxDeliveryDelay` |
| `RedeliveryDelay` | Redelivery arrives at or after its due time | Positive `MaxRedeliveryDelay` |
| `OrderingByKey` | 32 messages sent with one key arrive in native send order | `OrderedByKey` |
| `TracePropagation` | Producer creation context survives native instrumentation; process links, operation names/kinds, native settlement and system identifier match the conventions | None |
| `CreationContextCopies` | Stored `traceparent`/`tracestate` survive repeated serialized sends; each client send links to the original creation context | None |
| `Reconnect` | Delivery resumes after connection interruption and recovery | None |
| `CleanShutdown` | Stop with a handler in flight, drain before closure, native lifecycle/durability evidence and no failure logs through disposal | None; requires `ITransportShutdownConformanceFixture` |
| `SendDuringOutage` | Concurrent outage sends and offline shutdown finish within deadlines; recovery delivers every successfully acknowledged message at least once | Fixture supplies a positive `OutageSendTimeout` |

Delay assertions use the smaller of 200 ms and the endpoint's native maximum. They compare the declared due time with handler entry; they do not rely on sleeps or require a maximum delivery latency. Queue settlement spans must run inside their process span. Log commits may be amortized outside processing, as the native log model requires.

The copy contract exercises the adapter boundary used by outbox dispatch, scheduled re-sends and dead-letter copies. It does not run a durable outbox worker or replace its integration tests. Native dead-letter preservation has its own case. Transport-specific restart/rebalance chaos tests, batching, replay, transactions and client-specific guarantees remain additional adapter tests.

## Graceful stop

`CleanShutdown` sends one message (with a key on ordered endpoints, including Service Bus sessions) and holds its handler at an asynchronous gate. Once the handler has entered, the suite starts a bounded graceful stop, rejects an already-completed stop while processing is still held, releases the handler and awaits shutdown. A synchronous completion observer also rejects asynchronous stop completion before the full in-flight consumer pipeline, including settlement, finishes. No sleeps or process-wide log capture are used. The fixture must also implement `ITransportShutdownConformanceFixture`; absence of this evidence is a failure, never a capability skip.

Give each fixture its own `ConformanceLogCapture` and expose it as `Logs`. Register that provider with every logger factory used by the adapter and its native clients **before** creating them, with a minimum level of `Warning` or lower. Keep the capture available after fixture disposal. The suite rejects `Error` and `Critical` entries and warning-level `OperationCanceledException` entries, including those emitted during disposal. Informational cancellation messages are allowed. A transport with no logging, such as the in-memory transport, still exposes its own empty capture.

Implement `VerifyShutdownAsync` using native lifecycle instrumentation and durable broker state. Check that no settlement, renewal or send occurred after the corresponding receiver or sender closed. Verify that the in-flight message either completed durably or remains available for native redelivery. Do not treat a successful `StopAsync` alone as proof: the hook must throw on any violation. For an in-process transport, its receive-loop completion and actual message completion provide the reference evidence. External adapters need their real broker or emulator and client-specific observations; the harness does not emulate native closure or acknowledgements.

Queue and log in-memory fixtures, Kafka, Service Bus queue and session processors, and Google Pub/Sub run this case against their production receive/send lifecycles. Native cases use real brokers or emulators:

| Adapter | Native lifecycle and durability evidence |
|---|---|
| Kafka | Forwarding probes instrument the real Dekaf consumer, producer and their facets through disposal. The receive loop must drain and the broker must report the in-flight record's committed offset. Adapter and Dekaf logs share a fixture-local capture. |
| Service Bus | Fixture-filtered SDK events record successful broker completion, client closure, sends and automatic message/session renewals. Queue and session shutdown run separately. SDK events and production adapter logs feed the same capture through disposal. |
| Google Pub/Sub | A pass-through gRPC interceptor observes broker-confirmed acknowledgement RPCs, streaming-pull closure, renewal calls and admitted publish RPCs. A forwarding subscriber observes actual native stop/disposal completion. The native subscriber must finish and the broker subscription must remain durable for redelivery if an acknowledgement was not persisted. API, gRPC and high-level native client logs use fixture-local capture. |

SQS ([#224](https://github.com/thomhurst/Relayer/issues/224)), RabbitMQ ([#274](https://github.com/thomhurst/Relayer/issues/274)), RabbitMQ Streams ([#316](https://github.com/thomhurst/Relayer/issues/316)) and NATS ([#313](https://github.com/thomhurst/Relayer/issues/313)) do not yet implement production receivers. Their graceful-stop adoption is planned alongside receive execution; they are not passing cases or capability skips. A fixture must not simulate native delivery to claim coverage.

Shared outbox shutdown contracts separately validate claims, database disposal and owned leases; this transport case does not validate an outbox worker.

## Outage sends

Opt into `SendDuringOutage` by supplying `ITransportConformanceFixture.OutageSendTimeout` and, when needed, `IsExpectedOutageFailure` for native transient connectivity or deadline exceptions. Other exceptions fail the contract; caller/scenario cancellation is never treated as a successful outage. Fixtures without this fault-injection contract return an explicit skip. Kafka, Service Bus and Google Pub/Sub run this case against their broker or emulator; adapters whose receive paths remain planned do not claim this coverage.

The fixture must return from `DisconnectAsync` only after connectivity is interrupted. With consumers already started, the suite attempts three concurrent sends and passes a deadline token to the adapter. Configure a native publish/retry deadline as well when the native API cannot cancel admitted sends: the Pub/Sub fixture uses the native API client's publish expiration. An independent watchdog fails a send that ignores its budget. The suite then requests offline shutdown with a ten-second token and a fifteen-second watchdog, restores the broker, restarts consumers and sends a recovery marker.

Successful send acknowledgements must result in delivery after recovery. Failed or cancelled sends can still have been accepted by a broker, so their arrival and duplicate deliveries are allowed. The test does not retract messages, replace native retry policies, impose cross-message ordering, or reinterpret native acknowledgements.
