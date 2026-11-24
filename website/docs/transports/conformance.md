---
title: Transport conformance
description: "Run the reusable native transport contracts against a Relayer adapter."
---

`Relayer.Testing` provides `TransportConformanceSuite<TIncoming, TBinding>` for transport authors. It has no test-framework dependency. Each case creates a fresh fixture, runs the generated handler through the core receive pipeline, and calls the adapter's real send and settlement operations. Contract failures throw; missing endpoint capabilities return an explicit skip reason.

Add `Relayer.Testing` to your test project and implement `ITransportConformanceFixture<TIncoming>` using your native adapter. Use its existing `IMessageBinding<TIncoming>` as `TBinding`. The fixture receives the suite's isolated `MessageTracing` owner: pass it to the adapter's production tracing integration and enable the native client's instrumentation. The suite captures only that owner's `Relayer` source, so parallel fixtures do not share results.

```csharp
using Relayer.Testing;

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

The scenario deadline defaults to 30 seconds. An explicit `timeout` must be greater than zero and less than 4,294,967,295 milliseconds. The cancellation timer truncates fractional milliseconds. The suite constructor rejects values outside this range with `ArgumentOutOfRangeException` for `timeout`.

The initial `StartAsync` wait is bounded by the scenario deadline and caller cancellation. If the start task is still pending when that wait is canceled, the suite reports a failure during `transport start` and attempts shutdown.

Scenario failures identify the case and active phase, such as `send 2`, `receive completion` or `broker reconnect`, and retain the original exception as the inner exception. Scenario send, drain, restart and dead-letter destination read waits, plus the `DisconnectAsync` and `ReconnectAsync` hook waits in `Reconnect` and `SendDuringOutage`, fail at the scenario deadline or on caller cancellation. Cleanup is attempted even when the adapter, fixture or hook task stays pending. A send that outlives the wait retains access to its unchanged borrowed body and headers until the adapter operation completes. Transport shutdown receives a separate ten-second cancellation budget even when the scenario deadline or caller token has expired. If shutdown or fixture disposal also fails, an `AggregateException` retains both the scenario failure and cleanup failures.

If a lifecycle wait ends before its operation completes, the suite proceeds with cleanup and observes any eventual fault in the background. This applies to start, drain, restart, offline and final shutdown, broker lifecycle hooks, and native shutdown and settlement verification. In `CleanShutdown`, this also covers failures from checking in-flight stop ordering. The suite reports the original phase failure or watchdog timeout. Final transport shutdown has a fifteen-second watchdog for adapters that ignore the ten-second cancellation token.

After transport shutdown, the suite waits for fixture disposal with a separate fifteen-second cleanup budget, independent of the scenario deadline and caller cancellation. A stalled disposal fails with a `TimeoutException` identifying fixture disposal; any original scenario or shutdown failure remains in the aggregate. Each disposal operation is invoked and consumed once. The timeout ends only the suite's wait: disposal continues, late faults are observed, and the fixture remains responsible for stopping receivers and releasing its owned resources.

The scenario deadline and caller cancellation also bound the asynchronous fixture creation wait. A creation failure identifies the `fixture creation` phase and retains the original exception. If the caller token is already canceled, the suite fails before invoking the fixture factory and retains the `OperationCanceledException` as the inner exception. If the suite stops waiting before the factory completes, it disposes any fixture returned later exactly once. Late factory and disposal failures are observed in background cleanup; they cannot be added to the failure already returned to the caller. The factory must return promptly with its asynchronous operation, honor the supplied cancellation token, and clean up partially provisioned resources when it fails or cancels before returning a fixture. The suite can dispose only a fixture the factory returns.

## Fixture responsibilities

- Provision an empty, isolated endpoint with capacity for at least 32 small messages. Supply its destination, CloudEvents `HeaderNames`, expected OpenTelemetry `messaging.system` and native completion operation name.
- Set `DefaultOrderingKey` when the endpoint requires a native group or session key for every send. The suite uses it for ordinary sends and outage probes. The ordering case supplies its own key.
- Expose the production `ITransport`. `RegisterConsumer` connects the supplied receive callback to the native consumer without adding another process span. The suite composes generated dispatch, process tracing and settlement around the callback.
- Implement `SettleAsync` with the production native complete, redeliver and dead-letter operations. Use their existing settlement instrumentation; do not manufacture spans to satisfy the suite. Do not settle again when the callback returns `Disposition.SettledByHandler`.
- Read dead letters from the actual native destination and return an owned `ConformanceDeadLetter` snapshot. Preserve original headers and error reason/description.
- Make `DisconnectAsync` interrupt the broker connection and `ReconnectAsync` restore connectivity, allowing the adapter to recover. For a real broker, merely stopping and restarting a healthy consumer is insufficient. The in-memory fixture uses its native stop/start lifecycle because it has no connection.
- Observe cancellation in provisioning, receive waits, fault injection and lifecycle operations. Dispose receivers and all resources owned by this fixture, including after failures. The default scenario deadline is 30 seconds; increase it explicitly for container startup.

The [in-memory reference tests](https://github.com/thomhurst/Relayer/blob/main/tests/Relayer.Conformance.Tests/InMemoryConformanceTests.cs) run `SendReceiveSettle`, `Retry`, `DeadLetter`, `Delay`, `RedeliveryDelay`, `OrderingByKey`, `TracePropagation`, `CreationContextCopies`, `Reconnect` and `CleanShutdown` in both queue and partitioned log modes. Separate tests verify capability skips and prove that missing send tracing or overwritten creation headers fail. See the [controlled outage fixture](https://github.com/thomhurst/Relayer/blob/main/tests/Relayer.Conformance.Tests/OutageConformanceTests.cs) for `SendDuringOutage` and the [real-broker RabbitMQ fixture](https://github.com/thomhurst/Relayer/blob/main/tests/Relayer.Tests/RabbitMqConformanceTests.cs) for `FailureHandlingFailure`.

SQS runs receive/settle, immediate and delayed visibility retries, standard-queue timers, tracing, restart, outage recovery and clean shutdown against Floci. FIFO fixtures also run receive/settle, outage recovery and group ordering. Its native dead-letter case is a capability skip; separate production-host tests verify poison copies with failure metadata before deletion, for standard and FIFO queues.

RabbitMQ runs receive/settle, immediate requeue, recovery after failure handling throws, consumer restart, tracing, broker recovery, outage sends and clean shutdown against a real broker. Native metadata-preserving dead-lettering, delayed requeue and ordering-by-key remain capability skips; separate RabbitMQ tests cover confirmed poison copies and native DLX routing.

NATS JetStream runs receive/settle, tracing, creation-context copies, reconnect, outage sends and clean shutdown against a real server through the production durable pull consumer. Portable retry, native dead-lettering with error metadata, scheduled sends, delayed redelivery and ordering-by-key remain explicit capability skips. See [NATS delivery and failure boundaries](nats.md#delivery-and-failure-boundaries) for native acknowledgement behavior and durable poison-message copies.

The Kafka adapter runs its supported cases against Kafka in Testcontainers, including sends and consumer shutdown while the broker process is stopped. Service Bus runs queue delivery, immediate redelivery, native dead-lettering, scheduling, tracing, creation-context copies and emulator restart recovery, plus native session ordering. Service Bus explicitly skips positive delayed redelivery because native abandon has no delay. Its receive recovery fixtures put owned endpoints in the emulator startup configuration: administration-created entities do not persist across an emulator restart. A separate provisioning test creates a queue through administration, proves it is missing after restart, reapplies `Provision`, and verifies native send/receive/complete on the restored queue. Startup-config persistence alone is not evidence of provisioning. `Provision` is reapplied through `ApplyTopologyAsync`; native connection recovery does not automatically recreate deleted entities. Kafka’s restart test uses externally provisioned topics. The default `TopologyMode.Validate` checks that declared topics exist at startup and compares explicitly declared native settings. `TopologyMode.Provision` creates missing topics, and `TopologyMode.None` skips topology checks. See [Kafka topic topology](kafka.md#topic-topology). See [Service Bus validation boundaries](service-bus.md#health).

The PostgreSQL local fixture runs `SendReceiveSettle`, `OrderingByKey`, `TracePropagation`, `CreationContextCopies`, `Reconnect` and `CleanShutdown` against an isolated database on a real PostgreSQL server. It verifies capability skips for `Retry`, `DeadLetter`, `Delay` and `RedeliveryDelay`. Its `Reconnect` case disables new connections to that database and terminates its existing sessions, then verifies that PostgreSQL refuses a new connection. It restores access to the same database and verifies that delivery resumes. After the production dispatch workers drain, the test checks that `relayer_outbox` is empty, proving that the recovered delivery was durably settled. The interruption is scoped to the fixture's database; other databases on the shared server remain available.

## Contracts

If the caller cancels or the scenario deadline expires while the dead-letter read is pending, `DeadLetter` reports a failure during `dead-letter receive` and proceeds with bounded cleanup. The suite observes any later read fault in the background.

| Case | Checks | Native capability required |
|---|---|---|
| `SendReceiveSettle` | Body/header ownership, no replay after consumer restart and native completion of the restart marker | None; requires `VerifySettlementAsync` |
| `Retry` | Redelivery, unchanged body/creation headers and increasing delivery count when the broker supplies one | `MaxRedeliveryDelay` is not null |
| `FailureHandlingFailure` | The first handler attempt throws and the failure handler also throws. The native transport redelivers the original body and creation headers, then delivers a subsequent healthy message. | Native redelivery (no capability skip) |
| `DeadLetter` | Original body/creation headers, reason/description and healthy-message progress | `DeadLetter` |
| `Delay` | Scheduled message arrives at or after its due time | Positive `MaxDeliveryDelay` |
| `RedeliveryDelay` | Redelivery arrives at or after its due time | Positive `MaxRedeliveryDelay` |
| `OrderingByKey` | 32 messages sent with one key arrive in native send order | `OrderedByKey` |
| `TracePropagation` | Producer creation context survives native instrumentation; process links, operation names/kinds, native settlement and system identifier match the conventions | None |
| `CreationContextCopies` | Stored `traceparent`/`tracestate` survive repeated serialized sends; each client send links to the original creation context | None |
| `Reconnect` | The restarted receiver delivers a warm-up message before disconnect, then delivers a distinct recovery marker after reconnect | None |
| `CleanShutdown` | Stop with a handler in flight, drain before closure, native lifecycle/durability evidence and no failure logs through disposal | None; requires `ITransportShutdownConformanceFixture` |
| `SendDuringOutage` | The restarted receiver delivers a distinct warm-up before disconnect. Outage sends and offline shutdown finish within deadlines. Recovery delivers the marker and every acknowledged outage send. | Fixture supplies an `OutageSendTimeout` in the supported range |

`OrderingByKey` lets each handler complete and settle while the 32 same-key sends proceed. It verifies native send order without holding a delivery lease during later sends or requiring capacity for 32 unsettled messages. Native admission backpressure remains active.

Delay assertions use the smaller of 200 ms and the endpoint's native maximum. They compare the declared due time with handler entry; they do not rely on sleeps or require a maximum delivery latency. Queue settlement spans must run inside their process span. Log commits may be amortized outside processing, as the native log model requires.

The copy contract exercises the adapter boundary used by outbox dispatch, scheduled re-sends and dead-letter copies. It does not run a durable outbox worker or replace its integration tests. Native dead-letter preservation has its own case. Transport-specific restart/rebalance chaos tests, batching, replay, transactions and client-specific guarantees remain additional adapter tests.

## Send, receive and settle

Implement `ITransportConformanceFixture<TIncoming>.VerifySettlementAsync` to confirm that both messages, including the post-restart marker, completed durably. The suite stops the restarted consumer, then calls the hook with the scenario token before fixture disposal. Use native state or broker-confirmed settlement, such as a queue with no ready or in-flight messages or a committed consumer offset past the marker. Missing verification fails the case.

## Graceful stop

`CleanShutdown` sends one message (with a key on ordered endpoints, including Service Bus sessions) and holds its handler at an asynchronous gate. Once the handler has entered, the suite starts a bounded graceful stop, rejects an already-completed stop while processing is still held, releases the handler and awaits shutdown. A synchronous completion observer also rejects asynchronous stop completion before the full in-flight consumer pipeline, including settlement, finishes. If the stop task is still pending at the scenario deadline, the suite reports a failure during in-flight transport shutdown and attempts cleanup with a separate bounded budget. No sleeps or process-wide log capture are used. The fixture must also implement `ITransportShutdownConformanceFixture`; absence of this evidence is a failure, never a capability skip.

Give each fixture its own `ConformanceLogCapture` and expose it as `Logs`. Register that provider with every logger factory used by the adapter and its native clients **before** creating them, with a minimum level of `Warning` or lower. Keep the capture available after fixture disposal. The suite rejects `Error` and `Critical` entries and warning-level `OperationCanceledException` entries, including those emitted during disposal. Informational cancellation messages are allowed. The in-memory fixture registers its capture with the logger factory it passes to `InMemoryTransport`.

Implement `VerifyShutdownAsync` using native lifecycle instrumentation and durable broker state. The suite waits for verification until the scenario deadline or caller cancellation. If verification is still pending, the suite reports a failure during native shutdown verification, attempts transport cleanup with its separate budget, and disposes the fixture. Check that no settlement, renewal or send occurred after the corresponding receiver or sender closed. Verify that the in-flight message either completed durably or remains available for native redelivery. Do not treat a successful `StopAsync` alone as proof: the hook must throw on any violation. For an in-process transport, its receive-loop completion and actual message completion provide the reference evidence. External adapters need their real broker or emulator and client-specific observations; the harness does not emulate native closure or acknowledgements.

Queue and log in-memory fixtures, Kafka, RabbitMQ, Service Bus queue and session processors, Google Pub/Sub, NATS JetStream, SQS and PostgreSQL local run this case against their production receive/send lifecycles. Native cases use real brokers or emulators:

| Adapter | Native lifecycle and durability evidence |
|---|---|
| Kafka | Forwarding probes instrument the real Dekaf consumer, producer and their facets through disposal. The receive loop must drain and the broker must report the in-flight record's committed offset. Adapter and Dekaf logs share a fixture-local capture. |
| Service Bus | Fixture-filtered SDK events record successful broker completion, client closure, sends and automatic message/session renewals. Queue and session shutdown run separately. SDK events and production adapter logs feed the same capture through disposal. |
| RabbitMQ | The production consumer drains in-flight handlers and native acknowledgement before closing its channel; the real broker retains the queue and confirms completion across receiver restart. |
| NATS JetStream | The production pull worker must finish before shutdown verification. The fixture queries the real durable consumer and requires zero pending acknowledgements and an ACK floor at the message's stream sequence after confirmed ACK settlement. Native client and adapter logs remain captured through disposal. |
| SQS | The production long-poll workers drain processing and await native deletion. The fixture rejects settlement after shutdown and checks Floci's native ready and in-flight counts after the confirmed delete response; adapter logs remain captured through disposal. |
| Google Pub/Sub | A pass-through gRPC interceptor observes broker-confirmed acknowledgement RPCs, streaming-pull closure, renewal calls and admitted publish RPCs. A forwarding subscriber observes actual native stop/disposal completion; the native publisher shutdown callback records its drained closure. The native subscriber must finish and the broker subscription must remain durable for redelivery if an acknowledgement was not persisted. API, gRPC and high-level native client logs use fixture-local capture. |
| PostgreSQL local | The production dispatch and maintenance workers stop after the in-flight pipeline drains. The fixture queries PostgreSQL and requires zero rows in `relayer_outbox` after stop. Fixture-local worker logs are checked through disposal. |

RabbitMQ Streams ([#316](https://github.com/thomhurst/Relayer/issues/316)) does not yet implement a production receiver. Its graceful-stop adoption is planned alongside receive execution; it is not a passing case or capability skip. A fixture must not simulate native delivery to claim coverage.

Shared outbox shutdown contracts separately validate claims, database disposal and owned leases; this transport case does not validate an outbox worker.

## Reconnect

`Reconnect` sends and receives an initial message, drains and restarts the consumer, then sends a distinct warm-up message. The suite waits for the restarted receiver to deliver the warm-up before calling `DisconnectAsync`. After `ReconnectAsync` restores connectivity, the suite sends and waits for a separate recovery marker. While waiting for the warm-up, the suite skips replays of the initial message. During recovery, it skips replays of the initial message and the warm-up. Any other unexpected delivery fails the case. All steps use the same scenario deadline.

## Outage sends

Opt into `SendDuringOutage` by supplying `ITransportConformanceFixture.OutageSendTimeout` and, when needed, `IsExpectedOutageFailure` for native transient connectivity or deadline exceptions. Other exceptions and caller or scenario cancellation fail the case. Kafka, RabbitMQ, Service Bus, Google Pub/Sub, NATS JetStream and SQS run this case against their broker or emulator. Adapters with planned receive paths have no outage conformance coverage.

Set `OutageSendTimeout` to a positive duration under 4,294,962,295 milliseconds. The largest supported whole-millisecond budget is 4,294,962,294 milliseconds, which leaves five seconds for the watchdog. A null timeout skips the case. A timeout outside the supported range throws `InvalidOperationException` before the suite starts the transport, calls broker fault hooks or sends messages. The suite still disposes the fixture.

The fixture must return from `DisconnectAsync` only after connectivity is interrupted. The suite sends a distinct warm-up after restarting consumers. It skips replays of the initial message and waits for the warm-up within the scenario deadline before disconnecting, so the outage interrupts an established receiver. The suite then attempts three concurrent sends and passes a deadline token to the adapter. Configure a native publish/retry deadline as well when the native API cannot cancel admitted sends: the Pub/Sub fixture uses the native API client's publish expiration. An independent watchdog fails a send that ignores its budget. The suite then requests offline shutdown with a ten-second token and a fifteen-second watchdog, restores the broker, finishes shutdown with connectivity available, restarts consumers and sends a recovery marker. This second shutdown awaits any native drain that outlived the offline stop budget before attempting a restart.

Offline shutdown can surface the same native connectivity fault that already terminated a receiver. `IsExpectedOutageFailure` must recognize that specific observed native failure, not arbitrary shutdown exceptions. `ReconnectAsync` may explicitly dispose and recreate the application when the native client requires it, retaining the broker's topic, queue or subscription. This is application lifecycle recovery, not an adapter retry loop. The Pub/Sub fixture covers its native initial-stream-write TCP reset this way; a deterministic fault-injection test also verifies application replacement. Unrecognized shutdown errors still fail the case.

If the caller cancels, the scenario deadline expires, or the watchdog expires while outage sends are pending, the suite reports the failure in the `outage sends` phase and proceeds with bounded cleanup. Pending sends retain their borrowed buffers until completion. The suite observes late send faults in the background.

After reconnect, the suite requires delivery of the recovery marker and every successfully acknowledged outage send. A broker can also deliver failed or cancelled sends, duplicate deliveries, and replays of either pre-outage marker. The test does not retract messages, replace native retry policies, impose cross-message ordering, or reinterpret native acknowledgements.


Native span destinations can differ from route addresses. Fixtures can override `SendDestinationName` and
`ProcessDestinationName`; both default to `Destination`. RabbitMQ uses exchange/routing-key for sends and
the consumed queue for process and settlement spans. Routing keys and delivery tags are added only to
recorded spans, after sampling; receive samplers see the queue destination. `IMessageBinding.GetProcessTracing` can
return a cached process operation when the actual native destination is only known at delivery time;
null uses the consumer's startup operation. Keep operation caches bounded.
