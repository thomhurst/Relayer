---
title: Request and response
---

Request/response uses the normal [send](sending.md) and [consume](handlers.md) pipelines. Registration is explicit; there is no handler discovery or temporary-queue emulation. Every request requires a finite positive timeout.

## Declare the endpoints

This complete in-memory example registers the request route, reply consumer and responder:

```csharp
services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.InMemory("local", transport =>
    {
        transport.Request<GetPrice, Price>("prices", "price-replies");
        transport.Send<Price>("price-replies");
        transport.Consume("prices", consumer => consumer.Handler<GetPriceHandler>());
    });
});

[Message("get-price")]
public sealed record GetPrice(string Product);

[Message("price")]
public sealed record Price(decimal Amount);

public sealed class GetPriceHandler : IHandler<GetPrice>
{
    public ValueTask HandleAsync(GetPrice request, MessageContext context,
        CancellationToken cancellationToken) =>
        context.ReplyAsync(new Price(12.50m), cancellationToken);
}
```

Inject `IRequestClient<GetPrice, Price>` into the requesting application:

```csharp
var price = await client.RequestAsync(
    new GetPrice("coffee"), TimeSpan.FromSeconds(5), cancellationToken);
```

In separate applications, the requester declares `Request<GetPrice, Price>` and the responder declares `Send<Price>` plus its ordinary request consumer. Both register their stable message identities and serializers. Do not add a second `Send<GetPrice>` route: `Request` already declares it.

`ReplyAsync` requires the request's reply address to equal the response type's configured `Send` destination. This version supports one declared destination per response type, like ordinary sending. It does not create arbitrary endpoints, route to an untrusted header, or emulate native reply queues. Applications needing several independent reply destinations for one response type must currently use the native client's routing facilities. Give each requesting process its own reply endpoint and configure the responder's matching route. Do not run competing requesters on the same reply queue; process-local pending state cannot correlate another process's replies. For Kafka, also give each requester its own group and source topic.

## Timeouts and bounded state

The timeout covers sending and waiting. Zero, negative, infinite and values above 4,294,967,294 ms are rejected before sending. Timeout raises `TimeoutException`; caller cancellation raises `OperationCanceledException` with the caller's token. The normal send route timeout still applies independently. Cancellation is passed to the native send; a transport must honor that token. Cancellation cannot retract work already accepted by a broker or cancel a responder's business operation.

Pending state is provider-owned and bounded per request/response pair:

```csharp
transport.Request<GetPrice, Price>("prices", "price-replies",
    new RequestOptions { MaxPendingRequests = 128 });
```

The default capacity is 256; supported capacities are 1 through 65,536. Exhaustion raises `InvalidOperationException` immediately. Each request owns its correlation identity, completion and deadline resources; these request orchestration allocations are separate from the pooled send/consume pipeline. Replies use a bounded slot lookup without a dictionary lock or decoding their correlation ID into a string. Reservations are removed after success, timeout, caller cancellation or send failure. Duplicate, late, malformed and unmatched correlation IDs are ignored and settle through the ordinary consumer pipeline. Invalid message envelopes or payloads still follow the ordinary failure policy. Pending requests do not survive a process restart.

`RequestOptions.TimeProvider` allows deterministic deadline tests. A reply endpoint can use native consumer settings through `configureReplies`:

```csharp
kafka.Request<GetPrice, Price>("prices", "price-replies-instance-1",
    configureReplies: consumer => consumer.Options.Group = "price-client-instance-1");
```

Kafka keeps Dekaf's native offset-reset setting. For a new request or reply consumer group, configure `KafkaTransportOptions.ConfigureConsumer` with `native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest)` if messages may be sent before the initial partition assignment completes. `Latest` can skip those messages; host startup alone does not establish a committed group offset. Relayer does not override this native choice. `configureRequest` also exposes the ordinary typed route builder for native route options and ordering keys.

Batch reply handlers are unsupported and fail startup validation. Hosted Kafka, Service Bus, SQS, RabbitMQ and in-memory consumers compose the registered typed descriptor with the normal receive pipeline. Existing transport limits, including SQS attribute counts, still apply. Native ordering, retries, delivery and settlement semantics stay unchanged.

## Metadata and durability

Requests carry `replyto` and `correlationid` CloudEvents extensions using the route's header binding: `ce_replyto`/`ce_correlationid` for Kafka and in-memory, `cloudEvents:replyto`/`cloudEvents:correlationid` for AMQP. Responses preserve the request's correlation ID with their own outgoing binding and receive a new message identity. Both routes require CloudEvents mode.

Requests send immediately, bypassing the active outbox and handler send buffer, so waiting does not deadlock on an uncommitted transaction. An outbox-required request route fails startup validation. Do not use a request to depend on database changes that the requester has not committed yet.

Replies use the ordinary scoped sender. An active outbox enlists them in the transaction. With the executable `FailurePipeline`, replies buffer until successful handling and are discarded after a failed attempt. Lower-level consumers with an explicit failure delegate, including the hosted in-memory consumer, keep their existing direct-send behavior; their composer owns side-effect durability. A reply can arrive before its consumer's native settlement completes. Request/response does not provide exactly-once processing or transactional completion across requester and responder.
