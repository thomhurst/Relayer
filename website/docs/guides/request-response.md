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

`Request<GetPrice, Price>` registers one unkeyed `IRequestClient<GetPrice, Price>`.
An application can also register a keyed client for the same pair before or after `AddRelayer`.

Inject the unkeyed client into the requesting application:

```csharp
var price = await client.RequestAsync(
    new GetPrice("coffee"), TimeSpan.FromSeconds(5), cancellationToken);
```

In separate applications, the requester declares `Request<GetPrice, Price>` and the responder declares `Send<Price>` plus its ordinary request consumer. Both register their stable message identities and serializers. Do not add a second `Send<GetPrice>` route: `Request` already declares it.

When the requester also declares a response send route to its advertised reply destination on the same named transport, startup validates that the response type has exactly one send route across all transports. That route must use a fixed destination and CloudEvents mode. A requester with an external responder can start without declaring a local response route.

`ReplyAsync` requires the request's reply address to equal the response type's configured `Send` destination. Request and response types support exactly one declared route, even though ordinary sends can [fan out to several routes](sending.md#sending-to-several-routes). It does not create arbitrary endpoints, route to an untrusted header, or emulate native reply queues. Applications needing several independent reply destinations for one response type must currently use the native client's routing facilities. Give each requesting process its own reply endpoint and configure the responder's matching route. Do not run competing requesters on the same reply queue; process-local pending state cannot correlate another process's replies. For Kafka, also give each requester its own group and source topic.

## Timeouts and bounded state

The timeout covers sending and waiting. Zero, negative, infinite and values above 4,294,967,294 ms are rejected before sending. Timeout raises `TimeoutException`; caller cancellation raises `OperationCanceledException` with the caller's token. The normal send route timeout still applies independently. Cancellation is passed to the native send; a transport must honor that token. Cancellation cannot retract work already accepted by a broker or cancel a responder's business operation.

Pending state is provider-owned and bounded per request/response pair:

```csharp
transport.Request<GetPrice, Price>("prices", "price-replies",
    request => request.Options = new RequestOptions { MaxPendingRequests = 128 });
```

The default capacity is 256; supported capacities are 1 through 65,536. Exhaustion raises `InvalidOperationException` immediately. Each request owns its correlation identity, completion and deadline resources; these request orchestration allocations are separate from the pooled send/consume pipeline. Replies use a bounded slot lookup without a dictionary lock or decoding their correlation ID into a string. Reservations are removed after success, timeout, caller cancellation or send failure. Duplicate, late, malformed and unmatched correlation IDs are ignored and settle through the ordinary consumer pipeline. Invalid message envelopes or payloads still follow the ordinary failure policy. Pending requests do not survive a process restart.

`RequestOptions.TimeProvider` allows deterministic deadline tests.

## Configure the reply consumer

`Request<TRequest, TResponse>` adds a typed reply handler to the consumer for the declared reply address. Hosted transports compose it through their normal single-message receive pipeline. Batch reply handlers fail startup validation.

Use `request.Replies` to configure the reply consumer with the transport's usual options and failure settings. The consumer follows the transport's normal ordering, delivery and settlement rules. `Request` uses its second argument as the reply consumer source and advertises that address by default. When the responder publishes to a different address, set `request.ReplyDestination` to match the responder's fixed `Send<TResponse>` destination.

| Transport | Reply endpoint setup |
| --- | --- |
| [In-memory](../transports/in-memory.md) | Use a dedicated endpoint name. The transport creates declared endpoints in the requesting process. |
| [Kafka](../transports/kafka.md#register-a-consumer) | Use a dedicated reply topic and set a unique consumer group in `Replies`. See the example below. |
| [Azure Service Bus](../transports/service-bus.md#register-receiving) | Use a dedicated queue name, or use a dedicated topic name and set `consumer.Options.Subscription` in `Replies`. |
| [Amazon SQS](../transports/amazon-sqs.md#receiving-and-durable-settlement) | Use a declared, dedicated queue name and configure the dead-letter queue in `Replies`. SQS attribute limits apply. |
| [RabbitMQ](../transports/rabbitmq.md#receive-and-settle) | Use the dedicated queue name for both consuming and sending. |
| [PostgreSQL local](../transports/postgresql-local.md#register-routes-and-workers) | Use a dedicated local endpoint name with the matching PostgreSQL dispatch store and workers. Keep the store isolated from other dispatching processes so they cannot claim the replies. |
| [NATS JetStream](../transports/nats.md#register-sends-and-a-durable-consumer) | Use the same dedicated name for the reply subject and the durable consumer, such as `price-replies-instance-1`. Declare the durable in `NatsTransportOptions.Consumers` with a filter for that subject. Set `consumer.Options.Stream` and the dead-letter subject in `Replies`. |
| [Google Cloud Pub/Sub](../transports/google-pubsub.md#request-and-response) | Pass the fully qualified reply subscription name to `Request` and set `request.ReplyDestination` to the fully qualified reply topic name. Configure the dead-letter topic in `Replies`. |

This Kafka example configures a dedicated reply group and a process-specific CloudEvents source.

```csharp
kafka.Request<GetPrice, Price>("prices", "price-replies-instance-1",
    request =>
    {
        request.Replies(consumer => consumer.Options.Group = "price-client-instance-1");
        request.Send(route => route.SendOptions = new SendRouteOptions
        {
            Source = "/price-client-instance-1"
        });
    });
```

Kafka keeps Dekaf's native offset-reset setting. For a new request or reply consumer group, configure `KafkaTransportOptions.ConfigureConsumer` with `native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest)` if messages may be sent before the initial partition assignment completes. `Latest` can skip those messages; host startup alone does not establish a committed group offset. Relayer does not override this native choice. `request.Send` also exposes the ordinary typed route builder for native route options and ordering keys.

## Metadata and durability

Requests carry `replyto` and `correlationid` CloudEvents extensions using the route's header binding: `ce_replyto`/`ce_correlationid` for Kafka and in-memory, `cloudEvents:replyto`/`cloudEvents:correlationid` for AMQP. Responses preserve the request's correlation ID with their own outgoing binding and receive a new message identity. Both routes require CloudEvents mode.

Requests send immediately, bypassing the active outbox and handler send buffer, so waiting does not deadlock on an uncommitted transaction. An outbox-required request route fails startup validation. Do not use a request to depend on database changes that the requester has not committed yet.

`context.ReplyAsync(response)` uses `context.CancellationToken`. Supplying a token explicitly uses that token unchanged, including `CancellationToken.None` for an uncancelable reply.

Replies use the ordinary scoped sender. When an outbox is registered, `ReplyAsync` enlists in the responder's active outbox transaction or selected EF Core unit of work. It throws `InvalidOperationException` if the outbox is inactive. Configure the responder's consumer database boundary, begin an outbox transaction, or select the EF Core context with `db.EnlistRelayerOutbox()` before replying. See [outbox setup](outbox.md).

Without an outbox registration, hosted consumers, including in-memory consumers, buffer replies. Relayer flushes them after the handler, middleware and scope disposal succeed. A failed attempt discards its buffered replies.

A custom consumer built with the `ConsumePipelineBuilder` overload that takes an explicit failure delegate sends replies directly by default. Its composer owns side-effect durability. A reply can arrive before its consumer's native settlement completes. Request/response does not provide exactly-once processing or transactional completion across requester and responder.
