using Relayer.Pipeline;
using System.Buffers;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Relayer;
using Relayer.AzureServiceBus;
using Relayer.HealthChecks;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;
using Relayer.Serialization;
using Relayer.Transports;

using (var contractsProvider = new ServiceCollection().AddRelayer(builder =>
{
    builder.AddOrdersContracts();
    builder.AddBillingContracts();
    builder.AddMessageTypesFromThisAssembly();
}).BuildServiceProvider())
{
    var contractsSerializer = contractsProvider.GetRequiredService<IMessageSerializer>();
    var contractsBuffer = new ArrayBufferWriter<byte>();
    contractsSerializer.Serialize(new Relayer.TestContracts.Orders.Order(42), contractsBuffer);
    if (contractsSerializer.Deserialize<Relayer.TestContracts.Orders.Order>(new ReadOnlySequence<byte>(contractsBuffer.WrittenMemory)).MessageId != 42)
        throw new InvalidOperationException("Referenced orders contract roundtrip failed.");
    contractsBuffer.Clear();
    contractsSerializer.Serialize(new Relayer.TestContracts.Billing.Payment(7), contractsBuffer);
    if (contractsSerializer.Deserialize<Relayer.TestContracts.Billing.Payment>(new ReadOnlySequence<byte>(contractsBuffer.WrittenMemory)).MessageId != 7)
        throw new InvalidOperationException("Referenced billing contract roundtrip failed.");
}
Console.WriteLine("Cross-assembly generated message registration passed under Native AOT.");

using (var asynchronousReceive = new Relayer.TestFixtures.ConcurrentReceiveFixture())
{
    for (var i = 0; i < 4; i++) asynchronousReceive.Run(Relayer.TestFixtures.ConcurrentReceiveFixture.Mode.Receive);
}
Console.WriteLine("Concurrent cross-thread receive passed under Native AOT.");
foreach (var batch in new[] { false, true })
foreach (var listener in new[] { false, true })
{
    using var send = new Relayer.TestFixtures.SendTracingCompletionFixture(batch, listener);
    for (var i = 0; i < 16; i++) send.Run(traced: true);
    AssertSendAllocation(send, batch, listener, traced: true, asynchronousCaller: false);
    // Native AOT runtime-async dispatch lazily allocates thread-static storage (dotnet/runtime#137474).
    // Warm and check the native caller separately, then warm the wrapped continuation path as well.
    send.Run(traced: false, asynchronousCaller: true);
    AssertSendAllocation(send, batch, listener, traced: false, asynchronousCaller: true);
    send.Run(traced: true, asynchronousCaller: true);
    AssertSendAllocation(send, batch, listener, traced: true, asynchronousCaller: true);
}

static void AssertSendAllocation(Relayer.TestFixtures.SendTracingCompletionFixture send,
    bool batch, bool listener, bool traced, bool asynchronousCaller)
{
    var allocated = send.Run(traced, asynchronousCaller);
    if (allocated != 0)
        throw new InvalidOperationException($"Send allocates {allocated} B under Native AOT " +
            $"(caller={send.CallerBytes} B, worker={send.WorkerBytes} B, batch={batch}, " +
            $"listener={listener}, traced={traced}, asynchronousCaller={asynchronousCaller}).");
}

Console.WriteLine("Cross-thread single and batch send tracing passed under Native AOT.");
await KafkaSchemaSmoke.RunAsync();
await NatsTopologySmoke.RunAsync();
await RabbitMqStreamsSmoke.RunAsync();
await MySqlStoreSmoke.RunAsync();
ProtobufSmoke.Run();
MessagePackSmoke.Run();
using (var tenantProvider = new ServiceCollection().AddRelayerTenancy(1).BuildServiceProvider())
{
    using (var tenantScope = tenantProvider.CreateScope())
    {
        var tenant = tenantScope.ServiceProvider.GetRequiredService<TenantContext>();
        tenant.Id = new("tenant-aot"u8.ToArray());
        if (!tenant.Id.Utf8.Span.SequenceEqual("tenant-aot"u8))
            throw new InvalidOperationException("Native tenant storage failed under Native AOT.");
    }
    using var cleanTenantScope = tenantProvider.CreateScope();
    if (!cleanTenantScope.ServiceProvider.GetRequiredService<TenantContext>().Id.IsEmpty)
        throw new InvalidOperationException("Pooled tenant scope cleanup failed under Native AOT.");
}
Console.WriteLine("Tenant registration and pooled UTF-8 scope cleanup passed under Native AOT.");
SagaDiagramSmoke.Run();
AsyncApiSmoke.Run();
await SagaDispatchSmoke.RunAsync();
await ServiceBusReceiversSmoke.RunAsync();

var serviceBusMessage = ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData(new byte[] { 42 }),
    properties: new Dictionary<string, object> { ["cloudEvents:type"] = "smoke;v=1" }, deliveryCount: 2);
var serviceBusArgs = new ProcessMessageEventArgs(serviceBusMessage, new SmokeServiceBusReceiver(), "aot", default);
if (ServiceBusMessageBinding.GetBody(serviceBusArgs).First.Span[0] != 42 ||
    ServiceBusMessageBinding.GetDeliveryCount(serviceBusArgs) != 2 ||
    !ServiceBusMessageBinding.TryGetHeader(serviceBusArgs, "cloudEvents:type", out var serviceBusType) || serviceBusType.Text != "smoke;v=1")
    throw new InvalidOperationException("Service Bus binding failed under Native AOT.");

var postgresConnectionString = Environment.GetEnvironmentVariable("RELAYER_AOT_POSTGRES_CONNECTION");
await using (var dataSource = new Npgsql.NpgsqlSlimDataSourceBuilder(postgresConnectionString ?? "Host=127.0.0.1;Port=1;Timeout=1").EnableArrays().Build())
{
    var stores = new ServiceCollection().AddRelayerPostgreSqlOutboxStore("postgres-smoke", dataSource);
    stores.AddRelayerPostgreSqlOutbox(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["TablePrefix"] = "relayer_", ["Lanes"] = "8"
    }).Build());
    stores.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
    stores.AddRelayerPostgreSqlDirectDispatch("direct-smoke", dataSource,
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["QueueCapacity"] = "2", ["BatchSize"] = "2", ["ClaimDuration"] = "00:00:30"
        }).Build());
    await using var storeProvider = stores.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    var dispatchOptions = storeProvider.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<PostgreSqlDirectDispatchOptions>>().Get("direct-smoke");
    if (dispatchOptions.QueueCapacity != 2 || dispatchOptions.BatchSize != 2 || dispatchOptions.ClaimDuration != TimeSpan.FromSeconds(30))
        throw new InvalidOperationException("Direct dispatch immutable options failed under Native AOT.");
    foreach (var worker in storeProvider.GetServices<IHostedService>())
    {
        await worker.StartAsync(default);
        await worker.StopAsync(default);
    }
    await using var storeScope = storeProvider.CreateAsyncScope();
    var store = storeScope.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("postgres-smoke");
    var postgresOutbox = storeScope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
    if (postgresOutbox.IsActive || !ReferenceEquals(postgresOutbox, storeScope.ServiceProvider.GetRequiredService<IOutboxEnlistment>()) ||
        storeScope.ServiceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptionsSnapshot<PostgreSqlOutboxOptions>>().Value.Lanes != 8)
        throw new InvalidOperationException("PostgreSQL outbox registration failed under Native AOT.");
    if (await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), Memory<OutboxRecord>.Empty, default) != 0)
        throw new InvalidOperationException("Empty PostgreSQL reads failed under Native AOT.");
    try
    {
        await store.GetPendingCountAsync(DateTimeOffset.UtcNow, new CancellationToken(true));
        throw new InvalidOperationException("PostgreSQL cancellation failed under Native AOT.");
    }
    catch (OperationCanceledException) { }
    if (postgresConnectionString is not null)
    {
        await using (var schemaCommand = dataSource.CreateCommand(PostgreSqlOutboxSchema.CreateScript()))
            await schemaCommand.ExecuteNonQueryAsync();
        await using (var seedCommand = dataSource.CreateCommand("""
            INSERT INTO relayer_outbox (id, transport, destination, message_type, headers, body, created_at, deliver_at)
            VALUES ('01900000-0000-7000-8000-000000000001', 'smoke', 'orders', 'order.v1',
                '\x0100000001000000040000007479706502000000c3a9', '\x0102', now(), now()),
                ('01900000-0000-7000-8000-000000000002', 'smoke', 'orders', 'order.v1',
                '\x0100000001000000040000007479706502000000c3a9', '\x0304', now(), now())
            """))
            await seedCommand.ExecuteNonQueryAsync();
        var rows = new OutboxRecord[2];
        var now = DateTimeOffset.UtcNow.AddSeconds(1);
        if (await store.ReadPendingAsync(new(null, now), rows, default) != 2 || rows[0].Message.Headers.Span[0].Value.ToString() != "é")
            throw new InvalidOperationException("PostgreSQL row reads failed under Native AOT.");
        var claim = new OutboxClaim(rows[0].Id, Guid.NewGuid());
        if (await store.GetPendingCountAsync(now, default) != 2 ||
            !await store.TryClaimAsync(claim, now, now.AddMinutes(1), default))
            throw new InvalidOperationException("PostgreSQL claims failed under Native AOT.");
        OutboxClaim[] claims = [claim, new(rows[1].Id, Guid.NewGuid())];
        var claimed = new bool[2];
        var concreteStore = (PostgreSqlOutboxStore)store;
        await concreteStore.ClaimBatchAsync(claims, claimed, now, now.AddMinutes(1), default);
        if (claimed[0] || !claimed[1] ||
            await concreteStore.CompleteBatchAsync(claims, now, default) != 2 ||
            await store.GetPendingCountAsync(now, default) != 0)
            throw new InvalidOperationException("PostgreSQL claims failed under Native AOT.");
        Console.WriteLine("Real PostgreSQL reads, headers, single/batch claims and outcomes passed under Native AOT.");
        await using (var transaction = await postgresOutbox.BeginTransactionAsync(dataSource))
        {
            await using var command = transaction.Connection!.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT 1";
            if ((int)(await command.ExecuteScalarAsync())! != 1)
                throw new InvalidOperationException("PostgreSQL business command failed under Native AOT.");
            await postgresOutbox.EnlistAsync(new(Guid.CreateVersion7(), "smoke", "order.v1",
                new OutgoingMessage("orders", new byte[] { 1, 2 }, default, new WireString("key")), DateTimeOffset.UtcNow), default);
            await transaction.CommitAsync();
        }
        if (await store.GetPendingCountAsync(DateTimeOffset.UtcNow, default) != 1)
            throw new InvalidOperationException("PostgreSQL batched enqueue failed under Native AOT.");
        Console.WriteLine("Real PostgreSQL transaction wrapper and batched enqueue passed under Native AOT.");
    }
}
await using (var orderedSource = new Npgsql.NpgsqlSlimDataSourceBuilder(postgresConnectionString ?? "Host=127.0.0.1;Port=1;Timeout=1").EnableArrays().Build())
{
    var native = new OrderedSmokeTransport();
    var orderedServices = new ServiceCollection().AddLogging();
    orderedServices.AddSingleton<ITransport>(native);
    orderedServices.AddRelayerPostgreSqlOrderedDispatch("ordered-smoke", orderedSource,
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Lanes"] = "1", ["BatchSize"] = "2", ["PollInterval"] = "00:00:00.05"
        }).Build());
    await using var orderedProvider = orderedServices.BuildServiceProvider();
    var worker = orderedProvider.GetRequiredService<IHostedService>();
    var settings = orderedProvider.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<PostgreSqlOrderedDispatchOptions>>().Get("ordered-smoke");
    if (settings.Lanes != 1 || settings.BatchSize != 2) throw new InvalidOperationException("Ordered dispatch options failed under Native AOT.");
    if (postgresConnectionString is not null)
    {
        await using (var seed = orderedSource.CreateCommand("""
            INSERT INTO relayer_outbox (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at)
            VALUES ('01900000-0000-7000-8000-000000000002', 0, 'customer-42', 'ordered-smoke', 'orders', 'order.v1',
                '\x0100000001000000040000007479706502000000c3a9', '\x0102', now())
            """))
            await seed.ExecuteNonQueryAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await worker.StartAsync(timeout.Token);
        await native.Sent.Task.WaitAsync(timeout.Token);
        // Database completion happens after the native callback. Observe commit before stopping the worker.
        while (true)
        {
            await using var count = orderedSource.CreateCommand("SELECT count(*) FROM relayer_outbox WHERE lane = 0");
            if ((long)(await count.ExecuteScalarAsync(timeout.Token))! == 0) break;
            await Task.Yield();
        }
        await worker.StopAsync(timeout.Token);
        await using var leases = orderedSource.CreateCommand("SELECT count(*) FROM relayer_leases");
        if ((long)(await leases.ExecuteScalarAsync(timeout.Token))! != 0)
            throw new InvalidOperationException("Ordered leases were not released under Native AOT.");
        Console.WriteLine("Real PostgreSQL ordered dispatch and lease release passed under Native AOT.");
    }
}
Console.WriteLine("Ordered PostgreSQL dispatch registration and immutable options passed under Native AOT.");

await using (var maintenanceSource = new Npgsql.NpgsqlSlimDataSourceBuilder("Host=127.0.0.1;Port=1;Timeout=1").EnableArrays().Build())
{
    var maintenanceServices = new ServiceCollection().AddLogging();
    maintenanceServices.AddRelayerPostgreSqlMaintenance("maintenance-smoke", maintenanceSource,
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["BatchSize"] = "3", ["DeadRetention"] = "infinite", ["InboxRetention"] = "365.00:00:00"
        }).Build());
    maintenanceServices.AddRelayerPostgreSqlOutbox();
    maintenanceServices.AddSingleton<ITransport>(new SmokeTransport());
    maintenanceServices.AddRelayer(builder =>
    {
        builder.AddMessageTypesFromThisAssembly();
        builder.AddTransport<SmokeOptions, SmokeOptions>("smoke", broker =>
            broker.Send<Order>("orders", route => route.SendOptions = new()
            {
                RequiredDeliveryDelay = TimeSpan.FromDays(1),
                TransactionRequirement = SendTransactionRequirement.Outbox
            }));
    });
    await using var maintenanceProvider = maintenanceServices.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    foreach (var lifecycle in maintenanceProvider.GetServices<IHostedService>().OfType<IHostedLifecycleService>())
        await lifecycle.StartingAsync(default);
    await using (var schedulingScope = maintenanceProvider.CreateAsyncScope())
        if (!schedulingScope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().SupportsScheduling)
            throw new InvalidOperationException("Declared durable scheduling fallback failed under Native AOT.");
    var settings = maintenanceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<PostgreSqlMaintenanceOptions>>().Get("maintenance-smoke");
    if (settings.BatchSize != 3 || settings.DeadRetention is not null || settings.InboxRetention != TimeSpan.FromDays(365))
        throw new InvalidOperationException("Maintenance immutable options failed under Native AOT.");
    _ = maintenanceProvider.GetRequiredService<IHostedService>();
}
Console.WriteLine("PostgreSQL maintenance registration and immutable configuration passed under Native AOT.");

await using (var cdcSource = new Npgsql.NpgsqlSlimDataSourceBuilder("Host=127.0.0.1;Port=1;Timeout=1").EnableArrays().Build())
{
    var cdcServices = new ServiceCollection().AddLogging();
    cdcServices.AddRelayerPostgreSqlChangeCapture("cdc-smoke", cdcSource,
        static () => new Npgsql.Replication.LogicalReplicationConnection("Host=127.0.0.1;Port=1;Timeout=1"),
        new Npgsql.Replication.PgOutput.PgOutputReplicationSlot("cdc_smoke"),
        new Npgsql.Replication.PgOutput.PgOutputReplicationOptions("cdc_smoke", Npgsql.Replication.PgOutput.PgOutputProtocolVersion.V1),
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["BatchSize"] = "7" }).Build());
    await using var cdcProvider = cdcServices.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    var settings = cdcProvider.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<PostgreSqlChangeCaptureOptions>>().Get("cdc-smoke");
    if (settings.BatchSize != 7) throw new InvalidOperationException("CDC immutable configuration failed under Native AOT.");
    _ = cdcProvider.GetRequiredService<IHostedService>();
}
Console.WriteLine("PostgreSQL CDC registration and immutable configuration passed under Native AOT.");

Console.WriteLine("PostgreSQL keyed stores and cancellation passed under Native AOT.");
// This validates scoped registration and immutable configuration, without claiming an EF compiled-model round trip.
var outboxServices = new ServiceCollection();
outboxServices.AddRelayerEntityFrameworkCoreOutbox(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
{
    ["Schema"] = "messaging", ["TablePrefix"] = "smoke_", ["Lanes"] = "8"
}).Build());
using (var outboxProvider = outboxServices.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true }))
using (var outboxScope = outboxProvider.CreateScope())
{
    var options = outboxScope.ServiceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value;
    if (options.Schema != "messaging" || options.TablePrefix != "smoke_" || options.Lanes != 8 ||
        outboxScope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().IsActive)
        throw new InvalidOperationException("EF outbox registration or immutable configuration failed under Native AOT.");
}
Console.WriteLine("EF outbox registration and immutable configuration passed under Native AOT.");

var transport = new SmokeTransport();
var services = new ServiceCollection();
services.AddSingleton<ITransport>(transport);
services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
services.AddRelayerHealthChecks(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
{
    ["ConsumerStallTimeout"] = "00:00:30",
    ["OutboxDegradedThreshold"] = "5",
    ["OutboxUnhealthyThreshold"] = "10",
    ["OutboxNames:0"] = "smoke-store"
}).Build());
services.AddKeyedSingleton<ITransportHealthProbe, SmokeHealthProvider>("smoke");
services.AddKeyedSingleton<IConsumerHealthProvider, SmokeHealthProvider>("smoke");
services.AddKeyedScoped<IOutboxStore, SmokeOutboxStore>("smoke-store");
services.AddRelayerOutboxHealthProvider("smoke-store");
await using var provider = services.AddRelayer(builder =>
{
    builder.Telemetry();
    builder.AddMessageTypesFromThisAssembly(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    });
    builder.AddTransport<SmokeOptions, SmokeOptions>("smoke", broker =>
    {
        broker.Send<Order>("orders", route => route.OrderingKey(static order => order.Id));
        broker.Send<Count>("counts");
        broker.Consume("orders", consumer => consumer.Handler<OrderHandler>().Handler<CountHandler>().Handler<OrderTombstoneHandler>());
        broker.Consume("counts", consumer => consumer.Handler<CountHandler>().Batch(2, TimeSpan.FromMilliseconds(100)));
    });
    builder.UseSendMiddleware<SmokeMiddleware>();
}).BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
foreach (var service in provider.GetServices<IHostedService>().OfType<IHostedLifecycleService>())
    await service.StartingAsync(CancellationToken.None);
foreach (var service in provider.GetServices<IHostedService>())
    await service.StartAsync(CancellationToken.None);
if (provider.GetRequiredService<ITransport>() is not SmokeTransport { CapabilityLookups: 2 })
    throw new InvalidOperationException("Endpoint capability validation failed under Native AOT.");
Console.WriteLine("Endpoint capability validation passed under Native AOT.");
var health = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();
if (health.Status != HealthStatus.Degraded || health.Entries.Count != 3 ||
    health.Entries["relayer.transport"].Status != HealthStatus.Healthy ||
    health.Entries["relayer.consumer"].Status != HealthStatus.Healthy ||
    health.Entries["relayer.outbox"].Status != HealthStatus.Degraded)
    throw new InvalidOperationException("Health checks or immutable configuration binding failed under Native AOT.");
Console.WriteLine("Health checks and immutable configuration binding passed under Native AOT.");
using (var topologyJson = new MemoryStream())
{
    provider.GetRequiredService<ITopology>().ExportJson(topologyJson);
    using var topology = JsonDocument.Parse(topologyJson.ToArray());
    if (topology.RootElement.GetProperty("transports")[0].GetProperty("routes")[0]
        .GetProperty("message").GetProperty("name").GetString() != "smoke.order")
        throw new InvalidOperationException("Topology export failed under Native AOT.");
}
Console.WriteLine("Topology startup and JSON export passed with reflection disabled.");
var tracing = provider.GetRequiredService<MessageTracing>();
using (var listener = new ActivityListener
{
    ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
    Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
})
{
    ActivitySource.AddActivityListener(listener);
    var operation = new MessageOperationTracing(tracing,
        new() { System = "relayer.inmemory", Destination = "orders" }, MessagingOperation.Send);
    using var activity = operation.Start(new() { MessageId = "smoke", EventType = "smoke.order;v=1" });
    MessageOperationTracing.Fail(activity, "smoke_error");
    if (activity is not { Kind: ActivityKind.Producer, Status: ActivityStatusCode.Error } ||
        activity.GetTagItem("messaging.system") is not "relayer.inmemory" ||
        tracing.Source.TelemetrySchemaUrl != RelayerTelemetry.SchemaUrl)
        throw new InvalidOperationException("Tracing primitives failed under Native AOT.");
    var names = new HeaderNames();
    var headers = new Dictionary<string, WireString>
    {
        [names.TraceParent] = new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"u8.ToArray()),
    };
    var creation = MessageCreationContext.Read<Dictionary<string, WireString>, SmokeTraceHeaderReader>(headers, names);
    using var process = new MessageOperationTracing(tracing,
        new() { System = "relayer.inmemory", Destination = "orders" }, MessagingOperation.Process)
        .Start(new() { CreationContext = creation, PartitionId = "3" });
    if (!creation.IsRemote || creation.TraceId == default || process is null ||
        process.GetTagItem("messaging.destination.partition.id") is not "3" || process.Links.Single().Context != creation)
        throw new InvalidOperationException("UTF-8 creation context and pooled sampling state failed under Native AOT.");
}
Console.WriteLine("Tracing primitives passed with reflection disabled.");
var serializer = provider.GetRequiredService<IMessageSerializer>();
var buffer = new ArrayBufferWriter<byte>();
var message = new Order(42, new Customer("Ada"), [1, 2], OrderState.Placed);
serializer.Serialize(message, buffer);
var result = serializer.Deserialize<Order>(new ReadOnlySequence<byte>(buffer.WrittenMemory));
if (result.Id != message.Id || result.Customer.Name != "Ada" || result.Items.Length != 2 || result.State != OrderState.Placed)
    throw new InvalidOperationException("Generated JSON metadata failed to round-trip under Native AOT.");
buffer.Clear();
serializer.Serialize(new Count(7), buffer);
if (serializer.Deserialize<Count>(new ReadOnlySequence<byte>(buffer.WrittenMemory)).Value != 7)
    throw new InvalidOperationException("Value type round-trip failed under Native AOT.");
Console.WriteLine("Generated JSON serialization passed with reflection disabled.");
using var scope = provider.CreateScope();
var handlers = provider.GetRequiredService<HandlerRegistry>();
var orderDescriptor = handlers.GetHandler<Order, OrderHandler>();
var orderHandler = (OrderHandler)orderDescriptor.Resolve(scope.ServiceProvider);
var context = new SmokeContext(scope.ServiceProvider);
var classifier = new FailureClassifierBuilder()
    .On<ArgumentException>(FailureAction.Discard, static (_, context) => context.Attempt == 1).Build();
if (classifier.Classify(new ArgumentNullException("input"), context) != FailureAction.Discard ||
    classifier.Classify(new MessageEnvelopeException("invalid"), context) != FailureAction.DeadLetter)
    throw new InvalidOperationException("Typed exception rules failed under Native AOT.");
Console.WriteLine("Typed exception rules and default classification passed under Native AOT.");
await orderDescriptor.DispatchAsync(orderHandler, message, context, default);
var countDescriptor = handlers.GetBatchHandler<Count, CountHandler>();
var countHandler = (CountHandler)countDescriptor.Resolve(scope.ServiceProvider);
await countDescriptor.DispatchAsync(countHandler, new Batch<Count>([new(new Count(7), context)]), default);
if (orderHandler.Order != message || countHandler.Total != 7)
    throw new InvalidOperationException("Consumer-only generated dispatch failed under Native AOT.");
Console.WriteLine("Consumer-only single and batch dispatch passed under Native AOT.");
var tombstoneDescriptor = handlers.GetHandler<Tombstone<Order>, OrderTombstoneHandler>();
var tombstoneConsumer = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
    provider.GetRequiredService<MessageTypeRegistry>(), serializer, static (_, error, _) => ValueTask.FromException(error))
    .Handler(tombstoneDescriptor).Build(1);
await new ReceivePipeline<SmokeIncoming, SmokeBinding>(tombstoneConsumer, new(), 1)
    .ProcessAsync(new(default, Tombstone: true));
Console.WriteLine("Typed tombstone receive passed under Native AOT.");
buffer.Clear();
serializer.Serialize(message, buffer);
var receiveEndpoint = new MessagingEndpoint { System = "relayer.inmemory", Destination = "orders" };
var nativeSettlement = new MessageOperationTracing(tracing, receiveEndpoint, MessagingOperation.Settle, "complete")
    .WrapSettlement(static context =>
    {
        if (context.Features.Get<NativeMessage<SmokeIncoming>>() is null || Activity.Current?.OperationName != "complete orders")
            throw new InvalidOperationException("Traced settlement lost its native feature or span.");
        return default;
    });
var receiveSpans = new List<Activity>();
using var receiveListener = new ActivityListener
{
    ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
    Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
    ActivityStopped = activity => receiveSpans.Add(activity),
};
ActivitySource.AddActivityListener(receiveListener);
var consumer = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
        provider.GetRequiredService<MessageTypeRegistry>(), serializer,
        static (_, error, _) => ValueTask.FromException(error))
    .Handler(orderDescriptor)
    .Telemetry(provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
    {
        System = "relayer.inmemory", Destination = "orders",
    }))
    .Tracing(tracing, receiveEndpoint)
    .Settle(nativeSettlement).Build(1);
var receive = new ReceivePipeline<SmokeIncoming, SmokeBinding>(consumer, new HeaderNames(), 1);
var disposition = await receive.ProcessAsync(new(new ReadOnlySequence<byte>(buffer.WrittenMemory)));
if (disposition.Kind != DispositionKind.Complete)
    throw new InvalidOperationException("Native receive SPI failed under Native AOT.");
if (receiveSpans.Count != 2 || receiveSpans[0].ParentSpanId != receiveSpans[1].SpanId ||
    receiveSpans[1].GetTagItem("messaging.destination.partition.id") is not "0" ||
    receiveSpans[1].GetTagItem("messaging.message.id") is not "smoke-native" ||
    receiveSpans[1].GetTagItem("messaging.kafka.offset") is not 42L || receiveSpans[1].Links.Count() != 1)
    throw new InvalidOperationException("Delivery and settlement tracing failed under Native AOT.");
receiveListener.Dispose();
Console.WriteLine("Delivery spans, settlement wrappers and native attributes passed under Native AOT.");
Console.WriteLine("Native receive binding, lazy headers and pooled native feature passed under Native AOT.");
var failurePolicy = new FailurePipeline(provider, "smoke", "orders", "aot-consumer",
    new FailureOptions { RetryDelays = new[] { TimeSpan.Zero } });
var failingConsumer = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
        provider.GetRequiredService<MessageTypeRegistry>(), serializer, failurePolicy)
    .Handler(orderDescriptor).Build(1);
var failingReceive = new ReceivePipeline<SmokeIncoming, SmokeBinding>(failingConsumer, new HeaderNames(), 1);
var poison = await failingReceive.ProcessAsync(new(new ReadOnlySequence<byte>("invalid-json"u8.ToArray())));
if (poison.Kind != DispositionKind.DeadLetter || poison.DeadLetter.Headers.Length != 3)
    throw new InvalidOperationException("Permanent failure handling or generated error metadata failed under Native AOT.");
var healthy = await failingReceive.ProcessAsync(new(new ReadOnlySequence<byte>(buffer.WrittenMemory)));
if (healthy.Kind != DispositionKind.Complete || transport.CapabilityLookups != 2)
    throw new InvalidOperationException("Failure handling stalled the consumer or rediscovered capabilities.");
Console.WriteLine("Executable failure handling and generated error metadata passed under Native AOT.");
var batchDefinition = provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[1];
Activity? batchActivity = null;
using var batchListener = new ActivityListener
{
    ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
    Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
    ActivityStopped = activity => batchActivity = activity,
};
ActivitySource.AddActivityListener(batchListener);
var batchConsumer = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
    provider.GetRequiredService<MessageTypeRegistry>(), serializer, static (_, error, _) => ValueTask.FromException(error))
    .Tracing(tracing, receiveEndpoint)
    .BuildBatch(handlers.GetBatchHandler<Count, CountHandler>(), batchDefinition);
var countBuffer = new ArrayBufferWriter<byte>();
serializer.Serialize(new Count(7), countBuffer);
var countMessage = new ConsumeMessage { MessageId = "batch-count", MessageType = new("smoke.count", 1), Body = new(countBuffer.WrittenMemory),
    Headers = new SmokeBatchHeaders() };
var batchReceiver = new BatchReceiveExecutor<ConsumeMessage>(provider, "smoke", batchDefinition, "aot-batch",
    singleReceive: (destination, _, _) => { destination.Span[0] = countMessage; return new(1); });
var receivedBatch = new ConsumeMessage[2];
if (await batchReceiver.ReceiveAsync(receivedBatch) != 2 || batchReceiver.UsesNativeBatching)
    throw new InvalidOperationException("Client-side batch execution failed under Native AOT.");
using (var firstCount = batchConsumer.RentContext(receivedBatch[0]))
using (var secondCount = batchConsumer.RentContext(receivedBatch[1]))
    await batchConsumer.ProcessBatchAsync(new[] { firstCount, secondCount });
if (batchActivity is null || batchActivity.Links.Count() != 2 ||
    batchActivity.Links.Any(link => !link.Context.IsRemote || link.Context.TraceId.ToString() != "0123456789abcdef0123456789abcdef"))
    throw new InvalidOperationException("UTF-8 batch creation links failed under Native AOT.");
Console.WriteLine("Generated batch composition and client-side executor passed under Native AOT.");
var sender = scope.ServiceProvider.GetRequiredService<ISender>();
await sender.SendAsync(message, options => options.ConfigureNative<SmokeNative>(static native => native.Value = 7));
if (transport.Destination != "orders" || transport.OrderingKey != "42" || transport.NativeValue != 7)
    throw new InvalidOperationException("Send routing or native configuration failed under Native AOT.");
await sender.SendAsync(new Count(7));
if (transport.Destination != "counts" || transport.NativeValue != 0)
    throw new InvalidOperationException("Value type sending failed under Native AOT.");
if (transport.CapabilityLookups != 2)
    throw new InvalidOperationException("Endpoint capabilities were resolved on the per-message path.");
await sender.SendAsync(Tombstone.For<Order>("42"));
if (transport.OrderingKey != "42" || !transport.Tombstone)
    throw new InvalidOperationException("Typed tombstone sending failed under Native AOT.");
Console.WriteLine("Send routing, typed tombstones, middleware and native configuration passed with reflection disabled.");

await InMemoryTracingSmoke.RunAsync();
await RequestResponseSmoke.RunAsync();
await UpcasterSmoke.RunAsync();

[Message("smoke.order")]
public sealed record Order(int Id, Customer Customer, int[] Items, OrderState State);
public sealed record Customer(string Name);
[JsonConverter(typeof(JsonStringEnumConverter<OrderState>))]
public enum OrderState { Placed, Cancelled }
[Message("smoke.count")]
public readonly record struct Count(int Value);
public sealed class SmokeOptions;
public sealed class OrderTombstoneHandler : IHandler<Tombstone<Order>>
{
    public ValueTask HandleAsync(Tombstone<Order> message, MessageContext context, CancellationToken token)
    {
        if (message.Key.ToString() != "42" || context.MessageType != new MessageType("smoke.order", 1))
            throw new InvalidOperationException("Typed tombstone dispatch failed under Native AOT.");
        return default;
    }
}
public sealed class OrderHandler : IHandler<Order>
{
    public Order? Order { get; private set; }
    public ValueTask HandleAsync(Order message, MessageContext context, CancellationToken cancellationToken)
    {
        if (context.Features.Get<NativeMessage<SmokeIncoming>>() is { } native &&
            (native.Message.Body.IsEmpty || context.MessageId != "smoke-native" ||
             !context.Headers.TryGetValue("ce_type", out var type) || !type.Utf8.Span.SequenceEqual("smoke.order;v=1"u8)))
            throw new InvalidOperationException("Native message or lazy headers were lost.");
        Order = message;
        return default;
    }
}
internal readonly record struct SmokeIncoming(ReadOnlySequence<byte> Body, bool Tombstone = false);
internal readonly struct SmokeTraceHeaderReader : IMessageHeaderReader<Dictionary<string, WireString>>
{
    public static bool TryGetHeader(Dictionary<string, WireString> message, string name, out WireString value) => message.TryGetValue(name, out value);
}
internal sealed class SmokeBatchHeaders : MessageHeaders
{
    private readonly WireString _parent = new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"u8.ToArray());
    public override bool TryGetValue(string name, out WireString value)
    {
        value = name == "traceparent" ? _parent : default;
        return !value.IsEmpty;
    }
}
internal readonly struct SmokeBinding : IMessageBinding<SmokeIncoming>
{
    public static ReadOnlySequence<byte> GetBody(SmokeIncoming message) => message.Body;
    public static bool IsTombstone(SmokeIncoming message) => message.Tombstone;
    public static WireString? GetTombstoneKey(SmokeIncoming message) => new WireString("42");
    public static int? GetDeliveryCount(SmokeIncoming message) => null;
    public static string? GetPartitionId(SmokeIncoming message) => "0";
    public static void EnrichActivity(Activity activity, SmokeIncoming message) => activity.SetTag("messaging.kafka.offset", 42L);
    public static bool TryGetHeader(SmokeIncoming message, string name, out WireString value)
    {
        value = name switch
        {
            "ce_type" => new WireString(Type),
            "ce_id" => new WireString("smoke-native"),
            "traceparent" => new WireString("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"),
            _ => default,
        };
        return !value.IsEmpty;
    }
    private static ReadOnlyMemory<byte> Type { get; } = "smoke.order;v=1"u8.ToArray();
}
public sealed class CountHandler : IBatchHandler<Count>
{
    public int Total { get; private set; }
    public ValueTask<BatchResult> HandleAsync(Batch<Count> batch, CancellationToken cancellationToken)
    {
        for (var i = 0; i < batch.Count; i++) Total += batch.Items[i].Message.Value;
        return new(BatchResult.Success);
    }
}
internal sealed class SmokeContext(IServiceProvider services) : MessageContext
{
    public override string MessageId => "smoke";
    public override MessageType MessageType => new("smoke.order", 1);
    public override MessageHeaders Headers => MessageHeaders.Empty;
    public override int Attempt => 1;
    public override IServiceProvider Services { get; } = services;
    public override MessageFeatures Features { get; } = new();
    public override CancellationToken CancellationToken => default;
}

internal sealed class SmokeNative { public int Value { get; set; } }

internal sealed class SmokeMiddleware : ISendMiddleware
{
    public ValueTask InvokeAsync(SendContext context, SendDelegate next)
    {
        context.Options.Headers["smoke"] = "middleware";
        return next(context);
    }
}

internal sealed class SmokeTransport : ITransport
{
    public string Name => "smoke";
    public int CapabilityLookups { get; private set; }
    public TransportCapabilities GetCapabilities(string endpoint)
    {
        if (endpoint is not ("orders" or "counts")) throw new ArgumentException("Unknown smoke endpoint.", nameof(endpoint));
        CapabilityLookups++;
        return new() { OrderedByKey = true, DeadLetter = true, Tombstones = true };
    }
    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        => ValueTask.FromResult(new TopologyReport([]));
    public string? Destination { get; private set; }
    public string? OrderingKey { get; private set; }
    public int NativeValue { get; private set; }
    public bool Tombstone { get; private set; }
    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var native = new SmokeNative();
        message.ApplyNative(native);
        NativeValue = native.Value;
        Tombstone = message.IsTombstone;
        Destination = message.Destination;
        OrderingKey = message.OrderingKey?.ToString();
        var middlewareRan = false;
        foreach (var header in message.Headers.Span)
            if (header.Key == "smoke" && header.Value.ToString() == "middleware") middlewareRan = true;
        if (!middlewareRan) throw new InvalidOperationException("Send middleware did not run.");
        return ValueTask.CompletedTask;
    }
    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

public sealed class SmokeHealthProvider : ITransportHealthProbe, IConsumerHealthProvider
{
    public ValueTask<HealthCheckResult> CheckAsync(CancellationToken cancellationToken) => ValueTask.FromResult(HealthCheckResult.Healthy());
    public ValueTask<ConsumerHealthState?> GetStateAsync(IConsumerDefinition consumer, CancellationToken cancellationToken) =>
        ValueTask.FromResult<ConsumerHealthState?>(new(true, DateTimeOffset.MinValue));
}

public sealed class SmokeServiceBusReceiver : ServiceBusReceiver;

internal sealed class OrderedSmokeTransport : ITransport
{
    public string Name => "ordered-smoke";
    internal TaskCompletionSource Sent { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        if (message.OrderingKey?.ToString() != "customer-42" || !message.Body.Span.SequenceEqual(new byte[] { 1, 2 }) ||
            message.Headers.Span[0].Value.ToString() != "é" || message.DeliverAt is not null)
            throw new InvalidOperationException("Ordered payload failed under Native AOT.");
        Sent.TrySetResult();
        return ValueTask.CompletedTask;
    }
    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
