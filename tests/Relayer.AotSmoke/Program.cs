using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer;
using Relayer.InMemory;
using Relayer.Serialization;
using Relayer.Transports;

var transport = new SmokeTransport();
var services = new ServiceCollection();
services.AddSingleton<ITransport>(transport);
await using var provider = services.AddRelayer(builder =>
{
    builder.AddMessageTypesFromThisAssembly(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    });
    builder.AddTransport<SmokeOptions, SmokeOptions, SmokeOptions>("smoke", broker =>
    {
        broker.Send<Order>("orders", route => route.OrderingKey(static order => order.Id));
        broker.Send<Count>("counts");
        broker.Consume("orders", consumer => consumer.Handler<OrderHandler>().Handler<CountHandler>());
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
using (var topologyJson = new MemoryStream())
{
    provider.GetRequiredService<ITopology>().ExportJson(topologyJson);
    using var topology = JsonDocument.Parse(topologyJson.ToArray());
    if (topology.RootElement.GetProperty("transports")[0].GetProperty("routes")[0]
        .GetProperty("message").GetProperty("name").GetString() != "smoke.order")
        throw new InvalidOperationException("Topology export failed under Native AOT.");
}
Console.WriteLine("Topology startup and JSON export passed with reflection disabled.");
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
buffer.Clear();
serializer.Serialize(message, buffer);
var consumer = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
        provider.GetRequiredService<MessageTypeRegistry>(), serializer,
        static (_, error, _) => ValueTask.FromException(error))
    .Handler(orderDescriptor).Build(1);
var receive = new ReceivePipeline<SmokeIncoming, SmokeBinding>(consumer, new HeaderNames(), 1);
var disposition = await receive.ProcessAsync(new(new ReadOnlySequence<byte>(buffer.WrittenMemory)));
if (disposition.Kind != DispositionKind.Complete)
    throw new InvalidOperationException("Native receive SPI failed under Native AOT.");
Console.WriteLine("Native receive binding, lazy headers and pooled native feature passed under Native AOT.");
var sender = scope.ServiceProvider.GetRequiredService<ISender>();
await sender.SendAsync(message, options => options.ConfigureNative<SmokeNative>(static native => native.Value = 7));
if (transport.Destination != "orders" || transport.OrderingKey != "42" || transport.NativeValue != 7)
    throw new InvalidOperationException("Send routing or native configuration failed under Native AOT.");
await sender.SendAsync(new Count(7));
if (transport.Destination != "counts" || transport.NativeValue != 0)
    throw new InvalidOperationException("Value type sending failed under Native AOT.");
if (transport.CapabilityLookups != 2)
    throw new InvalidOperationException("Endpoint capabilities were resolved on the per-message path.");
Console.WriteLine("Send routing, middleware and native configuration passed with reflection disabled.");

var nativeReceived = Channel.CreateBounded<int>(1);
var localServices = new ServiceCollection();
localServices.AddSingleton(nativeReceived);
localServices.AddRelayer(builder =>
{
    builder.AddMessageTypesFromThisAssembly();
    builder.InMemory("local", local =>
    {
        local.Send<Count>("counts");
        local.Consume("counts", consumer => consumer.Handler<InMemorySmokeHandler>());
    });
});
await using (var localProvider = localServices.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true }))
{
    var hosted = localProvider.GetServices<IHostedService>().ToArray();
    foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
    foreach (var service in hosted) await service.StartAsync(default);
    using var localScope = localProvider.CreateScope();
    await localScope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Count(123));
    if (await nativeReceived.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)) != 123)
        throw new InvalidOperationException("In-memory generated-handler round trip failed under Native AOT.");
    for (var i = hosted.Length - 1; i >= 0; i--) await hosted[i].StopAsync(default);
}
Console.WriteLine("In-memory hosted transport and generated-handler round trip passed under Native AOT.");

[Message("smoke.order")]
public sealed record Order(int Id, Customer Customer, int[] Items, OrderState State);
public sealed record Customer(string Name);
[JsonConverter(typeof(JsonStringEnumConverter<OrderState>))]
public enum OrderState { Placed, Cancelled }
[Message("smoke.count")]
public readonly record struct Count(int Value);
public sealed class SmokeOptions;
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
internal readonly record struct SmokeIncoming(ReadOnlySequence<byte> Body);
internal readonly struct SmokeBinding : IMessageBinding<SmokeIncoming>
{
    public static ReadOnlySequence<byte> GetBody(SmokeIncoming message) => message.Body;
    public static int? GetDeliveryCount(SmokeIncoming message) => null;
    public static bool TryGetHeader(SmokeIncoming message, string name, out WireString value)
    {
        value = name switch
        {
            "ce_type" => new WireString(Type),
            "ce_id" => new WireString("smoke-native"),
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
        return new() { OrderedByKey = true };
    }
    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        => ValueTask.FromResult(new TopologyReport([]));
    public string? Destination { get; private set; }
    public string? OrderingKey { get; private set; }
    public int NativeValue { get; private set; }
    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var native = new SmokeNative();
        message.ApplyNative(native);
        NativeValue = native.Value;
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

public sealed class InMemorySmokeHandler(Channel<int> received) : IHandler<Count>
{
    public ValueTask HandleAsync(Count message, MessageContext context, CancellationToken cancellationToken)
    {
        if (context.Features.GetRequired<NativeMessage<InMemoryDelivery>>().Message.Message.Body.IsEmpty)
            throw new InvalidOperationException("In-memory native binding was lost.");
        received.Writer.TryWrite(message.Value);
        return default;
    }
}
