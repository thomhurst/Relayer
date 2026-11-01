using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer;
using Relayer.Serialization;
using Relayer.Transports;

await using var provider = new ServiceCollection().AddSingleton<ITransport, SmokeTransport>().AddRelayer(builder =>
{
    builder.AddMessageTypesFromThisAssembly(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    });
    builder.AddTransport<SmokeOptions, SmokeOptions, SmokeOptions>("smoke", transport =>
    {
        transport.Send<Order>("orders", route => route.OrderingKey(order => order.Id));
        transport.Consume("orders", consumer => consumer.Handler<OrderHandler>().Handler<CountHandler>());
    });
}).BuildServiceProvider();
foreach (var service in provider.GetServices<IHostedService>())
    await service.StartAsync(CancellationToken.None);
if (provider.GetRequiredService<ITransport>() is not SmokeTransport { CapabilityLookups: 1 })
    throw new InvalidOperationException("Endpoint capability validation failed under Native AOT.");
Console.WriteLine("Endpoint capability validation passed under Native AOT.");
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
await orderDescriptor.DispatchAsync(orderHandler, message, context, default);
var countDescriptor = handlers.GetBatchHandler<Count, CountHandler>();
var countHandler = (CountHandler)countDescriptor.Resolve(scope.ServiceProvider);
await countDescriptor.DispatchAsync(countHandler, new Batch<Count>([new(new Count(7), context)]), default);
if (orderHandler.Order != message || countHandler.Total != 7)
    throw new InvalidOperationException("Consumer-only generated dispatch failed under Native AOT.");
Console.WriteLine("Consumer-only single and batch dispatch passed under Native AOT.");

[Message("smoke.order")]
public sealed record Order(int Id, Customer Customer, int[] Items, OrderState State);
public sealed record Customer(string Name);
[JsonConverter(typeof(JsonStringEnumConverter<OrderState>))]
public enum OrderState { Placed, Cancelled }
[Message("smoke.count")]
public readonly record struct Count(int Value);
public sealed class SmokeOptions;
internal sealed class SmokeTransport : ITransport
{
    public string Name => "smoke";
    public int CapabilityLookups { get; private set; }
    public TransportCapabilities GetCapabilities(string endpoint)
    {
        if (endpoint != "orders") throw new ArgumentException("Unknown smoke endpoint.", nameof(endpoint));
        CapabilityLookups++;
        return new() { OrderedByKey = true };
    }
    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => throw new NotSupportedException();
    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task StartAsync(CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task StopAsync(CancellationToken cancellationToken) => throw new NotSupportedException();
    public ValueTask DisposeAsync() => default;
}
public sealed class OrderHandler : IHandler<Order>
{
    public Order? Order { get; private set; }
    public ValueTask HandleAsync(Order message, MessageContext context, CancellationToken cancellationToken)
    {
        Order = message;
        return default;
    }
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
    public override IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
    public override int Attempt => 1;
    public override IServiceProvider Services { get; } = services;
    public override MessageFeatures Features { get; } = new();
    public override CancellationToken CancellationToken => default;
}
