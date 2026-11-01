using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer;
using Relayer.Serialization;

using var provider = new ServiceCollection().AddRelayer(builder =>
    builder.AddMessageTypesFromThisAssembly(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    })).BuildServiceProvider();
foreach (var service in provider.GetServices<IHostedService>())
    await service.StartAsync(CancellationToken.None);
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

[Message("smoke.order")]
public sealed record Order(int Id, Customer Customer, int[] Items, OrderState State);
public sealed record Customer(string Name);
[JsonConverter(typeof(JsonStringEnumConverter<OrderState>))]
public enum OrderState { Placed, Cancelled }
[Message("smoke.count")]
public readonly record struct Count(int Value);
