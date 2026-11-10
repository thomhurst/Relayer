using Azure.Messaging.ServiceBus.Administration;
using Relayer;
using Relayer.AzureServiceBus;

var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("ServiceBus")
    ?? throw new InvalidOperationException("Set ConnectionStrings__ServiceBus to a namespace connection string with Send and Listen rights.");
builder.Services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.ServiceBus("commands", bus =>
    {
        bus.Options.Configure(options =>
        {
            options.ConnectionString = connectionString;
            options.Queues.Add(new CreateQueueOptions("sample-orders"));
        });
        bus.Send<OrderPlaced>("sample-orders");
        bus.Consume("sample-orders", consumer => consumer.Handler<OrderPlacedHandler>());
    });
});
var app = builder.Build();
app.MapPost("/orders/{id}", async (string id, ISender sender, CancellationToken cancellationToken) =>
{
    await sender.SendAsync(new OrderPlaced(id), cancellationToken);
    return Results.Accepted();
});
app.Run();

[Message("samples.servicebus.order-placed", Version = 1)]
public sealed record OrderPlaced(string OrderId);

public sealed class OrderPlacedHandler(ILogger<OrderPlacedHandler> logger) : IHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        logger.LogInformation("Received order {OrderId}", message.OrderId);
        return ValueTask.CompletedTask;
    }
}
