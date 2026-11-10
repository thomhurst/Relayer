using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer;
using Relayer.InMemory;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<BillingModule>();
builder.Services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.InMemory("local", local =>
    {
        local.Send<OrderPlaced>("orders");
        local.Consume("orders", consumer => consumer.Handler<OrderPlacedHandler>());
    });
});
using var host = builder.Build();
await host.StartAsync();
try
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    await using var scope = host.Services.CreateAsyncScope();
    var checkout = new CheckoutModule(scope.ServiceProvider.GetRequiredService<ISender>());
    await checkout.PlaceAsync(new OrderPlaced("order-1", "customer-1"), timeout.Token);
    await host.Services.GetRequiredService<BillingModule>().Completed.WaitAsync(timeout.Token);
}
finally
{
    await host.StopAsync();
}

[Message("samples.orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId, string CustomerId);

public sealed class CheckoutModule(ISender sender)
{
    public ValueTask PlaceAsync(OrderPlaced order, CancellationToken cancellationToken) =>
        sender.SendAsync(order, cancellationToken);
}

public sealed class BillingModule
{
    private readonly TaskCompletionSource _completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task Completed => _completed.Task;

    public void CreateInvoice(OrderPlaced message)
    {
        Console.WriteLine($"Invoice created for {message.OrderId} ({message.CustomerId})");
        _completed.TrySetResult();
    }
}

public sealed class OrderPlacedHandler(BillingModule billing) : IHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        billing.CreateInvoice(message);
        return ValueTask.CompletedTask;
    }
}
