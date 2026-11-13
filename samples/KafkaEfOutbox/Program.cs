using Dekaf.Consumer;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Relayer;
using Relayer.Kafka;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;

var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("Orders")
    ?? throw new InvalidOperationException("Set ConnectionStrings__Orders to a PostgreSQL connection string.");
var bootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
// Application-owned data source outlives every hosted dispatcher and DbContext.
await using var dataSource = NpgsqlDataSource.Create(connectionString);
builder.Services.AddDbContext<OrdersDbContext>(options => options.UseNpgsql(dataSource));
builder.Services.AddRelayer(relayer =>
{
    relayer.UsePostgreSqlOutbox<OrdersDbContext>(dataSource);
    relayer.AddMessageTypesFromThisAssembly();
    relayer.Kafka("events", kafka =>
    {
        // This sample delegates topic creation and validation to deployment tooling.
        // Omit this override to validate send, receive and dead-letter topics at startup.
        kafka.TopologyMode = TopologyMode.None;
        kafka.Options.Configure(options =>
        {
            options.BootstrapServers.Add(bootstrapServers);
            options.ConfigureConsumer = consumer => consumer.WithAutoOffsetReset(AutoOffsetReset.Earliest);
        });
        // The combined outbox registration also handles ordered and scheduled rows when needed.
        kafka.Send<OrderPlaced>("sample-orders");
        kafka.Consume("sample-orders", consumer =>
        {
            consumer.Options.Group = "sample-billing";
            consumer.EntityFrameworkCore<OrdersDbContext>("sample-billing");
            consumer.Handler<OrderPlacedHandler>();
        });
    });
});
await using var app = builder.Build();
// Demo database only. Deploy reviewed EF migrations and Relayer schema scripts in production.
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    await db.Database.EnsureCreatedAsync();
    await using var schema = dataSource.CreateCommand(PostgreSqlOutboxSchema.CreateScript());
    await schema.ExecuteNonQueryAsync();
}
app.MapPost("/orders/{id}", async (string id, OrdersDbContext db, ISender sender, CancellationToken cancellationToken) =>
{
    await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
    db.Orders.Add(new Order { Id = id });
    await sender.SendAsync(new OrderPlaced(id), cancellationToken);
    await db.SaveChangesAsync(cancellationToken);
    await transaction.CommitAsync(cancellationToken);
    return Results.Accepted();
});
app.Run();

[Message("samples.kafka.order-placed", Version = 1)]
public sealed record OrderPlaced(string OrderId);

public sealed class Order
{
    public required string Id { get; init; }
}

public sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
}

public sealed class OrderPlacedHandler(ILogger<OrderPlacedHandler> logger) : IHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        logger.LogInformation("Received committed order {OrderId}", message.OrderId);
        return ValueTask.CompletedTask;
    }
}
