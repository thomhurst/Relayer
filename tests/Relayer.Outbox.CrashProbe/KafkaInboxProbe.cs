using Dekaf;
using Dekaf.Consumer;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.Kafka;
using Relayer.Outbox.EntityFrameworkCore;

/// <summary>Kafka consumer whose handler writes business data and an outbox row through the EF Core inbox boundary.</summary>
public static class KafkaInboxProbe
{
    public const string Saved = "KAFKA_SAVED";

    public static void Configure(IServiceCollection services, KafkaInboxSettings settings)
    {
        services.AddSingleton(settings);
        services.AddRelayerEntityFrameworkCoreOutbox();
        services.AddDbContext<KafkaInboxContext>((provider, options) =>
            options.UseNpgsql(settings.ConnectionString).AddRelayerOutbox(provider));
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.None;
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(settings.Bootstrap);
                    options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                });
                // No dispatcher runs, so outgoing messages stay as outbox rows the test can count.
                kafka.Send<KafkaInboxOutput>(settings.Topic + ".out");
                kafka.Send<KafkaInboxWork>(settings.Topic);
                kafka.Consume(settings.Topic, consumer =>
                {
                    consumer.Options.Group = settings.Topic;
                    consumer.Options.Processing = new()
                    {
                        Ordering = PartitionedProcessingOrder.Key,
                        CommitPolicy = PartitionCommitPolicy.CommitCompletedPeriodically,
                        CommitInterval = settings.CommitInterval,
                        StopTimeout = TimeSpan.FromSeconds(1),
                    };
                    consumer.Failures(new() { RetryDelays = [TimeSpan.Zero], Jitter = 0 });
                    consumer.EntityFrameworkCore<KafkaInboxContext>("kafka.inbox");
                    consumer.Handler<KafkaInboxHandler>();
                });
            });
        });
    }
}

public sealed class KafkaInboxSettings(string bootstrap, string connectionString, string topic, TimeSpan commitInterval)
{
    private int _invocations;
    public string Bootstrap { get; } = bootstrap;
    public string ConnectionString { get; } = connectionString;
    public string Topic { get; } = topic;
    public TimeSpan CommitInterval { get; } = commitInterval;
    /// <summary>Wait inside the open transaction after saving, until killed or cancelled.</summary>
    public bool Block { get; init; }
    /// <summary>Throw after saving on the first attempt.</summary>
    public bool FailFirst { get; init; }
    public int Invocations => Volatile.Read(ref _invocations);
    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal int Invoke() => Interlocked.Increment(ref _invocations);
}

[Message("crash.kafka.work")]
public sealed record KafkaInboxWork(int Value);

[Message("crash.kafka.output")]
public sealed record KafkaInboxOutput(int Value);

internal sealed class KafkaInboxHandler(KafkaInboxContext db, ISender sender, KafkaInboxSettings settings) : IHandler<KafkaInboxWork>
{
    public async ValueTask HandleAsync(KafkaInboxWork message, MessageContext context, CancellationToken cancellationToken)
    {
        var attempt = settings.Invoke();
        db.Add(new KafkaInboxOrder { Id = Guid.CreateVersion7(), Value = message.Value });
        await sender.SendAsync(new KafkaInboxOutput(message.Value), cancellationToken);
        await db.SaveChangesAsync(cancellationToken);
        Console.WriteLine(KafkaInboxProbe.Saved);
        settings.Entered.TrySetResult();
        if (settings.Block) await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        if (settings.FailFirst && attempt == 1) throw new InvalidOperationException("Simulated failure after saving.");
    }
}

public sealed class KafkaInboxContext(DbContextOptions<KafkaInboxContext> options) : DbContext(options)
{
    // EF 10.0.15 has a first-use relational-model race (dotnet/efcore#39214); tests build this model concurrently.
    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder) =>
        configurationBuilder.Conventions.Add(static _ => new InitializeRelationalModel());

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<KafkaInboxOrder>().ToTable("kafka_orders").HasKey(order => order.Id);

    private sealed class InitializeRelationalModel : IModelFinalizedConvention
    {
        public IModel ProcessModelFinalized(IModel model)
        {
            _ = model.GetRelationalModel();
            return model;
        }
    }
}

public sealed class KafkaInboxOrder
{
    public Guid Id { get; set; }
    public int Value { get; set; }
}
