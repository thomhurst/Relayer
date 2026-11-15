using Dekaf;
using Dekaf.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

public class KafkaConfigurationTests
{
    [Test]
    public async Task Durable_redelivery_fails_before_native_clients_even_with_a_scheduler()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IFailureScheduler, UnexpectedScheduler>();
        services.AddRelayer(builder => builder.Kafka("events", kafka => kafka.Consume("orders", consumer =>
        {
            consumer.Options.Group = "orders";
            consumer.Failures(new() { RedeliveryDelays = new[] { TimeSpan.FromSeconds(1) } });
            consumer.Handler<KafkaReceiveHandler>();
        })));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray())
            .Throws<NotSupportedException>().WithMessageContaining("RetryDelays");
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task Invalid_circuit_options_fail_before_building_native_clients(int invalid)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Kafka("events", kafka => kafka.Consume("orders", consumer =>
        {
            consumer.Options.Group = "orders";
            consumer.Options.CircuitBreaker = new()
            {
                FailureRatio = invalid == 0 ? double.NaN : .5,
                MinimumThroughput = invalid == 1 ? 0 : 10,
                SamplingDuration = invalid == 2 ? TimeSpan.Zero : TimeSpan.FromSeconds(10),
                BreakDuration = invalid == 3 ? TimeSpan.MaxValue : TimeSpan.FromSeconds(5),
            };
            consumer.Handler<KafkaReceiveHandler>();
        })));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray()).Throws<ArgumentOutOfRangeException>();
    }

    private sealed class UnexpectedScheduler : IFailureScheduler
    {
        public ValueTask ScheduleAsync(string transportName, OutgoingMessage message, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Startup must reject this policy before scheduling.");
    }

    [Test]
    public async Task Named_options_and_route_keys_remain_isolated()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.Kafka("first", kafka =>
            {
                kafka.Options.Configure(options => options.BootstrapServers.Add("first:9092"));
                kafka.Send<Order>("orders", route => route.OrderingKey(static order => order.Customer));
            });
            builder.Kafka("second", kafka =>
            {
                kafka.Options.Configure(options => options.BootstrapServers.Add("second:9092"));
                kafka.Send<OtherOrder>("others", route => route.OrderingKey(static order => order.Number));
            });
        });
        await using var provider = services.BuildServiceProvider();
        var firstControl = provider.GetRequiredKeyedService<IKafkaConsumerControl>("first");
        var secondControl = provider.GetRequiredKeyedService<IKafkaConsumerControl>("second");
        await Assert.That(ReferenceEquals(firstControl, secondControl)).IsFalse();
        var options = provider.GetRequiredService<IOptionsMonitor<KafkaTransportOptions>>();
        await Assert.That(options.Get("first").BootstrapServers.Single()).IsEqualTo("first:9092");
        await Assert.That(options.Get("second").BootstrapServers.Single()).IsEqualTo("second:9092");
        var transports = provider.GetRequiredService<RelayerConfiguration>().Transports;
        var first = (RouteDefinition<Order>)transports[0].Routes.Single();
        var second = (RouteDefinition<OtherOrder>)transports[1].Routes.Single();
        await Assert.That(first.GetOrderingKeySelector<string>()!(new("customer-1", 1))).IsEqualTo("customer-1");
        await Assert.That(second.GetOrderingKeySelector<int>()!(new(42))).IsEqualTo(42);
    }

    [Test]
    public async Task Missing_group_fails_before_building_a_native_client()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Kafka("events", kafka => kafka.Consume("orders", consumer => consumer.Handler<KafkaReceiveHandler>())));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray())
            .Throws<InvalidOperationException>().WithMessageContaining("Options.Group");
    }

    [Test]
    [Arguments("handlers")]
    [Arguments("user-commits")]
    [Arguments("ignore-errors")]
    [Arguments("monitor")]
    [Arguments("transactional-producer")]
    [Arguments("batches")]
    public async Task Native_processing_options_that_break_durable_completion_fail_at_startup(string scenario)
    {
        await using var producer = new ProducerBuilder<byte[], byte[]>().WithBootstrapServers("localhost:9092").WithTransactionalId("unused").Build();
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Kafka("events", kafka => kafka.Consume("orders", consumer =>
        {
            consumer.Options.Group = "orders";
            consumer.Concurrency(2);
            consumer.Options.Processing = scenario switch
            {
                "handlers" => new() { MaxConcurrentHandlers = 4 },
                "user-commits" => new() { CommitPolicy = PartitionCommitPolicy.UserManaged },
                "ignore-errors" => new() { ErrorPolicy = PartitionWorkerErrorPolicy.Ignore },
                "monitor" => new() { Monitor = new() },
                "transactional-producer" => new()
                {
                    CommitPolicy = PartitionCommitPolicy.CommitCompletedInTransaction,
                    TransactionalProducer = PartitionedTransactionalProducer.Create(producer),
                },
                _ => new() { MaxHandlerBatchSize = 10 },
            };
            consumer.Handler<KafkaReceiveHandler>();
        })));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray())
            .Throws<InvalidOperationException>().WithMessageContaining("Processing.");
    }

    [Test]
    public async Task Exactly_once_consumers_reject_a_circuit_breaker_at_startup()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Kafka("events", kafka => kafka.Consume("orders", consumer =>
        {
            consumer.Options.Group = "orders";
            consumer.Options.Processing = new() { CommitPolicy = PartitionCommitPolicy.CommitCompletedInTransaction };
            consumer.Options.CircuitBreaker = new();
            consumer.Handler<KafkaReceiveHandler>();
        })));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray())
            .Throws<NotSupportedException>().WithMessageContaining("CircuitBreaker");
    }

    [Test]
    [Arguments(false, "unused")]
    [Arguments(true, " ")]
    public async Task Transactional_ids_are_validated_at_startup(bool transactional, string id)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Kafka("events", kafka => kafka.Consume("orders", consumer =>
        {
            consumer.Options.Group = "orders";
            if (transactional) consumer.Options.Processing = new() { CommitPolicy = PartitionCommitPolicy.CommitCompletedInTransaction };
            consumer.Options.TransactionalId = id;
            consumer.Handler<KafkaReceiveHandler>();
        })));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray())
            .Throws<InvalidOperationException>().WithMessageContaining("TransactionalId");
    }

    [Test]
    public async Task Parallel_consumers_map_concurrency_onto_native_key_ordering()
    {
        var options = KafkaProcessing.Defaults(4);
        await Assert.That(options.Ordering).IsEqualTo(PartitionedProcessingOrder.Key);
        await Assert.That(options.MaxConcurrentHandlersPerPartition).IsEqualTo(4);
        await Assert.That(options.CommitPolicy).IsEqualTo(PartitionCommitPolicy.CommitCompletedPeriodically);
        var composed = KafkaProcessing.Compose(new() { StopTimeout = TimeSpan.FromSeconds(3), MaxBufferedRecords = 100 }, 4,
            new(), null, new KafkaCommitObserver(new(new(Options.Create(new TracingOptions()), []),
                new() { System = "kafka", Destination = "orders" }, MessagingOperation.Settle, "commit")));
        await Assert.That(composed.MaxConcurrentHandlers).IsEqualTo(4);
        await Assert.That(composed.StopTimeout).IsEqualTo(TimeSpan.FromSeconds(3));
        await Assert.That(composed.MaxBufferedRecords).IsEqualTo(100);
        await Assert.That(composed.Control).IsNotNull();
        await Assert.That(composed.CommitObserver).IsNotNull();
    }
}
