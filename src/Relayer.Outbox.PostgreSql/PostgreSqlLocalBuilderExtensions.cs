using System.Diagnostics.CodeAnalysis;
using Relayer.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

/// <summary>Options for an outbox-backed local endpoint.</summary>
[Experimental("RLYEXP002", UrlFormat = "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyexp002")]
public sealed class PostgreSqlLocalOptions;
/// <summary>Per-consumer retry schedule for handler failures, persisted in the fenced outbox row.</summary>
[Experimental("RLYEXP002", UrlFormat = "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyexp002")]
public sealed class PostgreSqlLocalConsumerOptions
{
    /// <summary>Durable delay before the next attempt after each retryable handler failure. The last delay repeats. Empty (the default) makes the row due again at once.</summary>
    public TimeSpan[] RedeliveryDelays { get; set; } = [];

    /// <summary>Handler attempts before a retrying row is dead-lettered, or its ordering key parked. Null uses <see cref="PostgreSqlOutboxOptions.MaxDispatchAttempts"/>; <see cref="int.MaxValue"/> retries until the handler succeeds.</summary>
    public int? MaxAttempts { get; set; }

    /// <summary>How a successful delivery completes its row. <see cref="PostgreSqlLocalCompletion.Atomic"/> (the default) runs the handler inside the delivery transaction.</summary>
    public PostgreSqlLocalCompletion Completion { get; set; }
}

/// <summary>Selects how a PostgreSQL local consumer completes its outbox rows.</summary>
[Experimental("RLYEXP002", UrlFormat = "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyexp002")]
public enum PostgreSqlLocalCompletion
{
    /// <summary>The handler runs inside the row's fenced transaction; business writes through <see cref="PostgreSqlLocalDelivery.Transaction"/> and the row deletion commit together.</summary>
    Atomic,
    /// <summary>The row is claimed in a short transaction, the handler runs with no open delivery transaction or connection, and a second short transaction fenced by the claim completes or retries the row. Handlers must be idempotent.</summary>
    AtLeastOnce,
}

/// <summary>Declares named local consumption over existing PostgreSQL outbox dispatch.</summary>
[Experimental("RLYEXP002", UrlFormat = "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyexp002")]
public static class PostgreSqlLocalBuilderExtensions
{
    /// <summary>Registers local routes and consumers. Register shared direct, maintenance and ordered dispatch separately.</summary>
    /// <remarks>Every dispatching instance must declare the same local consumers. Use an active outbox to enlist sends with business writes.</remarks>
    public static IRelayerBuilder PostgreSqlLocal(this IRelayerBuilder builder, string name, NpgsqlDataSource dataSource,
        Action<TransportBuilder<PostgreSqlLocalOptions, PostgreSqlLocalConsumerOptions>> configure,
        PostgreSqlOutboxOptions? options = null, string? dispatchStoreName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(dataSource);
        options ??= new();
        var snapshot = options;
        builder.AddTransport(name, configure);
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            return new PostgreSqlLocalTransport(name, dataSource, snapshot,
                declaration.Routes.Select(route => route.Destination).Concat(declaration.Consumers.Select(consumer => consumer.Source)), provider.GetRequiredService<MessageTracing>());
        });
        builder.Services.AddSingleton<IHostedService>(provider => new LocalConsumers(provider, name, dispatchStoreName ?? name));
        return builder;
    }

    private sealed class LocalConsumers(IServiceProvider provider, string name, string storeName) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            var transport = PostgreSqlLocalTransport.Find(provider.GetServices<ITransport>().Single(item => item.Name == name))!;
            var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            transport.Dispatcher = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>(storeName);
            transport.Dispatcher.ValidateLocal(transport);
            var handlers = provider.GetRequiredService<HandlerRegistry>();
            provider.GetRequiredService<SenderPool>().Enable();
            foreach (var route in declaration.Routes)
                if (route.SendOptions.EnvelopeMode != MessageEnvelopeMode.CloudEvents || route.SendOptions.HeaderNames.Id != "ce_id")
                    throw new NotSupportedException("PostgreSQL local routes require CloudEvents headers. Raw envelopes and custom CloudEvents header bindings are unsupported.");
            foreach (var consumer in declaration.Consumers)
            {
                if (((ConsumerDefinition<PostgreSqlLocalConsumerOptions>)consumer).HasFailurePolicy)
                    throw new NotSupportedException($"PostgreSQL local consumer '{consumer.Source}': configure retries with consumer.Options.RedeliveryDelays and MaxAttempts. A separate consumer failure policy cannot participate in fenced row recovery.");
                var options = ((ConsumerDefinition<PostgreSqlLocalConsumerOptions>)consumer).Options;
                if (options.MaxAttempts <= 0)
                    throw new ArgumentOutOfRangeException(nameof(options), $"PostgreSQL local consumer '{consumer.Source}': MaxAttempts must be positive.");
                foreach (var delay in options.RedeliveryDelays ?? throw new ArgumentNullException(nameof(options), $"PostgreSQL local consumer '{consumer.Source}': RedeliveryDelays must not be null."))
                    if (delay < TimeSpan.Zero)
                        throw new ArgumentOutOfRangeException(nameof(options), $"PostgreSQL local consumer '{consumer.Source}': RedeliveryDelays must be non-negative.");
                if (!Enum.IsDefined(options.Completion))
                    throw new ArgumentOutOfRangeException(nameof(options), $"PostgreSQL local consumer '{consumer.Source}': Completion must be Atomic or AtLeastOnce.");
                if (consumer.BatchSize != 1)
                    throw new NotSupportedException($"PostgreSQL local consumer '{consumer.Source}': batch handlers are unsupported. Use single-message handlers.");
                var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                    provider.GetRequiredService<MessageTypeRegistry>(), provider.GetMessageSerializer(name),
                    PostgreSqlLocalTransport.HandleFailureAsync, consumer.FailureClassifier ?? provider.GetService<IFailureClassifier>());
                var tracing = provider.GetRequiredService<MessageTracing>();
                var endpoint = new MessagingEndpoint { System = "postgresql", Destination = consumer.Source };
                var telemetry = provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
                { System = "postgresql", Destination = consumer.Source });
                pipeline.Tracing(tracing, endpoint).Telemetry(telemetry).Settle(PostgreSqlLocalTransport.SettleAsync);
                pipeline.ConfigureConsumer(provider, consumer);
                foreach (var handler in handlers.Descriptors)
                    if (consumer.Selects(handler)) pipeline.Handler(handler);
                var composed = pipeline.Build(consumer.Concurrency);
                composed.BufferAttemptSends = true;
                var receive = new ReceivePipeline<PostgreSqlLocalDelivery, PostgreSqlLocalMessageBinding>(composed, new(), consumer.Concurrency);
                transport.Register(consumer.Source, receive.ProcessAsync, consumer.Concurrency, options.MaxAttempts, options.RedeliveryDelays,
                    options.Completion == PostgreSqlLocalCompletion.AtLeastOnce);
            }
            transport.ValidateConsumers();
            return Task.CompletedTask;
        }
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
