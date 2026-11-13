using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

/// <summary>Options for an outbox-backed local endpoint.</summary>
public sealed class PostgreSqlLocalOptions;
/// <summary>Local routes use the shared PostgreSQL outbox's ordering and scheduling options.</summary>
public sealed class PostgreSqlLocalRouteOptions;
/// <summary>Local consumers use the shared outbox retry, recovery and dead-letter policy.</summary>
public sealed class PostgreSqlLocalConsumerOptions;

/// <summary>Declares named local consumption over existing PostgreSQL outbox dispatch.</summary>
public static class PostgreSqlLocalBuilderExtensions
{
    /// <summary>Registers local routes and consumers. Register shared direct, maintenance and ordered dispatch separately.</summary>
    /// <remarks>Every dispatching instance must declare the same local consumers. Use an active outbox to enlist sends with business writes.</remarks>
    public static IRelayerBuilder PostgreSqlLocal(this IRelayerBuilder builder, string name, NpgsqlDataSource dataSource,
        Action<TransportBuilder<PostgreSqlLocalOptions, PostgreSqlLocalRouteOptions, PostgreSqlLocalConsumerOptions>> configure,
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
                    throw new NotSupportedException($"PostgreSQL local consumer '{consumer.Source}': configure retry and dead-letter behavior through PostgreSqlOutboxOptions. A separate consumer failure policy cannot participate in fenced row recovery.");
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
                    if (consumer.Handlers.Contains(handler.HandlerType)) pipeline.Handler(handler);
                var composed = pipeline.Build(consumer.Concurrency);
                composed.BufferAttemptSends = true;
                var receive = new ReceivePipeline<PostgreSqlLocalDelivery, PostgreSqlLocalMessageBinding>(composed, new(), consumer.Concurrency);
                transport.Register(consumer.Source, receive.ProcessAsync, consumer.Concurrency);
            }
            transport.ValidateConsumers();
            return Task.CompletedTask;
        }
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
