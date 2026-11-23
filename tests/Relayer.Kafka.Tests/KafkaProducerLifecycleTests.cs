using Dekaf;
using Dekaf.Admin;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Relayer.Kafka.Tests;

public class KafkaProducerLifecycleTests
{
    [Test]
    public async Task Canceled_producer_startup_allows_shutdown_or_a_retry()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = new KafkaRestartableBroker();
        await broker.StartAsync(token);
        var topic = TestContext.Current!.Isolation.GetIsolatedName("producer-startup").ToLowerInvariant();

        await using (var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build())
        {
            await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
                cancellationToken: token);
        }

        await broker.StopAsync(token);

        using (var stopping = CreateHost(broker.Bootstrap, topic, out var stoppingObserver))
        {
            await CancelStartupAsync(stopping, stoppingObserver, token);
            await stopping.StopAsync(token);
        }

        using var retrying = CreateHost(broker.Bootstrap, topic, out var retryingObserver);
        await CancelStartupAsync(retrying, retryingObserver, token);
        await broker.RestartAsync(token);
        await retrying.StartAsync(token);
        var sender = retrying.Services.GetRequiredService<ISender>();
        await sender.SendAsync(new Order("customer", 1), token);
        await retrying.StopAsync(token);
    }

    private static IHost CreateHost(string bootstrap, string topic, out InitializationObserver observer)
    {
        observer = new();
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Debug);
        builder.Logging.AddProvider(observer);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.None;
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(bootstrap);
                    options.ConfigureProducer = producer => producer.WithMaxBlock(TimeSpan.FromSeconds(30));
                });
                kafka.Send<Order>(topic);
            });
        });
        return builder.Build();
    }

    private static async Task CancelStartupAsync(IHost host, InitializationObserver observer, CancellationToken token)
    {
        using var startup = CancellationTokenSource.CreateLinkedTokenSource(token);
        var started = host.StartAsync(startup.Token);
        // Wait for Dekaf to attempt broker discovery, so cancellation exercises producer initialization.
        await observer.Retrying.Task.WaitAsync(token);
        await startup.CancelAsync();
        await Assert.That(async () => await started).Throws<OperationCanceledException>();
    }

    private sealed class InitializationObserver : ILoggerProvider, ILogger
    {
        internal TaskCompletionSource Retrying
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ILogger CreateLogger(string categoryName)
        {
            return categoryName == "Dekaf.Metadata.MetadataManager" ? this : NullLogger.Instance;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (eventId.Name is "LogMetadataInitializationFailedDebug" or "LogMetadataInitializationFailedWarning")
            {
                Retrying.TrySetResult();
            }
        }

        public void Dispose()
        {
        }
    }
}
