using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Stream.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ.Streams.Tests;

public class ConfigurationTests
{
    [Test]
    public async Task Registration_preserves_named_native_configuration_without_connecting()
    {
        var first = new StreamSystemConfig { UserName = "first" };
        var second = new StreamSystemConfig { UserName = "second" };
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.RabbitMQStreams("first", streams => streams.Options.Configure(options => options.Configuration = first));
            builder.RabbitMQStreams("second", streams => streams.Options.Configure(options => options.Configuration = second));
        });
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<RabbitMqStreamsOptions>>();
        await Assert.That(options.Get("first").Configuration).IsSameReferenceAs(first);
        await Assert.That(options.Get("second").Configuration).IsSameReferenceAs(second);
        var transports = provider.GetServices<ITransport>().ToArray();
        await Assert.That(transports.Select(transport => transport.Name).ToArray()).IsEquivalentTo(new[] { "first", "second" });
        var capabilities = transports[0].GetCapabilities("orders");
        await Assert.That(capabilities.ConsumptionModel).IsEqualTo(ConsumptionModel.Log);
        await Assert.That(capabilities.DeadLetter).IsFalse();
        await Assert.That(capabilities.OrderedByKey).IsFalse();
        await Assert.That(capabilities.Replay).IsFalse();
        await Assert.That(capabilities.MaxDeliveryDelay).IsNull();
        await Assert.That(capabilities.MaxRedeliveryDelay).IsNull();
    }

    [Test]
    public async Task None_cancellation_and_disposal_do_not_connect()
    {
        await using var provider = Provider(options =>
        {
            options.Streams.Add(new("orders"));
            options.SuperStreams.Add(new("events"));
        });
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.None, default)).Differences.Count).IsEqualTo(0);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.That(async () => await transport.StartAsync(cancelled.Token)).Throws<OperationCanceledException>();
        await Assert.That(async () => await transport.ApplyTopologyAsync(TopologyMode.Validate, cancelled.Token)).Throws<OperationCanceledException>();
        await Assert.That(async () => await transport.ApplyTopologyAsync((TopologyMode)99, default)).Throws<ArgumentOutOfRangeException>();
        await transport.StopAsync(default);
        await transport.DisposeAsync();
        await transport.DisposeAsync();
        await Assert.That(async () => await transport.StartAsync(default)).Throws<ObjectDisposedException>();
        await Assert.That(async () => await transport.ApplyTopologyAsync(TopologyMode.None, default)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Message_execution_fails_before_connecting()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQStreams("streams", streams => streams.Send<Sample>("orders")));
        await using var provider = services.BuildServiceProvider();
        var error = await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<NotSupportedException>();
        await Assert.That(error!.Message).Contains("rabbitmq/rabbitmq-stream-dotnet-client#498");
        var consumers = new ServiceCollection();
        consumers.AddRelayer(builder => builder.RabbitMQStreams("streams", streams => streams.Consume("orders", consumer => consumer.Handler<SampleHandler>())));
        await using var consumerProvider = consumers.BuildServiceProvider();
        await Assert.That(() => consumerProvider.GetRequiredService<ITransport>()).Throws<NotSupportedException>();
        await using var native = Provider(_ =>
        {
        });
        var transport = native.GetRequiredService<ITransport>();
        await Assert.That(async () => await transport.SendAsync(new("orders", default), default)).Throws<NotSupportedException>();
        await Assert.That(async () => await transport.SendBatchAsync(ReadOnlyMemory<OutgoingMessage>.Empty, default)).Throws<NotSupportedException>();
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    public async Task Invalid_declarations_fail_before_connecting(int scenario)
    {
        await using var provider = Provider(options =>
        {
            switch (scenario)
            {
                case 0:
                    options.Configuration = null!;
                    break;
                case 1:
                    options.Streams.Add(new(""));
                    break;
                case 2:
                    options.Streams.Add(new(" "));
                    break;
                case 3:
                    options.Streams.Add(new("orders"));
                    options.Streams.Add(new("orders"));
                    break;
                case 4:
                    options.Streams.Add(new("orders")
                    {
                        Configuration = new("different")
                    });
                    break;
            }
        });
        await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<ArgumentException>();
    }

    [Test]
    [Arguments("", null)]
    [Arguments(" ", null)]
    [Arguments("events", "different")]
    public async Task Invalid_super_stream_declarations_fail_before_connecting(string name, string? specName)
    {
        await using var provider = Provider(options =>
        {
            options.SuperStreams.Add(new(name)
            {
                Configuration = specName is null ? null : new PartitionsSuperStreamSpec(specName),
            });
        });
        await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Duplicate_super_stream_declarations_fail_before_connecting()
    {
        await using var provider = Provider(options =>
        {
            options.SuperStreams.Add(new("events"));
            options.SuperStreams.Add(new("events"));
        });
        await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Hosted_start_logs_native_failures_for_each_unavailable_endpoint()
    {
        // Keep both ports reserved without listening so connection attempts are refused.
        using var first = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        using var second = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        first.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        second.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var logs = new EndpointFailureLogs();
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(logs);
        builder.Services.AddRelayer(relayer => relayer.RabbitMQStreams("events", streams =>
            streams.Options.Configure(options => options.Configuration.Endpoints = [first.LocalEndPoint!, second.LocalEndPoint!])));

        var host = builder.Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        await Assert.That(async () => await host.StartAsync()).Throws<StreamSystemInitialisationException>();
        await Assert.That(logs.Endpoints.ToArray()).IsEquivalentTo(new[] { first.LocalEndPoint!, second.LocalEndPoint! });
    }

    private sealed class EndpointFailureLogs : ILoggerProvider, ILogger
    {
        public ConcurrentQueue<EndPoint> Endpoints
        {
            get;
        } = new();

        public ILogger CreateLogger(string categoryName)
        {
            return categoryName == typeof(StreamSystem).FullName ? this : NullLogger.Instance;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return logLevel == LogLevel.Error;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel) && exception is SocketException { SocketErrorCode: SocketError.ConnectionRefused } &&
                state is IReadOnlyList<KeyValuePair<string, object?>> properties)
            {
                foreach (var endpoint in properties.Select(property => property.Value).OfType<EndPoint>())
                {
                    Endpoints.Enqueue(endpoint);
                }
            }
        }

        public void Dispose()
        {
        }
    }

    internal static ServiceProvider Provider(Action<RabbitMqStreamsOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQStreams("streams", streams => streams.Options.Configure(configure)));
        return services.BuildServiceProvider();
    }

    [Message("streams.configuration.sample")]
    public sealed record Sample;

    public sealed class SampleHandler : IHandler<Sample>
    {
        public ValueTask HandleAsync(Sample message, MessageContext context, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }
    }
}
