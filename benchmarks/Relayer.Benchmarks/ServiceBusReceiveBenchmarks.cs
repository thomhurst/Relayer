using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.AzureServiceBus;

namespace Relayer.Benchmarks;

/// <summary>Native processor callbacks, default JSON and acknowledged completion, versus the raw Service Bus SDK.</summary>
/// <remarks>Iteration setup seeds 128 messages outside measurement. Cleanup drains the last settlement and stops the processor.</remarks>
[MemoryDiagnoser]
public class ServiceBusReceiveBenchmarks
{
    /// <summary>Include the optional closed-circuit admission/outcome work against the raw SDK baseline.</summary>
    [Params(false, true)]
    public bool CircuitEnabled { get; set; }

    private const int Records = 128;
    private ServiceBusAdministrationClient _admin = null!;
    private ServiceBusClient _client = null!;
    private ServiceBusSender _sender = null!;
    private ServiceBusProcessor _processor = null!;
    private ServiceBusConsumer? _adapter;
    private ServiceProvider _provider = null!;
    private string _queue = null!;
    private TaskCompletionSource _completion = null!;
    private int _count;

    [GlobalSetup(Target = nameof(RawSdk))]
    public Task SetupRaw() => Setup(false);

    [GlobalSetup(Target = nameof(Relayer))]
    public Task SetupRelayer() => Setup(true);

    private async Task Setup(bool relayer)
    {
        var connection = Environment.GetEnvironmentVariable("RELAYER_SERVICEBUS_CONNECTION_STRING")
            ?? throw new InvalidOperationException("Set RELAYER_SERVICEBUS_CONNECTION_STRING to a test Service Bus namespace.");
        _admin = new(Environment.GetEnvironmentVariable("RELAYER_SERVICEBUS_ADMIN_CONNECTION_STRING") ?? connection);
        _queue = "relayer-receive-benchmark-" + Guid.NewGuid().ToString("N");
        await _admin.CreateQueueAsync(new CreateQueueOptions(_queue) { DefaultMessageTimeToLive = TimeSpan.FromHours(1) });
        _client = new(connection, new ServiceBusClientOptions { RetryOptions = new() { TryTimeout = TimeSpan.FromSeconds(3) } });
        _sender = _client.CreateSender(_queue);
        var services = new ServiceCollection();
        services.AddSingleton(this);
        services.AddSingleton<ServiceBusBenchmarkHandler>();
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<ServiceBusBenchmarkPayload>("benchmarks.servicebus-receive");
            builder.UseJsonSerializer(ServiceBusReceiveBenchmarkJson.Default);

            if (!relayer) return;
            builder.ServiceBus("benchmark", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options => { options.Client = _client; options.Queues.Add(new(_queue)); });
                bus.Consume(_queue, consumer =>
                {
                    consumer.Options.ProcessorOptions.PrefetchCount = 32;
                    if (CircuitEnabled) consumer.Options.CircuitBreaker = new();
                    consumer.Handler<ServiceBusBenchmarkHandler>();
                });
            });
        });
        _provider = services.BuildServiceProvider();
        if (!relayer)
        {
            _processor = _client.CreateProcessor(_queue, new ServiceBusProcessorOptions { AutoCompleteMessages = false, PrefetchCount = 32 });
            _processor.ProcessMessageAsync += RawProcess;
            _processor.ProcessErrorAsync += args => { _completion.TrySetException(args.Exception); return Task.CompletedTask; };
            return;
        }
        // Start the public registration once to compose exactly the production pipeline, then seed while stopped.
        var hosted = _provider.GetServices<IHostedService>().ToArray();
        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
        foreach (var service in hosted) await service.StartAsync(default);
        var transport = _provider.GetRequiredKeyedService<ServiceBusSendTransport>("benchmark");
        _adapter = transport.Consumers.Single();
        await _adapter.StopAsync(default);
    }

    [IterationSetup]
    public void Seed() => SeedAsync().GetAwaiter().GetResult();

    private async Task SeedAsync()
    {
        _count = 0;
        _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var batch = await _sender.CreateMessageBatchAsync();
        for (var i = 0; i < Records; i++)
        {
            var message = new ServiceBusMessage("{\"Value\":42}"u8.ToArray());
            message.ApplicationProperties["cloudEvents:id"] = "benchmark";
            message.ApplicationProperties["cloudEvents:type"] = "benchmarks.servicebus-receive;v=1";
            if (!batch.TryAddMessage(message)) throw new InvalidOperationException("The benchmark workload exceeds one native batch.");
        }
        await _sender.SendMessagesAsync(batch);
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Records)]
    public async Task<int> RawSdk()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await _processor.StartProcessingAsync(timeout.Token);
        await _completion.Task.WaitAsync(timeout.Token);
        return _count;
    }

    [Benchmark(OperationsPerInvoke = Records)]
    public async Task<int> Relayer()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await _adapter!.StartAsync(timeout.Token);
        await _completion.Task.WaitAsync(timeout.Token);
        return _count;
    }

    private async Task RawProcess(ProcessMessageEventArgs args)
    {
        var native = args.Message.GetRawAmqpMessage();
        var body = native.Body.TryGetData(out var sections) && sections is IReadOnlyList<ReadOnlyMemory<byte>> { Count: 1 } indexed
            ? indexed[0] : args.Message.Body.ToMemory();
        var message = JsonSerializer.Deserialize(body.Span, ServiceBusReceiveBenchmarkJson.Default.ServiceBusBenchmarkPayload);
        if (message.Value != 42) throw new InvalidOperationException("Unexpected benchmark payload.");
        Completed();
        await args.CompleteMessageAsync(args.Message, args.CancellationToken);
    }

    [IterationCleanup]
    public void Drain() => DrainAsync().GetAwaiter().GetResult();

    private async Task DrainAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        if (_adapter is null) await _processor.StopProcessingAsync(timeout.Token);
        else await _adapter.StopAsync(timeout.Token);
    }

    internal void Completed()
    {
        if (Interlocked.Increment(ref _count) == Records) _completion.TrySetResult();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_adapter is null && _processor is not null) await _processor.DisposeAsync();
        if (_provider is not null) await _provider.DisposeAsync();
        if (_sender is not null) await _sender.DisposeAsync();
        if (_client is not null) await _client.DisposeAsync();
        if (_admin is not null && _queue is not null) await _admin.DeleteQueueAsync(_queue);
    }

}

[Message("benchmarks.servicebus-receive")]
public readonly record struct ServiceBusBenchmarkPayload(int Value);

public sealed class ServiceBusBenchmarkHandler(ServiceBusReceiveBenchmarks owner) : IHandler<ServiceBusBenchmarkPayload>
{
    public ValueTask HandleAsync(ServiceBusBenchmarkPayload message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Value != 42) return ValueTask.FromException(new InvalidOperationException("Unexpected benchmark payload."));
        owner.Completed();
        return default;
    }
}

[JsonSerializable(typeof(ServiceBusBenchmarkPayload))]
internal partial class ServiceBusReceiveBenchmarkJson : JsonSerializerContext;
