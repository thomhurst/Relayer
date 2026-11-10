using System.Text.Json;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Native consumer callbacks, default JSON and acknowledgement against the raw RabbitMQ.Client boundary.</summary>
/// <remarks>The suspended callback Task is required by the native API (D52). Seed and drain run outside measurement.</remarks>
[MemoryDiagnoser]
public class RabbitMqReceiveBenchmarks
{
    private const int Records = 128;
    private IConnection _connection = null!;
    private IChannel _publisher = null!;
    private IChannel? _raw;
    private ServiceProvider _provider = null!;
    private RabbitMqConsumer? _adapter;
    private string _queue = null!;
    private TaskCompletionSource _completion = null!;
    private int _count;
    private string? _tag;
    private BasicProperties _properties = null!;

    [GlobalSetup(Target = nameof(RawClient))]
    public Task SetupRaw() => Setup(false);
    [GlobalSetup(Target = nameof(Relayer))]
    public Task SetupRelayer() => Setup(true);

    private async Task Setup(bool relayer)
    {
        var uri = Environment.GetEnvironmentVariable("RELAYER_RABBITMQ_URI")
            ?? throw new InvalidOperationException("Set RELAYER_RABBITMQ_URI to a test RabbitMQ broker.");
        var factory = new ConnectionFactory { Uri = new(uri) };
        _connection = await factory.CreateConnectionAsync();
        _publisher = await _connection.CreateChannelAsync(new(true, true));
        _queue = "relayer-receive-benchmark-" + Guid.NewGuid().ToString("N");
        await _publisher.QueueDeclareAsync(_queue, false, false, false);
        _properties = new() { Persistent = true, ContentType = "application/json", Headers = new Dictionary<string, object?>
        { ["cloudEvents:id"] = "benchmark", ["cloudEvents:type"] = "benchmarks.rabbit-receive;v=1" } };
        var services = new ServiceCollection();
        services.AddSingleton(this);
        services.AddSingleton<RabbitMqBenchmarkHandler>();
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<RabbitMqBenchmarkPayload>("benchmarks.rabbit-receive");
            builder.UseJsonSerializer(RabbitMqReceiveBenchmarkJson.Default);
            builder.Handler<RabbitMqBenchmarkHandler>();
            if (!relayer) return;
            builder.RabbitMQ("benchmark", rabbit =>
            {
                rabbit.TopologyMode = TopologyMode.None;
                rabbit.Options.Configure(options => options.ConnectionFactory = factory);
                rabbit.Consume(_queue, consumer =>
                {
                    consumer.Options.PrefetchCount = 32;
                    consumer.Handler<RabbitMqBenchmarkHandler>();
                });
            });
        });
        _provider = services.BuildServiceProvider();
        if (!relayer)
        {
            return;
        }
        foreach (var service in _provider.GetServices<IHostedService>()) await service.StartAsync(default);
        var transport = (RabbitMqNativeTransport)_provider.GetRequiredService<ITransport>();
        _adapter = transport.Consumers.Single();
        await _adapter.StopAsync(default);
    }

    [IterationSetup]
    public void Seed() => SeedAsync().GetAwaiter().GetResult();
    private async Task SeedAsync()
    {
        _count = 0;
        _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        for (var i = 0; i < Records; i++)
            await _publisher.BasicPublishAsync("", _queue, true, _properties, "{\"Value\":42}"u8.ToArray());
    }
    [Benchmark(Baseline = true, OperationsPerInvoke = Records)]
    public async Task<int> RawClient()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        _raw = await _connection.CreateChannelAsync(new(false, false, consumerDispatchConcurrency: 1), timeout.Token);
        await _raw.BasicQosAsync(0, 32, false, timeout.Token);
        var native = new AsyncEventingBasicConsumer(_raw);
        native.ReceivedAsync += RawProcess;
        _tag = await _raw!.BasicConsumeAsync(_queue, false, native, timeout.Token);
        await _completion.Task.WaitAsync(timeout.Token);
        return _count;
    }
    [Benchmark(OperationsPerInvoke = Records)]
    public async Task<int> Relayer()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await _adapter!.StartAsync(_connection, timeout.Token);
        await _completion.Task.WaitAsync(timeout.Token);
        return _count;
    }
    private async Task RawProcess(object sender, BasicDeliverEventArgs message)
    {
        var payload = JsonSerializer.Deserialize(message.Body.Span, RabbitMqReceiveBenchmarkJson.Default.RabbitMqBenchmarkPayload);
        if (payload.Value != 42) throw new InvalidOperationException("Unexpected benchmark payload.");
        Completed();
        await _raw!.BasicAckAsync(message.DeliveryTag, false, message.CancellationToken);
    }
    internal void Completed()
    { if (Interlocked.Increment(ref _count) == Records) _completion.TrySetResult(); }
    [IterationCleanup]
    public void Drain() => DrainAsync().GetAwaiter().GetResult();
    private async Task DrainAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        if (_adapter is not null) await _adapter.StopAsync(timeout.Token);
        else if (_tag is not null)
        {
            await _raw!.BasicCancelAsync(_tag, false, timeout.Token);
            await _raw.DisposeAsync();
            _raw = null;
            _tag = null;
        }
    }
    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_provider is not null) await _provider.DisposeAsync();
        if (_raw is not null) await _raw.DisposeAsync();
        if (_publisher is not null)
        {
            await _publisher.QueueDeleteAsync(_queue, false, false);
            await _publisher.DisposeAsync();
        }
        if (_connection is not null) await _connection.DisposeAsync();
    }
}
[Message("benchmarks.rabbit-receive")]
public readonly record struct RabbitMqBenchmarkPayload(int Value);
public sealed class RabbitMqBenchmarkHandler(RabbitMqReceiveBenchmarks owner) : IHandler<RabbitMqBenchmarkPayload>
{
    public ValueTask HandleAsync(RabbitMqBenchmarkPayload message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Value != 42) return ValueTask.FromException(new InvalidOperationException("Unexpected benchmark payload."));
        owner.Completed();
        return default;
    }
}
[JsonSerializable(typeof(RabbitMqBenchmarkPayload))]
internal partial class RabbitMqReceiveBenchmarkJson : JsonSerializerContext;
