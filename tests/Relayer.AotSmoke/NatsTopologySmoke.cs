using System.Buffers;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Serialization;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer;
using Relayer.Nats;
using Relayer.Transports;

internal static class NatsTopologySmoke
{
    internal static async Task RunAsync()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Nats("nats-smoke", nats =>
        {
            nats.TopologyMode = TopologyMode.None;
            nats.Options.Configure(options =>
            {
                options.ConnectionOptions = new NatsOpts { Url = "nats://does-not-exist.invalid:4222" };
                options.Streams.Add(new("orders") { Configuration = new("orders", ["orders.>"]) });
                options.Consumers.Add(new("orders", "handler") { Configuration = new("handler") });
            });
        }));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<ITransport>();
        if ((await transport.ApplyTopologyAsync(TopologyMode.None, default)).Differences.Count != 0)
            throw new InvalidOperationException("NATS no-connect topology registration failed under Native AOT.");

        var buffer = new ArrayBufferWriter<byte>();
        NatsJSJsonSerializer<StreamConfig>.Default.Serialize(buffer, new StreamConfig("orders", ["orders.>"]));
        NatsJSJsonSerializer<ConsumerConfig>.Default.Serialize(buffer, new ConsumerConfig("handler"));
        if (buffer.WrittenCount == 0) throw new InvalidOperationException("Native NATS topology serialization failed under Native AOT.");
        Console.WriteLine("NATS topology registration and native configuration serialization passed under Native AOT.");
        if (Environment.GetEnvironmentVariable("NATS_URL") is { } url) await ExecuteAsync(url);
    }
    private static async Task ExecuteAsync(string url)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var loggerFactory = LoggerFactory.Create(logging => logging
            .SetMinimumLevel(LogLevel.Debug)
            .AddProvider(new NatsSmokeLoggerProvider()));
        var phase = "registration";
        void Phase(string value)
        {
            phase = value;
            Console.Error.WriteLine($"{DateTimeOffset.UtcNow:O} NATS AOT phase: {value}");
        }
        try { await ExecuteCoreAsync(url, timeout.Token, loggerFactory, Phase); }
        catch
        {
            Console.Error.WriteLine($"{DateTimeOffset.UtcNow:O} NATS AOT failed in {phase}; smoke deadline cancelled: {timeout.IsCancellationRequested}. Native connection/request timeouts and retry policy were not overridden.");
            throw;
        }
    }
    private static async Task ExecuteCoreAsync(string url, CancellationToken token, ILoggerFactory loggerFactory, Action<string> phase)
    {
        var subject = "aot_" + Guid.NewGuid().ToString("N");
        var completed = new NatsAotCompletion();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(completed);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.UseJsonSerializer(NatsAotJson.Default);
            builder.Nats("nats-aot", nats =>
            {
                nats.TopologyMode = TopologyMode.Provision;
                nats.Options.Configure(options =>
                {
                    options.ConnectionOptions = new() { Url = url, LoggerFactory = loggerFactory };
                    options.Streams.Add(new(subject) { Configuration = new(subject, [subject, subject + ".dead"]) });
                    options.Consumers.Add(new(subject, "worker") { Configuration = new("worker") { FilterSubject = subject } });
                });
                nats.Send<NatsAotMessage>(subject);
                nats.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = subject;
                    consumer.Options.Settings = new() { MaxMsgs = 1, DrainOnCancel = true, Expires = TimeSpan.FromSeconds(1) };
                    consumer.Failures(new() { DeadLetterEndpoint = subject + ".dead" });
                    consumer.Handler<NatsAotHandler>();
                });
            });
        });
        await using var provider = services.BuildServiceProvider();
        phase("topology and initial native connection");
        foreach (var lifecycle in provider.GetServices<IHostedService>().OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(token);
        phase("consumer startup");
        var workers = provider.GetServices<IHostedService>().ToArray();
        foreach (var worker in workers) await worker.StartAsync(token);
        phase("confirmed publication and handler completion");
        using var scope = provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new NatsAotMessage(42), token);
        await completed.Done.Task.WaitAsync(token);
        phase("consumer drain and confirmed settlement");
        foreach (var worker in workers.Reverse()) await worker.StopAsync(token);
        phase("independent native settlement verification");
        await using var connection = new NatsConnection(new NatsOpts { Url = url, LoggerFactory = loggerFactory });
        var js = new NatsJSContext(connection);
        var consumerInfo = await js.GetConsumerAsync(subject, "worker", token);
        if (consumerInfo.Info.NumAckPending != 0 || consumerInfo.Info.AckFloor.ConsumerSeq != 1)
            throw new InvalidOperationException("NATS AOT settlement did not confirm durable completion.");
        await js.DeleteStreamAsync(subject, token);
        Console.WriteLine("NATS native JSON send/receive and confirmed settlement passed under Native AOT.");
    }

    // The optional real-broker smoke must retain native startup diagnostics when an
    // initial connection fails. This adds no console package or product retry policy.
    private sealed class NatsSmokeLoggerProvider : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new NativeLogger(categoryName);
        public void Dispose() { }

        private sealed class NativeLogger(string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (!IsEnabled(logLevel)) return;
                Console.Error.WriteLine($"{DateTimeOffset.UtcNow:O} NATS AOT {logLevel} {category} [{eventId.Id}]: {formatter(state, exception)}");
                if (exception is not null) Console.Error.WriteLine(exception);
            }
        }
    }
}

[Message("nats.aot")]
public sealed record NatsAotMessage(int Value);
public sealed class NatsAotCompletion
{
    public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}
public sealed class NatsAotHandler(NatsAotCompletion completion) : IHandler<NatsAotMessage>
{
    public ValueTask HandleAsync(NatsAotMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Value != 42 || context.Nats().Message.Subject.Length == 0) throw new InvalidOperationException("NATS AOT native mapping changed.");
        completion.Done.TrySetResult();
        return default;
    }
}
[JsonSerializable(typeof(NatsAotMessage))]
internal partial class NatsAotJson : JsonSerializerContext;
